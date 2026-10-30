mod goodreads;
mod notion;

use std::{borrow::Cow, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use itertools::Itertools;
use matchr::score;
use mlm_db::{
    Flags, ListItem, ListItemTorrent, OldMainCat, Torrent, TorrentKey, TorrentMeta, TorrentStatus,
};
use mlm_mam::{
    api::MaM,
    enums::{Categories, SearchIn},
    search::{MaMTorrent, SearchFields, SearchQuery, SearchResult, Tor},
    serde::{DATE_FORMAT, DATE_TIME_FORMAT},
};
use mlm_parse::normalize_title;
use native_db::Database;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use time::UtcDateTime;
use tokio::{sync::watch::Sender, time::sleep};
use tracing::{debug, instrument, trace, warn};

use crate::{
    config::{Config, GoodreadsList, Grab, NotionList, TorrentFilter},
    lists::{goodreads::run_goodreads_import, notion::run_notion_import},
};

pub enum List {
    Goodreads(GoodreadsList),
    Notion(NotionList),
}

impl List {
    pub fn list_type(&self) -> &'static str {
        match self {
            List::Goodreads(_) => "Goodreads",
            List::Notion(_) => "Notion",
        }
    }

    pub fn display_name(&self, index: usize) -> String {
        match self {
            List::Goodreads(list) => list.name.clone().unwrap_or_else(|| index.to_string()),
            List::Notion(list) => list.name.clone(),
        }
    }

    pub fn search_interval(&self) -> Option<u64> {
        match self {
            List::Goodreads(list) => list.search_interval,
            List::Notion(list) => list.search_interval,
        }
    }

    fn unsat_buffer(&self) -> Option<u64> {
        match self {
            List::Goodreads(list) => list.unsat_buffer,
            List::Notion(list) => list.unsat_buffer,
        }
    }
}

pub fn get_lists(config: &Config) -> Vec<List> {
    let mut lists = vec![];
    for goodreads in &config.goodreads_lists {
        lists.push(List::Goodreads(goodreads.clone()));
    }
    for notion in &config.notion_lists {
        lists.push(List::Notion(notion.clone()));
    }
    lists
}

#[instrument(skip_all)]
pub async fn run_list_import(
    config: Arc<Config>,
    db: Arc<Database<'_>>,
    mam: Arc<MaM<'_>>,
    list: Arc<List>,
    index: usize,
    autograb_trigger: Sender<()>,
) -> Result<()> {
    let user_info = mam.user_info().await?;
    let max_torrents = user_info
        .snatch_summary
        .unsat
        .limit
        .saturating_sub(user_info.snatch_summary.unsat.count);
    debug!(
        "{} import, name: {}, unsats: {:#?}; max_torrents: {max_torrents}",
        list.list_type(),
        list.display_name(index),
        user_info.snatch_summary.unsat
    );

    let max_torrents =
        max_torrents.saturating_sub(list.unsat_buffer().unwrap_or(config.unsat_buffer));

    if max_torrents > 0 {
        match list.as_ref() {
            List::Goodreads(list) => {
                run_goodreads_import(config, db, mam, list, max_torrents).await?;
            }
            List::Notion(list) => {
                run_notion_import(config, db, mam, list, max_torrents).await?;
            }
        }
    }

    autograb_trigger.send(())?;

    Ok(())
}

static BAD_CHARATERS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"['`/?]|\s+[\(\[][^\)\]]+[\)\]]").unwrap());

static AND: Lazy<Regex> = Lazy::new(|| Regex::new(r"&|\band\b").unwrap());

#[instrument(skip_all)]
fn search_library(config: &Config, db: &Database<'_>, db_item: &mut ListItem) -> Result<bool> {
    let r = db.r_transaction()?;
    let title_search = normalize_title(&db_item.title);
    let mut library = {
        r.scan()
            .secondary::<Torrent>(TorrentKey::title_search)?
            .start_with(title_search.as_str())?
            .filter(|t| t.as_ref().is_ok_and(|t| db_item.matches(&t.meta)))
            .collect::<Result<Vec<_>, _>>()
    }?;

    library.sort_by_key(|torrent| {
        let preferred_types = config.preferred_types(&torrent.meta.media_type);
        preferred_types
            .iter()
            .position(|t| torrent.meta.filetypes.contains(t))
    });

    let audiobook = library
        .iter()
        .find(|t| t.meta.media_type.matches(OldMainCat::Audio.into()));
    let ebook = library
        .iter()
        .find(|t| t.meta.media_type.matches(OldMainCat::Ebook.into()));

    let mut updated_any = false;
    if let Some(audiobook) = audiobook {
        let updated = set_existing(&mut db_item.audio_torrent, audiobook);
        trace!(
            "Found old audiobook torrent matching RSS item: {}",
            audiobook.meta.title
        );
        updated_any = updated || updated_any;
    }
    if let Some(ebook) = ebook {
        let updated = set_existing(&mut db_item.ebook_torrent, ebook);
        trace!(
            "Found old ebook torrent matching RSS item: {}",
            ebook.meta.title
        );
        updated_any = updated || updated_any;
    }

    Ok(updated_any)
}

fn set_existing(field: &mut Option<ListItemTorrent>, torrent: &Torrent) -> bool {
    if let Some(field) = field {
        if field.status == TorrentStatus::Selected {
            return false;
        }
        if field.status == TorrentStatus::Existing && field.mam_id == torrent.meta.mam_id {
            return false;
        }
    }
    field.replace(ListItemTorrent {
        mam_id: torrent.meta.mam_id,
        status: TorrentStatus::Existing,
        at: torrent.created_at,
    });
    true
}

/// Results per search page, the search endpoint allows 5 to 1000
const SEARCH_PER_PAGE: u64 = 1000;
/// Most pages fetched for one list item, a title and author search rarely fills one
const SEARCH_MAX_PAGES: u64 = 3;

/// The torrents a grab block matches for a list item, most preferred first, with their metadata
/// and file type preference
type GrabMatches = Vec<(MaMTorrent, TorrentMeta, usize, Grab)>;

/// Searches for a list item once and returns what each grab block matches, in the same order as
/// `grabs`. The search uses the loosest of the blocks' filters and each block's own filter is
/// applied to the results, so each block gets what a search of its own would have found
#[instrument(skip_all)]
async fn search_grabs(
    config: &Config,
    mam: &MaM<'_>,
    db_item: &ListItem,
    grabs: &[Grab],
) -> Result<Vec<GrabMatches>> {
    if grabs.is_empty() {
        return Ok(vec![]);
    }
    let Some(query) = search_text(db_item) else {
        warn!(
            "Not searching for list item {:?} by {:?}, it needs a title and an author",
            db_item.title, db_item.authors
        );
        return Ok(vec![]);
    };

    let torrents = search_pages(mam, query, &search_filter(grabs, db_item)).await?;
    grabs
        .iter()
        .map(|grab| grab_matches(config, db_item, grab, &torrents))
        .collect()
}

/// The search text for a list item, None if it has no title or no author to search for
fn search_text(db_item: &ListItem) -> Option<String> {
    let title_query = db_item.title.replace("*", "\"*\"");
    let title_query = BAD_CHARATERS.replace_all(&title_query, " ");
    if title_query.trim().is_empty() || db_item.authors.iter().all(|a| a.trim().is_empty()) {
        return None;
    }
    let mut title_query = AND.replace_all(&title_query, "(and|&)");
    if let Some((primary_title, _)) = title_query.split_once(':') {
        title_query = Cow::from(format!("(\"{primary_title}\"|\"{title_query}\")"));
    }
    Some(format!(
        "@title {} @author ({})",
        title_query,
        db_item.authors.iter().map(|a| format!("\"{a}\"")).join("|")
    ))
}

/// The categories a grab block searches for a list item, leaving out the formats the item already
/// has. None when that leaves no categories to send, as the block's search then covers them all
fn search_categories(grab: &Grab, db_item: &ListItem) -> Option<Categories> {
    let mut categories = grab.filter.categories.clone();
    if db_item.audio_torrent.is_some() {
        categories.audio = Some(vec![])
    }
    if db_item.ebook_torrent.is_some() {
        categories.ebook = Some(vec![])
    }
    if categories.get_main_cats().is_empty() && categories.get_cats().is_empty() {
        None
    } else {
        Some(categories)
    }
}

/// The loosest of the grab blocks' search filters, so one search returns everything the blocks'
/// own searches would have. Only the fields sent to MAM are set, the results still have to be
/// checked with `grab_matches` for each block
fn search_filter(grabs: &[Grab], db_item: &ListItem) -> TorrentFilter {
    let categories = grabs
        .iter()
        .map(|grab| search_categories(grab, db_item))
        .collect::<Option<Vec<_>>>();
    let categories = match categories {
        Some(categories) => Categories {
            audio: union(categories.iter().map(|c| c.audio.as_deref())),
            ebook: union(categories.iter().map(|c| c.ebook.as_deref())),
            musicology: union(categories.iter().map(|c| c.musicology.as_deref())),
            radio: union(categories.iter().map(|c| c.radio.as_deref())),
        },
        // A block that sends no categories searches all of them
        None => Categories {
            audio: None,
            ebook: None,
            musicology: None,
            radio: None,
        },
    };

    // Flags are only sent when every block sends the same ones
    let first_flags = grabs[0].filter.flags.as_search_bitfield();
    let flags = if grabs
        .iter()
        .all(|grab| grab.filter.flags.as_search_bitfield() == first_flags)
    {
        grabs[0].filter.flags.clone()
    } else {
        Flags::default()
    };

    let filters = grabs.iter().map(|grab| &grab.filter);
    TorrentFilter {
        categories,
        // An empty list allows every language
        languages: union(
            filters
                .clone()
                .map(|f| Some(&f.languages[..]).filter(|l| !l.is_empty())),
        )
        .unwrap_or_default(),
        flags,
        min_size: filters.clone().map(|f| f.min_size).min().unwrap_or_default(),
        // A max_size of 0 has no limit
        max_size: loosest_max(
            filters
                .clone()
                .map(|f| Some(f.max_size).filter(|s| s.bytes() > 0)),
        )
        .unwrap_or_default(),
        uploaded_after: loosest_min(filters.clone().map(|f| f.uploaded_after)),
        uploaded_before: loosest_max(filters.clone().map(|f| f.uploaded_before)),
        min_seeders: loosest_min(filters.clone().map(|f| f.min_seeders)),
        max_seeders: loosest_max(filters.clone().map(|f| f.max_seeders)),
        min_leechers: loosest_min(filters.clone().map(|f| f.min_leechers)),
        max_leechers: loosest_max(filters.clone().map(|f| f.max_leechers)),
        min_snatched: loosest_min(filters.clone().map(|f| f.min_snatched)),
        max_snatched: loosest_max(filters.map(|f| f.max_snatched)),
        ..Default::default()
    }
}

/// Everything in any of the lists, or None (no restriction) if any of them is None
fn union<'a, T: Clone + PartialEq + 'a>(
    lists: impl Iterator<Item = Option<&'a [T]>>,
) -> Option<Vec<T>> {
    let mut union = vec![];
    for list in lists {
        for value in list? {
            if !union.contains(value) {
                union.push(value.clone());
            }
        }
    }
    Some(union)
}

/// The lowest limit, or None (no limit) if any of them is None
fn loosest_min<T: Ord>(limits: impl Iterator<Item = Option<T>>) -> Option<T> {
    limits.collect::<Option<Vec<_>>>()?.into_iter().min()
}

/// The highest limit, or None (no limit) if any of them is None
fn loosest_max<T: Ord>(limits: impl Iterator<Item = Option<T>>) -> Option<T> {
    limits.collect::<Option<Vec<_>>>()?.into_iter().max()
}

/// Runs a list item search, fetching a few pages at most with a pause between them
#[instrument(skip_all)]
async fn search_pages(
    mam: &MaM<'_>,
    query: String,
    filter: &TorrentFilter,
) -> Result<Vec<MaMTorrent>> {
    let (flags_is_hide, flags) = filter.flags.as_search_bitfield();

    let mut results: Option<SearchResult> = None;
    for page in 1.. {
        let mut page_results = mam
            .search(&SearchQuery {
                fields: SearchFields {
                    dl_link: true,
                    ..Default::default()
                },
                perpage: SEARCH_PER_PAGE,
                tor: Tor {
                    start_number: results.as_ref().map_or(0, |r| r.data.len() as u64),
                    text: query.clone(),
                    srch_in: vec![SearchIn::Title, SearchIn::Author],
                    main_cat: filter.categories.get_main_cats(),
                    cat: filter.categories.get_cats(),
                    browse_lang: filter.languages.iter().map(|l| l.to_id()).collect(),
                    browse_flags_hide_vs_show: if flags.is_empty() {
                        None
                    } else {
                        Some(if flags_is_hide { 0 } else { 1 })
                    },
                    browse_flags: flags.clone(),
                    start_date: filter
                        .uploaded_after
                        .map_or_else(|| Ok(String::new()), |d| d.format(&DATE_FORMAT))?,
                    end_date: filter
                        .uploaded_before
                        .map_or_else(|| Ok(String::new()), |d| d.format(&DATE_FORMAT))?,
                    min_size: filter.min_size.bytes(),
                    max_size: filter.max_size.bytes(),
                    unit: filter.min_size.unit().max(filter.max_size.unit()),
                    min_seeders: filter.min_seeders,
                    max_seeders: filter.max_seeders,
                    min_leechers: filter.min_leechers,
                    max_leechers: filter.max_leechers,
                    min_snatched: filter.min_snatched,
                    max_snatched: filter.max_snatched,
                    ..Default::default()
                },

                ..Default::default()
            })
            .await
            .context("search")?;

        debug!(
            "result: perpage: {}, start: {}, data: {}, total: {}, found: {}",
            page_results.perpage,
            page_results.start,
            page_results.data.len(),
            page_results.total,
            page_results.found
        );

        if page_results.data.is_empty() {
            if results.is_none() {
                results = Some(page_results);
            }
            break;
        }

        if let Some(results) = &mut results {
            results.data.append(&mut page_results.data);
        } else {
            results = Some(page_results);
        }

        let results = results.as_ref().unwrap();
        if results.data.len() >= results.found {
            break;
        }
        if page >= SEARCH_MAX_PAGES {
            warn!(
                "Search {query:?} found {} torrents, only the first {} were checked",
                results.found,
                results.data.len()
            );
            break;
        }
        sleep(Duration::from_millis(400)).await;
    }

    Ok(results.map(|r| r.data).unwrap_or_default())
}

/// Whether a torrent was added before the filter's `uploaded_before` date. MAM's search excludes
/// that date itself while `TorrentFilter::matches` includes it
fn added_before(filter: &TorrentFilter, torrent: &MaMTorrent) -> bool {
    filter.uploaded_before.is_none_or(|before| {
        UtcDateTime::parse(&torrent.added, &DATE_TIME_FORMAT).is_ok_and(|added| added.date() < before)
    })
}

/// Scores the search results that match a grab block and returns the best matches, most
/// preferred first
fn grab_matches(
    config: &Config,
    db_item: &ListItem,
    grab: &Grab,
    torrents: &[MaMTorrent],
) -> Result<GrabMatches> {
    let categories = search_categories(grab, db_item);
    let mut torrents = torrents
        .iter()
        .filter(|t| {
            grab.filter.matches(t)
                // The block's own search left these out, the shared search may not have
                && categories.as_ref().is_none_or(|c| c.matches(t.category))
                && added_before(&grab.filter, t)
        })
        .cloned()
        .map(|t| {
            let title_score = score(&db_item.title, &t.title);
            let author_score = t
                .author_info
                .values()
                .map(|author| {
                    db_item
                        .authors
                        .iter()
                        .map(|author_name| score(author_name, author))
                        .max()
                        .unwrap_or_default()
                })
                .max()
                .unwrap_or_default();
            let series_score: usize = db_item
                .series
                .iter()
                .map(|(i_name, i_num)| {
                    t.series_info
                        .values()
                        .map(|series| {
                            let Value::String(t_name) = series.first().unwrap_or(&Value::Null)
                            else {
                                return 0;
                            };
                            let Value::String(t_num) = series.get(1).unwrap_or(&Value::Null) else {
                                return 0;
                            };
                            score(i_name, t_name) + score(&i_num.to_string(), t_num)
                        })
                        .max()
                        .unwrap_or_default()
                })
                .sum();
            (t, title_score * 2 + author_score * 2 + series_score)
        })
        .collect::<Vec<_>>();
    torrents.sort_by_key(|t| -(t.1 as i64));

    if torrents.is_empty() {
        return Ok(Default::default());
    }
    let max_score = torrents[0].1;
    // Matches scoring within 100 of the best one are kept. A best match under 100 is too weak to
    // use, release builds skipped those items when this subtraction wrapped around
    let Some(min_score) = max_score.checked_sub(100) else {
        debug!(
            "Best match for {:?} only scores {max_score}, skipping it",
            db_item.title
        );
        return Ok(Default::default());
    };
    let mut torrents = torrents
        .into_iter()
        .take_while(|t| t.1 > min_score)
        .map(|(t, _)| {
            let meta = t.as_meta()?;
            let preferred_types = config.preferred_types(&meta.media_type);
            let preference = preferred_types
                .iter()
                .position(|t| meta.filetypes.contains(t));
            Ok((t, meta, preference.unwrap_or_default(), grab.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    torrents.sort_by(|a, b| {
        a.2.cmp(&b.2).then(
            a.0.numfiles
                .cmp(&b.0.numfiles)
                .then(a.1.size.bytes().cmp(&b.1.size.bytes()).reverse()),
        )
    });

    Ok(torrents)
}

#[cfg(test)]
mod tests {
    use mlm_db::{AudiobookCategory, EbookCategory, Language, Size, Timestamp};
    use serde::Deserialize;
    use time::macros::date;

    use super::*;
    use crate::config::Cost;

    pub(super) fn config() -> Config {
        toml::from_str("mam_id = \"test\"").unwrap()
    }

    /// Parses grab blocks the way they are written in the config file
    pub(super) fn grabs(toml: &str) -> Vec<Grab> {
        #[derive(Deserialize)]
        struct Grabs {
            grab: Vec<Grab>,
        }
        toml::from_str::<Grabs>(toml).unwrap().grab
    }

    /// English audiobooks: anything up to 350 MiB, then free ones, then with a wedge
    pub(super) const AUDIOBOOK_GRABS: &str = r#"
        [[grab]]
        cost = "all"
        languages = ["english"]
        max_size = "350 MiB"
        categories = { audio = true, ebook = false }

        [[grab]]
        cost = "free"
        languages = ["english"]
        categories = { audio = true, ebook = false }

        [[grab]]
        cost = "wedge"
        languages = ["english"]
        categories = { audio = true, ebook = false }
    "#;

    pub(super) fn list_item(title: &str, authors: &[&str]) -> ListItem {
        ListItem {
            guid: ("list".to_owned(), title.to_owned()),
            list_id: "list".to_owned(),
            title: title.to_owned(),
            authors: authors.iter().map(|a| a.to_string()).collect(),
            series: vec![],
            cover_url: String::new(),
            book_url: None,
            isbn: None,
            prefer_format: None,
            allow_audio: true,
            audio_torrent: None,
            allow_ebook: true,
            ebook_torrent: None,
            created_at: Timestamp::now(),
            marked_done_at: None,
        }
    }

    pub(super) fn audiobook(
        id: u64,
        title: &str,
        author: &str,
        size: &str,
        free: bool,
    ) -> MaMTorrent {
        MaMTorrent {
            id,
            title: title.to_owned(),
            author_info: [(1, author.to_owned())].into(),
            added: "2025-01-01 00:00:00".to_owned(),
            main_cat: 13,
            category: AudiobookCategory::GeneralFiction.to_id() as u64,
            language: Language::English.to_id(),
            filetype: "m4b".to_owned(),
            numfiles: 1,
            size: size.to_owned(),
            free,
            ..Default::default()
        }
    }

    pub(super) fn ebook(id: u64, title: &str, author: &str, size: &str) -> MaMTorrent {
        MaMTorrent {
            main_cat: 14,
            category: EbookCategory::GeneralFiction.to_id() as u64,
            filetype: "epub".to_owned(),
            ..audiobook(id, title, author, size, false)
        }
    }

    #[test]
    fn test_search_filter_merges_identical_blocks() {
        // The blocks only differ in cost and size, so one search covers them all
        let filter = search_filter(
            &grabs(AUDIOBOOK_GRABS),
            &list_item("The Example Book", &["Ann Author"]),
        );
        assert_eq!(filter.categories.get_main_cats(), vec![13]);
        assert_eq!(filter.categories.get_cats(), Vec::<u8>::new());
        assert_eq!(filter.languages, vec![Language::English]);
        assert_eq!(filter.min_size, Size::default());
        // The 350 MiB limit is only checked on the results for the first block
        assert_eq!(filter.max_size, Size::default());
    }

    #[test]
    fn test_search_filter_is_loosest() {
        let grabs = grabs(
            r#"
            [[grab]]
            languages = ["english"]
            categories = { audio = true, ebook = false }
            min_size = "100 MiB"
            max_size = "350 MiB"
            min_seeders = 2
            max_seeders = 100
            uploaded_after = "2020-01-01"
            uploaded_before = "2025-01-01"
            flags = { explicit = false }

            [[grab]]
            languages = ["german", "english"]
            categories = { audio = false, ebook = true }
            min_size = "50 MiB"
            max_size = "1 GiB"
            min_seeders = 5
            max_seeders = 200
            uploaded_after = "2021-01-01"
            flags = { explicit = false }
        "#,
        );
        let item = list_item("The Example Book", &["Ann Author"]);
        let filter = search_filter(&grabs, &item);
        assert_eq!(filter.categories.get_main_cats(), vec![13, 14]);
        assert_eq!(filter.categories.get_cats(), Vec::<u8>::new());
        assert_eq!(filter.languages, vec![Language::English, Language::German]);
        assert_eq!(filter.min_size, "50 MiB".parse().unwrap());
        assert_eq!(filter.max_size, "1 GiB".parse().unwrap());
        assert_eq!(filter.min_seeders, Some(2));
        assert_eq!(filter.max_seeders, Some(200));
        assert_eq!(filter.uploaded_after, Some(date!(2020 - 01 - 01)));
        // The second block has no upper date limit
        assert_eq!(filter.uploaded_before, None);
        assert_eq!(filter.min_leechers, None);
        // Both blocks hide explicit torrents
        assert_eq!(
            filter.flags.as_search_bitfield(),
            grabs[0].filter.flags.as_search_bitfield()
        );
        assert!(!filter.flags.as_search_bitfield().1.is_empty());

        // A block without limits takes them off the search
        let mut grabs = grabs;
        grabs.push(Grab {
            cost: Cost::Free,
            filter: TorrentFilter {
                categories: grabs[0].filter.categories.clone(),
                ..Default::default()
            },
        });
        let filter = search_filter(&grabs, &item);
        assert_eq!(filter.languages, vec![]);
        assert_eq!(filter.min_size, Size::default());
        assert_eq!(filter.max_size, Size::default());
        assert_eq!(filter.min_seeders, None);
        assert_eq!(filter.max_seeders, None);
        assert_eq!(filter.uploaded_after, None);
        assert!(filter.flags.as_search_bitfield().1.is_empty());
    }

    #[test]
    fn test_search_filter_leaves_out_found_formats() {
        let found = Some(ListItemTorrent {
            mam_id: 1,
            status: TorrentStatus::Selected,
            at: Timestamp::now(),
        });
        let mut item = list_item("The Example Book", &["Ann Author"]);
        item.ebook_torrent = found.clone();
        let filter = search_filter(
            &grabs("[[grab]]\ncategories = { audio = true, ebook = true }"),
            &item,
        );
        assert_eq!(filter.categories.get_main_cats(), vec![13]);

        // Without audiobooks the first block sends no categories at all, so its own search
        // covered every category and the merged one has to as well
        let grabs = grabs(
            r#"
            [[grab]]
            categories = { audio = true, ebook = false }

            [[grab]]
            categories = { audio = false, ebook = true }
        "#,
        );
        let mut item = list_item("The Example Book", &["Ann Author"]);
        item.audio_torrent = found;
        let filter = search_filter(&grabs, &item);
        assert_eq!(filter.categories.get_main_cats(), Vec::<u8>::new());
        assert_eq!(filter.categories.get_cats(), Vec::<u8>::new());
    }

    #[test]
    fn test_search_text() {
        assert_eq!(
            search_text(&list_item("The Example Book", &["Ann Author"])).as_deref(),
            Some("@title The Example Book @author (\"Ann Author\")")
        );
        assert_eq!(
            search_text(&list_item("Book: A Novel", &["Ann Author", "Bo Writer"])).as_deref(),
            Some("@title (\"Book\"|\"Book: A Novel\") @author (\"Ann Author\"|\"Bo Writer\")")
        );
    }

    #[test]
    fn test_search_text_skips_empty_items() {
        assert_eq!(search_text(&list_item("", &["Ann Author"])), None);
        assert_eq!(search_text(&list_item("  ", &["Ann Author"])), None);
        assert_eq!(search_text(&list_item("?", &["Ann Author"])), None);
        assert_eq!(search_text(&list_item("The Example Book", &[])), None);
        assert_eq!(search_text(&list_item("The Example Book", &[" "])), None);
    }

    #[test]
    fn test_grab_matches_end_date_is_exclusive() {
        let config = config();
        let grab = &grabs("[[grab]]\nuploaded_before = \"2025-01-01\"")[0];
        let item = list_item("The Example Book", &["Ann Author"]);
        let added = |id, added: &str| MaMTorrent {
            added: added.to_owned(),
            ..audiobook(id, "The Example Book", "Ann Author", "300 MiB", false)
        };

        // Like MAM's search, which other blocks may have widened
        let found = grab_matches(
            &config,
            &item,
            grab,
            &[
                added(1, "2024-12-31 23:59:59"),
                added(2, "2025-01-01 15:00:00"),
            ],
        )
        .unwrap();
        assert_eq!(found.iter().map(|t| t.0.id).collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn test_grab_matches_skips_weak_matches() {
        let config = config();
        let grab = &grabs(AUDIOBOOK_GRABS)[1];
        let item = list_item("The Example Book", &["Ann Author"]);

        let found = grab_matches(
            &config,
            &item,
            grab,
            &[audiobook(1, "The Example Book", "Ann Author", "300 MiB", false)],
        )
        .unwrap();
        assert_eq!(found.len(), 1);

        // The best match scores under 100, which used to underflow
        let found = grab_matches(
            &config,
            &item,
            grab,
            &[audiobook(1, "Zyx", "Qwv", "300 MiB", false)],
        )
        .unwrap();
        assert!(found.is_empty());
    }
}
