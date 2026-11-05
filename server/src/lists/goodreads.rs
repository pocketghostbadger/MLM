use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use mlm_db::{
    DatabaseExt as _, List, ListItem, ListItemKey, ListItemTorrent, OldDbMainCat, OldMainCat,
    Timestamp, TorrentMeta, TorrentStatus,
};
use mlm_mam::{
    api::{MaM, refused_or_paused},
    search::MaMTorrent,
};
use mlm_parse::clean_value;
use native_db::Database;
use once_cell::sync::Lazy;
use quick_xml::de::from_reader;
use regex::Regex;
use scraper::{Html, Selector};
use serde::Deserialize;
use tokio::time::sleep;
use tracing::{debug, instrument, trace, warn};

use crate::{
    autograbber::{select_torrents, vip_not_free},
    backoff::Backoff,
    config::{Config, Cost, GoodreadsList, Grab},
    lists::{search_grabs, search_library},
};

pub static SERIES_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(.*?) \(([^)]*?),? #?(\d+(?:\.\d+)?)\)$").unwrap());

/// Books whose searches keep finding nothing, or for stored books keep failing, wait 2 hours before
/// the next search, then twice as long each time, up to a day. Keyed by list item guid
static IMPORT_MUTEX: tokio::sync::Mutex<Backoff<(String, String)>> =
    tokio::sync::Mutex::const_new(Backoff::new(
        Duration::from_secs(2 * 60 * 60),
        Duration::from_secs(24 * 60 * 60),
    ));

#[instrument(skip_all)]
pub async fn run_goodreads_import(
    config: Arc<Config>,
    db: Arc<Database<'_>>,
    mam: Arc<MaM<'_>>,
    list: &GoodreadsList,
    mut max_torrents: u64,
) -> Result<()> {
    // Make sure we are only running one import at a time
    let mut backoff = IMPORT_MUTEX.lock().await;

    let mut rss = fetch_list(list).await?;
    trace!("Scanning Goodreads list {}", rss.channel.title);

    let list_id = list.list_id()?;

    if rss.channel.items.is_empty() {
        warn!(
            "Goodreads list {list_id} has no books, check that the shelf name is right"
        );
        return Ok(());
    }

    if !list.dry_run {
        let (_guard, rw) = db.rw_async().await?;
        rw.upsert(List {
            id: list_id.clone(),
            title: rss.channel.title,
            updated_at: Some(Timestamp::now()),
            // TODO: Parse
            build_date: Some(Timestamp::now()),
        })?;
        rw.commit()?;
    }

    for item in rss.channel.items.iter_mut() {
        if let Some((_, [title, series_name, series_num])) =
            SERIES_PATTERN.captures(&item.title).map(|c| c.extract())
        {
            item.series = Some((series_name.to_owned(), series_num.parse()?));
            item.title = title.to_owned();
        }
    }

    let fetched: BTreeSet<String> = rss
        .channel
        .items
        .iter()
        .map(|item| item.guid.clone())
        .collect();
    let fetched_books: BTreeSet<u64> = rss
        .channel
        .items
        .iter()
        .filter_map(|item| item.book_id)
        .collect();
    let shelf_size = rss.channel.entries.unwrap_or(rss.channel.items.len());

    for mut item in rss.channel.items.into_iter() {
        if let Ok(title) = clean_value(&item.title) {
            item.title = title;
        }
        if let Some(author_name) = &item.author_name
            && let Ok(author_name) = clean_value(author_name)
        {
            item.author_name = Some(author_name);
        }
        if let Some((series_name, num)) = &item.series
            && let Ok(series_name) = clean_value(series_name)
        {
            item.series = Some((series_name, *num));
        }
        let db_item = match db
            .r_transaction()?
            .get()
            .primary::<ListItem>((list_id.clone(), item.guid.clone()))?
        {
            Some(db_item) => {
                let (mut db_item, settings_changed) = apply_list_settings(db_item, list);
                if settings_changed
                    || db_item.title != item.title
                    || db_item.series.first() != item.series.as_ref()
                    || (db_item.book_url.is_none() && item.book_url.is_some())
                {
                    db_item.title = item.title.clone();
                    db_item.series = item.series.iter().cloned().collect();
                    // Books stored without a link get the one from the widget
                    if db_item.book_url.is_none() {
                        db_item.book_url = item.book_url.clone();
                    }
                    if !list.dry_run {
                        let (_guard, rw) = db.rw_async().await?;
                        rw.upsert(db_item.clone())?;
                        rw.commit()?;
                    }
                }
                if has_torrents(&db_item, list) {
                    continue;
                }
                db_item
            }
            None => {
                let db_item = item.as_list_item(&list_id, &list);
                if !list.dry_run {
                    let (_guard, rw) = db.rw_async().await?;
                    rw.insert(db_item.clone())?;
                    rw.commit()?;
                }
                db_item
            }
        };
        trace!("Searching for book {} from Goodreads list", item.title);
        let selected = search_item(
            &config,
            &db,
            &mam,
            list,
            item.book_id,
            db_item,
            max_torrents,
            &mut backoff,
        )
        .await
        .context("search goodreads book")?;
        // The books share the unsat slots that were free when the run started
        if let Some(selected) = selected {
            max_torrents = max_torrents.saturating_sub(selected);
            // Only the books that were searched are paced
            sleep(Duration::from_millis(400)).await;
        }
    }

    if list.search_stored_books {
        if read_whole_shelf(shelf_size) {
            debug!(
                "Goodreads list {list_id} has fewer than {WIDGET_BOOKS} books, so its other stored books have left the shelf and aren't searched"
            );
        } else if let Err(err) = search_stored_books(
            &config,
            &db,
            &mam,
            list,
            &list_id,
            &fetched,
            &fetched_books,
            max_torrents,
            &mut backoff,
        )
        .await
        {
            // The import still ends normally, so the books it selected are downloaded. The other
            // stored books are searched on the next import
            warn!("Stopped searching for the stored books of Goodreads list {list_id}: {err:#}");
        }
    }

    Ok(())
}

/// With `search_stored_books`, searches for the list's stored books that weren't in the fetched
/// shelf the same way as for the books in it, until the import's unsat slots are used up
#[allow(clippy::too_many_arguments)]
async fn search_stored_books(
    config: &Config,
    db: &Database<'_>,
    mam: &MaM<'_>,
    list: &GoodreadsList,
    list_id: &str,
    fetched: &BTreeSet<String>,
    fetched_books: &BTreeSet<u64>,
    mut max_torrents: u64,
    backoff: &mut Backoff<(String, String)>,
) -> Result<()> {
    let stored = stored_books(db, list_id, fetched, fetched_books)?;
    for guid in stored {
        // Without free slots nothing more is searched, so don't go through the rest
        if max_torrents == 0 {
            break;
        }
        // Read it again, earlier searches may take a while and other loops change list items
        let Some(db_item) = db.r_transaction()?.get().primary::<ListItem>(guid)? else {
            continue;
        };
        let (db_item, settings_changed) = apply_list_settings(db_item, list);
        if settings_changed && !list.dry_run {
            let (_guard, rw) = db.rw_async().await?;
            rw.upsert(db_item.clone())?;
            rw.commit()?;
        }
        if has_torrents(&db_item, list) {
            continue;
        }
        trace!(
            "Searching for stored book {} from Goodreads list",
            db_item.title
        );
        let book_id = db_item.book_url.as_deref().and_then(book_id_from_url);
        let key = db_item.guid.clone();
        let selected = match search_item(
            config,
            db,
            mam,
            list,
            book_id,
            db_item,
            max_torrents,
            backoff,
        )
        .await
        {
            Ok(selected) => selected,
            Err(err) => {
                // Wait before searching for it again, as after a search that found nothing, so
                // the next imports get to the stored books after it. Errors in the library check
                // come before the wait is checked, so they still stop the pass at this book. A
                // search MaM refused, or that wasn't sent while requests are paused, gets no wait
                if !refused_or_paused(&err) {
                    backoff.record(&key, false, Instant::now());
                }
                return Err(err.context("search stored goodreads book"));
            }
        };
        if let Some(selected) = selected {
            max_torrents = max_torrents.saturating_sub(selected);
            sleep(Duration::from_millis(400)).await;
        }
    }
    Ok(())
}

/// Most books the widget returns for a shelf, the most recently added
const WIDGET_BOOKS: usize = 100;

/// Whether the widget returned the whole shelf: fewer books than it can. Stored books that aren't
/// among them have then left the shelf, so they aren't searched
fn read_whole_shelf(shelf_size: usize) -> bool {
    shelf_size < WIDGET_BOOKS
}

/// The list's stored books that weren't in the fetched shelf, in the list page's order (the ones
/// MLM stored last first).
/// Leaves out books in the shelf under another review, as when a book was removed from the
/// shelf and added again
fn stored_books(
    db: &Database<'_>,
    list_id: &str,
    fetched: &BTreeSet<String>,
    fetched_books: &BTreeSet<u64>,
) -> Result<Vec<(String, String)>> {
    let mut stored = vec![];
    for item in db
        .r_transaction()?
        .scan()
        .secondary::<ListItem>(ListItemKey::created_at)?
        .all()?
        .rev()
    {
        let item = item?;
        if item.list_id != list_id || fetched.contains(&item.guid.1) {
            continue;
        }
        let book_id = item.book_url.as_deref().and_then(book_id_from_url);
        if book_id.is_some_and(|id| fetched_books.contains(&id)) {
            continue;
        }
        stored.push(item.guid);
    }
    Ok(stored)
}

/// Whether a stored book already has a torrent for both formats, or for the list's preferred
/// format, so the import doesn't search for it
fn has_torrents(db_item: &ListItem, list: &GoodreadsList) -> bool {
    (db_item.audio_torrent.is_some() && db_item.ebook_torrent.is_some())
        || (list.prefer_format == Some(OldDbMainCat::Audio) && db_item.audio_torrent.is_some())
        || (list.prefer_format == Some(OldDbMainCat::Ebook) && db_item.ebook_torrent.is_some())
}

/// Applies the list's current settings to a stored book, as the import does for the books in the
/// fetched shelf. Returns the book and whether that changed it
fn apply_list_settings(mut db_item: ListItem, list: &GoodreadsList) -> (ListItem, bool) {
    let changed = db_item.prefer_format != list.prefer_format
        || db_item.allow_audio != list.allow_audio()
        || db_item.allow_ebook != list.allow_ebook();
    db_item.prefer_format = list.prefer_format;
    db_item.allow_audio = list.allow_audio();
    db_item.allow_ebook = list.allow_ebook();
    (db_item, changed)
}

/// The Goodreads book id in a book link such as `https://www.goodreads.com/book/show/123` or
/// `.../book/show/123-title`
fn book_id_from_url(url: &str) -> Option<u64> {
    let (_, rest) = url.split_once("/book/show/")?;
    let id: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    id.parse().ok()
}

/// Checks the library for a book and searches MaM for it if it's still wanted. Returns how many
/// torrents it selected, or None when it stopped before searching: the book isn't wanted, the run
/// has no slots left or it's waiting after earlier searches. A book without a title and author,
/// or a list without grab blocks, gets Some(0) without a request
#[instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
async fn search_item(
    config: &Config,
    db: &Database<'_>,
    mam: &MaM<'_>,
    list: &GoodreadsList,
    book_id: Option<u64>,
    mut db_item: ListItem,
    max_torrents: u64,
    backoff: &mut Backoff<(String, String)>,
) -> Result<Option<u64>> {
    if !db_item.want_audio() && !db_item.want_ebook() {
        return Ok(None);
    }

    let has_updates = search_library(config, db, &mut db_item).context("search_library")?;
    if !list.dry_run && has_updates {
        let (_guard, rw) = db.rw_async().await?;
        rw.upsert(db_item.clone())?;
        rw.commit()?;
    }
    if !db_item.want_audio() && !db_item.want_ebook() {
        return Ok(None);
    }

    // This run has used all its unsat slots, search again on the next one
    if max_torrents == 0 {
        return Ok(None);
    }
    if backoff.waiting(&db_item.guid, Instant::now()) {
        trace!(
            "Not searching for {} yet, earlier searches found nothing",
            db_item.title
        );
        return Ok(None);
    }

    let torrents = search_grabs(config, mam, &db_item, &list.grab)
        .await
        .context("search_grabs")?;
    backoff.record(
        &db_item.guid,
        torrents.iter().any(|t| !t.is_empty()),
        Instant::now(),
    );
    let mut audiobook = select_torrent(&torrents, OldMainCat::Audio);
    let mut ebook = select_torrent(&torrents, OldMainCat::Ebook);

    let mut has_updates = false;
    if audiobook.is_some() && ebook.is_some() {
        match list.prefer_format {
            Some(OldDbMainCat::Audio) => {
                let updated = not_wanted(&mut db_item.ebook_torrent, &mut ebook);
                has_updates = updated || has_updates;
            }
            Some(OldDbMainCat::Ebook) => {
                let updated = not_wanted(&mut db_item.audio_torrent, &mut audiobook);
                has_updates = updated || has_updates;
            }
            None => {}
        }
    }
    if !list.dry_run && has_updates {
        let (_guard, rw) = db.rw_async().await?;
        rw.upsert(db_item.clone())?;
        rw.commit()?;
    }

    let mut has_updates = false;
    if check_cost(&mut db_item.audio_torrent, &mut audiobook) {
        has_updates = true;
    }
    if check_cost(&mut db_item.ebook_torrent, &mut ebook) {
        has_updates = true;
    }
    if !list.dry_run && has_updates {
        let (_guard, rw) = db.rw_async().await?;
        rw.upsert(db_item.clone())?;
        rw.commit()?;
    }

    let mut selected_torrents = 0;
    if let Some(audiobook) = audiobook {
        if mark_found(
            &mut db_item.audio_torrent,
            audiobook,
            TorrentStatus::Selected,
        ) && !list.dry_run
        {
            let (_guard, rw) = db.rw_async().await?;
            rw.upsert(db_item.clone())?;
            rw.commit()?;
        }
        let selection = select_torrents(
            config,
            db,
            mam,
            [audiobook.0.clone()].into_iter(),
            &audiobook.3.filter,
            audiobook.3.cost,
            list.unsat_buffer,
            list.wedge_buffer,
            None,
            list.dry_run,
            max_torrents,
            book_id,
        )
        .await
        .context("select_torrents")?;
        if selection.snatched > 0
            && mark_found(
                &mut db_item.audio_torrent,
                audiobook,
                TorrentStatus::Existing,
            )
            && !list.dry_run
        {
            let (_guard, rw) = db.rw_async().await?;
            rw.upsert(db_item.clone())?;
            rw.commit()?;
        }
        selected_torrents += selection.selected;
    }
    // When the audiobook took the last unsat slot, the ebook waits for the next run
    if let Some(ebook) = ebook
        && selected_torrents < max_torrents
    {
        if mark_found(&mut db_item.ebook_torrent, ebook, TorrentStatus::Selected) && !list.dry_run {
            let (_guard, rw) = db.rw_async().await?;
            rw.upsert(db_item.clone())?;
            rw.commit()?;
        }
        let selection = select_torrents(
            config,
            db,
            mam,
            [ebook.0.clone()].into_iter(),
            &ebook.3.filter,
            ebook.3.cost,
            list.unsat_buffer,
            list.wedge_buffer,
            None,
            list.dry_run,
            max_torrents - selected_torrents,
            book_id,
        )
        .await
        .context("select_torrents")?;
        if selection.snatched > 0
            && mark_found(&mut db_item.ebook_torrent, ebook, TorrentStatus::Existing)
            && !list.dry_run
        {
            let (_guard, rw) = db.rw_async().await?;
            rw.upsert(db_item.clone())?;
            rw.commit()?;
        }
        selected_torrents += selection.selected;
    }

    Ok(Some(selected_torrents))
}

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36";

static REVIEW_ID_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"/review/show/(\d+)").unwrap());
static COVER_SIZE_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\._[A-Z]{2}\d+_\.").unwrap());
static COVER_BOOK_ID_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"/books/\d+[a-z]/(\d+)\.[a-z]+$").unwrap());
static WHITESPACE_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());

async fn fetch(url: &str) -> Result<reqwest::Response> {
    reqwest::Client::new()
        .get(url)
        .header("User-Agent", USER_AGENT)
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("fetch Goodreads list {url}"))
}

async fn fetch_list(list: &GoodreadsList) -> Result<Rss> {
    // Goodreads stopped serving the shelf RSS feeds (/review/list_rss/...) in October 2026,
    // read the same shelf through the public bookshelf widget instead.
    // Goodreads caches the rendered widget per user and widget_id, whatever the other
    // parameters are, so every fetch uses a new widget_id to get the current shelf
    let widget_id = format!(
        "mlm{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    if let Some(url) = widget_url(&list.url, &widget_id)? {
        let content = fetch(&url).await?.text().await?;
        return parse_widget(&content, &list.list_id()?)
            .with_context(|| format!("parse Goodreads widget {url}"));
    }

    let content = fetch(&list.url).await?.bytes().await?;
    Ok(from_reader(&content[..])?)
}

/// Maps a Goodreads shelf RSS or shelf page url to the equivalent custom widget url,
/// fails for group bookshelves
fn widget_url(list_url: &str, widget_id: &str) -> Result<Option<String>> {
    let link: reqwest::Url = list_url.parse()?;
    if !matches!(link.host_str(), Some("www.goodreads.com" | "goodreads.com")) {
        return Ok(None);
    }
    if link.path().starts_with("/group/bookshelf/") {
        bail!(
            "Goodreads list url {list_url} is a group bookshelf, Goodreads no longer offers feeds for group bookshelves so they can't be imported"
        );
    }
    let Some(user) = link
        .path()
        .strip_prefix("/review/list_rss/")
        .or_else(|| link.path().strip_prefix("/review/list/"))
    else {
        return Ok(None);
    };
    let user_id: String = user.chars().take_while(|c| c.is_ascii_digit()).collect();
    if user_id.is_empty() {
        return Ok(None);
    }
    let Some(shelf) = link
        .query_pairs()
        .find(|(name, _)| name == "shelf")
        .map(|(_, shelf)| shelf.into_owned())
        .filter(|shelf| !shelf.is_empty() && !shelf.eq_ignore_ascii_case("#ALL#"))
    else {
        // Without a shelf the widget returns the read shelf, and it returns no books for #ALL#
        bail!(
            "Goodreads list url {list_url} needs a single shelf, such as shelf=to-read (importing all books is no longer supported)"
        );
    };

    let mut url = reqwest::Url::parse(&format!(
        "https://www.goodreads.com/review/custom_widget/{user_id}"
    ))?;
    url.query_pairs_mut()
        .append_pair("shelf", &shelf)
        .append_pair("num_books", &WIDGET_BOOKS.to_string())
        .append_pair("sort", "date_added")
        .append_pair("order", "d")
        .append_pair("show_author", "1")
        .append_pair("show_cover", "1")
        .append_pair("show_title", "1")
        .append_pair("widget_id", widget_id);
    Ok(Some(url.to_string()))
}

fn parse_widget(content: &str, list_id: &str) -> Result<Rss> {
    let (_, code) = content
        .split_once("var widget_code = '")
        .context("missing widget_code")?;
    let fragment = Html::parse_fragment(&unescape_js_string(code)?);

    // Class names end with the widget_id, so match on their prefix
    let container_selector = Selector::parse("[class^=\"gr_custom_container_\"]").unwrap();
    let header_selector = Selector::parse("[class^=\"gr_custom_header_\"] a").unwrap();
    let each_selector = Selector::parse("[class^=\"gr_custom_each_container_\"]").unwrap();
    let book_selector = Selector::parse("[class^=\"gr_custom_book_container_\"] a").unwrap();
    let cover_selector = Selector::parse("[class^=\"gr_custom_book_container_\"] img").unwrap();
    let title_selector = Selector::parse("[class^=\"gr_custom_title_\"] a").unwrap();
    let author_selector = Selector::parse("[class^=\"gr_custom_author_\"] a").unwrap();

    // An unknown user gives "This Goodreads widget is not correct." instead of a shelf
    let header = fragment
        .select(&container_selector)
        .next()
        .and_then(|container| container.select(&header_selector).next());
    let Some(header) = header else {
        let text = fragment.root_element().text().collect::<String>();
        let text = WHITESPACE_PATTERN.replace_all(text.trim(), " ");
        bail!(
            "unexpected Goodreads widget: {}",
            text.chars().take(200).collect::<String>()
        );
    };
    let owner = header.text().collect::<String>();
    let shelf = list_id.split_once(':').map(|(_, s)| s).unwrap_or_default();

    // The header links to the shelf the widget was rendered for
    let header_shelf = header
        .attr("href")
        .and_then(|href| href.parse::<reqwest::Url>().ok())
        .and_then(|url| {
            url.query_pairs()
                .find(|(name, _)| name == "shelf")
                .map(|(_, shelf)| shelf.into_owned())
        });
    if header_shelf.as_deref() != Some(shelf) {
        bail!("Goodreads widget is for shelf {header_shelf:?} instead of {shelf:?}");
    }

    // Keep the title in the same shape as the RSS feed: "Name's bookshelf: shelf".
    // The header is "Name's books", sometimes with a space before the 's
    let owner = owner.trim();
    let owner = owner.strip_suffix(" books").unwrap_or(owner);
    let owner = match owner.strip_suffix("'s") {
        Some(name) => format!("{}'s", name.trim_end()),
        None => owner.to_owned(),
    };
    let title = format!("{owner} bookshelf: {shelf}");

    let mut items = vec![];
    for each in fragment.select(&each_selector) {
        let Some(link) = each.select(&book_selector).next() else {
            continue;
        };
        let Some(review_id) = link
            .attr("href")
            .and_then(|href| REVIEW_ID_PATTERN.captures(href))
            .map(|c| c[1].to_owned())
        else {
            continue;
        };
        let title = link
            .attr("title")
            .map(|t| t.to_owned())
            .or_else(|| {
                each.select(&title_selector)
                    .next()
                    .map(|e| e.text().collect::<String>())
            })
            .unwrap_or_default();
        let cover = each
            .select(&cover_selector)
            .next()
            .and_then(|e| e.attr("src"))
            .map(|src| COVER_SIZE_PATTERN.replace(src, ".").into_owned());
        // The widget has no book id, the cover image is named after the book it belongs to
        let book_id = cover
            .as_deref()
            .and_then(|c| COVER_BOOK_ID_PATTERN.captures(c))
            .and_then(|c| c[1].parse::<u64>().ok());
        let author_name = each
            .select(&author_selector)
            .next()
            .map(|e| e.text().collect::<String>().trim().to_owned());

        items.push(Item {
            // Same guid as the RSS feed used, so existing list items are kept
            guid: format!(
                "https://www.goodreads.com/review/show/{review_id}?utm_medium=api&utm_source=rss"
            ),
            book_id,
            title: title.trim().to_owned(),
            author_name,
            series: None,
            book_large_image_url: cover,
            // Without a cover there's no book id, use the review page the widget links to instead
            book_url: Some(match book_id {
                Some(id) => format!("https://www.goodreads.com/book/show/{id}"),
                None => format!("https://www.goodreads.com/review/show/{review_id}"),
            }),
            isbn: None,
            description: String::new(),
        });
    }

    Ok(Rss {
        channel: Channel {
            title,
            items,
            entries: Some(fragment.select(&each_selector).count()),
        },
    })
}

/// Reads a single quoted JS string literal, stopping at the closing quote
fn unescape_js_string(s: &str) -> Result<String> {
    fn hex(chars: &mut std::str::Chars<'_>, len: usize) -> Result<u32> {
        let hex: String = chars.by_ref().take(len).collect();
        if hex.len() != len || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            bail!("invalid escape {hex:?}");
        }
        Ok(u32::from_str_radix(&hex, 16)?)
    }

    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\'' => return Ok(out),
            '\\' => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('b') => out.push('\u{8}'),
                Some('f') => out.push('\u{c}'),
                Some('v') => out.push('\u{b}'),
                Some('x') => out.push(char::from_u32(hex(&mut chars, 2)?).unwrap()),
                Some('u') => {
                    let mut units = vec![hex(&mut chars, 4)? as u16];
                    // Characters outside the BMP are written as a surrogate pair
                    if (0xD800..0xDC00).contains(&units[0]) && chars.as_str().starts_with("\\u") {
                        chars.nth(1);
                        units.push(hex(&mut chars, 4)? as u16);
                    }
                    for c in char::decode_utf16(units) {
                        out.push(c.context("invalid unicode escape")?);
                    }
                }
                Some(c) => out.push(c),
                None => break,
            },
            c => out.push(c),
        }
    }
    bail!("unterminated widget_code string")
}

#[derive(Debug, Deserialize)]
struct Rss {
    channel: Channel,
}

#[derive(Debug, Deserialize)]
struct Channel {
    title: String,
    #[serde(rename = "item")]
    items: Vec<Item>,
    /// How many books the widget listed, including any MLM couldn't read. None for RSS
    #[serde(skip)]
    entries: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct Item {
    guid: String,
    book_id: Option<u64>,
    title: String,
    author_name: Option<String>,
    series: Option<(String, f64)>,
    book_large_image_url: Option<String>,
    #[serde(skip)]
    book_url: Option<String>,
    // book_description: String,
    isbn: Option<String>,
    description: String,
}

impl Item {
    fn as_list_item(&self, list_id: &str, list: &GoodreadsList) -> ListItem {
        let fragment = Html::parse_fragment(&self.description);

        let cover_url = if let Some(cover) = &self.book_large_image_url {
            cover.clone()
        } else {
            let cover_selector = Selector::parse(".cover img").unwrap();
            fragment
                .select(&cover_selector)
                .next()
                .and_then(|e| e.attr("src"))
                .map(|s| s.to_string())
                .unwrap_or_default()
        };

        let user_list_selector =
            Selector::parse("a[href^=\"https://www.goodreads.com/book/show/\"]").unwrap();
        let group_list_selector = Selector::parse("a[href^=\"/book/show/\"]").unwrap();
        let book_url = self
            .book_url
            .clone()
            .or_else(|| {
                fragment
                    .select(&user_list_selector)
                    .next()
                    .and_then(|e| e.attr("href"))
                    .map(|s| s.to_string())
            })
            .or_else(|| {
                fragment
                    .select(&group_list_selector)
                    .next()
                    .and_then(|e| e.attr("href"))
                    .map(|url| format!("https://www.goodreads.com{url}"))
            });

        let authors = if let Some(author_name) = &self.author_name {
            vec![author_name.clone().replace('.', " ")]
        } else {
            let fragment = Html::parse_fragment(&self.description);
            let author_selector =
                Selector::parse("[itemprop=\"author\"] [itemprop=\"name\"]").unwrap();
            fragment
                .select(&author_selector)
                .map(|e| e.text().collect::<String>().replace('.', " "))
                .collect()
        };

        ListItem {
            guid: (list_id.to_owned(), self.guid.clone()),
            list_id: list_id.to_owned(),
            title: self.title.clone(),
            authors,
            series: self.series.iter().cloned().collect(),
            cover_url,
            book_url,
            isbn: self.isbn.as_ref().and_then(|isbn| isbn.parse().ok()),
            prefer_format: list.prefer_format,
            allow_audio: list.allow_audio(),
            audio_torrent: None,
            allow_ebook: list.allow_ebook(),
            ebook_torrent: None,
            created_at: Timestamp::now(),
            marked_done_at: None,
        }
    }
}

fn select_torrent(
    torrents: &[Vec<(MaMTorrent, TorrentMeta, usize, Grab)>],
    main_cat: OldMainCat,
) -> Option<&(MaMTorrent, TorrentMeta, usize, Grab)> {
    torrents
        .iter()
        .flatten()
        .filter(|t| t.1.media_type.matches(main_cat.into()))
        .find(|t| (t.3.cost != Cost::Free || t.0.is_free()) && !passed_over(&t.0))
        .or_else(|| {
            torrents
                .iter()
                .flatten()
                .find(|t| t.1.media_type.matches(main_cat.into()))
        })
}

/// A VIP torrent without a free flag, which no block selects. One the account already snatched
/// isn't passed over, so `select_torrents` stores it without files and the book is marked Existing
fn passed_over(torrent: &MaMTorrent) -> bool {
    vip_not_free(torrent) && !torrent.my_snatched
}

/// Records a torrent for a list item, as selected or, when it was skipped as already snatched, as
/// found in the library where `select_torrents` stored it. Returns whether that changed the item
fn mark_found(
    field: &mut Option<ListItemTorrent>,
    found: &(MaMTorrent, TorrentMeta, usize, Grab),
    status: TorrentStatus,
) -> bool {
    if field
        .as_ref()
        .is_some_and(|t| t.status == status && t.mam_id == found.0.id)
    {
        return false;
    }
    field.replace(ListItemTorrent {
        mam_id: found.0.id,
        status,
        at: Timestamp::now(),
    });
    true
}

fn not_wanted(
    field: &mut Option<ListItemTorrent>,
    unwanted: &mut Option<&(MaMTorrent, TorrentMeta, usize, Grab)>,
) -> bool {
    let found = unwanted.take().unwrap();
    if field
        .as_ref()
        .is_none_or(|t| t.status != TorrentStatus::NotWanted)
    {
        debug!("Skipped {:?} torrent as is not wanted", found.1.main_cat);
        field.replace(ListItemTorrent {
            mam_id: found.0.id,
            status: TorrentStatus::NotWanted,
            at: Timestamp::now(),
        });
        true
    } else {
        false
    }
}
fn check_cost(
    field: &mut Option<ListItemTorrent>,
    selected: &mut Option<&(MaMTorrent, TorrentMeta, usize, Grab)>,
) -> bool {
    // A VIP torrent without a free flag that the account hasn't snatched isn't selected by any
    // block, so it's wanted too, rather than left as selected with nothing selected
    let take = selected.is_some_and(|selected| {
        (selected.3.cost == Cost::Free && !selected.0.is_free()) || passed_over(&selected.0)
    });
    if take {
        let found = selected.take().unwrap();
        warn!("Skipped {:?} torrent as it is not free", found.1.main_cat);
        if field
            .as_ref()
            .is_none_or(|t| !(t.status == TorrentStatus::Wanted && t.mam_id == found.0.id))
        {
            field.replace(ListItemTorrent {
                mam_id: found.0.id,
                status: TorrentStatus::Wanted,
                at: Timestamp::now(),
            });
            true
        } else {
            false
        }
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lists::{
        GrabMatches, grab_matches,
        tests::{AUDIOBOOK_GRABS, audiobook, config, ebook, grabs, list_item},
    };

    // Same markup as Goodreads returns, with made up books
    const WIDGET: &str = r#"  var widget_code = '  <div class=\"gr_custom_container_mlm1700000000000\">\n    <h2 class=\"gr_custom_header_mlm1700000000000\">\n    <a style=\"text-decoration: none;\" rel=\"nofollow\" href=\"https://www.goodreads.com/review/list/1-jane-doe?shelf=to-read&amp;utm_medium=api&amp;utm_source=custom_widget\">Jane &#39;s books<\/a>\n    <\/h2>\n      <div class=\"gr_custom_each_container_mlm1700000000000\">\n          <div class=\"gr_custom_book_container_mlm1700000000000\">\n            <a title=\"The Example Book (Example Series #3)\" rel=\"nofollow\" href=\"https://www.goodreads.com/review/show/1000000001?utm_medium=api&amp;utm_source=custom_widget\"><img alt=\"The Example Book\" border=\"0\" src=\"https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1700000000l/87654321._SY75_.jpg\" /><\/a>\n          <\/div>\n          <div class=\"gr_custom_rating_mlm1700000000000\">\n            <span class=\" staticStars notranslate\"><img style=\"width:15px;height:15px\" src=\"https://s.gr-assets.com/images/layout/gr_flat_red_star_inactive.png\" /><\/span>\n          <\/div>\n          <div class=\"gr_custom_title_mlm1700000000000\">\n            <a rel=\"nofollow\" href=\"https://www.goodreads.com/review/show/1000000001?utm_medium=api&amp;utm_source=custom_widget\">The Example Book<\/a>\n          <\/div>\n          <div class=\"gr_custom_author_mlm1700000000000\">\n            by <a rel=\"nofollow\" href=\"https://www.goodreads.com/author/show/1000.Ann_Author\">Ann Author<\/a>\n          <\/div>\n      <\/div>\n      <div class=\"gr_custom_each_container_mlm1700000000000\">\n          <div class=\"gr_custom_book_container_mlm1700000000000\">\n            <a title=\"Don&#39;t Look Back &amp; Other Stories\" rel=\"nofollow\" href=\"https://www.goodreads.com/review/show/123?utm_medium=api&amp;utm_source=custom_widget\"><img alt=\"Don&#39;t Look Back\" border=\"0\" src=\"https://s.gr-assets.com/assets/nophoto/book/50x75-a91bf249278a81aabab721ef782c4a74.png\" /><\/a>\n          <\/div>\n          <div class=\"gr_custom_author_mlm1700000000000\">\n            by <a rel=\"nofollow\" href=\"https://www.goodreads.com/author/show/1.A_B\">A.B. Writer<\/a>\n          <\/div>\n      <\/div>\n  <br style=\"clear: both\"/>\n  <center>\n    <a rel=\"nofollow\" href=\"https://www.goodreads.com/\"><img alt=\"goodreads.com\" style=\"border:0\" src=\"https://s.gr-assets.com/images/widget/widget_logo.gif\" /><\/a>\n  <\/center>\n  <\/div>\n'
  var widget_div = document.getElementById('gr_custom_widget_mlm1700000000000')
"#;

    const EMPTY_WIDGET: &str = r#"  var widget_code = '  <div class=\"gr_custom_container_mlm1700000000000\">\n    <h2 class=\"gr_custom_header_mlm1700000000000\">\n    <a style=\"text-decoration: none;\" rel=\"nofollow\" href=\"https://www.goodreads.com/review/list/1-jane-doe?shelf=no-such-shelf&amp;utm_medium=api&amp;utm_source=custom_widget\">Jane&#39;s books<\/a>\n    <\/h2>\n  <br style=\"clear: both\"/>\n  <\/div>\n'
  var widget_div = document.getElementById('gr_custom_widget_mlm1700000000000')
"#;

    const INVALID_WIDGET: &str = r#"  var widget_code = '  This <a rel=\"nofollow\" href=\"https://www.goodreads.com/\">Goodreads<\/a>\n  widget is not correct.\n  <a rel=\"nofollow\" href=\"https://www.goodreads.com/user/edit#widgets\">Get the corrected widget code.<\/a>\n'
  var widget_div = document.getElementById('gr_custom_widget_mlm1700000000001')
"#;

    #[test]
    fn test_widget_url() {
        assert_eq!(
            widget_url(
                "https://www.goodreads.com/review/list_rss/12345678?key=KEY&shelf=to-read",
                "mlm1"
            )
            .unwrap()
            .unwrap(),
            "https://www.goodreads.com/review/custom_widget/12345678?shelf=to-read&num_books=100&sort=date_added&order=d&show_author=1&show_cover=1&show_title=1&widget_id=mlm1"
        );
        assert!(
            widget_url(
                "https://www.goodreads.com/review/list/12345678-jane-doe?shelf=my%20audio",
                "mlm1"
            )
            .unwrap()
            .unwrap()
            .starts_with("https://www.goodreads.com/review/custom_widget/12345678?shelf=my+audio&")
        );
        // The widget can't return all books
        assert!(widget_url("https://www.goodreads.com/review/list_rss/12345678", "mlm1").is_err());
        assert!(
            widget_url(
                "https://www.goodreads.com/review/list_rss/12345678?shelf=",
                "mlm1"
            )
            .is_err()
        );
        assert!(
            widget_url(
                "https://www.goodreads.com/review/list/12345678-jane-doe?shelf=%23ALL%23",
                "mlm1"
            )
            .is_err()
        );
        // Goodreads no longer serves group bookshelf feeds
        let err = widget_url("https://www.goodreads.com/group/bookshelf/1.rss", "mlm1")
            .unwrap_err()
            .to_string();
        assert!(err.contains("group bookshelf"), "{err}");
        assert!(
            widget_url(
                "https://goodreads.com/group/bookshelf/1-example-group?shelf=to-read",
                "mlm1"
            )
            .is_err()
        );
        // Anything else is still read as RSS
        assert_eq!(
            widget_url("https://example.com/review/list/1?shelf=read", "mlm1").unwrap(),
            None
        );
    }

    #[test]
    fn test_parse_widget() {
        let rss = parse_widget(WIDGET, "1:to-read").unwrap();
        assert_eq!(rss.channel.title, "Jane's bookshelf: to-read");
        assert_eq!(rss.channel.items.len(), 2);
        assert_eq!(rss.channel.entries, Some(2));

        let item = &rss.channel.items[0];
        assert_eq!(
            item.guid,
            "https://www.goodreads.com/review/show/1000000001?utm_medium=api&utm_source=rss"
        );
        assert_eq!(item.title, "The Example Book (Example Series #3)");
        assert_eq!(item.author_name.as_deref(), Some("Ann Author"));
        assert_eq!(item.book_id, Some(87654321));
        assert_eq!(
            item.book_large_image_url.as_deref(),
            Some(
                "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1700000000l/87654321.jpg"
            )
        );
        assert_eq!(
            item.book_url.as_deref(),
            Some("https://www.goodreads.com/book/show/87654321")
        );

        let item = &rss.channel.items[1];
        assert_eq!(item.title, "Don't Look Back & Other Stories");
        assert_eq!(item.author_name.as_deref(), Some("A.B. Writer"));
        assert_eq!(item.book_id, None);
        assert_eq!(
            item.book_url.as_deref(),
            Some("https://www.goodreads.com/review/show/123")
        );
    }

    #[test]
    fn test_parse_empty_widget() {
        let rss = parse_widget(EMPTY_WIDGET, "1:no-such-shelf").unwrap();
        assert_eq!(rss.channel.title, "Jane's bookshelf: no-such-shelf");
        assert!(rss.channel.items.is_empty());
        assert_eq!(rss.channel.entries, Some(0));
    }

    #[test]
    fn test_widget_entries_count_unreadable_books() {
        // A book without a review link isn't read, but the widget still listed it
        let widget = WIDGET.replacen("/review/show/123?", "/book/show/123?", 1);
        let rss = parse_widget(&widget, "1:to-read").unwrap();
        assert_eq!(rss.channel.items.len(), 1);
        assert_eq!(rss.channel.entries, Some(2));
    }

    #[test]
    fn test_parse_invalid_widget() {
        let err = parse_widget(INVALID_WIDGET, "1:to-read").unwrap_err();
        assert!(err.to_string().contains("widget is not correct"), "{err}");
        // A widget for another shelf than requested
        let err = parse_widget(WIDGET, "1:read").unwrap_err();
        assert!(err.to_string().contains("instead of"), "{err}");
        assert!(parse_widget("<html><body>Not found</body></html>", "1:to-read").is_err());
    }

    #[test]
    fn test_unescape_js_string() {
        assert_eq!(
            unescape_js_string(r#"a\"b\/c\nd\'e' trailing"#).unwrap(),
            "a\"b/c\nd'e"
        );
        assert_eq!(unescape_js_string(r"\x41é😀'").unwrap(), "Aé😀");
        assert!(unescape_js_string(r"\u+041'").is_err());
        assert!(unescape_js_string(r"\ude00'").is_err());
        assert!(unescape_js_string("no closing quote").is_err());
    }

    /// Each grab block's matches from one search's results
    fn block_matches(item: &ListItem, grabs: &[Grab], results: &[MaMTorrent]) -> Vec<GrabMatches> {
        let config = config();
        grabs
            .iter()
            .map(|grab| grab_matches(&config, item, grab, results).unwrap())
            .collect()
    }

    fn ids(block: &GrabMatches) -> Vec<u64> {
        block.iter().map(|t| t.0.id).collect()
    }

    /// The selected torrent and its cost, and what the list item records when a torrent was found
    /// but not selected
    type Selection = (Option<(u64, Cost)>, Option<(u64, TorrentStatus)>);

    /// Picks a torrent the way search_item does
    fn select(torrents: &[GrabMatches], main_cat: OldMainCat) -> Selection {
        let mut selected = select_torrent(torrents, main_cat);
        let mut field = None;
        check_cost(&mut field, &mut selected);
        (
            selected.map(|t| (t.0.id, t.3.cost)),
            field.map(|t: ListItemTorrent| (t.mam_id, t.status)),
        )
    }

    #[test]
    fn test_select_from_one_search() {
        let item = list_item("The Example Book", &["Ann Author"]);
        let grabs = grabs(AUDIOBOOK_GRABS);
        let small = audiobook(1, "The Example Book", "Ann Author", "300 MiB", false);
        let large = audiobook(2, "The Example Book", "Ann Author", "700 MiB", false);
        let large_free = audiobook(3, "The Example Book", "Ann Author", "600 MiB", true);
        let audio = OldMainCat::Audio;

        // Up to 350 MiB it's taken on ratio
        let torrents = block_matches(
            &item,
            &grabs,
            &[large.clone(), small, large_free.clone()],
        );
        assert_eq!(ids(&torrents[0]), vec![1]);
        assert_eq!(ids(&torrents[1]), vec![2, 3, 1]);
        assert_eq!(select(&torrents, audio), (Some((1, Cost::Ratio)), None));

        // Otherwise a free one, even when a larger one sorts first
        let torrents = block_matches(&item, &grabs, &[large.clone(), large_free]);
        assert_eq!(ids(&torrents[0]), Vec::<u64>::new());
        assert_eq!(ids(&torrents[1]), vec![2, 3]);
        assert_eq!(select(&torrents, audio), (Some((3, Cost::Free)), None));

        // Otherwise with a wedge
        let torrents = block_matches(&item, &grabs, std::slice::from_ref(&large));
        assert_eq!(select(&torrents, audio), (Some((2, Cost::Wedge)), None));

        // Without the wedge block it's wanted until it turns free
        let torrents = block_matches(&item, &grabs[..2], &[large]);
        assert_eq!(
            select(&torrents, audio),
            (None, Some((2, TorrentStatus::Wanted)))
        );
    }

    #[test]
    fn test_select_passes_over_vip_without_free_flag() {
        let item = list_item("The Example Book", &["Ann Author"]);
        let grabs = grabs(AUDIOBOOK_GRABS);
        let vip_only = |id, size| MaMTorrent {
            vip: true,
            ..audiobook(id, "The Example Book", "Ann Author", size, false)
        };
        let audio = OldMainCat::Audio;

        // Another upload is taken instead, whichever block matches it
        let other = audiobook(2, "The Example Book", "Ann Author", "200 MiB", false);
        let torrents = block_matches(&item, &grabs, &[vip_only(1, "200 MiB"), other]);
        assert_eq!(ids(&torrents[0]), vec![1, 2]);
        assert_eq!(select(&torrents, audio), (Some((2, Cost::Ratio)), None));

        // When it's the only one, the book is wanted rather than shown as selected, since no
        // block selects it
        for size in ["200 MiB", "600 MiB"] {
            let torrents = block_matches(&item, &grabs, &[vip_only(1, size)]);
            assert_eq!(
                select(&torrents, audio),
                (None, Some((1, TorrentStatus::Wanted)))
            );
        }

        // One the account already snatched goes to select_torrents, which stores it without files
        // so the book is marked Existing, ahead of another upload
        let snatched = MaMTorrent {
            my_snatched: true,
            ..vip_only(1, "200 MiB")
        };
        let other = audiobook(2, "The Example Book", "Ann Author", "200 MiB", false);
        let torrents = block_matches(&item, &grabs, &[snatched, other]);
        assert_eq!(select(&torrents, audio), (Some((1, Cost::Ratio)), None));
        // Too big for the ratio block, the free block passes it over, since it isn't free, and
        // the wedge block takes it
        let snatched = MaMTorrent {
            my_snatched: true,
            ..vip_only(1, "600 MiB")
        };
        let torrents = block_matches(&item, &grabs, &[snatched]);
        assert_eq!(select(&torrents, audio), (Some((1, Cost::Wedge)), None));

        // With a free flag it's selected as usual
        let fl_vip = MaMTorrent {
            fl_vip: true,
            ..vip_only(1, "200 MiB")
        };
        let torrents = block_matches(&item, &grabs, &[fl_vip]);
        assert_eq!(select(&torrents, audio), (Some((1, Cost::Ratio)), None));
    }

    #[test]
    fn test_select_leaves_out_found_formats() {
        // The audiobook is wanted once it's free, the ebook can be taken on ratio
        let mut item = list_item("The Example Book", &["Ann Author"]);
        item.audio_torrent = Some(ListItemTorrent {
            mam_id: 9,
            status: TorrentStatus::Wanted,
            at: Timestamp::now(),
        });
        let grabs = grabs(
            r#"
            [[grab]]
            cost = "all"
            max_size = "100 MiB"

            [[grab]]
            cost = "free"
            categories = { audio = true, ebook = false }
        "#,
        );
        let small_audiobook = audiobook(1, "The Example Book", "Ann Author", "90 MiB", false);
        let ebook = ebook(2, "The Example Book", "Ann Author", "5 MiB");

        // The first block searched ebooks only. The second searched everything, so the shared
        // search does too, but the first block still doesn't get the audiobook
        let torrents = block_matches(&item, &grabs, &[small_audiobook, ebook]);
        assert_eq!(ids(&torrents[0]), vec![2]);
        assert_eq!(ids(&torrents[1]), vec![1]);
        assert_eq!(
            select(&torrents, OldMainCat::Audio),
            (None, Some((1, TorrentStatus::Wanted)))
        );
        assert_eq!(
            select(&torrents, OldMainCat::Ebook),
            (Some((2, Cost::Ratio)), None)
        );
    }

    #[test]
    fn test_snatched_torrent_shows_as_found() {
        let mut item = list_item("The Example Book", &["Ann Author"]);
        let grabs = grabs(AUDIOBOOK_GRABS);
        let torrents = block_matches(
            &item,
            &grabs,
            &[audiobook(
                1,
                "The Example Book",
                "Ann Author",
                "300 MiB",
                false,
            )],
        );
        let found = select_torrent(&torrents, OldMainCat::Audio).unwrap();

        // search_item marks it selected before select_torrents skips it as snatched
        let mut mark = |status| mark_found(&mut item.audio_torrent, found, status);
        assert!(mark(TorrentStatus::Selected));
        assert!(mark(TorrentStatus::Existing));
        assert!(!mark(TorrentStatus::Existing));
        let field = item.audio_torrent.as_ref().unwrap();
        assert_eq!((field.mam_id, &field.status), (1, &TorrentStatus::Existing));
        // The audiobook isn't searched for again
        assert!(!item.want_audio());
    }

    fn audiobook_list(toml_extra: &str) -> GoodreadsList {
        toml::from_str(&format!(
            "url = \"https://www.goodreads.com/review/list/1?shelf=to-read\"\n{toml_extra}\n{AUDIOBOOK_GRABS}"
        ))
        .unwrap()
    }

    #[test]
    fn test_search_stored_books_setting() {
        assert!(!audiobook_list("").search_stored_books);
        assert!(audiobook_list("search_stored_books = true").search_stored_books);
    }

    #[test]
    fn test_book_id_from_url() {
        let book = "https://www.goodreads.com/book/show/";
        for (url, id) in [
            (format!("{book}87654321"), Some(87654321)),
            (format!("{book}123-the-example-book"), Some(123)),
            (format!("{book}123.The_Example_Book?ac=1"), Some(123)),
            (book.to_owned(), None),
            (
                "https://www.goodreads.com/review/show/1000000001".to_owned(),
                None,
            ),
        ] {
            assert_eq!(book_id_from_url(&url), id, "{url}");
        }
    }

    #[test]
    fn test_stored_book_gets_list_settings() {
        // The list only grabs audiobooks, so a book stored while it took ebooks too stops wanting
        // the ebook
        let list = audiobook_list("prefer_format = \"audio\"");
        let item = list_item("The Example Book", &["Ann Author"]);
        assert!(item.want_ebook());
        let (item, changed) = apply_list_settings(item, &list);
        assert!(changed);
        assert_eq!(
            (item.prefer_format, item.allow_audio, item.allow_ebook),
            (Some(OldDbMainCat::Audio), true, false)
        );
        assert!(item.want_audio() && !item.want_ebook());
        // Applying them again changes nothing
        let (_, changed) = apply_list_settings(item, &list);
        assert!(!changed);
    }

    #[test]
    fn test_stored_books() {
        let db = native_db::Builder::new()
            .create_in_memory(&mlm_db::MODELS)
            .unwrap();
        let now = time::UtcDateTime::now();
        let book = |list: &str, guid: &str, hours_ago: i64, book_id: Option<&str>| ListItem {
            guid: (list.to_owned(), guid.to_owned()),
            list_id: list.to_owned(),
            book_url: book_id.map(|id| format!("https://www.goodreads.com/book/show/{id}")),
            created_at: Timestamp::from(now - time::Duration::hours(hours_ago)),
            ..list_item(guid, &["Ann Author"])
        };
        let rw = db.rw_transaction().unwrap();
        for item in [
            book("list", "in the shelf", 1, Some("1")),
            book("list", "in the shelf without a cover", 6, None),
            book("list", "older", 3, Some("2")),
            book("list", "newer", 2, Some("3-the-example-book")),
            book("list", "without a link", 5, None),
            book("other list", "of another list", 0, None),
            // Removed from the shelf and added again, so it's in the shelf under another review
            book("list", "added again", 4, Some("4")),
        ] {
            rw.insert(item).unwrap();
        }
        rw.commit().unwrap();

        let fetched = [
            "in the shelf",
            "in the shelf without a cover",
            "added again, new review",
        ]
        .map(str::to_owned)
        .into();
        let fetched_books = [1, 4].into();
        let stored = stored_books(&db, "list", &fetched, &fetched_books).unwrap();
        let guids: Vec<_> = stored.iter().map(|guid| guid.1.as_str()).collect();
        assert_eq!(guids, ["newer", "older", "without a link"]);
        assert!(stored.iter().all(|guid| guid.0 == "list"));
    }

    #[test]
    fn test_read_whole_shelf() {
        assert!(read_whole_shelf(0));
        assert!(read_whole_shelf(99));
        assert!(!read_whole_shelf(100));
    }

    #[test]
    fn test_has_torrents() {
        let found = |status| ListItemTorrent {
            mam_id: 1,
            status,
            at: Timestamp::now(),
        };
        let audio_list = audiobook_list("prefer_format = \"audio\"");
        let any_list = audiobook_list("");
        let mut item = list_item("The Example Book", &["Ann Author"]);
        assert!(!has_torrents(&item, &audio_list));

        // An audiobook is enough when the list prefers audiobooks, not otherwise
        item.audio_torrent = Some(found(TorrentStatus::Selected));
        assert!(has_torrents(&item, &audio_list));
        assert!(!has_torrents(&item, &any_list));

        // Both formats are enough for any list
        item.ebook_torrent = Some(found(TorrentStatus::Existing));
        assert!(has_torrents(&item, &any_list));
    }

    /// Fetches a real shelf and another shelf of the same user through the widget, run with
    /// GOODREADS_RSS_URL=... cargo test -p mlm live_widget -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn live_widget() {
        let list = GoodreadsList {
            url: std::env::var("GOODREADS_RSS_URL").unwrap(),
            name: None,
            prefer_format: None,
            grab: vec![],
            search_stored_books: false,
            search_interval: None,
            unsat_buffer: None,
            wedge_buffer: None,
            dry_run: true,
        };
        let shelf = list.list_id().unwrap().split_once(':').unwrap().1.to_owned();
        // Another shelf of the same user. It doesn't need to exist: the widget names the
        // requested shelf even when it has no books
        let other_shelf = if shelf == "read" { "to-read" } else { "read" };
        let mut other_url: reqwest::Url = list.url.parse().unwrap();
        let pairs: Vec<(String, String)> = other_url
            .query_pairs()
            .map(|(name, value)| {
                let value = if name == "shelf" {
                    other_shelf.to_owned()
                } else {
                    value.into_owned()
                };
                (name.into_owned(), value)
            })
            .collect();
        other_url.query_pairs_mut().clear().extend_pairs(pairs);
        let other = GoodreadsList {
            url: other_url.to_string(),
            ..list.clone()
        };
        // Switching shelves of the same user must return each requested shelf, not a cached
        // render of the previous one. parse_widget fails if the widget is for another shelf
        for list in [&list, &other, &list] {
            let list_id = list.list_id().unwrap();
            let rss = fetch_list(list).await.unwrap();
            println!(
                "{list_id}: {} ({} items)",
                rss.channel.title,
                rss.channel.items.len()
            );
            for item in &rss.channel.items {
                println!(
                    "{} | {} | {:?} | {:?}",
                    item.guid, item.title, item.author_name, item.book_id
                );
            }
            let list_shelf = list_id.split_once(':').unwrap().1;
            assert!(rss.channel.title.ends_with(&format!(": {list_shelf}")));
            if list_shelf == shelf {
                assert!(!rss.channel.items.is_empty());
            }
            sleep(Duration::from_millis(1100)).await;
        }
    }
}
