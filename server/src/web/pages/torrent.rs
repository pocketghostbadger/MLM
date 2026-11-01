use std::{collections::BTreeSet, path::PathBuf};

use anyhow::Result;
use askama::Template;
use axum::{
    body::Body,
    extract::{OriginalUri, Path, State},
    response::{Html, IntoResponse, Redirect},
};
use axum_extra::extract::Form;
use itertools::Itertools;
use mlm_db::{
    ClientStatus, DatabaseExt as _, Event, EventKey, EventType, Size, Torrent, TorrentCost,
    TorrentKey, TorrentMeta,
};
use mlm_mam::{
    api::MaM,
    enums::SearchIn,
    search::{MaMTorrent, SearchFields, SearchQuery, Tor},
};
use native_db::Database;
use qbit::{
    models::{Category, Torrent as QbitTorrent, Tracker},
    parameters::TorrentState,
};
use regex::Regex;
use reqwest::header;
use serde::Deserialize;
use time::UtcDateTime;
use tokio_util::io::ReaderStream;

use crate::{
    audiobookshelf::{Abs, LibraryItemMinified},
    cleaner::clean_torrent,
    config::Config,
    linker::{find_library, library_dir, map_path, refresh_metadata, refresh_metadata_relink},
    qbittorrent::{self, ensure_category_exists},
    stats::Context,
    web::{
        AppError, Conditional, MaMTorrentsTemplate, Page, TorrentLink, flag_icons,
        pages::{search::select_torrent, torrents::TorrentsPageFilter},
        tables::table_styles,
        time,
    },
};

pub async fn torrent_file(
    State(context): State<Context>,
    Path((id, filename)): Path<(String, String)>,
) -> impl IntoResponse {
    let config = context.config().await;
    let Some(torrent) = context.db.r_transaction()?.get().primary::<Torrent>(id)? else {
        return Err(AppError::NotFound);
    };
    let Some(path) = (if let (Some(library_path), Some(library_file)) = (
        &torrent.library_path,
        torrent
            .library_files
            .iter()
            .find(|f| f.to_string_lossy() == filename),
    ) {
        Some(library_path.join(library_file))
    } else if let Some((torrent, qbit, qbit_config)) =
        qbittorrent::get_torrent(&config, &torrent.id).await?
    {
        qbit.files(&torrent.hash, None)
            .await?
            .into_iter()
            .find(|f| f.name == filename)
            .map(|file| map_path(&qbit_config.path_mapping, &torrent.save_path).join(&file.name))
    } else {
        None
    }) else {
        return Err(AppError::NotFound);
    };
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(_) => return Err(AppError::NotFound),
    };
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let headers = [
        (header::CONTENT_TYPE, "text/toml; charset=utf-8".to_string()),
        (
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        ),
    ];

    Ok((headers, body))
}

pub async fn torrent_page(
    State(context): State<Context>,
    Path(id_or_mam_id): Path<String>,
) -> std::result::Result<Html<String>, AppError> {
    if let Ok(id) = id_or_mam_id.parse() {
        torrent_page_mam_id(State(context), Path(id)).await
    } else {
        torrent_page_id(State(context), Path(id_or_mam_id)).await
    }
}

async fn torrent_page_mam_id(
    State(context): State<Context>,
    Path(mam_id): Path<u64>,
) -> std::result::Result<Html<String>, AppError> {
    if let Some(torrent) = context
        .db
        .r_transaction()?
        .get()
        .secondary::<Torrent>(TorrentKey::mam_id, mam_id)?
    {
        return torrent_page_id(State(context), Path(torrent.id)).await;
    };

    let mam = context.mam()?;
    let Some(mam_torrent) = mam.get_torrent_info_by_id(mam_id).await? else {
        return Err(AppError::NotFound);
    };
    let meta = mam_torrent.as_meta()?;

    println!("mam_torrent: {:?}", mam_torrent);
    println!("mam_meta: {:?}", meta);
    let config = context.config.lock().await.clone();
    let other_torrents = other_torrents(&config, &context.db, &mam, &meta).await?;

    let template = TorrentMamPageTemplate {
        mam_torrent,
        meta,
        other_torrents,
    };
    Ok::<_, AppError>(Html(template.to_string()))
}

async fn torrent_page_id(
    State(context): State<Context>,
    Path(id): Path<String>,
) -> std::result::Result<Html<String>, AppError> {
    let config = context.config().await;
    let abs = config.audiobookshelf.as_ref().map(Abs::new);
    let Some(mut torrent) = context.db.r_transaction()?.get().primary::<Torrent>(id)? else {
        return Err(AppError::NotFound);
    };
    let replacement_torrent = torrent
        .replaced_with
        .as_ref()
        .map(|(id, _)| {
            context
                .db
                .r_transaction()?
                .get()
                .primary::<Torrent>(id.to_string())
        })
        .transpose()?
        .flatten();

    if replacement_torrent.is_none() && torrent.replaced_with.is_some() {
        let (_guard, rw) = context.db.rw_async().await?;
        torrent.replaced_with = None;
        rw.upsert(torrent.clone())?;
        rw.commit()?;
    }
    let book = match abs {
        Some(abs) => abs?.get_book(&torrent).await?,
        None => None,
    };

    let events = context
        .db
        .r_transaction()?
        .scan()
        .secondary::<Event>(EventKey::mam_id)?;
    let events = events.range(Some(torrent.mam_id)..=Some(torrent.mam_id))?;
    let mut events = events.collect::<Result<Vec<_>, _>>()?;
    events.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let mam = context.mam()?;
    let mam_torrent = mam.get_torrent_info_by_id(torrent.mam_id).await?;
    let mam_meta = mam_torrent.as_ref().map(|t| t.as_meta()).transpose()?;

    if let Some(mam_meta) = &mam_meta
        && torrent.meta.uploaded_at.0 == UtcDateTime::UNIX_EPOCH
    {
        let (_guard, rw) = context.db.rw_async().await?;
        torrent.meta.uploaded_at = mam_meta.uploaded_at;
        rw.upsert(torrent.clone())?;
        rw.commit()?;
    }

    let mut qbit_data = None;
    let mut wanted_path = None;
    let mut qbit_files = vec![];
    if torrent.id_is_hash
        && let Some((qbit_torrent, qbit, _)) =
            qbittorrent::get_torrent(&config, &torrent.id).await?
    {
        let trackers = qbit.trackers(&torrent.id).await?;
        let mut categories = qbit.categories().await?.into_values().collect_vec();
        categories.sort_by(|a, b| a.name.cmp(&b.name));
        let tags = qbit.tags().await?;

        wanted_path = find_library(&config, &qbit_torrent).and_then(|library| {
            library_dir(
                config.exclude_narrator_in_library_dir,
                library,
                &torrent.meta,
            )
        });

        qbit_data = Some(QbitData {
            torrent_tags: split_tags(&qbit_torrent.tags).map(str::to_string).collect(),
            category_listed: qbit_torrent.category.is_empty()
                || categories.iter().any(|cat| cat.name == qbit_torrent.category),
            torrent: qbit_torrent,
            trackers,
            categories,
            tags,
        });

        qbit_files = qbit.files(&torrent.id, None).await?;
    }

    println!("book: {:?}", book);
    println!("mam_torrent: {:?}", mam_torrent);
    println!(
        "mam_meta: {} {:?}",
        Some(&torrent.meta) == mam_meta.as_ref(),
        mam_meta
    );
    println!("qbit: {:?}", qbit_data);

    if !config.qbittorrent.is_empty()
        && qbit_data.is_none()
        && torrent.client_status != Some(ClientStatus::NotInClient)
    {
        let (_guard, rw) = context.db.rw_async().await?;
        torrent.client_status = Some(ClientStatus::NotInClient);
        rw.upsert(torrent.clone())?;
        rw.commit()?;
    }
    let other_torrents = other_torrents(&config, &context.db, &mam, &torrent.meta).await?;

    let template = TorrentPageTemplate {
        abs_url: config
            .audiobookshelf
            .as_ref()
            .map(|abs| abs.url.clone())
            .unwrap_or_default(),
        torrent,
        replacement_torrent,
        events,
        book,
        mam_torrent,
        mam_meta,
        qbit_data,
        wanted_path,
        qbit_files,
        other_torrents,
    };
    Ok::<_, AppError>(Html(template.to_string()))
}

pub async fn torrent_page_post(
    State(context): State<Context>,
    Path(id_or_mam_id): Path<String>,
    uri: OriginalUri,
    Form(form): Form<TorrentPageForm>,
) -> Result<Redirect, AppError> {
    if let Ok(id) = id_or_mam_id.parse() {
        torrent_page_post_mam_id(State(context), Path(id), uri, Form(form)).await
    } else {
        torrent_page_post_id(State(context), Path(id_or_mam_id), uri, Form(form)).await
    }
}

pub async fn torrent_page_post_mam_id(
    State(context): State<Context>,
    Path(mam_id): Path<u64>,
    uri: OriginalUri,
    Form(form): Form<TorrentPageForm>,
) -> Result<Redirect, AppError> {
    let mam_id = form.mam_id.unwrap_or(mam_id);
    if let Some(torrent) = context
        .db
        .r_transaction()?
        .get()
        .secondary::<Torrent>(TorrentKey::mam_id, mam_id)?
    {
        if form.mam_id.is_some() {
            return Err(anyhow::Error::msg("torrent is already downloaded").into());
        }
        return torrent_page_post_id(State(context), Path(torrent.id), uri, Form(form)).await;
    };

    match form.action.as_str() {
        "select" | "wedge" => {
            select_torrent(&context, mam_id, form.action == "wedge").await?;
        }
        action => {
            eprintln!("unknown action: {action}");
        }
    }

    Ok(Redirect::to(&uri.to_string()))
}

pub async fn torrent_page_post_id(
    State(context): State<Context>,
    Path(id): Path<String>,
    uri: OriginalUri,
    Form(form): Form<TorrentPageForm>,
) -> Result<Redirect, AppError> {
    let config = context.config().await;
    match form.action.as_str() {
        "select" | "wedge" => {
            let Some(mam_id) = form.mam_id else {
                return Err(anyhow::Error::msg("torrent is already downloaded").into());
            };
            select_torrent(&context, mam_id, form.action == "wedge").await?;
        }
        "clean" => {
            let Some(torrent) = context.db.r_transaction()?.get().primary(id)? else {
                return Err(anyhow::Error::msg("Could not find torrent").into());
            };
            clean_torrent(&config, &context.db, torrent, true).await?;
        }
        "refresh" => {
            let mam = context.mam()?;
            refresh_metadata(&config, &context.db, &mam, id).await?;
        }
        "refresh-relink" => {
            let mam = context.mam()?;
            refresh_metadata_relink(&config, &context.db, &mam, id).await?;
        }
        "remove" => {
            let (_guard, rw) = context.db.rw_async().await?;
            let Some(torrent) = rw.get().primary::<Torrent>(id)? else {
                return Err(anyhow::Error::msg("Could not find torrent").into());
            };
            rw.remove(torrent)?;
            rw.commit()?;
        }
        "torrent-start" => {
            let Some((_torrent, qbit, _)) = qbittorrent::get_torrent(&config, &id).await? else {
                return Err(anyhow::Error::msg("Could not find torrent").into());
            };
            qbit.start(vec![&id]).await?;
        }
        "torrent-stop" => {
            let Some((_torrent, qbit, _)) = qbittorrent::get_torrent(&config, &id).await? else {
                return Err(anyhow::Error::msg("Could not find torrent").into());
            };
            qbit.stop(vec![&id]).await?;
        }
        "clear-replacement" => {
            let (_guard, rw) = context.db.rw_async().await?;
            let Some(mut torrent) = rw.get().primary::<Torrent>(id)? else {
                return Err(anyhow::Error::msg("Could not find torrent").into());
            };
            torrent.replaced_with.take();
            rw.upsert(torrent)?;
            rw.commit()?;
        }
        "qbit" => {
            let Some((torrent, qbit, qbit_conf)) = qbittorrent::get_torrent(&config, &id).await? else {
                return Err(anyhow::Error::msg("Could not find torrent").into());
            };

            // A page opened before the loaded fields existed is compared with the torrent as it
            // is now.
            let loaded = form.loaded.is_some();
            let loaded_category = if loaded {
                &form.loaded_category
            } else {
                &torrent.category
            };
            if category_changed(&form.category, loaded_category, &torrent.category) {
                ensure_category_exists(&qbit, &qbit_conf.url, &form.category).await?;
                qbit.set_category(Some(vec![&id]), &form.category).await?;
            }
            let (add_tags, remove_tags) = tag_changes(
                &torrent.tags,
                loaded.then_some(form.loaded_tags.as_slice()),
                &form.tags,
            );
            if !add_tags.is_empty() {
                println!("add tags {:?}", add_tags);
                qbit.add_tags(Some(vec![&id]), add_tags.into_iter().collect())
                    .await?;
            }
            if !remove_tags.is_empty() {
                println!("remove tags {remove_tags:?}");
                qbit.remove_tags(Some(vec![&id]), remove_tags.into_iter().collect())
                    .await?;
            }
        }
        "remove-torrent" => {
            // let Some(torrent) = context.db.r_transaction()?.get().primary(id)? else {
            //     return Err(anyhow::Error::msg("Could not find torrent").into());
            // };
            // remove_library_files(&torrent)?;
            for qbit_conf in config.qbittorrent.iter() {
                let qbit = qbit::Api::new_login_username_password(
                    &qbit_conf.url,
                    &qbit_conf.username,
                    &qbit_conf.password,
                )
                .await?;
                qbit.delete(vec![&id], true).await?;
            }
        }
        action => {
            eprintln!("unknown action: {action}");
        }
    }

    Ok(Redirect::to(&uri.to_string()))
}

#[derive(Debug, Deserialize)]
pub struct TorrentPageForm {
    action: String,
    #[serde(default)]
    category: String,
    /// Set by pages that send loaded_category and loaded_tags: the torrent's category and tags
    /// when the page loaded.
    loaded: Option<String>,
    #[serde(default)]
    loaded_category: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    loaded_tags: Vec<String>,
    mam_id: Option<u64>,
}

#[derive(Template)]
#[template(path = "pages/torrent.html")]
struct TorrentPageTemplate {
    abs_url: String,
    torrent: Torrent,
    replacement_torrent: Option<Torrent>,
    events: Vec<Event>,
    book: Option<LibraryItemMinified>,
    mam_torrent: Option<MaMTorrent>,
    mam_meta: Option<TorrentMeta>,
    qbit_data: Option<QbitData>,
    wanted_path: Option<PathBuf>,
    qbit_files: Vec<qbit::models::TorrentContent>,
    other_torrents: MaMTorrentsTemplate,
}

impl TorrentPageTemplate {
    fn torrent_title<'a>(&'a self, torrent: &'a Option<Torrent>) -> Conditional<TorrentLink<'a>> {
        Conditional {
            template: torrent.as_ref().map(|t| TorrentLink {
                id: &t.id,
                title: &t.meta.title,
            }),
        }
    }
}

impl Page for TorrentPageTemplate {
    fn item_path(&self) -> &'static str {
        "/torrents"
    }
}

#[derive(Template)]
#[template(path = "pages/torrent_mam.html")]
struct TorrentMamPageTemplate {
    mam_torrent: MaMTorrent,
    meta: TorrentMeta,
    other_torrents: MaMTorrentsTemplate,
}

impl Page for TorrentMamPageTemplate {
    fn item_path(&self) -> &'static str {
        "/torrents"
    }
}

#[derive(Debug)]
struct QbitData {
    torrent: QbitTorrent,
    trackers: Vec<Tracker>,
    categories: Vec<Category>,
    tags: Vec<String>,
    torrent_tags: BTreeSet<String>,
    category_listed: bool,
}

#[allow(unused)]
fn duration(seconds: f64) -> String {
    let mut seconds = seconds as u64;
    let hours = seconds / 3600;
    seconds -= hours * 3600;
    let minutes = seconds / 60;
    seconds -= minutes * 60;
    let mut duration = vec![];
    if hours > 0 {
        duration.push(format!("{hours}h"));
    }
    if minutes > 0 {
        duration.push(format!("{minutes}m"));
    }
    if seconds > 0 {
        duration.push(format!("{seconds}s"));
    }
    duration.join(" ")
}

async fn other_torrents(
    config: &Config,
    db: &Database<'_>,
    mam: &MaM<'_>,
    meta: &TorrentMeta,
) -> Result<MaMTorrentsTemplate> {
    let title = meta
        .title
        .split_once(":")
        .map_or(meta.title.as_str(), |(a, _)| a);

    let title = Regex::new(r#"([\*\?])"#)
        .unwrap()
        .replace_all(title, r#""$1""#);
    let title = Regex::new(r#"(?:['`/-])"#)
        .unwrap()
        .replace_all(&title, " ");
    let title = Regex::new(r#"\s+\[[^\]\[]+?\]"#)
        .unwrap()
        .replace_all(&title, "");
    let title = Regex::new(r#"\s+\([^<>\)\(]+?\)|\s+\[[^<>\]\[]+?\]|@"#)
        .unwrap()
        .replace_all(&title, "");
    let title = Regex::new(r#"&|\band\b"#)
        .unwrap()
        .replace_all(&title, "(&|and)");

    let result = mam
        .search(&SearchQuery {
            fields: SearchFields {
                media_info: true,
                ..Default::default()
            },
            tor: Tor {
                text: if meta.authors.is_empty() {
                    title.to_string()
                } else {
                    format!(
                        "{} ({})",
                        title,
                        meta.authors.iter().map(|a| format!("\"{a}\"")).join(" | ")
                    )
                },
                srch_in: vec![SearchIn::Title, SearchIn::Author],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;

    let r = db.r_transaction()?;
    let torrents = result
        .data
        .into_iter()
        .filter(|t| t.id != meta.mam_id)
        .map(|mam_torrent| {
            let meta = mam_torrent.as_meta()?;
            let torrent = r
                .get()
                .secondary::<Torrent>(TorrentKey::mam_id, meta.mam_id)?;
            let selected_torrent = r.get().primary(mam_torrent.id)?;

            Ok((mam_torrent, meta, torrent, selected_torrent))
        })
        .collect::<Result<_>>()?;

    Ok(MaMTorrentsTemplate {
        config: config.search.clone(),
        torrents,
    })
}

/// Splits a qBittorrent tag list, joined by ", ".
fn split_tags(tags: &str) -> impl Iterator<Item = &str> {
    tags.split(", ").filter(|tag| !tag.is_empty())
}

/// Whether saving the torrent page changes the torrent's category: only if the user `picked`
/// another one than the page showed (`loaded`), so a category set elsewhere in the meantime
/// stays. The page doesn't clear a category: "(none)" is only offered for a torrent without one.
fn category_changed(picked: &str, loaded: &str, current: &str) -> bool {
    !picked.is_empty() && picked != loaded && picked != current
}

/// Returns the tags to add to and remove from a torrent when the torrent page is saved. Only the
/// tags the user checked or unchecked change: `loaded` are the torrent's tags when the page
/// loaded, `checked` the tags checked on save and `current` the torrent's tags now, so a tag
/// added or removed elsewhere in the meantime is left as it is. Without `loaded`, the checked
/// tags are compared with `current`.
fn tag_changes<'a>(
    current: &'a str,
    loaded: Option<&'a [String]>,
    checked: &'a [String],
) -> (BTreeSet<&'a str>, BTreeSet<&'a str>) {
    let current = split_tags(current).collect::<BTreeSet<&str>>();
    let loaded = match loaded {
        Some(loaded) => loaded.iter().map(String::as_str).collect::<BTreeSet<&str>>(),
        None => current.clone(),
    };
    let checked = checked.iter().map(String::as_str).collect::<BTreeSet<&str>>();
    let add = checked
        .difference(&loaded)
        .filter(|tag| !current.contains(*tag))
        .copied()
        .collect();
    let remove = loaded
        .difference(&checked)
        .filter(|tag| current.contains(*tag))
        .copied()
        .collect();
    (add, remove)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(tags: &[&str]) -> Vec<String> {
        tags.iter().map(|tag| tag.to_string()).collect()
    }

    fn set<'a>(tags: &[&'a str]) -> BTreeSet<&'a str> {
        tags.iter().copied().collect()
    }

    #[test]
    fn unchecking_a_tag_removes_only_that_tag() {
        let loaded = tags(&["a", "b", "c"]);
        let checked = tags(&["a", "c"]);
        let (add, remove) = tag_changes("a, b, c", Some(&loaded[..]), &checked);
        assert_eq!(add, set(&[]));
        assert_eq!(remove, set(&["b"]));
    }

    #[test]
    fn checking_a_tag_adds_only_that_tag() {
        let loaded = tags(&["a"]);
        let checked = tags(&["a", "b"]);
        let (add, remove) = tag_changes("a", Some(&loaded[..]), &checked);
        assert_eq!(add, set(&["b"]));
        assert_eq!(remove, set(&[]));
    }

    #[test]
    fn checking_and_unchecking_at_once() {
        let loaded = tags(&["a", "b"]);
        let checked = tags(&["b", "c"]);
        let (add, remove) = tag_changes("a, b", Some(&loaded[..]), &checked);
        assert_eq!(add, set(&["c"]));
        assert_eq!(remove, set(&["a"]));
    }

    #[test]
    fn unchecking_every_tag_removes_them_all() {
        let loaded = tags(&["a", "b"]);
        let (add, remove) = tag_changes("a, b", Some(&loaded[..]), &[]);
        assert_eq!(add, set(&[]));
        assert_eq!(remove, set(&["a", "b"]));
    }

    #[test]
    fn keeps_tags_added_after_the_page_loaded() {
        let loaded = tags(&["a", "b"]);
        let checked = tags(&["a"]);
        let (add, remove) = tag_changes("a, b, x", Some(&loaded[..]), &checked);
        assert_eq!(add, set(&[]));
        assert_eq!(remove, set(&["b"]));
    }

    #[test]
    fn does_not_add_back_tags_removed_after_the_page_loaded() {
        let loaded = tags(&["a", "b"]);
        let checked = tags(&["a", "b"]);
        let (add, remove) = tag_changes("a", Some(&loaded[..]), &checked);
        assert_eq!(add, set(&[]));
        assert_eq!(remove, set(&[]));
    }

    #[test]
    fn skips_changes_already_made_elsewhere() {
        let loaded = tags(&["a"]);
        let checked = tags(&["b"]);
        let (add, remove) = tag_changes("b", Some(&loaded[..]), &checked);
        assert_eq!(add, set(&[]));
        assert_eq!(remove, set(&[]));
    }

    #[test]
    fn torrent_without_tags() {
        let (add, remove) = tag_changes("", Some(&[][..]), &[]);
        assert_eq!(add, set(&[]));
        assert_eq!(remove, set(&[]));

        let checked = tags(&["a"]);
        let (add, remove) = tag_changes("", Some(&[][..]), &checked);
        assert_eq!(add, set(&["a"]));
        assert_eq!(remove, set(&[]));
    }

    #[test]
    fn unchanged_tags() {
        let loaded = tags(&["a", "b"]);
        let checked = tags(&["a", "b"]);
        let (add, remove) = tag_changes("a, b", Some(&loaded[..]), &checked);
        assert_eq!(add, set(&[]));
        assert_eq!(remove, set(&[]));
    }

    #[test]
    fn without_loaded_tags_compares_with_current_tags() {
        let checked = tags(&["a", "c"]);
        let (add, remove) = tag_changes("a, b", None, &checked);
        assert_eq!(add, set(&["c"]));
        assert_eq!(remove, set(&["b"]));
    }

    #[test]
    fn split_tags_skips_empty_list() {
        assert_eq!(split_tags("").count(), 0);
        assert_eq!(split_tags("a, b").collect::<Vec<_>>(), vec!["a", "b"]);
    }

    #[test]
    fn category_changes_only_when_picked() {
        assert!(!category_changed("books", "books", "books"));
        assert!(category_changed("audio", "books", "books"));
        assert!(category_changed("books", "", ""));
        // The page only offers "(none)" for a torrent without a category, so never clears one.
        assert!(!category_changed("", "books", "books"));
        // Set elsewhere after the page loaded.
        assert!(!category_changed("", "", "books"));
        assert!(!category_changed("books", "books", "audio"));
    }

    async fn parse_form(body: &'static str) -> TorrentPageForm {
        use axum::{body::Body, extract::FromRequest, http::Request};
        let request = Request::post("/")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let Form(form) = Form::<TorrentPageForm>::from_request(request, &())
            .await
            .unwrap();
        form
    }

    #[tokio::test]
    async fn parses_page_without_category_or_tags() {
        let form = parse_form("loaded=1&loaded_category=&category=&action=qbit").await;
        assert!(form.loaded.is_some());
        assert_eq!(form.loaded_category, "");
        assert!(form.loaded_tags.is_empty());
        assert!(form.tags.is_empty());
    }

    #[tokio::test]
    async fn parses_repeated_tags() {
        let form = parse_form(
            "loaded=1&loaded_category=books&category=books&tags=a&tags=b&loaded_tags=a&loaded_tags=c&action=qbit",
        )
        .await;
        assert_eq!(form.loaded_category, "books");
        assert_eq!(form.tags, tags(&["a", "b"]));
        assert_eq!(form.loaded_tags, tags(&["a", "c"]));
    }

    #[tokio::test]
    async fn parses_page_from_before_loaded_fields() {
        let form = parse_form("category=books&tags=a&action=qbit").await;
        assert!(form.loaded.is_none());
    }
}
