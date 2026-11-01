//! Links ebooks into the folders of matching audiobooks, for libraries with link_to_audiobooks on,
//! so Audiobookshelf shows the ebook with the audiobook

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, BufRead as _, ErrorKind},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use anyhow::Result;
use file_id::get_file_id;
use mlm_db::{
    DatabaseExt as _, EbookLink, EbookLinkKey, Event, EventKey, EventType, MediaType, Timestamp,
    Torrent, TorrentMeta,
};
use native_db::Database;
use once_cell::sync::Lazy;
use qbit::parameters::TorrentListParams;
use tokio::sync::MutexGuard;
use tracing::{debug, info, instrument, warn};

use crate::{
    config::{Config, Library, LibraryLinkMethod},
    linker::windows_fs_path,
};

/// With link_to_audiobooks_existing off, only books linked since this time are paired
static STARTED_AT: Lazy<Timestamp> = Lazy::new(Timestamp::now);

/// Held while ebook links are added or removed, and while library files are removed or linked,
/// so a pass never pairs a torrent whose library files are changing
static RUN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Messages already logged, so each skip or failure is logged once per start
static LOGGED: Lazy<Mutex<HashSet<String>>> = Lazy::new(Default::default);

/// Sets the time from which books count as new for link_to_audiobooks_existing
pub fn note_start() {
    Lazy::force(&STARTED_AT);
}

/// Proof that the caller holds the lock on ebook links
pub struct LinksGuard {
    _guard: MutexGuard<'static, ()>,
}

pub async fn lock() -> LinksGuard {
    LinksGuard {
        _guard: RUN.lock().await,
    }
}

/// Removes recorded ebook links that no longer fit, then links an ebook into each audiobook
/// folder that has none yet, from libraries with link_to_audiobooks on
#[instrument(skip_all)]
pub async fn link_ebooks_to_audiobooks(config: &Config, db: &Database<'_>) -> Result<()> {
    let any_links = db.r_transaction()?.len().primary::<EbookLink>()? > 0;
    if !any_links
        && !config.libraries.iter().any(|l| {
            l.tag_filters().link_to_audiobooks && l.method() != LibraryLinkMethod::NoLink
        })
    {
        return Ok(());
    }
    let tags = qbit_tags(config).await;
    pair(config, db, tags.as_ref()).await
}

/// Each torrent's qBittorrent tags, by hash, when a library has link_to_audiobooks_deny_tags.
/// None if a qBittorrent instance couldn't be asked, so the tags aren't known
async fn qbit_tags(config: &Config) -> Option<HashMap<String, Vec<String>>> {
    let mut tags = HashMap::new();
    if config
        .libraries
        .iter()
        .all(|l| l.tag_filters().link_to_audiobooks_deny_tags.is_empty())
    {
        return Some(tags);
    }
    for qbit_conf in &config.qbittorrent {
        // Every qBittorrent instance's linker runs this pass, so one that doesn't answer mustn't
        // hold up the others
        let torrents = tokio::time::timeout(Duration::from_secs(30), async {
            let qbit = qbit::Api::new_login_username_password(
                &qbit_conf.url,
                &qbit_conf.username,
                &qbit_conf.password,
            )
            .await?;
            qbit.torrents(Some(TorrentListParams::default())).await
        })
        .await
        .map_err(|_| "timed out".to_owned())
        .and_then(|torrents| torrents.map_err(|err| err.to_string()));
        match torrents {
            Ok(torrents) => {
                // So the next outage is logged again
                LOGGED
                    .lock()
                    .unwrap()
                    .remove(&format!("tags {}", qbit_conf.url));
                for torrent in torrents {
                    tags.entry(torrent.hash).or_insert_with(Vec::new).extend(
                        torrent
                            .tags
                            .split(", ")
                            .filter(|tag| !tag.is_empty())
                            .map(ToOwned::to_owned),
                    );
                }
            }
            Err(err) => {
                log_once(
                    format!("tags {}", qbit_conf.url),
                    true,
                    format!(
                        "Not linking or unlinking ebooks for deny tags: could not get tags from \
                         qbit {}: {err}",
                        qbit_conf.url
                    ),
                );
                return None;
            }
        }
    }
    Some(tags)
}

/// Whether either torrent has one of the ebook library's link_to_audiobooks_deny_tags. None when
/// the library has such tags but the torrents' tags aren't known
fn denied(
    config: &Config,
    tags: Option<&HashMap<String, Vec<String>>>,
    ebook: &Torrent,
    audiobook: &Torrent,
) -> Option<bool> {
    let deny_tags: Vec<&String> = match ebook_library(config, ebook) {
        Some(library) => library.tag_filters().link_to_audiobooks_deny_tags.iter().collect(),
        // Its library isn't known, as when several share its folder: any of their tags counts
        None => {
            let Some(dir) = &ebook.library_path else {
                return Some(false);
            };
            config
                .libraries
                .iter()
                .filter(|l| dir.starts_with(l.library_dir()))
                .flat_map(|l| &l.tag_filters().link_to_audiobooks_deny_tags)
                .collect()
        }
    };
    if deny_tags.is_empty() {
        return Some(false);
    }
    let tags = tags?;
    Some([ebook, audiobook].iter().any(|torrent| {
        tags.get(&torrent.id)
            .is_some_and(|tags| tags.iter().any(|tag| deny_tags.contains(&tag)))
    }))
}

async fn pair(
    config: &Config,
    db: &Database<'_>,
    tags: Option<&HashMap<String, Vec<String>>>,
) -> Result<()> {
    let _links = lock().await;
    let links: Vec<EbookLink> = {
        let r = db.r_transaction()?;
        let links = r.scan().primary::<EbookLink>()?;
        links.all()?.filter_map(readable_link).collect()
    };
    let libraries = config
        .libraries
        .iter()
        .filter(|l| {
            l.tag_filters().link_to_audiobooks && l.method() != LibraryLinkMethod::NoLink
        })
        .collect::<Vec<_>>();
    if links.is_empty() && libraries.is_empty() {
        return Ok(());
    }

    let mut unreadable = false;
    let torrents: HashMap<String, Torrent> = {
        let r = db.r_transaction()?;
        let torrents = r.scan().primary::<Torrent>()?;
        torrents
            .all()?
            .filter_map(|t| {
                unreadable |= t.is_err();
                t.ok()
            })
            .map(|t| (t.id.clone(), t))
            .collect()
    };

    // Links stay when the option is turned off or a format is removed, but not once they're wrong
    // Folders that have a linked ebook: one per folder, even if several audiobooks share it
    let mut paired: HashSet<PathBuf> = HashSet::new();
    // Audiobooks whose link record stays, since a record is keyed by the audiobook
    let mut has_record: HashSet<String> = HashSet::new();
    for link in links {
        let audiobook = torrents.get(&link.audiobook_id);
        let ebook = torrents.get(&link.ebook_id);
        // A torrent that can't be read isn't known to be removed
        let reason = if unreadable && (audiobook.is_none() || ebook.is_none()) {
            None
        } else {
            stale_reason(&link, audiobook, ebook)
        };
        let reason = reason.or_else(|| match (audiobook, ebook) {
            (Some(audiobook), Some(ebook))
                if denied(config, tags, ebook, audiobook) == Some(true) =>
            {
                Some("a torrent has a deny tag")
            }
            _ => None,
        });
        let state = reason.is_none().then(|| file_state(config, &link));
        let reason = reason.or(match state {
            Some(FileState::Missing) => Some("the linked file is gone"),
            Some(FileState::Other) => Some("another file is there now"),
            Some(FileState::Linked) if dangling(config, &link) => Some("the ebook's file is gone"),
            _ => None,
        });
        if matches!(state, Some(FileState::Linked)) {
            if let Err(err) = refresh_file_id(db, &link).await {
                log_once(
                    format!("file id {:?}", link.path),
                    true,
                    format!("Could not update linked ebook {:?}: {err}", link.path),
                );
            }
        }
        match reason {
            Some(reason) => {
                info!("Removing linked ebook {:?}: {reason}", link.path);
                let folder = link.path.parent().map(Path::to_path_buf);
                let audiobook_id = link.audiobook_id.clone();
                if !remove_link(db, config, link).await? {
                    has_record.insert(audiobook_id);
                    if let Some(folder) = folder {
                        paired.insert(folder);
                    }
                }
            }
            None => {
                if let Some(folder) = link.path.parent() {
                    paired.insert(folder.to_path_buf());
                }
                has_record.insert(link.audiobook_id);
            }
        }
    }

    if libraries.is_empty() {
        return Ok(());
    }
    let mut candidates: HashMap<&str, Vec<Candidate>> = HashMap::new();
    for ebook in torrents.values() {
        if let Some(candidate) = candidate(config, ebook) {
            candidates
                .entry(ebook.title_search.as_str())
                .or_default()
                .push(candidate);
        }
    }

    // Audiobooks by folder: a folder gets one ebook, whichever of its audiobooks it matches
    let mut folders: BTreeMap<&PathBuf, Vec<&Torrent>> = BTreeMap::new();
    for audiobook in torrents.values() {
        if audiobook.meta.media_type == MediaType::Audiobook && audiobook.replaced_with.is_none()
            && let Some(dir) = &audiobook.library_path
        {
            folders.entry(dir).or_default().push(audiobook);
        }
    }

    let mut linked_since_start = HashMap::new();
    for (dir, audiobooks) in folders {
        if paired.contains(dir) {
            continue;
        }
        let mut folder_files = None;
        let mut eligible = vec![];
        for &audiobook in &audiobooks {
            // Its record stays, for a link that couldn't be removed
            if has_record.contains(&audiobook.id) {
                continue;
            }
            let Some(matching) = candidates.get(audiobook.title_search.as_str()) else {
                continue;
            };
            for candidate in matching {
                if !pairs(candidate.ebook, audiobook)
                    || denied(config, tags, candidate.ebook, audiobook) != Some(false)
                {
                    continue;
                }
                let filters = candidate.library.tag_filters();
                if !filters.link_to_audiobooks_existing
                    && !(is_linked_since_start(db, &mut linked_since_start, audiobook)?
                        || is_linked_since_start(db, &mut linked_since_start, candidate.ebook)?)
                {
                    continue;
                }
                // An audiobook's own torrent's ebook, or one put there by hand
                let folder_files =
                    folder_files.get_or_insert_with(|| folder_files_of(dir, &audiobooks));
                if folder_files
                    .iter()
                    .any(|file| format_rank(&filters.link_to_audiobooks_types, file).is_some())
                {
                    log_skip_once(audiobook, candidate.ebook, false, "the folder has an ebook");
                    continue;
                }
                eligible.push((audiobook, *candidate));
            }
        }
        let Some((audiobook, chosen)) = choose(eligible) else {
            continue;
        };
        let (Some(ebook_dir), Some(file_name)) =
            (&chosen.ebook.library_path, chosen.file.file_name())
        else {
            continue;
        };
        let source = ebook_dir.join(chosen.file);
        let path = dir.join(file_name);
        if let Err(err) = link_file(chosen.library.method(), &source, &path) {
            let reason = match err.kind() {
                ErrorKind::AlreadyExists => format!("{path:?} already exists"),
                ErrorKind::NotFound if !windows_fs_path(&source).exists() => {
                    format!("the ebook's file {source:?} is missing")
                }
                _ => format!("linking {source:?} to {path:?} failed: {err}"),
            };
            log_skip_once(audiobook, chosen.ebook, true, reason);
            continue;
        }
        // Without it, the link couldn't be told apart from a file put there later, and could
        // never be removed
        let is_symlink =
            fs::symlink_metadata(windows_fs_path(&path)).is_ok_and(|m| m.file_type().is_symlink());
        let file_id = file_id(&path);
        if file_id.is_none() && !is_symlink {
            fs::remove_file(windows_fs_path(&path)).ok();
            log_skip_once(
                audiobook,
                chosen.ebook,
                true,
                format!("could not identify the linked file {path:?}"),
            );
            continue;
        }
        let link = EbookLink {
            audiobook_id: audiobook.id.clone(),
            ebook_id: chosen.ebook.id.clone(),
            path: path.clone(),
            file_id,
            source,
            created_at: Timestamp::now(),
        };
        let recorded = async {
            let (_guard, rw) = db.rw_async().await?;
            rw.insert(link)?;
            rw.commit()?;
            anyhow::Ok(())
        }
        .await;
        if let Err(err) = recorded {
            fs::remove_file(windows_fs_path(&path)).ok();
            log_skip_once(
                audiobook,
                chosen.ebook,
                true,
                format!("recording the link failed: {err}"),
            );
            continue;
        }
        info!(
            "Linked ebook \"{}\" {} into the folder of audiobook {}: {path:?}",
            chosen.ebook.meta.title, chosen.ebook.meta.mam_id, audiobook.meta.mam_id
        );
        paired.insert(dir.clone());
    }

    Ok(())
}

/// Removes the ebook linked into this audiobook's folder, and this ebook's links in audiobook
/// folders. Called before a torrent's library files are removed, for a relink, replacement or
/// clean. Returns false if a linked file couldn't be removed
pub async fn remove_ebook_links(
    db: &Database<'_>,
    config: &Config,
    torrent_id: &str,
    _links: &LinksGuard,
) -> Result<bool> {
    let links: Vec<EbookLink> = {
        let r = db.r_transaction()?;
        let links = r.scan().secondary::<EbookLink>(EbookLinkKey::ebook_id)?;
        let mut links = links
            .range(torrent_id.to_owned()..=torrent_id.to_owned())?
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(link) = r.get().primary::<EbookLink>(torrent_id.to_owned())? {
            links.push(link);
        }
        links
    };
    let mut removed = true;
    for link in links {
        debug!("Removing linked ebook {:?}", link.path);
        removed &= remove_link(db, config, link).await?;
    }
    Ok(removed)
}

/// Removes the ebooks linked into a folder, before a torrent is linked into it, so its own files
/// never land on a linked ebook. Returns false if a linked file couldn't be removed
pub async fn remove_ebook_links_in(
    db: &Database<'_>,
    config: &Config,
    dir: &Path,
    _links: &LinksGuard,
) -> Result<bool> {
    let links: Vec<EbookLink> = {
        let r = db.r_transaction()?;
        let links = r.scan().primary::<EbookLink>()?;
        links
            .all()?
            .filter_map(readable_link)
            .filter(|link| link.path.parent() == Some(dir))
            .collect()
    };
    let mut removed = true;
    for link in links {
        debug!("Removing linked ebook {:?} before linking into its folder", link.path);
        removed &= remove_link(db, config, link).await?;
    }
    Ok(removed)
}

/// Whether the ebooks linked into a folder can be looked at, which removing them needs. Checks
/// without removing anything, for a folder a torrent may not end up in
pub fn can_remove_ebook_links_in(
    db: &Database<'_>,
    config: &Config,
    dir: &Path,
    _links: &LinksGuard,
) -> Result<bool> {
    let r = db.r_transaction()?;
    let links = r.scan().primary::<EbookLink>()?;
    let mut removable = true;
    for link in links
        .all()?
        .filter_map(readable_link)
        .filter(|link| link.path.parent() == Some(dir))
    {
        let key = format!("check {:?}", link.path);
        if matches!(file_state(config, &link), FileState::Unknown) {
            log_once(
                key,
                true,
                format!("Could not check linked ebook {:?}", link.path),
            );
            removable = false;
        } else {
            // So it's logged again if it can't be checked later
            LOGGED.lock().unwrap().remove(&key);
        }
    }
    Ok(removable)
}

fn readable_link(link: Result<EbookLink, native_db::db_type::Error>) -> Option<EbookLink> {
    link.inspect_err(|err| {
        log_once(
            format!("read {err}"),
            true,
            format!("Could not read a linked ebook's record: {err}"),
        )
    })
    .ok()
}

/// Deletes the linked file if it's still the one MLM linked, and drops the record. Returns false
/// when the file couldn't be deleted, which keeps the record to try again on the next run
async fn remove_link(db: &Database<'_>, config: &Config, link: EbookLink) -> Result<bool> {
    match file_state(config, &link) {
        FileState::Linked => {
            if let Err(err) = fs::remove_file(windows_fs_path(&link.path))
                && err.kind() != ErrorKind::NotFound
            {
                log_once(
                    format!("remove {:?}", link.path),
                    true,
                    format!("Could not remove linked ebook {:?}: {err}", link.path),
                );
                return Ok(false);
            }
        }
        FileState::Missing => {}
        FileState::Other => {
            info!(
                "Leaving {:?} in place, it isn't the ebook MLM linked there",
                link.path
            );
        }
        FileState::Unknown => {
            log_once(
                format!("remove {:?}", link.path),
                true,
                format!("Could not check linked ebook {:?}", link.path),
            );
            return Ok(false);
        }
    }
    // So a later failure for the same file is logged again
    LOGGED
        .lock()
        .unwrap()
        .remove(&format!("remove {:?}", link.path));
    let (_guard, rw) = db.rw_async().await?;
    rw.remove(link)?;
    rw.commit()?;
    Ok(true)
}

enum FileState {
    /// Still the file MLM linked
    Linked,
    Missing,
    /// Another file has taken its place
    Other,
    /// It couldn't be looked at
    Unknown,
}

fn file_state(config: &Config, link: &EbookLink) -> FileState {
    let path = windows_fs_path(&link.path);
    match fs::symlink_metadata(&path) {
        Err(err) if err.kind() == ErrorKind::NotFound && is_mounted(config, &link.path) => {
            FileState::Missing
        }
        Err(_) => FileState::Unknown,
        Ok(meta) if meta.file_type().is_symlink() => {
            // symlink links to the absolute path
            if fs::read_link(&path).is_ok_and(|target| {
                std::path::absolute(&link.source).is_ok_and(|source| source == target)
            }) {
                FileState::Linked
            } else {
                FileState::Other
            }
        }
        Ok(_) => match file_id(&link.path) {
            Some(current) if link.file_id.as_ref() == Some(&current) => FileState::Linked,
            // A hardlink is still the ebook's own file, and a copy still has its content, even
            // if file ids changed since it was linked
            Some(current)
                if file_id(&link.source).as_ref() == Some(&current)
                    || same_content(&link.source, &link.path) =>
            {
                FileState::Linked
            }
            Some(_) => FileState::Other,
            None => FileState::Unknown,
        },
    }
}

/// A symlink in an audiobook folder to an ebook's file that's gone
fn dangling(config: &Config, link: &EbookLink) -> bool {
    fs::symlink_metadata(windows_fs_path(&link.path)).is_ok_and(|m| m.file_type().is_symlink())
        && file_gone(config, &link.source)
}

/// Whether an ebook's file in its library folder was deleted, or the download it's a symlink to,
/// rather than on a share that isn't mounted
fn file_gone(config: &Config, file: &Path) -> bool {
    match fs::symlink_metadata(windows_fs_path(file)) {
        Err(err) => err.kind() == ErrorKind::NotFound && is_mounted(config, file),
        // With the symlink method, the ebook's file is a symlink to the download
        Ok(meta) if meta.file_type().is_symlink() => {
            fs::read_link(windows_fs_path(file)).is_ok_and(|target| {
                let download = match file.parent() {
                    Some(dir) => dir.join(target),
                    None => target,
                };
                fs::metadata(windows_fs_path(&download))
                    .is_err_and(|err| err.kind() == ErrorKind::NotFound)
                    && download_gone(&download)
            })
        }
        Ok(_) => false,
    }
}

/// Whether a missing download is really gone, rather than on a share that isn't mounted: the first
/// thing above it that exists is a folder with something in it, such as the downloads folder when
/// the torrent's own folder was deleted with it. An unmounted share often shows as an empty folder,
/// or as a symlink to a folder that isn't there
fn download_gone(download: &Path) -> bool {
    for dir in download.ancestors().skip(1) {
        let dir = windows_fs_path(dir);
        if dir.is_dir() {
            return fs::read_dir(&dir).is_ok_and(|mut entries| entries.next().is_some());
        }
        if fs::symlink_metadata(&dir).is_ok() {
            return false;
        }
    }
    false
}

/// Whether a missing file is really gone, rather than on a share that isn't mounted: its folder
/// is there, the library folder holding it has files, or it's in no library folder any more
fn is_mounted(config: &Config, path: &Path) -> bool {
    if path
        .parent()
        .is_some_and(|dir| windows_fs_path(dir).is_dir())
    {
        return true;
    }
    let mut libraries = config
        .libraries
        .iter()
        .filter(|l| path.starts_with(l.library_dir()))
        .peekable();
    libraries.peek().is_none()
        || libraries.any(|l| {
            fs::read_dir(windows_fs_path(l.library_dir()))
                .is_ok_and(|mut entries| entries.next().is_some())
        })
}

/// Stores the linked file's current id, after it was recognised by its source or content, so the
/// content isn't compared again on every run
async fn refresh_file_id(db: &Database<'_>, link: &EbookLink) -> Result<()> {
    let is_symlink = fs::symlink_metadata(windows_fs_path(&link.path))
        .is_ok_and(|m| m.file_type().is_symlink());
    let current = file_id(&link.path);
    if is_symlink || current.is_none() || current == link.file_id {
        return Ok(());
    }
    let (_guard, rw) = db.rw_async().await?;
    rw.upsert(EbookLink {
        file_id: current,
        ..link.clone()
    })?;
    rw.commit()?;
    Ok(())
}

fn same_content(a: &Path, b: &Path) -> bool {
    let (Ok(a), Ok(b)) = (
        File::open(windows_fs_path(a)),
        File::open(windows_fs_path(b)),
    ) else {
        return false;
    };
    if a.metadata().map(|m| m.len()).ok() != b.metadata().map(|m| m.len()).ok() {
        return false;
    }
    let (mut a, mut b) = (io::BufReader::new(a), io::BufReader::new(b));
    loop {
        let (Ok(chunk_a), Ok(chunk_b)) = (a.fill_buf(), b.fill_buf()) else {
            return false;
        };
        let len = chunk_a.len().min(chunk_b.len());
        if len == 0 {
            return chunk_a.is_empty() && chunk_b.is_empty();
        }
        if chunk_a[..len] != chunk_b[..len] {
            return false;
        }
        a.consume(len);
        b.consume(len);
    }
}

fn file_id(path: &Path) -> Option<String> {
    get_file_id(windows_fs_path(path))
        .ok()
        .map(|id| format!("{id:?}"))
}

/// Whether an ebook goes into an audiobook's folder: the same title and language, and at least
/// one shared author. Editions only keep them apart when both have a numbered edition and the
/// numbers differ, so named editions like "Unabridged Edition" don't
pub fn pairs(ebook: &Torrent, audiobook: &Torrent) -> bool {
    let (e, a) = (&ebook.meta, &audiobook.meta);
    e.media_type == MediaType::Ebook
        && a.media_type == MediaType::Audiobook
        && ebook.title_search == audiobook.title_search
        && e.language == a.language
        && !matches!(
            (&e.edition, &a.edition),
            (Some((_, x)), Some((_, y))) if *x != 0 && *y != 0 && x != y
        )
        && e.authors.iter().any(|author| a.authors.contains(author))
}

fn same_edition(a: &TorrentMeta, b: &TorrentMeta) -> bool {
    match (&a.edition, &b.edition) {
        (None, None) => true,
        (Some(x), Some(y)) => x.1 == y.1 && (x.1 != 0 || x.0.to_lowercase() == y.0.to_lowercase()),
        _ => false,
    }
}

/// Why a recorded link no longer fits the two torrents, if it doesn't
fn stale_reason(
    link: &EbookLink,
    audiobook: Option<&Torrent>,
    ebook: Option<&Torrent>,
) -> Option<&'static str> {
    let (Some(audiobook), Some(ebook)) = (audiobook, ebook) else {
        return Some("a torrent was removed");
    };
    if audiobook.replaced_with.is_some() || ebook.replaced_with.is_some() {
        return Some("a torrent was replaced");
    }
    if audiobook.library_path.as_deref() != link.path.parent() {
        return Some("the audiobook's folder changed");
    }
    let source_linked = ebook.library_path.as_ref().is_some_and(|dir| {
        ebook
            .library_files
            .iter()
            .any(|file| dir.join(file) == link.source)
    });
    if !source_linked {
        return Some("the ebook's file changed");
    }
    if !pairs(ebook, audiobook) {
        return Some("the books no longer match");
    }
    None
}

#[derive(Clone, Copy)]
struct Candidate<'a> {
    ebook: &'a Torrent,
    library: &'a Library,
    /// The ebook's file, relative to its library_path
    file: &'a Path,
    /// Position of the file's format in link_to_audiobooks_types
    rank: usize,
}

/// The ebook's linked file in the best format its library allows into audiobook folders
fn candidate<'a>(config: &'a Config, ebook: &'a Torrent) -> Option<Candidate<'a>> {
    if ebook.meta.media_type != MediaType::Ebook || ebook.replaced_with.is_some() {
        return None;
    }
    let Some(library) = ebook_library(config, ebook) else {
        let dir = ebook.library_path.as_ref()?;
        if config
            .libraries
            .iter()
            .any(|l| dir.starts_with(l.library_dir()) && l.tag_filters().link_to_audiobooks)
        {
            log_once(
                format!("library {}", ebook.id),
                true,
                format!(
                    "Not linking ebook \"{}\" {} into audiobook folders: several libraries use \
                     its folder {dir:?}, give them names to tell them apart",
                    ebook.meta.title, ebook.meta.mam_id
                ),
            );
        }
        return None;
    };
    let filters = library.tag_filters();
    if !filters.link_to_audiobooks || library.method() == LibraryLinkMethod::NoLink {
        return None;
    }
    let dir = ebook.library_path.as_ref()?;
    ebook
        .library_files
        .iter()
        .filter_map(|file| Some((format_rank(&filters.link_to_audiobooks_types, file)?, file)))
        // A file that's gone, as when its download was deleted, mustn't keep another ebook out.
        // One on a share that isn't mounted still counts, so the folder waits for it
        .filter(|(_, file)| !file_gone(config, &dir.join(file)))
        .min()
        .map(|(rank, file)| Candidate {
            ebook,
            library,
            file,
            rank,
        })
}

/// The library an ebook was linked by. Several libraries can share a library_dir, so the
/// library's name or category picks one when it's stored, and otherwise the library_dir has to
/// belong to a single library
fn ebook_library<'a>(config: &'a Config, ebook: &Torrent) -> Option<&'a Library> {
    let dir = ebook.library_path.as_ref()?;
    let in_dir = config
        .libraries
        .iter()
        .filter(|l| dir.starts_with(l.library_dir()))
        .collect::<Vec<_>>();
    if let Some(name) = &ebook.linker
        && let Some(library) = in_dir
            .iter()
            .find(|l| l.tag_filters().name.as_ref() == Some(name))
    {
        return Some(library);
    }
    let by_category = in_dir
        .iter()
        .filter(|l| match l {
            Library::ByCategory(l) => ebook.category.as_ref() == Some(&l.category),
            Library::ByDir(_) => false,
        })
        .copied()
        .collect::<Vec<_>>();
    match (by_category.as_slice(), in_dir.as_slice()) {
        ([library], _) | ([], [library]) => Some(*library),
        _ => None,
    }
}

/// For a folder, from the candidates of each audiobook in it: the best format first, then an
/// ebook with its audiobook's edition, then the ebook MLM added last. Ids break the last ties, so
/// the choice doesn't depend on the order the candidates come in
fn choose<'a>(
    candidates: impl IntoIterator<Item = (&'a Torrent, Candidate<'a>)>,
) -> Option<(&'a Torrent, Candidate<'a>)> {
    candidates.into_iter().min_by(|(a_book, a), (b_book, b)| {
        a.rank
            .cmp(&b.rank)
            .then_with(|| {
                same_edition(&b.ebook.meta, &b_book.meta)
                    .cmp(&same_edition(&a.ebook.meta, &a_book.meta))
            })
            .then_with(|| b.ebook.created_at.cmp(&a.ebook.created_at))
            .then_with(|| a.ebook.id.cmp(&b.ebook.id))
            .then_with(|| a_book.id.cmp(&b_book.id))
    })
}

fn format_rank(types: &[String], file: &Path) -> Option<usize> {
    let name = file.file_name()?.to_string_lossy().to_lowercase();
    types.iter().position(|ext| {
        let ext = ext.trim_start_matches('.').to_lowercase();
        name.ends_with(&format!(".{ext}"))
    })
}

/// The audiobooks' own files and the files in their folder
fn folder_files_of(dir: &Path, audiobooks: &[&Torrent]) -> Vec<PathBuf> {
    let mut files = audiobooks
        .iter()
        .flat_map(|audiobook| audiobook.library_files.clone())
        .collect::<Vec<_>>();
    if let Ok(entries) = fs::read_dir(windows_fs_path(dir)) {
        files.extend(entries.flatten().map(|entry| PathBuf::from(entry.file_name())));
    }
    files
}

fn is_linked_since_start(
    db: &Database<'_>,
    cache: &mut HashMap<u64, bool>,
    torrent: &Torrent,
) -> Result<bool> {
    if let Some(linked) = cache.get(&torrent.mam_id) {
        return Ok(*linked);
    }
    let r = db.r_transaction()?;
    let events = r.scan().secondary::<Event>(EventKey::mam_id)?;
    let mut linked = false;
    for event in events.range(Some(torrent.mam_id)..=Some(torrent.mam_id))? {
        let event = event?;
        if event.created_at >= *STARTED_AT && matches!(event.event, EventType::Linked { .. }) {
            linked = true;
            break;
        }
    }
    cache.insert(torrent.mam_id, linked);
    Ok(linked)
}

fn log_skip_once(audiobook: &Torrent, ebook: &Torrent, failed: bool, reason: impl AsRef<str>) {
    log_once(
        format!("skip {} {} {}", audiobook.id, ebook.id, reason.as_ref()),
        failed,
        format!(
            "Not linking ebook \"{}\" {} into the folder of audiobook {}: {}",
            ebook.meta.title,
            ebook.meta.mam_id,
            audiobook.meta.mam_id,
            reason.as_ref()
        ),
    );
}

fn log_once(key: String, failed: bool, message: String) {
    if !LOGGED.lock().unwrap().insert(key) {
        return;
    }
    if failed {
        warn!("{message}");
    } else {
        info!("{message}");
    }
}

/// Links with the ebook library's method, and never replaces a file that's already there
fn link_file(method: LibraryLinkMethod, source: &Path, path: &Path) -> io::Result<()> {
    let path_fs = windows_fs_path(path);
    // The ebook's library file can itself be a symlink to the download: link what it points to,
    // not the symlink
    let source_fs = match method {
        LibraryLinkMethod::Symlink => windows_fs_path(source),
        _ => fs::canonicalize(windows_fs_path(source))?,
    };
    let hard_link = || fs::hard_link(&source_fs, &path_fs);
    match method {
        LibraryLinkMethod::Hardlink => hard_link(),
        LibraryLinkMethod::HardlinkOrCopy => {
            fall_back(hard_link(), || copy_new(&source_fs, &path_fs))
        }
        LibraryLinkMethod::Copy => copy_new(&source_fs, &path_fs),
        LibraryLinkMethod::HardlinkOrSymlink => {
            fall_back(hard_link(), || symlink(source, &path_fs))
        }
        LibraryLinkMethod::Symlink => {
            // A symlink to a missing file would link nothing
            fs::metadata(&source_fs)?;
            symlink(source, &path_fs)
        }
        LibraryLinkMethod::NoLink => Ok(()),
    }
}

/// Tries the fallback when the first method failed, unless the file already exists or the
/// ebook's file is missing
fn fall_back(result: io::Result<()>, fallback: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
    match result {
        Err(err) if !matches!(err.kind(), ErrorKind::AlreadyExists | ErrorKind::NotFound) => {
            fallback()
        }
        result => result,
    }
}

fn copy_new(source: &Path, path: &Path) -> io::Result<()> {
    let mut from = File::open(source)?;
    let mut to = OpenOptions::new().write(true).create_new(true).open(path)?;
    io::copy(&mut from, &mut to).map(|_| ()).inspect_err(|_| {
        fs::remove_file(path).ok();
    })
}

/// A symlink's target is read from the folder it's in, so a relative library_dir is made absolute
fn symlink(source: &Path, path: &Path) -> io::Result<()> {
    #[cfg(target_family = "unix")]
    return std::os::unix::fs::symlink(std::path::absolute(source)?, path);
    #[cfg(not(target_family = "unix"))]
    {
        let _ = (source, path);
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "symlink is not supported on Windows",
        ))
    }
}

#[cfg(test)]
mod tests {
    use mlm_db::{Language, MetadataSource, Size};

    use super::*;

    /// A folder under the system's temp dir, removed again at the end of the test
    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("mlm-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).ok();
        }
    }

    fn config(dir: &Path, ebook_library: &str) -> Config {
        toml::from_str(&format!(
            r#"
            mam_id = "test"

            [[library]]
            category = "audiobooks"
            library_dir = '{audio}'

            [[library]]
            category = "ebooks"
            library_dir = '{ebooks}'
            {ebook_library}
            "#,
            audio = dir.join("audio").display(),
            ebooks = dir.join("ebooks").display(),
        ))
        .unwrap()
    }

    fn database() -> Database<'static> {
        native_db::Builder::new()
            .create_in_memory(&mlm_db::MODELS)
            .unwrap()
    }

    fn torrent(id: &str, media_type: MediaType, edition: Option<(&str, u64)>) -> Torrent {
        let mam_id = id
            .bytes()
            .fold(0u64, |hash, b| hash.wrapping_mul(31).wrapping_add(u64::from(b)));
        Torrent {
            id: id.to_owned(),
            id_is_hash: true,
            mam_id,
            abs_id: None,
            goodreads_id: None,
            library_path: None,
            library_files: vec![],
            linker: None,
            category: Some(match media_type {
                MediaType::Audiobook => "audiobooks".to_owned(),
                _ => "ebooks".to_owned(),
            }),
            selected_audio_format: None,
            selected_ebook_format: None,
            title_search: "dune".to_owned(),
            meta: TorrentMeta {
                mam_id,
                vip_status: None,
                cat: None,
                media_type,
                main_cat: None,
                categories: vec![],
                language: Some(Language::English),
                flags: None,
                filetypes: vec![],
                num_files: 1,
                size: Size::from_bytes(0),
                title: "Dune".to_owned(),
                edition: edition.map(|(name, n)| (name.to_owned(), n)),
                authors: vec!["Frank Herbert".to_owned()],
                narrators: vec![],
                series: vec![],
                source: MetadataSource::Mam,
                uploaded_at: Timestamp::now(),
            },
            created_at: Timestamp::now(),
            replaced_with: None,
            request_matadata_update: false,
            library_mismatch: None,
            client_status: None,
        }
    }

    /// Writes the torrent's library files and records it
    fn linked(db: &Database<'_>, mut torrent: Torrent, dir: PathBuf, files: &[&str]) -> Torrent {
        fs::create_dir_all(&dir).unwrap();
        for file in files {
            fs::write(dir.join(file), format!("{} {file}", torrent.id)).unwrap();
        }
        torrent.library_path = Some(dir);
        torrent.library_files = files.iter().map(PathBuf::from).collect();
        save(db, &torrent);
        torrent
    }

    fn save(db: &Database<'_>, torrent: &Torrent) {
        let rw = db.rw_transaction().unwrap();
        rw.upsert(torrent.clone()).unwrap();
        rw.commit().unwrap();
    }

    fn links(db: &Database<'_>) -> Vec<EbookLink> {
        let r = db.r_transaction().unwrap();
        r.scan()
            .primary::<EbookLink>()
            .unwrap()
            .all()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// An audiobook and an ebook of Dune, linked into their library folders
    fn dune(db: &Database<'_>, dir: &Path, suffix: &str) -> (Torrent, Torrent, PathBuf) {
        let audio_dir = dir.join("audio/Frank Herbert/Dune {Scott Brick}");
        let audiobook = linked(
            db,
            torrent(&format!("audio-{suffix}"), MediaType::Audiobook, None),
            audio_dir.clone(),
            &["Dune.m4b"],
        );
        let ebook = linked(
            db,
            torrent(&format!("ebook-{suffix}"), MediaType::Ebook, None),
            dir.join("ebooks/Frank Herbert/Dune"),
            &["Dune.epub"],
        );
        (audiobook, ebook, audio_dir)
    }

    #[test]
    fn test_pairs() {
        let audiobook = torrent("a", MediaType::Audiobook, None);
        let ebook = torrent("e", MediaType::Ebook, None);
        assert!(pairs(&ebook, &audiobook));
        assert!(!pairs(&audiobook, &ebook));

        let mut comic = ebook.clone();
        comic.meta.media_type = MediaType::ComicBook;
        assert!(!pairs(&comic, &audiobook));

        let mut other_title = ebook.clone();
        other_title.title_search = "dune messiah".to_owned();
        assert!(!pairs(&other_title, &audiobook));

        let mut no_language = ebook.clone();
        no_language.meta.language = None;
        assert!(!pairs(&no_language, &audiobook));
        let mut no_language_audiobook = audiobook.clone();
        no_language_audiobook.meta.language = None;
        assert!(pairs(&no_language, &no_language_audiobook));

        let mut other_author = ebook.clone();
        other_author.meta.authors = vec!["Brian Herbert".to_owned()];
        assert!(!pairs(&other_author, &audiobook));
        other_author.meta.authors.push("Frank Herbert".to_owned());
        assert!(pairs(&other_author, &audiobook));
    }

    #[test]
    fn test_pairs_editions() {
        let pair = |ebook: Option<(&str, u64)>, audiobook: Option<(&str, u64)>| {
            pairs(
                &torrent("e", MediaType::Ebook, ebook),
                &torrent("a", MediaType::Audiobook, audiobook),
            )
        };
        assert!(!pair(Some(("2nd Edition", 2)), Some(("3rd Edition", 3))));
        assert!(pair(Some(("2nd Edition", 2)), Some(("2nd Edition", 2))));
        assert!(pair(Some(("40th Anniversary Edition", 40)), None));
        assert!(pair(Some(("Kindle Edition", 0)), Some(("Unabridged Edition", 0))));
        assert!(pair(Some(("Anniversary Edition", 0)), Some(("3rd Edition", 3))));
    }

    #[test]
    fn test_choose() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let library = &config.libraries[1];
        let audiobook = torrent("a", MediaType::Audiobook, None);
        let mut older = torrent("older", MediaType::Ebook, None);
        older.created_at = Timestamp::from(time::UtcDateTime::UNIX_EPOCH);
        let newer = torrent("newer", MediaType::Ebook, None);
        let anniversary = torrent(
            "anniversary",
            MediaType::Ebook,
            Some(("40th Anniversary Edition", 40)),
        );
        fn epub<'a>(ebook: &'a Torrent, library: &'a Library, rank: usize) -> Candidate<'a> {
            Candidate {
                ebook,
                library,
                file: Path::new("Dune.epub"),
                rank,
            }
        }
        let chosen = |candidates: [Candidate<'_>; 2]| {
            choose(candidates.map(|candidate| (&audiobook, candidate)))
                .unwrap()
                .1
                .ebook
                .id
                .clone()
        };

        assert_eq!(
            chosen([epub(&older, library, 0), epub(&newer, library, 0)]),
            "newer"
        );
        assert_eq!(
            chosen([epub(&anniversary, library, 0), epub(&older, library, 0)]),
            "older"
        );
        assert_eq!(
            chosen([epub(&newer, library, 1), epub(&anniversary, library, 0)]),
            "anniversary"
        );

        // Each ebook's edition is compared with its own audiobook's
        let anniversary_audiobook = torrent(
            "anniversary audiobook",
            MediaType::Audiobook,
            Some(("40th Anniversary Edition", 40)),
        );
        let (chosen_audiobook, _) = choose([
            (&audiobook, epub(&anniversary, library, 0)),
            (&anniversary_audiobook, epub(&anniversary, library, 0)),
        ])
        .unwrap();
        assert_eq!(chosen_audiobook.id, "anniversary audiobook");
    }

    #[test]
    fn test_format_rank() {
        let types = vec!["epub".to_owned(), ".pdf".to_owned()];
        assert_eq!(format_rank(&types, Path::new("Dune.epub")), Some(0));
        assert_eq!(format_rank(&types, Path::new("Dune.EPUB")), Some(0));
        assert_eq!(format_rank(&types, Path::new("sub/Dune.pdf")), Some(1));
        assert_eq!(format_rank(&types, Path::new("Dune.mobi")), None);
        assert_eq!(format_rank(&types, Path::new("epub")), None);
    }

    #[test]
    fn test_ebook_library() {
        let dir = TestDir::new();
        let ebooks = dir.0.join("ebooks").display().to_string();
        let config: Config = toml::from_str(&format!(
            r#"
            mam_id = "test"

            [[library]]
            category = "ebooks"
            library_dir = '{ebooks}'
            link_to_audiobooks = true

            [[library]]
            category = "uploads"
            library_dir = '{ebooks}'
            link_to_audiobooks_deny_tags = ["no-ebook-link"]

            [[library]]
            name = "named"
            download_dir = "/downloads"
            library_dir = '{ebooks}'
            "#
        ))
        .unwrap();
        let mut ebook = torrent("e", MediaType::Ebook, None);
        ebook.library_path = Some(dir.0.join("ebooks/Frank Herbert/Dune"));
        assert!(ebook_library(&config, &ebook).unwrap().tag_filters().link_to_audiobooks);

        ebook.category = Some("uploads".to_owned());
        assert!(!ebook_library(&config, &ebook).unwrap().tag_filters().link_to_audiobooks);

        ebook.category = None;
        assert!(ebook_library(&config, &ebook).is_none());

        // Its library isn't known: a deny tag of any library in its folder keeps it out
        let audiobook = torrent("a", MediaType::Audiobook, None);
        let tagged = HashMap::from([("e".to_owned(), vec!["no-ebook-link".to_owned()])]);
        assert_eq!(denied(&config, Some(&tagged), &ebook, &audiobook), Some(true));
        assert_eq!(denied(&config, Some(&HashMap::new()), &ebook, &audiobook), Some(false));
        assert_eq!(denied(&config, None, &ebook, &audiobook), None);

        ebook.linker = Some("named".to_owned());
        assert_eq!(
            ebook_library(&config, &ebook).unwrap().tag_filters().name.as_deref(),
            Some("named")
        );
    }

    #[test]
    fn test_link_file_never_overwrites() {
        let dir = TestDir::new();
        let source = dir.0.join("source.epub");
        let path = dir.0.join("path.epub");
        fs::write(&source, "ebook").unwrap();
        fs::write(&path, "placed by hand").unwrap();
        let mut methods = vec![
            LibraryLinkMethod::Hardlink,
            LibraryLinkMethod::HardlinkOrCopy,
            LibraryLinkMethod::HardlinkOrSymlink,
            LibraryLinkMethod::Copy,
        ];
        if cfg!(target_family = "unix") {
            methods.push(LibraryLinkMethod::Symlink);
        }
        for method in methods {
            let err = link_file(method, &source, &path).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::AlreadyExists, "{method:?}");
            assert_eq!(fs::read_to_string(&path).unwrap(), "placed by hand");
        }

        let copy = dir.0.join("copy.epub");
        link_file(LibraryLinkMethod::Copy, &source, &copy).unwrap();
        assert_eq!(fs::read_to_string(&copy).unwrap(), "ebook");
    }

    #[test]
    fn test_link_file_needs_the_source() {
        let dir = TestDir::new();
        let source = dir.0.join("missing.epub");
        for method in [
            LibraryLinkMethod::HardlinkOrSymlink,
            LibraryLinkMethod::Symlink,
        ] {
            let path = dir.0.join("path.epub");
            assert!(link_file(method, &source, &path).is_err(), "{method:?}");
            assert!(fs::symlink_metadata(&path).is_err(), "{method:?}");
        }
    }

    #[tokio::test]
    async fn test_links_ebook_into_audiobook_folder() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (audiobook, _, audio_dir) = dune(&db, &dir.0, "1");

        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(
            fs::read_to_string(audio_dir.join("Dune.epub")).unwrap(),
            "ebook-1 Dune.epub"
        );
        assert!(!audio_dir.join("metadata.json").exists());
        let recorded = links(&db);
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].audiobook_id, "audio-1");
        assert_eq!(recorded[0].ebook_id, "ebook-1");

        // A second pass changes nothing
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(links(&db), recorded);

        // Removing the audiobook's library files takes the ebook out first, so the folder goes
        let guard = lock().await;
        crate::cleaner::remove_library_files(&config, &db, &audiobook, &guard)
            .await
            .unwrap();
        drop(guard);
        assert!(!audio_dir.exists());
        assert!(links(&db).is_empty());
    }

    #[tokio::test]
    async fn test_clean_removes_links_before_qbittorrent() {
        let dir = TestDir::new();
        // Nothing listens there, so on_cleaned fails
        let config = config(
            &dir.0,
            r#"link_to_audiobooks = true

            [[qbittorrent]]
            url = "http://127.0.0.1:9"

            [qbittorrent.on_cleaned]
            tags = ["cleaned"]"#,
        );
        let db = database();
        let (_, ebook, audio_dir) = dune(&db, &dir.0, "clean");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(audio_dir.join("Dune.epub").exists());

        let err = crate::cleaner::clean_torrent(&config, &db, ebook.clone(), false)
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("ebook"), "{err:?}");
        // The linked ebook went before qBittorrent was asked; the ebook's own files stay
        assert!(!audio_dir.join("Dune.epub").exists());
        assert!(links(&db).is_empty());
        assert!(ebook.library_path.unwrap().join("Dune.epub").exists());
    }

    #[tokio::test]
    async fn test_skips_folder_with_an_ebook() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (_, _, audio_dir) = dune(&db, &dir.0, "2");
        fs::write(audio_dir.join("Dune by hand.EPUB"), "by hand").unwrap();

        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(!audio_dir.join("Dune.epub").exists());
        assert!(links(&db).is_empty());
    }

    #[tokio::test]
    async fn test_option_off() {
        let dir = TestDir::new();
        let db = database();
        let (_, mut ebook, audio_dir) = dune(&db, &dir.0, "3");

        let off = config(&dir.0, "");
        link_ebooks_to_audiobooks(&off, &db).await.unwrap();
        assert!(links(&db).is_empty());

        // Turned off again: the link stays while it fits
        link_ebooks_to_audiobooks(&config(&dir.0, "link_to_audiobooks = true"), &db)
            .await
            .unwrap();
        link_ebooks_to_audiobooks(&off, &db).await.unwrap();
        assert_eq!(links(&db).len(), 1);
        assert!(audio_dir.join("Dune.epub").exists());

        // and goes once the books no longer match, without coming back while off
        ebook.meta.authors = vec!["Someone Else".to_owned()];
        save(&db, &ebook);
        link_ebooks_to_audiobooks(&off, &db).await.unwrap();
        assert!(links(&db).is_empty());
        assert!(!audio_dir.join("Dune.epub").exists());
    }

    #[tokio::test]
    async fn test_stale_links() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (mut audiobook, mut ebook, audio_dir) = dune(&db, &dir.0, "5");
        let epub = audio_dir.join("Dune.epub");

        // The ebook relinked to another file
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        ebook.library_files = vec![PathBuf::from("Dune (2).epub")];
        fs::write(ebook.library_path.as_ref().unwrap().join("Dune (2).epub"), "new").unwrap();
        save(&db, &ebook);
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(!epub.exists());
        assert_eq!(
            fs::read_to_string(audio_dir.join("Dune (2).epub")).unwrap(),
            "new"
        );
        assert!(links(&db)[0].source.ends_with("Dune (2).epub"));

        // The audiobook moved to another folder: the epub leaves the old one and follows
        let new_dir = dir.0.join("audio/Frank Herbert/Dune");
        fs::create_dir_all(&new_dir).unwrap();
        audiobook.library_path = Some(new_dir.clone());
        save(&db, &audiobook);
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(!audio_dir.join("Dune (2).epub").exists());
        assert!(new_dir.join("Dune (2).epub").exists());

        // The ebook's record was removed
        let rw = db.rw_transaction().unwrap();
        rw.remove(ebook).unwrap();
        rw.commit().unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(!new_dir.join("Dune (2).epub").exists());
        assert!(links(&db).is_empty());
    }

    #[tokio::test]
    async fn test_leaves_a_file_mlm_didnt_link() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (_, mut ebook, audio_dir) = dune(&db, &dir.0, "6");
        let epub = audio_dir.join("Dune.epub");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();

        // Replaced by hand, then the books stop matching
        fs::remove_file(&epub).unwrap();
        fs::write(&epub, "by hand").unwrap();
        ebook.meta.authors = vec!["Someone Else".to_owned()];
        save(&db, &ebook);
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(fs::read_to_string(&epub).unwrap(), "by hand");
        assert!(links(&db).is_empty());
    }

    #[tokio::test]
    async fn test_removes_links_in_a_folder() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (_, _, audio_dir) = dune(&db, &dir.0, "7");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();

        // Before another torrent is linked into the folder
        let guard = lock().await;
        assert!(remove_ebook_links_in(&db, &config, &audio_dir, &guard).await.unwrap());
        drop(guard);
        assert!(!audio_dir.join("Dune.epub").exists());
        assert!(links(&db).is_empty());
    }

    #[tokio::test]
    async fn test_missing_files() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (audiobook, _, audio_dir) = dune(&db, &dir.0, "8");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        let link = links(&db).remove(0);

        // A share that isn't mounted: the record stays, and blocks linking into the folder
        let audio_library = dir.0.join("audio");
        let unmounted = dir.0.join("audio unmounted");
        fs::rename(&audio_library, &unmounted).unwrap();
        fs::create_dir(&audio_library).unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(links(&db), vec![link.clone()]);
        let guard = lock().await;
        assert!(!remove_ebook_links(&db, &config, &audiobook.id, &guard).await.unwrap());
        assert!(!remove_ebook_links_in(&db, &config, &audio_dir, &guard).await.unwrap());
        assert!(!can_remove_ebook_links_in(&db, &config, &audio_dir, &guard).unwrap());
        drop(guard);
        assert_eq!(links(&db), vec![link.clone()]);

        // Mounted again
        fs::remove_dir(&audio_library).unwrap();
        fs::rename(&unmounted, &audio_library).unwrap();
        let guard = lock().await;
        assert!(can_remove_ebook_links_in(&db, &config, &audio_dir, &guard).unwrap());
        drop(guard);
        assert_eq!(links(&db), vec![link.clone()]);

        // The audiobook's folder deleted, as from Audiobookshelf: removing its links works
        fs::remove_dir_all(&audio_dir).unwrap();
        let guard = lock().await;
        assert!(remove_ebook_links(&db, &config, &audiobook.id, &guard).await.unwrap());
        drop(guard);
        assert!(links(&db).is_empty());
    }

    #[tokio::test]
    async fn test_record_without_file_id() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (_, _, audio_dir) = dune(&db, &dir.0, "10");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        let mut link = links(&db).remove(0);

        // Still the ebook's own file, by its source
        link.file_id = None;
        let rw = db.rw_transaction().unwrap();
        rw.upsert(link).unwrap();
        rw.commit().unwrap();
        let guard = lock().await;
        assert!(remove_ebook_links_in(&db, &config, &audio_dir, &guard).await.unwrap());
        drop(guard);
        assert!(!audio_dir.join("Dune.epub").exists());
    }

    #[tokio::test]
    async fn test_links_again_when_the_file_is_gone() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (_, _, audio_dir) = dune(&db, &dir.0, "9");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        fs::remove_file(audio_dir.join("Dune.epub")).unwrap();

        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(audio_dir.join("Dune.epub").exists());
        assert_eq!(links(&db).len(), 1);
    }

    #[tokio::test]
    async fn test_deny_tags() {
        let dir = TestDir::new();
        let config = config(
            &dir.0,
            "link_to_audiobooks = true
link_to_audiobooks_deny_tags = [\"no-ebook-link\"]",
        );
        let db = database();
        let (audiobook, ebook, audio_dir) = dune(&db, &dir.0, "13");
        let epub = audio_dir.join("Dune.epub");
        let tagged = |id: &str| HashMap::from([(id.to_owned(), vec!["no-ebook-link".to_owned()])]);

        // Tags not known: nothing is linked
        pair(&config, &db, None).await.unwrap();
        assert!(!epub.exists());

        // A tag on the audiobook keeps the ebook out
        pair(&config, &db, Some(&tagged(&audiobook.id))).await.unwrap();
        assert!(!epub.exists());

        pair(&config, &db, Some(&HashMap::new())).await.unwrap();
        assert!(epub.exists());

        // Tags not known: the link stays
        pair(&config, &db, None).await.unwrap();
        assert!(epub.exists());

        // A tag on the ebook takes it out again
        pair(&config, &db, Some(&tagged(&ebook.id))).await.unwrap();
        assert!(!epub.exists());
        assert!(links(&db).is_empty());
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn test_symlink_from_a_relative_library_dir() {
        let dir = TestDir::new();
        // Relative to the working directory, as a relative library_dir is
        let relative = TestDir(PathBuf::from(format!("mlm-test-{}", uuid::Uuid::new_v4())));
        fs::create_dir(&relative.0).unwrap();
        let source = relative.0.join("Dune.epub");
        fs::write(&source, "ebook").unwrap();
        let path = dir.0.join("Dune.epub");

        let linked = link_file(LibraryLinkMethod::Symlink, &source, &path);
        let target = fs::read_link(&path);
        let content = fs::read_to_string(&path);
        let config = config(&dir.0, "");
        let state = file_state(
            &config,
            &EbookLink {
                audiobook_id: "a".to_owned(),
                ebook_id: "e".to_owned(),
                path: path.clone(),
                source: source.clone(),
                file_id: None,
                created_at: Timestamp::now(),
            },
        );
        drop(relative);

        linked.unwrap();
        assert!(target.unwrap().is_absolute());
        assert_eq!(content.unwrap(), "ebook");
        assert!(matches!(state, FileState::Linked));
    }

    #[cfg(target_family = "unix")]
    #[tokio::test]
    async fn test_symlink_to_a_deleted_ebook() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true
method = \"symlink\"");
        let db = database();
        let (_, ebook, audio_dir) = dune(&db, &dir.0, "dangling");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        let linked = audio_dir.join("Dune.epub");
        assert!(fs::symlink_metadata(&linked).unwrap().file_type().is_symlink());

        // The ebook's own file deleted by hand: the symlink to it goes
        fs::remove_file(ebook.library_path.unwrap().join("Dune.epub")).unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(fs::symlink_metadata(&linked).is_err());
        assert!(links(&db).is_empty());
    }

    #[cfg(target_family = "unix")]
    #[tokio::test]
    async fn test_symlink_to_a_deleted_download() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true\nmethod = \"symlink\"");
        let db = database();
        let (_, ebook, audio_dir) = dune(&db, &dir.0, "download");
        // The ebook library linked its file as a symlink to the download, in the torrent's folder
        let downloads = dir.0.join("downloads");
        let torrent_dir = downloads.join("Dune [epub]");
        fs::create_dir_all(&torrent_dir).unwrap();
        fs::write(downloads.join("other.epub"), "other").unwrap();
        let download = torrent_dir.join("Dune.epub");
        fs::write(&download, "download").unwrap();
        let library_file = ebook.library_path.as_ref().unwrap().join("Dune.epub");
        fs::remove_file(&library_file).unwrap();
        std::os::unix::fs::symlink(&download, &library_file).unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        let linked = audio_dir.join("Dune.epub");
        assert_eq!(fs::read_to_string(&linked).unwrap(), "download");

        // The downloads share isn't mounted, an empty folder in its place: the link stays
        let unmounted = dir.0.join("downloads unmounted");
        fs::rename(&downloads, &unmounted).unwrap();
        fs::create_dir(&downloads).unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(fs::symlink_metadata(&linked).is_ok());
        assert_eq!(links(&db).len(), 1);
        // and the ebook still counts as a candidate, so no other one takes a free folder meanwhile
        assert!(!file_gone(&config, &library_file));

        // The downloads folder a symlink to a share that isn't mounted: the link stays
        fs::remove_dir(&downloads).unwrap();
        std::os::unix::fs::symlink(dir.0.join("not mounted"), &downloads).unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(fs::symlink_metadata(&linked).is_ok());
        assert_eq!(links(&db).len(), 1);
        assert!(!file_gone(&config, &library_file));

        // An older ebook of the book, from another torrent
        let mut older = self::linked(
            &db,
            torrent("ebook-older", MediaType::Ebook, None),
            dir.0.join("ebooks/Frank Herbert/Dune (older)"),
            &["Dune.epub"],
        );
        older.created_at = Timestamp::from(time::UtcDateTime::UNIX_EPOCH);
        save(&db, &older);

        // The torrent deleted with its folder: the link goes, and the older ebook takes its place
        fs::remove_file(&downloads).unwrap();
        fs::rename(&unmounted, &downloads).unwrap();
        fs::remove_dir_all(&torrent_dir).unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(fs::read_to_string(&linked).unwrap(), "ebook-older Dune.epub");
        assert_eq!(links(&db).len(), 1);
        assert_eq!(links(&db)[0].ebook_id, "ebook-older");
    }

    #[cfg(target_family = "unix")]
    #[tokio::test]
    async fn test_symlinked_ebook_file() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let db = database();
        let (_, ebook, audio_dir) = dune(&db, &dir.0, "11");
        // The ebook library linked its file as a symlink to the download
        let download = dir.0.join("download.epub");
        fs::write(&download, "download").unwrap();
        let library_file = ebook.library_path.as_ref().unwrap().join("Dune.epub");
        fs::remove_file(&library_file).unwrap();
        std::os::unix::fs::symlink(&download, &library_file).unwrap();

        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        let linked = audio_dir.join("Dune.epub");
        assert!(!fs::symlink_metadata(&linked).unwrap().file_type().is_symlink());
        assert_eq!(fs::read_to_string(&linked).unwrap(), "download");

        // and it's still recognised as MLM's link
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(links(&db).len(), 1);
        assert!(matches!(file_state(&config, &links(&db)[0]), FileState::Linked));
    }

    #[tokio::test]
    async fn test_copy_recognised_by_content() {
        let dir = TestDir::new();
        let config = config(
            &dir.0,
            "link_to_audiobooks = true
method = \"copy\"",
        );
        let db = database();
        let (_, _, _) = dune(&db, &dir.0, "12");
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        let mut link = links(&db).remove(0);
        let real_id = link.file_id.clone();

        // File ids changed, as after a remount
        link.file_id = Some("old id".to_owned());
        let rw = db.rw_transaction().unwrap();
        rw.upsert(link.clone()).unwrap();
        rw.commit().unwrap();
        assert!(matches!(file_state(&config, &link), FileState::Linked));
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(links(&db)[0].file_id, real_id);
    }

    #[test]
    fn test_missing_outside_libraries() {
        let dir = TestDir::new();
        let config = config(&dir.0, "link_to_audiobooks = true");
        let link = EbookLink {
            audiobook_id: "a".to_owned(),
            ebook_id: "e".to_owned(),
            path: dir.0.join("moved/Frank Herbert/Dune/Dune.epub"),
            source: dir.0.join("ebooks/Dune.epub"),
            file_id: None,
            created_at: Timestamp::now(),
        };
        assert!(matches!(file_state(&config, &link), FileState::Missing));
    }

    #[tokio::test]
    async fn test_one_ebook_per_shared_folder() {
        let dir = TestDir::new();
        let config = config(
            &dir.0,
            &format!(
                "link_to_audiobooks = true\n\n[[library]]\ncategory = \"pdfs\"\nlibrary_dir = '{}'\n\
                 link_to_audiobooks = true\nlink_to_audiobooks_types = [\"pdf\"]",
                dir.0.join("pdfs").display()
            ),
        );
        let db = database();
        // Two audiobooks in one folder, as with different languages
        let audio_dir = dir.0.join("audio/Frank Herbert/Dune");
        linked(
            &db,
            torrent("audio-14", MediaType::Audiobook, None),
            audio_dir.clone(),
            &["Dune.m4b"],
        );
        let mut german = torrent("audio-15", MediaType::Audiobook, None);
        german.meta.language = Some(Language::German);
        linked(&db, german, audio_dir.clone(), &["Dune.mp3"]);
        let mut epub = torrent("ebook-14", MediaType::Ebook, None);
        epub.created_at = Timestamp::from(time::UtcDateTime::UNIX_EPOCH);
        linked(&db, epub, dir.0.join("ebooks/Dune"), &["Dune.epub"]);
        let mut pdf = torrent("ebook-15", MediaType::Ebook, None);
        pdf.meta.language = Some(Language::German);
        pdf.category = Some("pdfs".to_owned());
        linked(&db, pdf, dir.0.join("pdfs/Dune"), &["Dune.pdf"]);

        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(links(&db).len(), 1);
        // Both rank first in their own library's list and have their audiobook's edition: the
        // ebook added last wins, whatever order the audiobooks come in
        assert_eq!(links(&db)[0].ebook_id, "ebook-15");
        assert_eq!(links(&db)[0].audiobook_id, "audio-15");
        let types = ["epub".to_owned(), "pdf".to_owned()];
        let ebooks = fs::read_dir(&audio_dir)
            .unwrap()
            .flatten()
            .filter(|entry| format_rank(&types, Path::new(&entry.file_name())).is_some())
            .count();
        assert_eq!(ebooks, 1);
    }

    #[test]
    fn test_same_content() {
        let dir = TestDir::new();
        let (a, b, c) = (dir.0.join("a"), dir.0.join("b"), dir.0.join("c"));
        fs::write(&a, "ebook").unwrap();
        fs::write(&b, "ebook").unwrap();
        fs::write(&c, "other").unwrap();
        assert!(same_content(&a, &b));
        assert!(!same_content(&a, &c));
        assert!(!same_content(&a, &dir.0.join("missing")));
    }

    #[tokio::test]
    async fn test_not_existing() {
        let dir = TestDir::new();
        let config = config(
            &dir.0,
            "link_to_audiobooks = true\nlink_to_audiobooks_existing = false",
        );
        let db = database();
        let (_, ebook, _) = dune(&db, &dir.0, "4");
        let linked_event = |at: Timestamp| {
            let mut event = Event::new(
                Some(ebook.id.clone()),
                Some(ebook.mam_id),
                EventType::Linked {
                    linker: None,
                    library_path: ebook.library_path.clone().unwrap(),
                },
            );
            event.created_at = at;
            let rw = db.rw_transaction().unwrap();
            rw.insert(event).unwrap();
            rw.commit().unwrap();
        };

        note_start();
        // Linked before MLM started
        linked_event(Timestamp::from(time::UtcDateTime::UNIX_EPOCH));
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert!(links(&db).is_empty());

        // Linked after MLM started
        linked_event(Timestamp::now());
        link_ebooks_to_audiobooks(&config, &db).await.unwrap();
        assert_eq!(links(&db).len(), 1);
    }
}
