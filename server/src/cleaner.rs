use std::{fs, io::ErrorKind, ops::Deref, sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail};
use mlm_db::{
    self, DatabaseExt as _, ErroredTorrentId, Event, EventType, Timestamp, Torrent, TorrentKey,
};
use native_db::Database;
use tracing::{debug, info, instrument, trace, warn};

use crate::{
    audiobookshelf::Abs,
    config::Config,
    ebook_links::{self, LinksGuard, remove_ebook_links},
    linker::file_size,
    logging::{TorrentMetaError, update_errored_torrent, write_event},
    qbittorrent::ensure_category_exists,
};

#[instrument(skip_all)]
pub async fn run_library_cleaner(config: Arc<Config>, db: Arc<Database<'_>>) -> Result<()> {
    let torrents: Vec<Torrent> = {
        let r = db.r_transaction()?;
        let torrents = r.scan().secondary::<Torrent>(TorrentKey::title_search)?;
        torrents
            .all()?
            .filter_map(|t| t.ok())
            .filter(|t| t.library_path.is_some())
            .collect()
    };
    for batch in batches(torrents) {
        process_batch(&config, &db, batch).await?;
    }

    Ok(())
}

/// Groups copies of the same book. The torrents come sorted by title_search, and each one joins
/// the first group of its title in which it matches every torrent, so copies of a book are grouped
/// even when other editions, media types, languages or narrators of the same title sort between
/// them, and the copy that's kept matches every copy that's removed
fn batches(torrents: Vec<Torrent>) -> Vec<Vec<Torrent>> {
    let mut batches: Vec<Vec<Torrent>> = vec![];
    let mut title_start = 0;
    for torrent in torrents {
        if batches
            .get(title_start)
            .is_some_and(|batch| batch[0].title_search != torrent.title_search)
        {
            title_start = batches.len();
        }
        match batches[title_start..]
            .iter_mut()
            .find(|batch| batch.iter().all(|t| t.matches(&torrent)))
        {
            Some(batch) => batch.push(torrent),
            None => batches.push(vec![torrent]),
        }
    }
    batches
}

#[instrument(skip_all)]
async fn process_batch(config: &Config, db: &Database<'_>, batch: Vec<Torrent>) -> Result<()> {
    if batch.len() < 2 {
        return Ok(());
    };
    let mut batch = batch
        .into_iter()
        .map(|torrent| {
            let preferred_types = config.preferred_types(&torrent.meta.media_type);
            let preference = preferred_types
                .iter()
                .position(|t| torrent.meta.filetypes.contains(t))
                .unwrap_or(usize::MAX);
            (torrent, preference)
        })
        .collect::<Vec<_>>();
    batch.sort_by_key(|(_, preference)| *preference);
    if batch[0].1 == batch[1].1 {
        trace!(
            "need to compare torrent \"{}\" and \"{}\" by size",
            batch[0].0.meta.title, batch[1].0.meta.title
        );
        let mut new_batch = batch
            .into_iter()
            .map(|(torrent, preference)| {
                let mut size = 0;
                if let Some(library_path) = &torrent.library_path {
                    for file in &torrent.library_files {
                        let path = library_path.join(file);
                        size += fs::metadata(path).map_or(0, |s| file_size(&s));
                    }
                }
                (torrent, preference, size)
            })
            .collect::<Vec<_>>();
        new_batch.sort_by(|a, b| a.1.cmp(&b.1).then(b.2.cmp(&a.2)));
        trace!("new_batch {:?}", new_batch);
        batch = new_batch
            .into_iter()
            .map(|(torrent, preference, _)| (torrent, preference))
            .collect();
    }
    let (keep, _) = batch.remove(0);
    for (mut remove, _) in batch {
        info!(
            "Replacing library torrent \"{}\" {} with {}",
            remove.meta.title, remove.meta.mam_id, keep.meta.mam_id
        );
        remove.replaced_with = Some((keep.id.clone(), Timestamp::now()));
        let result = clean_torrent(
            config,
            db,
            remove.clone(),
            keep.library_path.is_some() && keep.library_path != remove.library_path,
        )
        .await
        .map_err(|err| anyhow::Error::new(TorrentMetaError(remove.meta.clone(), err)));
        update_errored_torrent(
            db,
            ErroredTorrentId::Cleaner(remove.id),
            remove.meta.title,
            result,
        )
        .await
    }

    Ok(())
}

#[instrument(skip_all)]
pub async fn clean_torrent(
    config: &Config,
    db: &Database<'_>,
    mut remove: Torrent,
    delete_in_abs: bool,
) -> Result<()> {
    // Held until the record says the files are gone, so ebooks aren't linked back in meanwhile
    let links = ebook_links::lock().await;
    // Before changing anything in qBittorrent, so a linked ebook that can't be removed stops the
    // clean first
    if !remove_ebook_links(db, config, &remove.id, &links).await? {
        bail!("Could not remove an ebook linked into an audiobook folder, see the log");
    }
    if remove.id_is_hash {
        // The ebook-links lock is held, so a qBittorrent that doesn't answer mustn't hold up the
        // others for long
        tokio::time::timeout(Duration::from_secs(30), apply_on_cleaned(config, &remove.id))
            .await
            .unwrap_or_else(|_| Err(anyhow!("timed out updating qBittorrent")))?;
    }

    remove_library_files(config, db, &remove, &links).await?;
    let abs_book = remove.clone();

    let id = remove.id.clone();
    let mam_id = remove.meta.mam_id;
    let library_path = remove.library_path.take();
    let mut library_files = remove.library_files.clone();
    remove.library_mismatch = None;
    remove.abs_id = None;
    library_files.sort();
    {
        let (_guard, rw) = db.rw_async().await?;
        rw.upsert(remove)?;
        rw.commit()?;
    }
    drop(links);
    // After the files are gone, so a clean that fails leaves the book in Audiobookshelf
    if delete_in_abs {
        delete_from_abs(config, &abs_book).await;
    }

    if let Some(library_path) = library_path {
        write_event(
            db,
            Event::new(
                Some(id),
                Some(mam_id),
                EventType::Cleaned {
                    library_path,
                    files: library_files,
                },
            ),
        )
        .await;
    }

    Ok(())
}

/// Sets each qBittorrent instance's on_cleaned category and tags on the torrent
async fn apply_on_cleaned(config: &Config, id: &str) -> Result<()> {
    for qbit_conf in config.qbittorrent.iter() {
        if let Some(on_cleaned) = &qbit_conf.on_cleaned {
            let qbit = qbit::Api::new_login_username_password(
                &qbit_conf.url,
                &qbit_conf.username,
                &qbit_conf.password,
            )
            .await?;

            if let Some(category) = &on_cleaned.category {
                ensure_category_exists(&qbit, &qbit_conf.url, category).await?;
                qbit.set_category(Some(vec![id]), category).await?;
            }

            if !on_cleaned.tags.is_empty() {
                qbit.add_tags(
                    Some(vec![id]),
                    on_cleaned.tags.iter().map(Deref::deref).collect(),
                )
                .await?;
            }
        }
        trace!("qbit updated");
    }
    Ok(())
}

pub async fn delete_from_abs(config: &Config, remove: &Torrent) {
    if let (Some(abs_id), Some(abs_config)) = (&remove.abs_id, &config.audiobookshelf) {
        // A relink holds the ebook-links lock during this, so it mustn't wait for long
        let result = match Abs::new(abs_config) {
            Ok(abs) => tokio::time::timeout(Duration::from_secs(30), abs.delete_book(abs_id))
                .await
                .unwrap_or_else(|_| Err(anyhow!("timed out"))),
            Err(err) => Err(err),
        };
        if let Err(err) = result {
            warn!("Failed deleting book from abs: {err}");
        }
    }
}

#[instrument(skip_all)]
pub async fn remove_library_files(
    config: &Config,
    db: &Database<'_>,
    remove: &Torrent,
    links: &LinksGuard,
) -> Result<()> {
    // First, so an audiobook's folder can be removed once its own files are gone
    if !remove_ebook_links(db, config, &remove.id, links).await? {
        bail!("Could not remove an ebook linked into an audiobook folder, see the log");
    }

    if let Some(library_path) = &remove.library_path {
        debug!("Removing library files for torrent {}", remove.meta.mam_id);
        for file in remove.library_files.iter() {
            let path = library_path.join(file);
            fs::remove_file(path).or_else(|err| {
                if err.kind() == ErrorKind::NotFound {
                    trace!("file already missing");
                    Ok(())
                } else {
                    Err(err)
                }
            })?;
            if let Some(sub_dir) = file.parent() {
                fs::remove_dir(sub_dir).ok();
            }
        }
        let mut remove_files = true;
        let mut files_to_remove = vec![];
        if let Ok(files) = fs::read_dir(library_path) {
            for file in files {
                if let Ok(file) = file {
                    if file.file_name() == "cover.jpg" || file.file_name() == "metadata.json" {
                        files_to_remove.push(file);
                    } else {
                        remove_files = false;
                    }
                } else {
                    remove_files = false;
                }
            }
            if remove_files {
                for file in files_to_remove {
                    fs::remove_file(file.path()).ok();
                }
            }
        }
        fs::remove_dir(library_path).ok();
        trace!("files removed");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use mlm_db::{MediaType, MetadataSource, Size, TorrentMeta};

    use super::*;

    fn torrent(id: &str, title_search: &str, edition: Option<u64>) -> Torrent {
        Torrent {
            id: id.to_owned(),
            id_is_hash: false,
            mam_id: 0,
            abs_id: None,
            goodreads_id: None,
            library_path: None,
            library_files: vec![],
            linker: None,
            category: None,
            selected_audio_format: None,
            selected_ebook_format: None,
            title_search: title_search.to_owned(),
            meta: TorrentMeta {
                mam_id: 0,
                vip_status: None,
                cat: None,
                media_type: MediaType::Audiobook,
                main_cat: None,
                categories: vec![],
                language: None,
                flags: None,
                filetypes: vec![],
                num_files: 0,
                size: Size::from_bytes(0),
                title: title_search.to_owned(),
                edition: edition.map(|n| (format!("Edition {n}"), n)),
                authors: vec!["Author".to_owned()],
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

    fn ids(batches: &[Vec<Torrent>]) -> Vec<Vec<&str>> {
        batches
            .iter()
            .map(|batch| batch.iter().map(|t| t.id.as_str()).collect())
            .collect()
    }

    #[test]
    fn test_batches_keep_editions_apart() {
        let batches = batches(vec![
            torrent("a", "book", Some(2)),
            torrent("b", "book", Some(3)),
        ]);
        assert_eq!(ids(&batches), vec![vec!["a"], vec!["b"]]);
    }

    #[test]
    fn test_batches_group_interleaved_copies() {
        let batches = batches(vec![
            torrent("a", "book", Some(2)),
            torrent("b", "book", Some(3)),
            torrent("c", "book", Some(2)),
            torrent("d", "book", Some(3)),
        ]);
        assert_eq!(ids(&batches), vec![vec!["a", "c"], vec!["b", "d"]]);
    }

    #[test]
    fn test_batches_group_copies_around_other_media_types() {
        let mut ebook = torrent("b", "book", None);
        ebook.meta.media_type = MediaType::Ebook;
        let batches = batches(vec![
            torrent("a", "book", None),
            ebook,
            torrent("c", "book", None),
        ]);
        assert_eq!(ids(&batches), vec![vec!["a", "c"], vec!["b"]]);
    }

    #[test]
    fn test_batches_need_every_copy_to_match() {
        let mut a = torrent("a", "book", None);
        a.meta.narrators = vec!["N1".to_owned(), "N2".to_owned()];
        let mut b = torrent("b", "book", None);
        b.meta.narrators = vec!["N1".to_owned()];
        let mut c = torrent("c", "book", None);
        c.meta.narrators = vec!["N2".to_owned()];
        let batches = batches(vec![a, b, c]);
        assert_eq!(ids(&batches), vec![vec!["a", "b"], vec!["c"]]);
    }

    #[test]
    fn test_batches_split_titles() {
        let batches = batches(vec![
            torrent("a", "book", None),
            torrent("b", "book", None),
            torrent("c", "other", None),
        ]);
        assert_eq!(ids(&batches), vec![vec!["a", "b"], vec!["c"]]);
    }
}
