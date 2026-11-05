use std::{
    fs::File,
    io::{BufWriter, Write as _},
    ops::RangeInclusive,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Error, Result};
use itertools::Itertools as _;
use lava_torrent::torrent::v1::Torrent;
use mlm_db::{
    ClientStatus, DatabaseExt as _, DuplicateTorrent, Event, EventType, MetadataSource,
    SelectedTorrent, Timestamp, TorrentCost, TorrentKey, TorrentMeta, VipStatus,
};
use mlm_mam::{
    api::MaM,
    enums::{SearchKind, SearchTarget},
    meta::MetaError,
    search::{MaMTorrent, SearchFields, SearchQuery, SearchResult, Tor},
    serde::DATE_FORMAT,
};
use mlm_parse::normalize_title;
use native_db::{Database, db_type, transaction::RwTransaction};
use tokio::{
    fs,
    sync::{MutexGuard, watch::Sender},
    time::sleep,
};
use tracing::{Level, debug, enabled, error, info, instrument, trace, warn};
use uuid::Uuid;

use crate::{
    audiobookshelf::{self as abs, Abs},
    config::{Config, Cost, SortBy, TorrentFilter, TorrentSearch, Type},
    logging::write_event,
};

static AUTOGRABBER_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[instrument(skip_all)]
pub async fn run_autograbber(
    config: Arc<Config>,
    db: Arc<Database<'_>>,
    mam: Arc<MaM<'_>>,
    autograb_trigger: Sender<()>,
    index: usize,
    autograb_config: Arc<TorrentSearch>,
) -> Result<()> {
    // Make sure we are only running one autograbber at a time
    let _guard = AUTOGRABBER_MUTEX.lock().await;

    let user_info = mam.user_info().await?;
    let max_torrents = user_info
        .snatch_summary
        .unsat
        .limit
        .saturating_sub(user_info.snatch_summary.unsat.count);
    let name = autograb_config
        .filter
        .name
        .clone()
        .unwrap_or_else(|| index.to_string());
    debug!(
        "autograbber {}, unsats: {:#?}; max_torrents: {max_torrents}",
        name, user_info.snatch_summary.unsat
    );

    let unsat_buffer = autograb_config.unsat_buffer.unwrap_or(config.unsat_buffer);
    let mut max_torrents = max_torrents.saturating_sub(unsat_buffer);

    if max_torrents > 0
        && let Some(max_active_downloads) = autograb_config.max_active_downloads
    {
        let r = db.r_transaction()?;
        let downloading_torrents = r
            .scan()
            .primary::<SelectedTorrent>()?
            .all()?
            .filter(|t| {
                t.as_ref()
                    .is_ok_and(|t| t.grabber.as_ref() == Some(&name) && t.removed_at.is_none())
            })
            .count() as u64;
        max_torrents = max_torrents.min(max_active_downloads.saturating_sub(downloading_torrents));
    }

    if max_torrents > 0
        || autograb_config.cost == Cost::MetadataOnly
        || autograb_config.cost == Cost::MetadataOnlyAdd
    {
        search_and_select_torrents(
            &config,
            &db,
            &autograb_config,
            SearchFields {
                dl_link: true,
                ..Default::default()
            },
            &mam,
            max_torrents,
        )
        .await
        .context("search_torrents")?;
    }

    if !config.qbittorrent.is_empty() {
        autograb_trigger.send(())?;
    }

    Ok(())
}

#[instrument(skip_all)]
pub async fn search_and_select_torrents(
    config: &Config,
    db: &Database<'_>,
    torrent_search: &TorrentSearch,
    fields: SearchFields,
    mam: &MaM<'_>,
    max_torrents: u64,
) -> Result<Selection> {
    let torrents = search_torrents(torrent_search, fields, mam)
        .await
        .context("search_torrents")?;

    if torrent_search.mark_removed {
        let torrents = torrents.collect::<Vec<_>>();
        mark_removed_torrents(db, mam, &torrents)
            .await
            .context("mark_removed_torrents")?;

        return select_torrents(
            config,
            db,
            mam,
            torrents.into_iter(),
            &torrent_search.filter,
            torrent_search.cost,
            torrent_search.unsat_buffer,
            torrent_search.wedge_buffer,
            torrent_search.category.clone(),
            torrent_search.dry_run,
            max_torrents,
            None,
        )
        .await
        .context("select_torrents");
    }

    select_torrents(
        config,
        db,
        mam,
        torrents,
        &torrent_search.filter,
        torrent_search.cost,
        torrent_search.unsat_buffer,
        torrent_search.wedge_buffer,
        torrent_search.category.clone(),
        torrent_search.dry_run,
        max_torrents,
        None,
    )
    .await
    .context("select_torrents")
}

#[instrument(skip_all)]
pub async fn search_torrents(
    torrent_search: &TorrentSearch,
    fields: SearchFields,
    mam: &MaM<'_>,
) -> Result<impl Iterator<Item = MaMTorrent>> {
    let target = match torrent_search.kind {
        Type::Bookmarks => Some(SearchTarget::Bookmarks),
        Type::Mine => Some(SearchTarget::Mine),
        Type::Uploader(id) => Some(SearchTarget::Uploader(id)),
        _ => None,
    };
    let kind = match (torrent_search.kind, torrent_search.cost) {
        (Type::Freeleech, _) => Some(SearchKind::Freeleech),
        (_, Cost::Free) => Some(SearchKind::Free),
        _ => None,
    };
    let sort_type = torrent_search
        .sort_by
        .map(|sort_by| match sort_by {
            SortBy::LowSeeders => "seedersAsc",
            SortBy::LowSnatches => "snatchedAsc",
            SortBy::OldestFirst => "dateAsc",
            SortBy::Random => "random",
        })
        .unwrap_or(match torrent_search.kind {
            Type::New => "dateDesc",
            _ => "",
        });
    let (flags_is_hide, flags) = torrent_search.filter.flags.as_search_bitfield();
    let max_pages = torrent_search
        .max_pages
        .unwrap_or(match torrent_search.kind {
            Type::Bookmarks | Type::Freeleech | Type::Mine => 50,
            _ => 0,
        });

    let mut results: Option<SearchResult> = None;
    for page in 1.. {
        let mut page_results = mam
            .search(&SearchQuery {
                fields,
                perpage: 100,
                tor: Tor {
                    start_number: results.as_ref().map_or(0, |r| r.data.len() as u64),
                    target,
                    kind,
                    text: torrent_search.query.clone().unwrap_or_default(),
                    srch_in: torrent_search.search_in.clone(),
                    main_cat: torrent_search.filter.categories.get_main_cats(),
                    cat: torrent_search.filter.categories.get_cats(),
                    browse_lang: torrent_search
                        .filter
                        .languages
                        .iter()
                        .map(|l| l.to_id())
                        .collect(),
                    browse_flags_hide_vs_show: if flags.is_empty() {
                        None
                    } else {
                        Some(if flags_is_hide { 0 } else { 1 })
                    },
                    browse_flags: flags.clone(),
                    start_date: torrent_search
                        .filter
                        .uploaded_after
                        .map_or_else(|| Ok(String::new()), |d| d.format(&DATE_FORMAT))?,
                    end_date: torrent_search
                        .filter
                        .uploaded_before
                        .map_or_else(|| Ok(String::new()), |d| d.format(&DATE_FORMAT))?,
                    min_size: torrent_search.filter.min_size.bytes(),
                    max_size: torrent_search.filter.max_size.bytes(),
                    unit: torrent_search
                        .filter
                        .min_size
                        .unit()
                        .max(torrent_search.filter.max_size.unit()),
                    min_seeders: torrent_search.filter.min_seeders,
                    max_seeders: torrent_search.filter.max_seeders,
                    min_leechers: torrent_search.filter.min_leechers,
                    max_leechers: torrent_search.filter.max_leechers,
                    min_snatched: torrent_search.filter.min_snatched,
                    max_snatched: torrent_search.filter.max_snatched,
                    sort_type: sort_type.to_string(),
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

        if enabled!(Level::TRACE) {
            trace!(
                "torrents in result: {:?}",
                page_results.data.iter().map(|t| t.id).collect::<Vec<_>>()
            )
        }
        if let Some(results) = &mut results {
            results.data.append(&mut page_results.data);
        } else {
            results = Some(page_results);
        }

        let results = results.as_ref().unwrap();
        if page >= max_pages || results.data.len() >= results.found {
            break;
        }
        sleep(Duration::from_millis(400)).await;
    }

    let torrents = results
        .unwrap()
        .data
        .into_iter()
        .filter(|t| torrent_search.filter.matches(t));

    Ok(torrents)
}

#[instrument(skip_all)]
pub async fn mark_removed_torrents(
    db: &Database<'_>,
    mam: &MaM<'_>,
    torrents: &[MaMTorrent],
) -> Result<()> {
    if let (Some(first), Some(last)) = (torrents.first(), torrents.last()) {
        let ids = first.id.min(last.id)..=first.id.max(last.id);
        for id in ids {
            let is_removed = torrents.iter().all(|t| t.id != id);
            if is_removed {
                let (guard, rw) = db.rw_async().await?;
                let torrent = rw
                    .get()
                    .secondary::<mlm_db::Torrent>(TorrentKey::mam_id, id)?;
                if let Some(mut torrent) = torrent
                    && torrent.client_status != Some(ClientStatus::RemovedFromMam)
                {
                    if mam.get_torrent_info_by_id(id).await?.is_none() {
                        torrent.client_status = Some(ClientStatus::RemovedFromMam);
                        let tid = Some(torrent.id.clone());
                        rw.upsert(torrent)?;
                        rw.commit()?;
                        drop(guard);
                        write_event(db, Event::new(tid, Some(id), EventType::RemovedFromMam)).await;
                    }
                    sleep(Duration::from_millis(400)).await;
                }
            }
        }
    }
    Ok(())
}

/// What `select_torrents` did with the torrents it was given
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Selection {
    /// Torrents selected for download
    pub selected: u64,
    /// Torrents skipped because the account already snatched them
    pub snatched: u64,
}

#[instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
pub async fn select_torrents<T: Iterator<Item = MaMTorrent>>(
    config: &Config,
    db: &Database<'_>,
    mam: &MaM<'_>,
    torrents: T,
    grabber: &TorrentFilter,
    cost: Cost,
    unsat_buffer: Option<u64>,
    wedge_buffer: Option<u64>,
    filter_category: Option<String>,
    dry_run: bool,
    max_torrents: u64,
    goodreads_id: Option<u64>,
) -> Result<Selection> {
    let mut selected_torrents = 0;
    let mut snatched_torrents = 0;
    'torrent: for torrent in torrents {
        if config.ignore_torrents.contains(&torrent.id) {
            trace!("Torrent {} is ignored", torrent.id);
            continue;
        }

        let meta = match torrent.as_meta() {
            Ok(it) => it,
            Err(err) => match err {
                MetaError::UnknownMediaType(_) => {
                    warn!("{err} for torrent {} {}", torrent.id, torrent.title);
                    continue;
                }
                _ => return Err(err.into()),
            },
        };
        let rw_opt = if dry_run {
            None
        } else {
            Some(db.rw_async().await?)
        };
        if let Some((_, rw)) = &rw_opt
            && let Some(old_selected) = rw
                .get()
                .primary::<mlm_db::SelectedTorrent>(torrent.id)
                .ok()
                .flatten()
        {
            if let Some(unsat_buffer) = unsat_buffer
                && old_selected.unsat_buffer.is_none_or(|u| unsat_buffer < u)
            {
                let mut updated = old_selected.clone();
                updated.unsat_buffer = Some(unsat_buffer);
                if updated.meta != meta {
                    update_selected_torrent_meta(db, rw_opt.unwrap(), mam, updated, meta).await?;
                } else {
                    rw.update(old_selected, updated)?;
                    rw_opt.unwrap().1.commit()?;
                }
            } else if old_selected.meta != meta {
                update_selected_torrent_meta(db, rw_opt.unwrap(), mam, old_selected, meta).await?;
            }
            trace!("Torrent {} is already selected", torrent.id);
            continue;
        }
        if let Some((_, rw)) = &rw_opt {
            let old_library = rw
                .get()
                .secondary::<mlm_db::Torrent>(TorrentKey::mam_id, meta.mam_id)?;
            if let Some(old) = old_library {
                if old.meta != meta
                    || (cost == Cost::MetadataOnlyAdd
                        && old.linker.is_none()
                        && !torrent.owner_name.is_empty())
                {
                    update_torrent_meta(
                        config,
                        db,
                        rw_opt.unwrap(),
                        &torrent,
                        old,
                        meta,
                        false,
                        cost == Cost::MetadataOnlyAdd,
                    )
                    .await?;
                }
                trace!("Torrent {} is already in library", torrent.id);
                continue 'torrent;
            }
        }
        if cost == Cost::MetadataOnlyAdd {
            let mam_id = meta.mam_id;
            info!("Adding metadata only torrent \"{}\"", meta.title);
            add_metadata_only_torrent(rw_opt.unwrap(), torrent, meta, true)
                .await
                .or_else(|err| {
                    let err = err.downcast::<db_type::Error>()?;
                    if let db_type::Error::DuplicateKey { .. } = err {
                        warn!("Got dup key when adding torrent {}", mam_id);
                        Result::<(), anyhow::Error>::Ok(())
                    } else {
                        Err(err.into())
                    }
                })?;
            continue 'torrent;
        }
        if cost == Cost::MetadataOnly {
            continue 'torrent;
        }
        let Some(torrent_cost) = selection_cost(cost, &torrent) else {
            debug!(
                "Skipping torrent {} as its grab block's cost doesn't allow it (not free, or VIP without a free flag), cost: {:?}",
                torrent.id, cost
            );
            continue 'torrent;
        };
        let title_search = normalize_title(&meta.title);
        let preferred_types = config.preferred_types(&meta.media_type);
        let preference = preferred_types
            .iter()
            .position(|t| meta.filetypes.contains(t));
        if preference.is_none() {
            debug!(
                "Could not find any wanted formats in torrent {}, formats: {:?}, wanted: {:?}",
                meta.mam_id, meta.filetypes, preferred_types
            );
            continue 'torrent;
        }
        // The account snatched it outside MLM, or its record was deleted. It's stored like a
        // metadata only torrent, so list imports don't look it up again and other uploads of the
        // same book are compared against it. It's checked before the selected uploads of the same
        // book, so it doesn't unselect any of them
        if torrent.my_snatched {
            info!(
                "Skipping torrent \"{}\" in format {} as it was already snatched, storing it without files",
                torrent.title, torrent.filetype
            );
            snatched_torrents += 1;
            if let Some(rw) = rw_opt {
                add_metadata_only_torrent(rw, torrent, meta, false).await?;
            }
            continue 'torrent;
        }
        if let Some((_, rw)) = &rw_opt {
            let old_selected = {
                rw.scan()
                    .secondary::<mlm_db::SelectedTorrent>(mlm_db::SelectedTorrentKey::title_search)?
                    .range::<RangeInclusive<&str>>(title_search.as_str()..=title_search.as_str())?
                    .collect::<Result<Vec<_>, native_db::db_type::Error>>()
            }?;
            for old in old_selected {
                if old.mam_id == meta.mam_id {
                    if old.meta != meta {
                        update_selected_torrent_meta(db, rw_opt.unwrap(), mam, old, meta).await?;
                    }
                    trace!("Torrent {} is already selected2", torrent.id);
                    continue 'torrent;
                }
                trace!(
                    "Checking old torrent {} with formats {:?}",
                    old.title_search, old.meta.filetypes
                );
                if meta.matches(&old.meta) {
                    let old_preference = preferred_types
                        .iter()
                        .position(|t| old.meta.filetypes.contains(t));
                    if old_preference <= preference {
                        if let Err(err) =
                            add_duplicate_torrent(rw, None, torrent.dl.clone(), title_search, meta)
                        {
                            error!("Error writing duplicate torrent: {err}");
                        }
                        rw_opt.unwrap().1.commit()?;
                        trace!(
                            "Skipping torrent {} as we have {} selected",
                            torrent.id, old.meta.mam_id
                        );
                        continue 'torrent;
                    } else {
                        if let Err(err) = add_duplicate_torrent(
                            rw,
                            None,
                            torrent.dl.clone(),
                            title_search.clone(),
                            old.meta.clone(),
                        ) {
                            error!("Error writing duplicate torrent: {err}");
                        }
                        info!(
                            "Unselecting torrent \"{}\" with formats {:?}",
                            old.meta.title, old.meta.filetypes
                        );
                        rw.remove(old)?;
                    }
                }
            }
        }
        if let Some((_, rw)) = &rw_opt {
            let old_library = {
                rw.scan()
                    .secondary::<mlm_db::Torrent>(mlm_db::TorrentKey::title_search)?
                    .range::<RangeInclusive<&str>>(title_search.as_str()..=title_search.as_str())?
                    .collect::<Result<Vec<_>, native_db::db_type::Error>>()
            }?;
            for old in old_library {
                if old.meta.mam_id == meta.mam_id {
                    if old.meta != meta {
                        update_torrent_meta(
                            config,
                            db,
                            rw_opt.unwrap(),
                            &torrent,
                            old,
                            meta,
                            false,
                            false,
                        )
                        .await?;
                    }
                    trace!("Torrent {} is already in library2", torrent.id);
                    continue 'torrent;
                }
                trace!(
                    "Checking old torrent {} with formats {:?}",
                    old.title_search, old.meta.filetypes
                );
                if meta.matches(&old.meta) {
                    let old_preference = preferred_types
                        .iter()
                        .position(|t| old.meta.filetypes.contains(t));
                    if old_preference <= preference {
                        if let Err(err) = add_duplicate_torrent(
                            rw,
                            Some(old.id),
                            torrent.dl.clone(),
                            title_search,
                            meta,
                        ) {
                            error!("Error writing duplicate torrent: {err}");
                        }
                        rw_opt.unwrap().1.commit()?;
                        trace!(
                            "Skipping torrent {} as we have {} in libary",
                            torrent.id, old.meta.mam_id
                        );
                        continue 'torrent;
                    } else {
                        info!(
                            "Selecting replacement for library torrent \"{}\" with formats {:?}",
                            old.meta.title, old.meta.filetypes
                        );
                    }
                }
            }
        }
        let tags: Vec<_> = config
            .tags
            .iter()
            .filter(|t| t.filter.matches(&torrent))
            .collect();
        let category = filter_category
            .clone()
            .or_else(|| tags.iter().find_map(|t| t.category.clone()));
        let tags = tags.iter().flat_map(|t| t.tags.clone()).collect();
        info!(
            "Selecting torrent \"{}\" in format {}, cost: {:?}, with category {:?} and tags {:?}",
            torrent.title, torrent.filetype, torrent_cost, category, tags
        );
        if let Some((_, rw)) = &rw_opt {
            selected_torrents += 1;
            rw.insert(mlm_db::SelectedTorrent {
                mam_id: torrent.id,
                goodreads_id,
                hash: None,
                dl_link: torrent
                    .dl
                    .clone()
                    .ok_or_else(|| Error::msg(format!("no dl field for torrent {}", torrent.id)))?,
                unsat_buffer,
                wedge_buffer,
                cost: torrent_cost,
                category,
                tags,
                title_search,
                meta,
                grabber: grabber.name.clone(),
                by_hand: false,
                created_at: Timestamp::now(),
                started_at: None,
                removed_at: None,
            })?;
            rw_opt.unwrap().1.commit()?;
            if selected_torrents >= max_torrents {
                break;
            }
        }
    }

    Ok(Selection {
        selected: selected_torrents,
        snatched: snatched_torrents,
    })
}

/// The cost a torrent is selected with, or None when its grab block's cost doesn't allow
/// downloading it.
///
/// A free selection is only grabbed while it's still free, and dropped once it isn't. So only
/// `free` and `wedge` blocks store a torrent that's free now as free: a `ratio` or `try_wedge`
/// block keeps its own cost, which the downloader grabs whether or not the torrent is still free
/// then, and without a wedge while it's free. A `wedge` block stores it as free because a wedge
/// selection waits while there are no wedges to spare, even for a torrent that's free (#32).
///
/// No block selects a VIP torrent without any of the free flags (`vip_not_free`)
fn selection_cost(cost: Cost, torrent: &MaMTorrent) -> Option<TorrentCost> {
    match cost {
        Cost::MetadataOnly | Cost::MetadataOnlyAdd => None,
        _ if vip_not_free(torrent) => None,
        Cost::Free => free_cost(torrent),
        Cost::Wedge => Some(free_cost(torrent).unwrap_or(TorrentCost::UseWedge)),
        Cost::TryWedge => Some(TorrentCost::TryWedge),
        Cost::Ratio => Some(TorrentCost::Ratio),
    }
}

/// A VIP torrent with none of the free flags. MaM's docs describe `fl_vip` as set for an item
/// that's freeleech or VIP, so one without it may be a VIP torrent the account can't download
/// (#39). It isn't selected: grabbing it would be refused, or retried on every run
pub(crate) fn vip_not_free(torrent: &MaMTorrent) -> bool {
    torrent.vip && !torrent.is_free()
}

/// How a torrent is free now, or None when it isn't. A torrent that's free only through fl_vip
/// counts as VIP, which the downloader checks the same way
pub(crate) fn free_cost(torrent: &MaMTorrent) -> Option<TorrentCost> {
    if !torrent.is_free() {
        None
    } else if torrent.vip {
        Some(TorrentCost::Vip)
    } else if torrent.personal_freeleech {
        Some(TorrentCost::PersonalFreeleech)
    } else if torrent.free {
        Some(TorrentCost::GlobalFreeleech)
    } else {
        Some(TorrentCost::Vip)
    }
}

/// Stores a torrent without files. With `linker_is_owner`, its uploader is stored as its linker
#[instrument(skip_all)]
pub async fn add_metadata_only_torrent(
    (_guard, rw): (MutexGuard<'_, ()>, RwTransaction<'_>),
    torrent: MaMTorrent,
    meta: TorrentMeta,
    linker_is_owner: bool,
) -> Result<()> {
    let id = Uuid::new_v4().to_string();

    let mam_id = torrent.id;
    {
        rw.insert(mlm_db::Torrent {
            id,
            id_is_hash: false,
            mam_id,
            abs_id: None,
            goodreads_id: None,
            library_path: None,
            library_files: Default::default(),
            linker: if !linker_is_owner || torrent.owner_name.is_empty() {
                None
            } else {
                Some(torrent.owner_name)
            },
            category: None,
            selected_audio_format: None,
            selected_ebook_format: None,
            title_search: normalize_title(&meta.title),
            meta,
            created_at: Timestamp::now(),
            replaced_with: None,
            request_matadata_update: false,
            library_mismatch: None,
            client_status: None,
        })?;
        rw.commit()?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn update_torrent_meta(
    config: &Config,
    db: &Database<'_>,
    (guard, rw): (MutexGuard<'_, ()>, RwTransaction<'_>),
    mam_torrent: &MaMTorrent,
    mut torrent: mlm_db::Torrent,
    meta: TorrentMeta,
    allow_non_mam: bool,
    linker_is_owner: bool,
) -> Result<()> {
    if !allow_non_mam && torrent.meta.source != MetadataSource::Mam {
        // Update VIP status and uploaded_at still
        if torrent.meta.vip_status != meta.vip_status
            || torrent.meta.uploaded_at != meta.uploaded_at
        {
            torrent.meta.vip_status = meta.vip_status;
            torrent.meta.uploaded_at = meta.uploaded_at;
            rw.upsert(torrent.clone())?;
            rw.commit()?;
        }
        return Ok(());
    }

    // Check expiring VIP
    if torrent.meta.vip_status != meta.vip_status
        && torrent
            .meta
            .vip_status
            .as_ref()
            .is_some_and(|s| !s.is_vip())
        && meta.vip_status == Some(VipStatus::NotVip)
    {
        torrent.meta.vip_status = meta.vip_status.clone();
        // If expiring VIP was the only change, just silently update the database
        if torrent.meta == meta {
            rw.upsert(torrent.clone())?;
            rw.commit()?;
            return Ok(());
        }
    }

    // Check uploaded_at and num_files
    if torrent.meta.uploaded_at != meta.uploaded_at || torrent.meta.num_files != meta.num_files {
        torrent.meta.uploaded_at = meta.uploaded_at;
        torrent.meta.num_files = meta.num_files;
        // If uploaded_at or num_files was the only change, just silently update the database
        if torrent.meta == meta {
            rw.upsert(torrent.clone())?;
            rw.commit()?;
            return Ok(());
        }
    }

    if linker_is_owner && torrent.linker.is_none() {
        torrent.linker = Some(mam_torrent.owner_name.clone());
    }

    let id = torrent.id.clone();
    let mam_id = meta.mam_id;
    let diff = torrent.meta.diff(&meta);
    debug!(
        "Updating meta for torrent {}, diff:\n{}",
        mam_id,
        diff.iter()
            .map(|field| format!("  {}: {} → {}", field.field, field.from, field.to))
            .join("\n")
    );
    torrent.meta = meta.clone();
    torrent.title_search = normalize_title(&meta.title);
    rw.upsert(torrent.clone())?;
    rw.commit()?;
    drop(guard);

    if let Some(library_path) = &torrent.library_path
        && let serde_json::Value::Object(new) = abs::create_metadata(mam_torrent, &meta)
    {
        let metadata_path = library_path.join("metadata.json");
        if metadata_path.exists() {
            let existing = fs::read_to_string(&metadata_path).await?;
            let mut existing: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(&existing)?;
            for (key, value) in new {
                existing.insert(key, value);
            }
            let file = File::create(&metadata_path)?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer(&mut writer, &serde_json::Value::Object(existing))?;
            writer.flush()?;
            debug!("updated ABS metadata file {}", torrent.meta.mam_id);
        }
        if let (Some(abs_id), Some(abs_config)) = (&torrent.abs_id, &config.audiobookshelf) {
            let abs = Abs::new(abs_config)?;
            match abs.update_book(abs_id, mam_torrent, &meta).await {
                Ok(_) => debug!("updated ABS via API {}", torrent.meta.mam_id),
                Err(err) => warn!("Failed updating book {} in abs: {err}", torrent.meta.mam_id),
            }
        }
    }

    if !diff.is_empty() {
        write_event(
            db,
            Event::new(Some(id), Some(mam_id), EventType::Updated { fields: diff }),
        )
        .await;
    }
    Ok(())
}

async fn update_selected_torrent_meta(
    db: &Database<'_>,
    (guard, rw): (MutexGuard<'_, ()>, RwTransaction<'_>),
    mam: &MaM<'_>,
    torrent: SelectedTorrent,
    meta: TorrentMeta,
) -> Result<()> {
    let mam_id = meta.mam_id;
    let diff = torrent.meta.diff(&meta);
    debug!(
        "Updating meta for selected torrent {}, diff:\n{}",
        mam_id,
        diff.iter()
            .map(|field| format!("  {}: {} -> {}", field.field, field.from, field.to))
            .join("\n")
    );
    let hash = get_mam_torrent_hash(mam, &torrent.dl_link, torrent.mam_id)
        .await
        .ok();
    let mut torrent = torrent;
    torrent.meta = meta;
    rw.upsert(torrent)?;
    rw.commit()?;
    drop(guard);
    write_event(
        db,
        Event::new(hash, Some(mam_id), EventType::Updated { fields: diff }),
    )
    .await;
    Ok(())
}

pub async fn get_mam_torrent_hash(mam: &MaM<'_>, dl_hash: &str, tid: u64) -> Result<String> {
    let torrent_file_bytes = mam.get_torrent_file(dl_hash, tid, false).await?;
    let torrent_file = Torrent::read_from_bytes(torrent_file_bytes.clone())?;
    let hash = torrent_file.info_hash();
    Ok(hash)
}

fn add_duplicate_torrent(
    rw: &RwTransaction<'_>,
    duplicate_of: Option<String>,
    dl_link: Option<String>,
    title_search: String,
    meta: TorrentMeta,
) -> Result<()> {
    rw.upsert(DuplicateTorrent {
        mam_id: meta.mam_id,
        dl_link,
        title_search,
        meta,
        created_at: Timestamp::now(),
        duplicate_of,
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use mlm_db::{AudiobookCategory, Language};

    use super::*;

    fn config() -> Config {
        toml::from_str("mam_id = \"test\"").unwrap()
    }

    fn database() -> Arc<Database<'static>> {
        Arc::new(
            native_db::Builder::new()
                .create_in_memory(&mlm_db::MODELS)
                .unwrap(),
        )
    }

    fn audiobook(id: u64, title: &str, filetype: &str, my_snatched: bool) -> MaMTorrent {
        MaMTorrent {
            id,
            title: title.to_owned(),
            author_info: [(1, "Ann Author".to_owned())].into(),
            added: "2025-01-01 00:00:00".to_owned(),
            main_cat: 13,
            category: AudiobookCategory::GeneralFiction.to_id() as u64,
            language: Language::English.to_id(),
            filetype: filetype.to_owned(),
            numfiles: 1,
            size: "100 MiB".to_owned(),
            dl: Some(format!("dl{id}")),
            my_snatched,
            ..Default::default()
        }
    }

    async fn select(
        db: &Arc<Database<'static>>,
        torrents: Vec<MaMTorrent>,
        cost: Cost,
        dry_run: bool,
    ) -> Selection {
        let mam = MaM::offline(db.clone());
        select_torrents(
            &config(),
            db,
            &mam,
            torrents.into_iter(),
            &TorrentFilter::default(),
            cost,
            None,
            None,
            None,
            dry_run,
            u64::MAX,
            None,
        )
        .await
        .unwrap()
    }

    fn selected(db: &Database<'_>, id: u64) -> Option<SelectedTorrent> {
        db.r_transaction().unwrap().get().primary(id).unwrap()
    }

    fn stored(db: &Database<'_>, id: u64) -> Option<mlm_db::Torrent> {
        db.r_transaction()
            .unwrap()
            .get()
            .secondary(TorrentKey::mam_id, id)
            .unwrap()
    }

    #[tokio::test]
    async fn test_skips_snatched_torrents() {
        let db = database();
        let selection = select(
            &db,
            vec![
                audiobook(1, "Snatched Book", "m4b", true),
                audiobook(2, "Other Book", "m4b", false),
            ],
            Cost::Ratio,
            false,
        )
        .await;
        assert_eq!(
            selection,
            Selection {
                selected: 1,
                snatched: 1
            }
        );
        assert!(selected(&db, 1).is_none());
        assert!(selected(&db, 2).is_some());

        // The snatched torrent is stored without files, so it's skipped as already stored from now
        // on
        let snatched = stored(&db, 1).unwrap();
        assert!(snatched.library_path.is_none());
        assert!(snatched.linker.is_none());
        assert!(stored(&db, 2).is_none());
        let selection = select(
            &db,
            vec![audiobook(1, "Snatched Book", "m4b", true)],
            Cost::Ratio,
            false,
        )
        .await;
        assert_eq!(selection, Selection::default());
    }

    #[tokio::test]
    async fn test_snatched_torrent_counts_as_stored_upload() {
        let db = database();
        // A selected upload of the book in a format that isn't preferred stays selected
        select(
            &db,
            vec![audiobook(1, "Book", "mp3", false)],
            Cost::Ratio,
            false,
        )
        .await;
        let selection = select(
            &db,
            vec![audiobook(2, "Book", "m4b", true)],
            Cost::Ratio,
            false,
        )
        .await;
        assert_eq!(selection.snatched, 1);
        assert!(selected(&db, 1).is_some());
        assert!(stored(&db, 2).is_some());

        // Another upload that isn't preferred over the snatched one isn't selected
        let db = database();
        select(
            &db,
            vec![audiobook(1, "Book", "m4b", true)],
            Cost::Ratio,
            false,
        )
        .await;
        let selection = select(
            &db,
            vec![audiobook(2, "Book", "m4b", false)],
            Cost::Ratio,
            false,
        )
        .await;
        assert_eq!(selection, Selection::default());
        assert!(selected(&db, 2).is_none());
    }

    #[tokio::test]
    async fn test_metadata_only_add_still_stores_snatched_torrents() {
        let db = database();
        let torrent = MaMTorrent {
            owner_name: "uploader".to_owned(),
            ..audiobook(1, "Book", "m4b", true)
        };
        let selection = select(&db, vec![torrent], Cost::MetadataOnlyAdd, false).await;
        assert_eq!(selection, Selection::default());
        assert_eq!(stored(&db, 1).unwrap().linker.as_deref(), Some("uploader"));
    }

    #[tokio::test]
    async fn test_dry_run_stores_no_snatched_torrents() {
        let db = database();
        let selection = select(
            &db,
            vec![audiobook(1, "Book", "m4b", true)],
            Cost::Ratio,
            true,
        )
        .await;
        assert_eq!(selection.snatched, 1);
        assert!(stored(&db, 1).is_none());
    }

    #[test]
    fn test_selection_cost() {
        let plain = MaMTorrent::default();
        let free = MaMTorrent {
            free: true,
            fl_vip: true,
            ..Default::default()
        };
        let personal = MaMTorrent {
            personal_freeleech: true,
            ..Default::default()
        };
        let vip = MaMTorrent {
            vip: true,
            fl_vip: true,
            ..Default::default()
        };
        let fl_vip = MaMTorrent {
            fl_vip: true,
            ..Default::default()
        };
        let vip_only = MaMTorrent {
            vip: true,
            ..Default::default()
        };

        // Free and wedge blocks store a free torrent as free. A torrent that's free only through
        // fl_vip counts as VIP
        for cost in [Cost::Free, Cost::Wedge] {
            assert_eq!(
                selection_cost(cost, &free),
                Some(TorrentCost::GlobalFreeleech)
            );
            assert_eq!(
                selection_cost(cost, &personal),
                Some(TorrentCost::PersonalFreeleech)
            );
            assert_eq!(selection_cost(cost, &vip), Some(TorrentCost::Vip));
            assert_eq!(selection_cost(cost, &fl_vip), Some(TorrentCost::Vip));
        }

        // Ratio and try_wedge blocks keep their own cost even for a free torrent, so it isn't
        // dropped when it stops being free before it's grabbed
        for info in [&plain, &free, &personal, &vip, &fl_vip] {
            assert_eq!(
                selection_cost(Cost::TryWedge, info),
                Some(TorrentCost::TryWedge)
            );
            assert_eq!(selection_cost(Cost::Ratio, info), Some(TorrentCost::Ratio));
        }

        // Other torrents take the block's cost, and free blocks skip them
        assert_eq!(
            selection_cost(Cost::Wedge, &plain),
            Some(TorrentCost::UseWedge)
        );
        assert_eq!(selection_cost(Cost::Free, &plain), None);

        // VIP without any of the free flags may be one the account can't download, so no block
        // selects it
        for cost in [Cost::Free, Cost::Wedge, Cost::TryWedge, Cost::Ratio] {
            assert_eq!(selection_cost(cost, &vip_only), None);
        }

        // Metadata only blocks don't download anything
        for cost in [Cost::MetadataOnly, Cost::MetadataOnlyAdd] {
            assert_eq!(selection_cost(cost, &free), None);
            assert_eq!(selection_cost(cost, &plain), None);
        }
    }
}
