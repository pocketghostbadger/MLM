use std::time::Duration;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use lava_torrent::torrent::v1::Torrent;
use mlm_db::{
    DatabaseExt as _, ErroredTorrentId, Event, EventType, SelectedTorrent, Size, Timestamp,
    TorrentCost, TorrentKey,
};
use mlm_mam::{
    api::{MaM, RateLimitError},
    search::MaMTorrent,
};
use native_db::Database;
use qbit::{
    models::Torrent as QbitTorrent,
    parameters::{AddTorrent, AddTorrentType, TorrentFile, TorrentListParams, TorrentState},
};
use mlm_mam::api::AccountBlockedError;
use tokio::time::sleep;
use tracing::{debug, info, instrument, trace, warn};

use crate::{
    config::Config,
    logging::{TorrentMetaError, update_errored_torrent, write_event},
    qbittorrent::add_torrent_with_category,
};

#[instrument(skip_all)]
pub async fn grab_selected_torrents(
    config: &Config,
    db: &Database<'_>,
    qbit: &qbit::Api,
    qbit_url: &str,
    mam: &MaM<'_>,
) -> Result<()> {
    let selected_torrents = {
        let r = db.r_transaction()?;
        r.scan()
            .primary::<SelectedTorrent>()?
            .all()?
            .filter(|t| t.as_ref().is_ok_and(|t| t.removed_at.is_none()))
            .collect::<Result<Vec<_>, native_db::db_type::Error>>()
    }?;
    if selected_torrents.is_empty() {
        trace!("no selected torrents");
        return Ok(());
    }

    let user_info = mam.user_info().await?;
    let max_torrents = user_info
        .snatch_summary
        .unsat
        .limit
        .saturating_sub(user_info.snatch_summary.unsat.count);

    let downloading_size: f64 = selected_torrents
        .iter()
        .filter(|t| t.started_at.is_some())
        .map(|t| t.meta.size.bytes() as f64)
        .sum();

    let mut remaining_buffer = ratio_room(
        user_info.uploaded_bytes,
        user_info.downloaded_bytes,
        downloading_size,
        config.min_ratio,
    );
    debug!(
        "downloader, unsats: {:#?}; max_torrents: {max_torrents}; buffer: {}",
        user_info.snatch_summary.unsat,
        Size::from_bytes(remaining_buffer as u64)
    );

    let mut snatched_torrents = 0;
    for torrent in selected_torrents
        .into_iter()
        .filter(|t| t.started_at.is_none())
    {
        let max_torrents = max_torrents
            .saturating_sub(torrent.unsat_buffer.unwrap_or(config.unsat_buffer))
            .saturating_sub(snatched_torrents);
        if max_torrents == 0 {
            continue;
        }
        let buffer_after = remaining_buffer - torrent.meta.size.bytes() as f64;
        if buffer_after <= 0.0 {
            debug!(
                "Not grabbing torrent \"{}\", not enough room above min_ratio",
                torrent.meta.title
            );
            continue;
        }

        let result = grab_torrent(config, db, qbit, qbit_url, mam, torrent.clone()).await;

        // if the account is temporarly blocked, mlm will still try to download new files every second
        // resulting in a unnecessary load on the server and a prolonged penalty
        // here it gets checked if a blocked message gets returned
        let account_blocked = result.as_ref().err().is_some_and(|err| err.is::<AccountBlockedError>());
                
        // A stale dl link, or a torrent that can't be grabbed at its cost, can't be
        // grabbed on a later pass either, so drop the selection instead of asking the
        // site for it every run. It stays on the errors page, and restoring it from
        // the selected page retries.
        if result.as_ref().err().is_some_and(drops_selection) {
            let (_guard, rw) = db.rw_async().await?;
            if let Some(mut stale) = rw.get().primary::<SelectedTorrent>(torrent.mam_id)? {
                stale.removed_at = Some(Timestamp::now());
                rw.upsert(stale)?;
            }
            rw.commit()?;
        }

        let result =
            result.map_err(|err| anyhow::Error::new(TorrentMetaError(torrent.meta.clone(), err)));


        if result.is_ok() {
            snatched_torrents += 1;
            remaining_buffer = buffer_after;
        }

        update_errored_torrent(
            db,
            ErroredTorrentId::Grabber(torrent.mam_id),
            torrent.meta.title,
            result,
        )
        .await;

        if account_blocked {
            warn!("Account issue (406 Not Acceptable) detected. Stopping current autograb cycle.");
            break;
        }

        sleep(Duration::from_millis(1000)).await;
    }
    Ok(())
}

/// How many more bytes can be downloaded before the ratio would drop below `min_ratio`,
/// counting torrents that are still downloading as downloaded
pub(crate) fn ratio_room(uploaded: f64, downloaded: f64, downloading: f64, min_ratio: f64) -> f64 {
    uploaded / min_ratio - downloaded - downloading
}

/// What grabbing a selection spends, decided before its torrent file is downloaded
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Spend {
    Nothing,
    Wedge,
    Ratio,
}

/// Why a selection wasn't grabbed
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum NotGrabbed {
    #[error("Fewer wedges ({wedges}) than wedge_buffer ({wedge_buffer})")]
    FewerWedges { wedges: u64, wedge_buffer: u64 },
    #[error("Could not get torrent from MaM")]
    NotOnMam,
    #[error("Torrent was already snatched, so no wedge is used on it")]
    AlreadySnatched,
}

/// Whether a grab error would come back on every later pass, so the selection is dropped
/// instead of retried. Wedges can come back, so running out of them is retried
fn drops_selection(err: &anyhow::Error) -> bool {
    err.is::<StaleDlLink>()
        || err
            .downcast_ref::<NotGrabbed>()
            .is_some_and(|err| !matches!(err, NotGrabbed::FewerWedges { .. }))
}

/// Whether downloading a torrent spends nothing. `fl` would still spend a wedge on VIP torrents
fn already_free(info: &MaMTorrent) -> bool {
    info.is_free() || info.vip
}

/// Whether deciding what a selection spends needs the torrent as MaM returns it now. This has to
/// agree with `decide_spend`, which takes a missing lookup to mean that MaM didn't return it
fn needs_lookup(cost: TorrentCost, wedges: u64, wedge_buffer: u64) -> bool {
    match cost {
        TorrentCost::GlobalFreeleech | TorrentCost::PersonalFreeleech | TorrentCost::Vip => false,
        TorrentCost::UseWedge | TorrentCost::TryWedge => wedges > wedge_buffer,
        TorrentCost::Ratio => false,
    }
}

/// Decides what grabbing a selection spends. `info` is the torrent as MaM returns it now,
/// looked up when `needs_lookup` says so.
///
/// No wedge is used on a torrent that's already free or VIP, or on one that was already
/// snatched, which is probably in a client already. Free costs are checked against MaM once the
/// torrent file is in hand.
fn decide_spend(
    cost: TorrentCost,
    wedges: u64,
    wedge_buffer: u64,
    info: Option<&MaMTorrent>,
) -> Result<Spend, NotGrabbed> {
    match cost {
        TorrentCost::GlobalFreeleech | TorrentCost::PersonalFreeleech | TorrentCost::Vip => {
            Ok(Spend::Nothing)
        }
        TorrentCost::Ratio => Ok(Spend::Ratio),
        TorrentCost::UseWedge | TorrentCost::TryWedge => {
            let no_wedge = if wedges <= wedge_buffer {
                NotGrabbed::FewerWedges {
                    wedges,
                    wedge_buffer,
                }
            } else {
                match info {
                    None => NotGrabbed::NotOnMam,
                    Some(info) if already_free(info) => return Ok(Spend::Nothing),
                    Some(info) if info.my_snatched => NotGrabbed::AlreadySnatched,
                    Some(_) => return Ok(Spend::Wedge),
                }
            };
            // try_wedge still downloads, on ratio, when it can't use a wedge
            if cost == TorrentCost::TryWedge {
                Ok(Spend::Ratio)
            } else {
                Err(no_wedge)
            }
        }
    }
}

/// The event for a torrent that was grabbed, or that a wedge was used on
fn grabbed_event(hash: String, torrent: &SelectedTorrent, wedged: bool) -> Event {
    Event::new(
        Some(hash),
        Some(torrent.mam_id),
        EventType::Grabbed {
            grabber: torrent.grabber.clone(),
            cost: Some(torrent.cost),
            wedged,
        },
    )
}

#[instrument(skip_all)]
async fn grab_torrent(
    config: &Config,
    db: &Database<'_>,
    qbit: &qbit::Api,
    qbit_url: &str,
    mam: &MaM<'_>,
    torrent: SelectedTorrent,
) -> Result<()> {
    info!(
        "Grabbing torrent \"{}\", with category {:?} and tags {:?}",
        torrent.meta.title, torrent.category, torrent.tags,
    );

    // Nothing needs to be sent for a torrent that's already in the library
    let library_existing = {
        let r = db.r_transaction()?;
        r.get()
            .secondary::<mlm_db::Torrent>(TorrentKey::mam_id, torrent.mam_id)?
    };
    if library_existing.is_some_and(|existing| existing.library_path.is_some()) {
        debug!(
            "Torrent \"{}\" is already in the library",
            torrent.meta.title
        );
        let (_guard, rw) = db.rw_async().await?;
        rw.remove(torrent)?;
        rw.commit()?;
        return Ok(());
    }

    let user_info = mam.user_info().await?;
    let wedge_buffer = torrent.wedge_buffer.unwrap_or(config.wedge_buffer);
    let info = if needs_lookup(torrent.cost, user_info.wedges, wedge_buffer) {
        mam.get_torrent_info_by_id(torrent.mam_id).await?
    } else {
        None
    };
    // A torrent that isn't grabbed at its cost may still be in a client already. It's then
    // recorded like any other duplicate, after downloading its torrent file without `fl`
    let (will_wedge, refused) =
        match decide_spend(torrent.cost, user_info.wedges, wedge_buffer, info.as_ref()) {
            Ok(spend) => {
                debug!(
                    "Grabbing torrent \"{}\" spends {spend:?}",
                    torrent.meta.title
                );
                (spend == Spend::Wedge, None)
            }
            Err(err @ NotGrabbed::AlreadySnatched) => (false, Some(err)),
            Err(err) => return Err(err.into()),
        };
    if will_wedge {
        info!("Using wedge on torrent \"{}\"", torrent.meta.title);
    }
    let fresh_dl_link = info.as_ref().and_then(|info| info.dl.as_deref());
    let (torrent_file_bytes, torrent_file) =
        fetch_torrent_file(db, mam, &torrent, will_wedge, fresh_dl_link).await?;
    let hash = torrent_file.info_hash();
    // The wedge is spent now, so it shows up even if the torrent is a duplicate or can't be added
    if will_wedge {
        if let Some((_, user_info)) = mam.user.lock().await.as_mut() {
            user_info.wedges = user_info.wedges.saturating_sub(1);
        }
        write_event(db, grabbed_event(hash.clone(), &torrent, true)).await;
    }

    if let Some(qbit_torrent) = get_existing_qbit_torrent(config, qbit, qbit_url, &hash).await {
        let is_completed = matches!(
            qbit_torrent.state,
            TorrentState::Uploading
                | TorrentState::StoppedUploading
                | TorrentState::QueuedUploading
                | TorrentState::StalledUploading
                | TorrentState::CheckingUploading
                | TorrentState::ForcedUploading
        );

        let (_guard, rw) = db.rw_async().await?;

        if is_completed {
            if rw.get().primary::<mlm_db::Torrent>(hash.clone())?.is_none() {
                rw.upsert(mlm_db::Torrent {
                    id: hash.clone(),
                    id_is_hash: true,
                    mam_id: torrent.meta.mam_id,
                    abs_id: None,
                    goodreads_id: torrent.goodreads_id,
                    library_path: None,
                    library_files: Default::default(),
                    linker: None,
                    category: torrent.category.clone(),
                    selected_audio_format: None,
                    selected_ebook_format: None,
                    title_search: torrent.title_search.clone(),
                    meta: torrent.meta.clone(),
                    created_at: Timestamp::now(),
                    replaced_with: None,
                    request_matadata_update: false,
                    library_mismatch: None,
                    client_status: None,
                })?;
            }
            rw.remove(torrent)?;
        } else {
            let mut t = torrent;
            t.started_at = Some(Timestamp::now());
            rw.upsert(t)?;
        }
        rw.commit()?;

        return Ok(());
    }

    let library_existing = {
        let r = db.r_transaction()?;
        r.get().primary::<mlm_db::Torrent>(hash.clone())?
    };

    if let Some(existing) = library_existing
        && existing.library_path.is_some()
    {
        let (_guard, rw) = db.rw_async().await?;
        rw.remove(torrent)?;
        rw.commit()?;
        return Ok(());
    }

    if let Some(err) = refused {
        return Err(err.into());
    }

    if !matches!(
        torrent.cost,
        TorrentCost::Ratio | TorrentCost::UseWedge | TorrentCost::TryWedge
    ) {
        let Some(torrent_info) = mam.get_torrent_info(&hash).await? else {
            return Err(anyhow!("Could not get torrent from MaM"));
        };
        if !torrent_info.is_free() {
            return Err(anyhow!(
                "Torrent is no longer free, expected: {:?}",
                torrent.cost
            ));
        }
    }

    mam.add_unsats(1).await;
    add_torrent_with_category(
        qbit,
        qbit_url,
        AddTorrent {
            torrents: AddTorrentType::Files(vec![TorrentFile {
                filename: format!("{}.torrent", torrent.mam_id),
                data: torrent_file_bytes.iter().copied().collect(),
            }]),
            stopped: config.add_torrents_stopped,
            category: torrent.category.clone(),
            tags: if torrent.tags.is_empty() {
                None
            } else {
                Some(torrent.tags.clone())
            },
            ..Default::default()
        },
    )
    .await?;

    let event = (!will_wedge).then(|| grabbed_event(hash.clone(), &torrent, false));
    {
        let (_guard, rw) = db.rw_async().await?;
        rw.upsert(mlm_db::Torrent {
            id: hash.clone(),
            id_is_hash: true,
            mam_id: torrent.meta.mam_id,
            abs_id: None,
            goodreads_id: torrent.goodreads_id,
            library_path: None,
            library_files: Default::default(),
            linker: None,
            category: torrent.category.clone(),
            selected_audio_format: None,
            selected_ebook_format: None,
            title_search: torrent.title_search.clone(),
            meta: torrent.meta.clone(),
            created_at: Timestamp::now(),
            replaced_with: None,
            request_matadata_update: false,
            library_mismatch: None,
            client_status: None,
        })?;
        let mut torrent = torrent;
        torrent.hash = Some(hash.clone());
        torrent.started_at = Some(Timestamp::now());
        rw.upsert(torrent)?;
        rw.commit()?;
    }

    if let Some(event) = event {
        write_event(db, event).await;
    }

    Ok(())
}

async fn get_existing_qbit_torrent(
    config: &Config,
    qbit: &qbit::Api,
    qbit_url: &str,
    hash: &str,
) -> Option<QbitTorrent> {
    if let Ok(Some(qbit_torrent)) = qbit
        .torrents(Some(TorrentListParams {
            hashes: Some(vec![hash.to_string()]),
            ..TorrentListParams::default()
        }))
        .await
        .map(|t| t.into_iter().next())
    {
        return Some(qbit_torrent);
    }

    for qbit_conf in config.qbittorrent.iter().filter(|q| q.url != qbit_url) {
        let Ok(qbit) = qbit::Api::new_login_username_password(
            &qbit_conf.url,
            &qbit_conf.username,
            &qbit_conf.password,
        )
        .await
        else {
            continue;
        };

        if let Ok(Some(qbit_torrent)) = qbit
            .torrents(Some(TorrentListParams {
                hashes: Some(vec![hash.to_string()]),
                ..TorrentListParams::default()
            }))
            .await
            .map(|t| t.into_iter().next())
        {
            return Some(qbit_torrent);
        }
    }

    None
}

#[derive(thiserror::Error, Debug)]
#[error("MaM did not return a torrent file, the dl link is probably stale")]
pub struct StaleDlLink;

/// Fetch and parse a torrent file, refreshing the stored dl link once if what
/// comes back isn't a torrent. A dl hash goes stale after a while, and the site
/// then answers with something that isn't bencode at all. A dl link the site
/// returned just now is used instead of the stored one, and isn't refreshed.
async fn fetch_torrent_file(
    db: &Database<'_>,
    mam: &MaM<'_>,
    torrent: &SelectedTorrent,
    wedge: bool,
    fresh_dl_link: Option<&str>,
) -> Result<(Bytes, Torrent)> {
    if let Some(dl_link) = fresh_dl_link {
        let bytes = get_mam_torrent_file(mam, dl_link, torrent.mam_id, wedge).await?;
        let parsed =
            Torrent::read_from_bytes(bytes.clone()).map_err(|_| anyhow::Error::new(StaleDlLink))?;
        return Ok((bytes, parsed));
    }

    let bytes = get_mam_torrent_file(mam, &torrent.dl_link, torrent.mam_id, wedge).await?;
    if let Ok(parsed) = Torrent::read_from_bytes(bytes.clone()) {
        return Ok((bytes, parsed));
    }

    let Some(info) = mam.get_torrent_info_by_id(torrent.mam_id).await? else {
        return Err(anyhow::Error::new(StaleDlLink));
    };
    let Some(dl_link) = info.dl else {
        return Err(anyhow::Error::new(StaleDlLink));
    };
    if dl_link == torrent.dl_link {
        return Err(anyhow::Error::new(StaleDlLink));
    }

    debug!("refreshing stale dl link for torrent {}", torrent.mam_id);
    let bytes = get_mam_torrent_file(mam, &dl_link, torrent.mam_id, wedge).await?;
    let parsed =
        Torrent::read_from_bytes(bytes.clone()).map_err(|_| anyhow::Error::new(StaleDlLink))?;

    let (_guard, rw) = db.rw_async().await?;
    if let Some(mut stored) = rw.get().primary::<SelectedTorrent>(torrent.mam_id)? {
        stored.dl_link = dl_link;
        rw.upsert(stored)?;
    }
    rw.commit()?;

    Ok((bytes, parsed))
}

/// How many times to retry a single download after being rate limited before
/// giving up on it. Retrying forever means one torrent can keep requesting for
/// as long as the process runs.
const MAX_RATE_LIMIT_RETRIES: u32 = 3;

pub(crate) async fn get_mam_torrent_file(
    mam: &MaM<'_>,
    dl_hash: &str,
    tid: u64,
    wedge: bool,
) -> Result<Bytes> {
    let mut attempts = 0;
    loop {
        let result = mam.get_torrent_file(dl_hash, tid, wedge).await;

        match result {
            Ok(v) => return Ok(v),
            Err(e) => match e.downcast::<RateLimitError>() {
                Ok(rate_limit) => {
                    attempts += 1;
                    if attempts > MAX_RATE_LIMIT_RETRIES {
                        warn!("Giving up on torrent {tid} after {attempts} rate limited requests");
                        return Err(anyhow::Error::new(rate_limit));
                    }
                    sleep(Duration::from_millis(30_000)).await;
                }
                Err(e) => return Err(e),
            },
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    #[test]
    fn test_ratio_room() {
        // The example from the docs: 100 GiB up and 40 GiB down leave 10 GiB at a ratio of 2
        assert_eq!(ratio_room(100.0 * GIB, 40.0 * GIB, 0.0, 2.0), 10.0 * GIB);
        // Torrents that are still downloading count as downloaded
        assert_eq!(
            ratio_room(100.0 * GIB, 40.0 * GIB, 4.0 * GIB, 2.0),
            6.0 * GIB
        );
        // Already below min_ratio, so there's no room at all
        assert!(ratio_room(100.0 * GIB, 60.0 * GIB, 0.0, 2.0) < 0.0);

        // Using all of the room ends exactly at min_ratio, whether it's below, at or above 1
        for min_ratio in [0.5, 1.0, 2.0] {
            let (uploaded, downloaded, downloading) = (100.0 * GIB, 30.0 * GIB, 5.0 * GIB);
            let room = ratio_room(uploaded, downloaded, downloading, min_ratio);
            assert!(room > 0.0);
            let ratio = uploaded / (downloaded + downloading + room);
            assert!((ratio - min_ratio).abs() < 1e-9, "{min_ratio}: {ratio}");
        }
    }

    /// A torrent as MaM returns it: plain, free, personal freeleech, VIP, snatched, and free
    /// and snatched
    fn infos() -> [MaMTorrent; 6] {
        [
            MaMTorrent::default(),
            MaMTorrent {
                free: true,
                fl_vip: true,
                ..Default::default()
            },
            MaMTorrent {
                personal_freeleech: true,
                ..Default::default()
            },
            MaMTorrent {
                vip: true,
                fl_vip: true,
                ..Default::default()
            },
            MaMTorrent {
                my_snatched: true,
                ..Default::default()
            },
            MaMTorrent {
                free: true,
                fl_vip: true,
                my_snatched: true,
                ..Default::default()
            },
        ]
    }

    #[test]
    fn test_spend_wedge() {
        let [plain, free, personal, vip, snatched, free_snatched] = infos();
        for cost in [TorrentCost::UseWedge, TorrentCost::TryWedge] {
            // The torrent is only looked up when there's a wedge to spend
            assert!(needs_lookup(cost, 1, 0));
            assert!(!needs_lookup(cost, 0, 0));
            assert!(!needs_lookup(cost, 2, 2));

            assert_eq!(decide_spend(cost, 1, 0, Some(&plain)), Ok(Spend::Wedge));
            assert_eq!(decide_spend(cost, 3, 2, Some(&plain)), Ok(Spend::Wedge));
            // No wedge on torrents that are already free, `fl` would spend one on VIP too
            for info in [&free, &personal, &vip, &free_snatched] {
                assert_eq!(decide_spend(cost, 1, 0, Some(info)), Ok(Spend::Nothing));
            }
        }

        // No wedge either on a torrent that was already snatched, or that MaM doesn't return, or
        // when wedges run out. wedge isn't grabbed then, try_wedge falls back to ratio
        let wedge = |wedges, wedge_buffer, info| {
            decide_spend(TorrentCost::UseWedge, wedges, wedge_buffer, info)
        };
        let try_wedge = |wedges, wedge_buffer, info| {
            decide_spend(TorrentCost::TryWedge, wedges, wedge_buffer, info)
        };
        assert_eq!(
            wedge(1, 0, Some(&snatched)),
            Err(NotGrabbed::AlreadySnatched)
        );
        assert_eq!(try_wedge(1, 0, Some(&snatched)), Ok(Spend::Ratio));
        assert_eq!(wedge(1, 0, None), Err(NotGrabbed::NotOnMam));
        assert_eq!(try_wedge(1, 0, None), Ok(Spend::Ratio));
        assert_eq!(
            wedge(2, 2, None),
            Err(NotGrabbed::FewerWedges {
                wedges: 2,
                wedge_buffer: 2
            })
        );
        assert_eq!(try_wedge(2, 2, None), Ok(Spend::Ratio));
        assert_eq!(try_wedge(0, 0, None), Ok(Spend::Ratio));
    }

    #[test]
    fn test_spend_without_wedge() {
        assert!(!needs_lookup(TorrentCost::Ratio, 1, 0));
        assert_eq!(
            decide_spend(TorrentCost::Ratio, 1, 0, None),
            Ok(Spend::Ratio)
        );
        // Free costs are checked against MaM once the torrent file is in hand
        for cost in [
            TorrentCost::GlobalFreeleech,
            TorrentCost::PersonalFreeleech,
            TorrentCost::Vip,
        ] {
            assert!(!needs_lookup(cost, 1, 0));
            assert_eq!(decide_spend(cost, 1, 0, None), Ok(Spend::Nothing));
        }
    }

    #[test]
    fn test_lookup_agrees_with_spend() {
        // Without a lookup, decide_spend must not take the torrent to be missing on MaM, which
        // would drop the selection
        for cost in [
            TorrentCost::GlobalFreeleech,
            TorrentCost::PersonalFreeleech,
            TorrentCost::Vip,
            TorrentCost::UseWedge,
            TorrentCost::TryWedge,
            TorrentCost::Ratio,
        ] {
            for (wedges, wedge_buffer) in [(0, 0), (1, 0), (2, 2), (3, 2)] {
                if !needs_lookup(cost, wedges, wedge_buffer) {
                    assert_ne!(
                        decide_spend(cost, wedges, wedge_buffer, None),
                        Err(NotGrabbed::NotOnMam),
                        "{cost:?}, {wedges} wedges, buffer {wedge_buffer}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_drops_selection() {
        assert!(drops_selection(&anyhow::Error::new(StaleDlLink)));
        assert!(drops_selection(&anyhow::Error::new(NotGrabbed::NotOnMam)));
        assert!(drops_selection(&anyhow::Error::new(
            NotGrabbed::AlreadySnatched
        )));
        // Wedges can come back, so that's tried again
        assert!(!drops_selection(&anyhow::Error::new(
            NotGrabbed::FewerWedges {
                wedges: 0,
                wedge_buffer: 0
            }
        )));
        assert!(!drops_selection(&anyhow::anyhow!("Could not add torrent")));
    }
}
