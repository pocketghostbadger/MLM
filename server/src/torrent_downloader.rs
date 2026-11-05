use std::time::Duration;

use anyhow::{Result, bail};
use bytes::Bytes;
use lava_torrent::torrent::v1::Torrent;
use mlm_db::{
    DatabaseExt as _, ErroredTorrentId, Event, EventType, SelectedTorrent, Size, Timestamp,
    TorrentCost, TorrentKey,
};
use mlm_mam::{
    api::{MaM, RequestsPausedError, refused_or_paused},
    search::MaMTorrent,
};
use native_db::{Database, transaction::RwTransaction};
use qbit::{
    models::Torrent as QbitTorrent,
    parameters::{AddTorrent, AddTorrentType, TorrentFile, TorrentListParams, TorrentState},
};
use tokio::time::sleep;
use tracing::{debug, info, instrument, trace};

use crate::{
    autograbber::{add_metadata_only_torrent, free_cost},
    config::Config,
    lists::{clear_dropped_selection, mark_snatched_existing},
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
        // A selection skipped as already snatched uses no unsat slot and no ratio
        let skipped = matches!(result, Ok(Grabbed::SkippedSnatched));
        let result = result.map(|_| ());

        // Once MaM rate limits a request or blocks the account, requests to MaM are paused, so
        // the other selections wait for a later pass. A grab that wasn't sent because of the
        // pause isn't recorded as an error of the torrent
        if let Err(err) = &result
            && err.is::<RequestsPausedError>()
        {
            return result;
        }
        let mam_refused = result.as_ref().err().is_some_and(refused_or_paused);

        // A stale dl link, or a torrent that can't be grabbed at its cost, can't be
        // grabbed on a later pass either, so drop the selection instead of asking the
        // site for it every run. It stays on the errors page
        if let Some(err) = result.as_ref().err()
            && drops_selection(err)
        {
            drop_selection(db, &torrent, err).await?;
        }

        let result =
            result.map_err(|err| anyhow::Error::new(TorrentMetaError(torrent.meta.clone(), err)));

        if result.is_ok() && !skipped {
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

        if mam_refused {
            match mam.requests_paused() {
                Some(paused) => bail!("{paused}, other grabs wait for a later pass"),
                None => bail!(
                    "MaM rate limited the downloader or blocked the account, other grabs wait for a later pass"
                ),
            }
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
    #[error("Torrent was already snatched, so it isn't downloaded again")]
    SkippedSnatched,
    #[error("Torrent is no longer free, expected: {0:?}")]
    NoLongerFree(TorrentCost),
}

/// Whether a grab error would come back on every later pass, so the selection is dropped
/// instead of retried. Wedges can come back, so running out of them is retried
fn drops_selection(err: &anyhow::Error) -> bool {
    err.is::<StaleDlLink>()
        || err
            .downcast_ref::<NotGrabbed>()
            .is_some_and(|err| !matches!(err, NotGrabbed::FewerWedges { .. }))
}

/// Whether a dropped selection is deleted, so that its torrent, or another upload of the same book,
/// can be selected again: an automatic selection whose torrent is no longer free or is gone from
/// MaM. A later search only selects the torrent again as its grab block allows, a free block not
/// while it isn't free. A stale dl link, and a selection made by hand, are kept as removed, so
/// they can be restored from the selected page
fn picks_up_again(torrent: &SelectedTorrent, err: &anyhow::Error) -> bool {
    !torrent.by_hand
        && err
            .downcast_ref::<NotGrabbed>()
            .is_some_and(|err| matches!(err, NotGrabbed::NoLongerFree(_) | NotGrabbed::NotOnMam))
}

/// Drops a selection the downloader can't grab. One that `picks_up_again` is deleted, and the list
/// items that selected it no longer show it as selected, so the next import searches for them
/// again. Others are marked removed
async fn drop_selection(
    db: &Database<'_>,
    torrent: &SelectedTorrent,
    err: &anyhow::Error,
) -> Result<()> {
    let (_guard, rw) = db.rw_async().await?;
    if let Some(mut stored) = rw.get().primary::<SelectedTorrent>(torrent.mam_id)? {
        if picks_up_again(torrent, err) {
            info!(
                "Dropping torrent \"{}\" ({err}), it can be selected again",
                torrent.meta.title
            );
            clear_dropped_selection(&rw, torrent.mam_id)?;
            rw.remove(stored)?;
        } else {
            stored.removed_at = Some(Timestamp::now());
            rw.upsert(stored)?;
        }
    }
    rw.commit()?;
    Ok(())
}

/// Whether a wedge would be wasted on a torrent: it's free already, or it's VIP, where `fl` would
/// still spend one. Not a check that downloading it is free, which is `is_free()`
fn wedge_not_needed(info: &MaMTorrent) -> bool {
    info.is_free() || info.vip
}

/// Whether deciding what a selection spends needs the torrent as MaM returns it now. With
/// `skip_snatched`, every selection that can be downloaded is looked up, so that one the account
/// already snatched is skipped. A wedge selection without enough wedges isn't downloaded. This has
/// to agree with `decide_spend`, which takes a missing lookup to mean that MaM didn't return it
fn needs_lookup(cost: TorrentCost, wedges: u64, wedge_buffer: u64, skip_snatched: bool) -> bool {
    match cost {
        TorrentCost::GlobalFreeleech | TorrentCost::PersonalFreeleech | TorrentCost::Vip => true,
        TorrentCost::UseWedge => wedges > wedge_buffer,
        TorrentCost::TryWedge => wedges > wedge_buffer || skip_snatched,
        TorrentCost::Ratio => skip_snatched,
    }
}

/// Decides what grabbing a selection spends. `info` is the torrent as MaM returns it now,
/// looked up when `needs_lookup` says so.
///
/// With `skip_snatched`, a torrent the account already snatched isn't grabbed at all, and one that
/// MaM didn't return isn't grabbed on ratio either: these selections are always looked up when
/// they can be downloaded. A selection that was free when it was picked is only grabbed while it's
/// still free. No wedge is used on a torrent that's already free or VIP. A try_wedge selection made
/// by hand downloads a torrent the account already snatched on ratio, since it's probably in a
/// client already, while a wedge selection made by hand, picked with "Download with Wedge", uses
/// the wedge.
fn decide_spend(
    cost: TorrentCost,
    wedges: u64,
    wedge_buffer: u64,
    skip_snatched: bool,
    info: Option<&MaMTorrent>,
) -> Result<Spend, NotGrabbed> {
    if skip_snatched && info.is_some_and(|info| info.my_snatched) {
        return Err(NotGrabbed::SkippedSnatched);
    }
    let ratio = || {
        if skip_snatched && info.is_none() {
            Err(NotGrabbed::NotOnMam)
        } else {
            Ok(Spend::Ratio)
        }
    };
    match cost {
        TorrentCost::GlobalFreeleech | TorrentCost::PersonalFreeleech | TorrentCost::Vip => {
            match info {
                None => Err(NotGrabbed::NotOnMam),
                Some(info) if info.is_free() => Ok(Spend::Nothing),
                Some(_) => Err(NotGrabbed::NoLongerFree(cost)),
            }
        }
        TorrentCost::Ratio => ratio(),
        TorrentCost::UseWedge | TorrentCost::TryWedge => {
            let no_wedge = if wedges <= wedge_buffer {
                NotGrabbed::FewerWedges {
                    wedges,
                    wedge_buffer,
                }
            } else {
                match info {
                    None => NotGrabbed::NotOnMam,
                    Some(info) if wedge_not_needed(info) => return Ok(Spend::Nothing),
                    Some(info) if info.my_snatched && cost == TorrentCost::TryWedge => {
                        return Ok(Spend::Ratio);
                    }
                    Some(_) => return Ok(Spend::Wedge),
                }
            };
            // try_wedge still downloads, on ratio, when it can't use a wedge
            if cost == TorrentCost::TryWedge {
                ratio()
            } else {
                Err(no_wedge)
            }
        }
    }
}

/// The event for a torrent that was grabbed, or that a wedge was used on
fn grabbed_event(
    hash: String,
    torrent: &SelectedTorrent,
    cost: TorrentCost,
    wedged: bool,
) -> Event {
    Event::new(
        Some(hash),
        Some(torrent.mam_id),
        EventType::Grabbed {
            grabber: torrent.grabber.clone(),
            cost: Some(cost),
            wedged,
        },
    )
}

/// The cost a grab is recorded with: how the torrent is free now when it was looked up and isn't
/// wedged, else the selection's cost. A ratio or try_wedge selection is stored with its block's
/// cost even when the torrent was free. A wedge or try_wedge selection that spends nothing because
/// the torrent is VIP is recorded as VIP, even without a free flag
fn grabbed_cost(cost: TorrentCost, spend: Spend, info: Option<&MaMTorrent>) -> TorrentCost {
    match (spend, info) {
        (Spend::Wedge, _) | (_, None) => cost,
        (Spend::Nothing, Some(info)) if info.vip => TorrentCost::Vip,
        (_, Some(info)) => free_cost(info).unwrap_or(cost),
    }
}

/// What became of a selection that was handled without an error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grabbed {
    /// Added to qBittorrent, or found in a client or the library already
    Done,
    /// Not downloaded because the account already snatched it, and stored without files
    SkippedSnatched,
}

#[instrument(skip_all)]
async fn grab_torrent(
    config: &Config,
    db: &Database<'_>,
    qbit: &qbit::Api,
    qbit_url: &str,
    mam: &MaM<'_>,
    torrent: SelectedTorrent,
) -> Result<Grabbed> {
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
        return Ok(Grabbed::Done);
    }

    let user_info = mam.user_info().await?;
    let wedge_buffer = torrent.wedge_buffer.unwrap_or(config.wedge_buffer);
    // Selections made by hand are downloaded even when the account already snatched them
    let skip_snatched = !torrent.by_hand;
    let info = if needs_lookup(torrent.cost, user_info.wedges, wedge_buffer, skip_snatched) {
        mam.get_torrent_info_by_id(torrent.mam_id).await?
    } else {
        None
    };
    // A torrent that isn't grabbed at its cost, or that the account already snatched, may still
    // be in a client already. It's then recorded like any other duplicate, after downloading its
    // torrent file without `fl`
    let (spend, refused) = match decide_spend(
        torrent.cost,
        user_info.wedges,
        wedge_buffer,
        skip_snatched,
        info.as_ref(),
    ) {
        Ok(spend) => {
            debug!(
                "Grabbing torrent \"{}\" spends {spend:?}",
                torrent.meta.title
            );
            (spend, None)
        }
        // Not grabbed, its torrent file is only downloaded to check the clients
        Err(err @ (NotGrabbed::SkippedSnatched | NotGrabbed::NoLongerFree(_))) => {
            (Spend::Nothing, Some(err))
        }
        Err(err) => return Err(err.into()),
    };
    let will_wedge = spend == Spend::Wedge;
    if will_wedge {
        info!("Using wedge on torrent \"{}\"", torrent.meta.title);
    }
    let cost = grabbed_cost(torrent.cost, spend, info.as_ref());
    let fresh_dl_link = info.as_ref().and_then(|info| info.dl.as_deref());
    let (torrent_file_bytes, torrent_file) =
        fetch_torrent_file(db, mam, &torrent, will_wedge, fresh_dl_link).await?;
    let hash = torrent_file.info_hash();
    // The wedge is spent now, so it shows up even if the torrent is a duplicate or can't be added
    if will_wedge {
        if let Some((_, user_info)) = mam.user.lock().await.as_mut() {
            user_info.wedges = user_info.wedges.saturating_sub(1);
        }
        write_event(db, grabbed_event(hash.clone(), &torrent, cost, true)).await;
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
                upsert_hash_record(
                    &rw,
                    mlm_db::Torrent {
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
                    },
                )?;
            }
            rw.remove(torrent)?;
        } else {
            let mut t = torrent;
            t.started_at = Some(Timestamp::now());
            rw.upsert(t)?;
        }
        rw.commit()?;

        return Ok(Grabbed::Done);
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
        return Ok(Grabbed::Done);
    }

    if let Some(NotGrabbed::SkippedSnatched) = refused
        && let Some(info) = info
    {
        info!(
            "Skipping torrent \"{}\" as it was already snatched, storing it without files",
            torrent.meta.title
        );
        store_snatched(db, torrent, info).await?;
        return Ok(Grabbed::SkippedSnatched);
    }
    if let Some(err) = refused {
        return Err(err.into());
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

    let event = (!will_wedge).then(|| grabbed_event(hash.clone(), &torrent, cost, false));
    {
        let (_guard, rw) = db.rw_async().await?;
        upsert_hash_record(
            &rw,
            mlm_db::Torrent {
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
            },
        )?;
        let mut torrent = torrent;
        torrent.hash = Some(hash.clone());
        torrent.started_at = Some(Timestamp::now());
        rw.upsert(torrent)?;
        rw.commit()?;
    }

    if let Some(event) = event {
        write_event(db, event).await;
    }

    Ok(Grabbed::Done)
}

/// Stores a selection the account already snatched the way `select_torrents` stores a torrent it
/// skips: without files, unless MLM has a record of it already, and as found in the library on
/// the list items that selected it
async fn store_snatched(
    db: &Database<'_>,
    torrent: SelectedTorrent,
    info: MaMTorrent,
) -> Result<()> {
    let (guard, rw) = db.rw_async().await?;
    mark_snatched_existing(&rw, torrent.mam_id)?;
    let recorded = rw
        .get()
        .secondary::<mlm_db::Torrent>(TorrentKey::mam_id, torrent.mam_id)?
        .is_some();
    let meta = torrent.meta.clone();
    rw.remove(torrent)?;
    if recorded {
        rw.commit()?;
        return Ok(());
    }
    add_metadata_only_torrent((guard, rw), info, meta, false).await
}

/// Stores a torrent's record under its hash. A metadata only record of the same torrent, such as
/// the one stored for a torrent the account had already snatched, is replaced
fn upsert_hash_record(rw: &RwTransaction<'_>, mut record: mlm_db::Torrent) -> Result<()> {
    if let Some(other) = rw
        .get()
        .secondary::<mlm_db::Torrent>(TorrentKey::mam_id, record.mam_id)?
        && !other.id_is_hash
        && other.library_path.is_none()
    {
        record.abs_id = record.abs_id.or(other.abs_id.clone());
        record.goodreads_id = record.goodreads_id.or(other.goodreads_id);
        record.replaced_with = record.replaced_with.or(other.replaced_with.clone());
        rw.remove(other)?;
    }
    rw.upsert(record)?;
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
        let bytes = mam.get_torrent_file(dl_link, torrent.mam_id, wedge).await?;
        let parsed =
            Torrent::read_from_bytes(bytes.clone()).map_err(|_| anyhow::Error::new(StaleDlLink))?;
        return Ok((bytes, parsed));
    }

    let bytes = mam
        .get_torrent_file(&torrent.dl_link, torrent.mam_id, wedge)
        .await?;
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
    let bytes = mam
        .get_torrent_file(&dl_link, torrent.mam_id, wedge)
        .await?;
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

#[cfg(test)]
mod tests {
    use mlm_db::TorrentStatus;

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

    fn database() -> Database<'static> {
        native_db::Builder::new()
            .create_in_memory(&mlm_db::MODELS)
            .unwrap()
    }

    fn stored<T: native_db::ToInput>(db: &Database<'_>) -> Vec<T> {
        db.r_transaction()
            .unwrap()
            .scan()
            .primary()
            .unwrap()
            .all()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn put<T: native_db::ToInput>(db: &Database<'_>, item: T) {
        let rw = db.rw_transaction().unwrap();
        rw.upsert(item).unwrap();
        rw.commit().unwrap();
    }

    /// A torrent as a search returns it
    fn book() -> MaMTorrent {
        MaMTorrent {
            id: 1,
            title: "Book".to_owned(),
            author_info: [(1, "Ann Author".to_owned())].into(),
            added: "2025-01-01 00:00:00".to_owned(),
            main_cat: 13,
            category: mlm_db::AudiobookCategory::GeneralFiction.to_id() as u64,
            language: mlm_db::Language::English.to_id(),
            filetype: "m4b".to_owned(),
            numfiles: 1,
            size: "100 MiB".to_owned(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_hash_record_replaces_metadata_only_record() {
        let store_by_hash = |db: &Database<'_>, record: mlm_db::Torrent| -> Result<()> {
            let rw = db.rw_transaction()?;
            upsert_hash_record(&rw, record)?;
            rw.commit()?;
            Ok(())
        };

        let torrent = book();
        let meta = torrent.as_meta().unwrap();
        // A metadata only record, as stored for a torrent the account had already snatched
        let scratch = database();
        crate::autograbber::add_metadata_only_torrent(
            scratch.rw_async().await.unwrap(),
            torrent,
            meta,
            false,
        )
        .await
        .unwrap();
        let mut metadata_only = stored::<mlm_db::Torrent>(&scratch).pop().unwrap();
        metadata_only.goodreads_id = Some(5);
        let by_hash = mlm_db::Torrent {
            id: "hash".to_owned(),
            id_is_hash: true,
            goodreads_id: None,
            ..metadata_only.clone()
        };

        // It's replaced, and its Goodreads id kept
        let db = database();
        put(&db, metadata_only.clone());
        store_by_hash(&db, by_hash.clone()).unwrap();
        let records = stored::<mlm_db::Torrent>(&db);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "hash");
        assert_eq!(records[0].goodreads_id, Some(5));

        // A record under another hash is kept, even without files, so storing still fails
        let db = database();
        put(
            &db,
            mlm_db::Torrent {
                id: "other".to_owned(),
                ..by_hash.clone()
            },
        );
        assert!(store_by_hash(&db, by_hash.clone()).is_err());
        assert_eq!(stored::<mlm_db::Torrent>(&db)[0].id, "other");

        // So is a metadata only record with files
        let db = database();
        put(
            &db,
            mlm_db::Torrent {
                library_path: Some("/library/Book".into()),
                ..metadata_only
            },
        );
        assert!(store_by_hash(&db, by_hash).is_err());
        assert!(stored::<mlm_db::Torrent>(&db)[0].library_path.is_some());
    }

    #[tokio::test]
    async fn test_store_snatched() {
        let torrent = book();
        let meta = torrent.as_meta().unwrap();
        let selection = SelectedTorrent {
            mam_id: 1,
            goodreads_id: None,
            hash: None,
            dl_link: "dl".to_owned(),
            unsat_buffer: None,
            wedge_buffer: None,
            cost: TorrentCost::Ratio,
            category: None,
            tags: vec![],
            title_search: "book".to_owned(),
            meta,
            grabber: None,
            by_hand: false,
            created_at: Timestamp::now(),
            started_at: None,
            removed_at: None,
        };
        let found = |mam_id, status| {
            Some(mlm_db::ListItemTorrent {
                mam_id,
                status,
                at: Timestamp::now(),
            })
        };
        let list_item = |guid: &str, audio_torrent, ebook_torrent| mlm_db::ListItem {
            guid: ("list".to_owned(), guid.to_owned()),
            list_id: "list".to_owned(),
            title: "Book".to_owned(),
            authors: vec![],
            series: vec![],
            cover_url: String::new(),
            book_url: None,
            isbn: None,
            prefer_format: None,
            allow_audio: true,
            audio_torrent,
            allow_ebook: true,
            ebook_torrent,
            created_at: Timestamp::now(),
            marked_done_at: None,
        };
        let status = |field: &Option<mlm_db::ListItemTorrent>| {
            field.as_ref().map(|field| field.status.clone())
        };

        // The selection is replaced by a metadata only record, and the list items that selected
        // it show it as found in the library
        let db = database();
        put(&db, selection.clone());
        put(
            &db,
            list_item(
                "a",
                found(1, TorrentStatus::Selected),
                found(2, TorrentStatus::Selected),
            ),
        );
        put(&db, list_item("b", found(1, TorrentStatus::Wanted), None));
        store_snatched(&db, selection.clone(), torrent.clone())
            .await
            .unwrap();
        assert!(stored::<SelectedTorrent>(&db).is_empty());
        let records = stored::<mlm_db::Torrent>(&db);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].mam_id, 1);
        assert!(!records[0].id_is_hash);
        assert!(records[0].library_path.is_none());
        let items = stored::<mlm_db::ListItem>(&db);
        let item = |guid: &str| items.iter().find(|item| item.guid.1 == guid).unwrap();
        assert_eq!(
            status(&item("a").audio_torrent),
            Some(TorrentStatus::Existing)
        );
        assert_eq!(
            status(&item("a").ebook_torrent),
            Some(TorrentStatus::Selected)
        );
        assert_eq!(status(&item("b").audio_torrent), Some(TorrentStatus::Wanted));

        // When MLM already has a record of the torrent, only the selection is removed
        let db = database();
        let record = records[0].clone();
        put(&db, record.clone());
        put(&db, selection.clone());
        store_snatched(&db, selection, torrent).await.unwrap();
        assert!(stored::<SelectedTorrent>(&db).is_empty());
        let records = stored::<mlm_db::Torrent>(&db);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, record.id);
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
            // A selection made by hand is only looked up when there's a wedge to spend
            assert!(needs_lookup(cost, 1, 0, false));
            assert!(!needs_lookup(cost, 0, 0, false));
            assert!(!needs_lookup(cost, 2, 2, false));

            for skip_snatched in [false, true] {
                let spend = |wedges, wedge_buffer, info| {
                    decide_spend(cost, wedges, wedge_buffer, skip_snatched, Some(info))
                };
                assert_eq!(spend(1, 0, &plain), Ok(Spend::Wedge));
                assert_eq!(spend(3, 2, &plain), Ok(Spend::Wedge));
                // No wedge on torrents that are already free, `fl` would spend one on VIP too
                for info in [&free, &personal, &vip] {
                    assert_eq!(spend(1, 0, info), Ok(Spend::Nothing));
                }
            }
            assert_eq!(
                decide_spend(cost, 1, 0, false, Some(&free_snatched)),
                Ok(Spend::Nothing)
            );
        }

        // By hand, a wedge picked with "Download with Wedge" is used on a torrent the account
        // already snatched. try_wedge downloads it on ratio instead, as it's probably in a
        // client already
        let wedge = |wedges, wedge_buffer, info| {
            decide_spend(TorrentCost::UseWedge, wedges, wedge_buffer, false, info)
        };
        let try_wedge = |wedges, wedge_buffer, info| {
            decide_spend(TorrentCost::TryWedge, wedges, wedge_buffer, false, info)
        };
        assert_eq!(wedge(1, 0, Some(&snatched)), Ok(Spend::Wedge));
        assert_eq!(wedge(3, 2, Some(&snatched)), Ok(Spend::Wedge));
        assert_eq!(try_wedge(1, 0, Some(&snatched)), Ok(Spend::Ratio));
        // Neither uses a wedge on one that's VIP now
        let vip_snatched = MaMTorrent {
            vip: true,
            fl_vip: true,
            my_snatched: true,
            ..Default::default()
        };
        assert_eq!(wedge(1, 0, Some(&vip_snatched)), Ok(Spend::Nothing));
        assert_eq!(try_wedge(1, 0, Some(&vip_snatched)), Ok(Spend::Nothing));

        // By hand, no wedge on a torrent that MaM doesn't return, or when wedges run out. wedge
        // isn't grabbed then, try_wedge falls back to ratio
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
        let [plain, free, personal, vip, snatched, free_snatched] = infos();
        // A ratio selection made by hand isn't looked up
        assert!(!needs_lookup(TorrentCost::Ratio, 1, 0, false));
        assert_eq!(
            decide_spend(TorrentCost::Ratio, 1, 0, false, None),
            Ok(Spend::Ratio)
        );

        for cost in [
            TorrentCost::GlobalFreeleech,
            TorrentCost::PersonalFreeleech,
            TorrentCost::Vip,
        ] {
            for skip_snatched in [false, true] {
                // Free costs are checked before the torrent file is downloaded, wedges or not
                assert!(needs_lookup(cost, 1, 0, skip_snatched));
                assert!(needs_lookup(cost, 0, 0, skip_snatched));
                let spend =
                    |wedges, info| decide_spend(cost, wedges, 0, skip_snatched, info);
                for info in [&free, &personal, &vip] {
                    assert_eq!(spend(0, Some(info)), Ok(Spend::Nothing));
                }
                assert_eq!(spend(1, Some(&plain)), Err(NotGrabbed::NoLongerFree(cost)));
                assert_eq!(spend(1, None), Err(NotGrabbed::NotOnMam));
            }
            assert_eq!(
                decide_spend(cost, 0, 0, false, Some(&free_snatched)),
                Ok(Spend::Nothing)
            );
            assert_eq!(
                decide_spend(cost, 1, 0, false, Some(&snatched)),
                Err(NotGrabbed::NoLongerFree(cost))
            );
        }
    }

    #[test]
    fn test_skip_snatched() {
        let [plain, free, _, _, snatched, free_snatched] = infos();
        for cost in [
            TorrentCost::GlobalFreeleech,
            TorrentCost::PersonalFreeleech,
            TorrentCost::Vip,
            TorrentCost::UseWedge,
            TorrentCost::TryWedge,
            TorrentCost::Ratio,
        ] {
            for (wedges, wedge_buffer) in [(0, 0), (1, 0), (2, 2), (3, 2)] {
                // Every selection that can be downloaded is looked up. A wedge selection isn't
                // downloaded without enough wedges
                let can_download = cost != TorrentCost::UseWedge || wedges > wedge_buffer;
                assert_eq!(
                    needs_lookup(cost, wedges, wedge_buffer, true),
                    can_download,
                    "{cost:?}, {wedges} wedges, buffer {wedge_buffer}"
                );
                // It isn't grabbed once the account snatched it, whatever it costs now
                if can_download {
                    for info in [&snatched, &free_snatched] {
                        assert_eq!(
                            decide_spend(cost, wedges, wedge_buffer, true, Some(info)),
                            Err(NotGrabbed::SkippedSnatched),
                            "{cost:?}, {wedges} wedges, buffer {wedge_buffer}"
                        );
                    }
                }
            }
        }

        // Ratio and try_wedge without enough wedges still download on ratio, whether or not the
        // torrent is free now. One that MaM doesn't return isn't downloaded with the stored dl
        // link, it's gone from MaM
        for info in [&plain, &free] {
            assert_eq!(
                decide_spend(TorrentCost::Ratio, 1, 0, true, Some(info)),
                Ok(Spend::Ratio)
            );
            assert_eq!(
                decide_spend(TorrentCost::TryWedge, 2, 2, true, Some(info)),
                Ok(Spend::Ratio)
            );
        }
        for (wedges, wedge_buffer) in [(0, 0), (1, 0), (2, 2), (3, 2)] {
            for cost in [TorrentCost::Ratio, TorrentCost::TryWedge] {
                assert_eq!(
                    decide_spend(cost, wedges, wedge_buffer, true, None),
                    Err(NotGrabbed::NotOnMam),
                    "{cost:?}, {wedges} wedges, buffer {wedge_buffer}"
                );
            }
        }

        // A selection made by hand is grabbed anyway
        assert_eq!(
            decide_spend(TorrentCost::Ratio, 1, 0, false, Some(&snatched)),
            Ok(Spend::Ratio)
        );
        assert_eq!(
            decide_spend(TorrentCost::Vip, 1, 0, false, Some(&free_snatched)),
            Ok(Spend::Nothing)
        );
    }

    #[test]
    fn test_vip_without_free_flags() {
        let vip_only = MaMTorrent {
            vip: true,
            ..Default::default()
        };
        // A free selection is only grabbed while one of the free flags is set
        for cost in [
            TorrentCost::GlobalFreeleech,
            TorrentCost::PersonalFreeleech,
            TorrentCost::Vip,
        ] {
            for skip_snatched in [false, true] {
                assert_eq!(
                    decide_spend(cost, 1, 0, skip_snatched, Some(&vip_only)),
                    Err(NotGrabbed::NoLongerFree(cost))
                );
            }
        }
        // But no wedge is used on it, since `fl` spends one even on VIP torrents
        for cost in [TorrentCost::UseWedge, TorrentCost::TryWedge] {
            for skip_snatched in [false, true] {
                assert_eq!(
                    decide_spend(cost, 1, 0, skip_snatched, Some(&vip_only)),
                    Ok(Spend::Nothing)
                );
            }
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
                for skip_snatched in [false, true] {
                    if !needs_lookup(cost, wedges, wedge_buffer, skip_snatched) {
                        assert_ne!(
                            decide_spend(cost, wedges, wedge_buffer, skip_snatched, None),
                            Err(NotGrabbed::NotOnMam),
                            "{cost:?}, {wedges} wedges, buffer {wedge_buffer}, {skip_snatched}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_drops_selection() {
        assert!(drops_selection(&anyhow::Error::new(StaleDlLink)));
        assert!(drops_selection(&anyhow::Error::new(NotGrabbed::NotOnMam)));
        assert!(drops_selection(&anyhow::Error::new(
            NotGrabbed::NoLongerFree(TorrentCost::GlobalFreeleech)
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

    #[test]
    fn test_grabbed_cost() {
        let [plain, free, personal, vip, _, _] = infos();
        // A free selection, or a wedge or try_wedge one that needs no wedge, is recorded as the
        // torrent is free now
        for (cost, spend) in [
            (TorrentCost::GlobalFreeleech, Spend::Nothing),
            (TorrentCost::Vip, Spend::Nothing),
            (TorrentCost::UseWedge, Spend::Nothing),
            (TorrentCost::TryWedge, Spend::Nothing),
            // A ratio or try_wedge selection downloaded on ratio while the torrent is free
            (TorrentCost::Ratio, Spend::Ratio),
            (TorrentCost::TryWedge, Spend::Ratio),
        ] {
            assert_eq!(
                grabbed_cost(cost, spend, Some(&free)),
                TorrentCost::GlobalFreeleech
            );
            assert_eq!(
                grabbed_cost(cost, spend, Some(&personal)),
                TorrentCost::PersonalFreeleech
            );
            assert_eq!(grabbed_cost(cost, spend, Some(&vip)), TorrentCost::Vip);
        }
        // Downloaded on ratio while it isn't free, or not looked up (made by hand), it keeps its
        // own cost
        for cost in [TorrentCost::Ratio, TorrentCost::TryWedge] {
            assert_eq!(grabbed_cost(cost, Spend::Ratio, Some(&plain)), cost);
            assert_eq!(grabbed_cost(cost, Spend::Ratio, None), cost);
        }
        // A wedge selection that gets no wedge because the torrent is VIP is recorded as VIP, even
        // without any of the free flags. Downloaded on ratio, it keeps its own cost
        let vip_only = MaMTorrent {
            vip: true,
            ..Default::default()
        };
        for cost in [TorrentCost::UseWedge, TorrentCost::TryWedge] {
            assert_eq!(
                grabbed_cost(cost, Spend::Nothing, Some(&vip_only)),
                TorrentCost::Vip
            );
        }
        for cost in [TorrentCost::TryWedge, TorrentCost::Ratio] {
            assert_eq!(grabbed_cost(cost, Spend::Ratio, Some(&vip_only)), cost);
        }
        // A wedged grab keeps the selection's cost
        assert_eq!(
            grabbed_cost(TorrentCost::UseWedge, Spend::Wedge, Some(&plain)),
            TorrentCost::UseWedge
        );
    }

    #[tokio::test]
    async fn test_drop_selection() {
        let torrent = MaMTorrent {
            dl: Some("dl".to_owned()),
            ..book()
        };
        let selection = SelectedTorrent {
            mam_id: 1,
            goodreads_id: None,
            hash: None,
            dl_link: "dl".to_owned(),
            unsat_buffer: None,
            wedge_buffer: None,
            cost: TorrentCost::GlobalFreeleech,
            category: None,
            tags: vec![],
            title_search: mlm_parse::normalize_title("Book"),
            meta: torrent.as_meta().unwrap(),
            grabber: None,
            by_hand: false,
            created_at: Timestamp::now(),
            started_at: None,
            removed_at: None,
        };
        let found = |mam_id, status| {
            Some(mlm_db::ListItemTorrent {
                mam_id,
                status,
                at: Timestamp::now(),
            })
        };
        let list_item = |guid: &str, audio_torrent, ebook_torrent| mlm_db::ListItem {
            guid: ("list".to_owned(), guid.to_owned()),
            list_id: "list".to_owned(),
            title: "Book".to_owned(),
            authors: vec![],
            series: vec![],
            cover_url: String::new(),
            book_url: None,
            isbn: None,
            prefer_format: None,
            allow_audio: true,
            audio_torrent,
            allow_ebook: true,
            ebook_torrent,
            created_at: Timestamp::now(),
            marked_done_at: None,
        };
        let status = |items: &[mlm_db::ListItem], guid: &str| {
            let item = items.iter().find(|item| item.guid.1 == guid).unwrap();
            [&item.audio_torrent, &item.ebook_torrent]
                .map(|field| field.as_ref().map(|t| (t.mam_id, t.status.clone())))
        };
        let no_longer_free = || anyhow::Error::new(NotGrabbed::NoLongerFree(TorrentCost::Vip));

        // A torrent that's no longer free, or gone from MaM, is deleted, and the list items that
        // selected it want it again
        for err in [no_longer_free(), anyhow::Error::new(NotGrabbed::NotOnMam)] {
            let db = database();
            put(&db, selection.clone());
            put(
                &db,
                list_item(
                    "a",
                    found(1, TorrentStatus::Selected),
                    found(2, TorrentStatus::Selected),
                ),
            );
            put(&db, list_item("b", found(1, TorrentStatus::Wanted), None));
            put(&db, list_item("c", None, found(1, TorrentStatus::Selected)));
            drop_selection(&db, &selection, &err).await.unwrap();
            assert!(stored::<SelectedTorrent>(&db).is_empty());
            let items = stored::<mlm_db::ListItem>(&db);
            assert_eq!(
                status(&items, "a"),
                [None, Some((2, TorrentStatus::Selected))]
            );
            assert_eq!(
                status(&items, "b"),
                [Some((1, TorrentStatus::Wanted)), None]
            );
            assert_eq!(status(&items, "c"), [None, None]);
            // The item wants the audiobook again
            let item = items.iter().find(|item| item.guid.1 == "a").unwrap();
            assert!(item.want_audio());
        }

        // Once deleted, an import can select the torrent again as its block allows, here on ratio
        // now that it's no longer free, and not by a free block
        let db = std::sync::Arc::new(database());
        let mam = MaM::offline(db.clone());
        let config: Config = toml::from_str("mam_id = \"test\"").unwrap();
        let select = |cost: crate::config::Cost, torrent: MaMTorrent| {
            let (db, mam, config) = (&db, &mam, &config);
            async move {
                crate::autograbber::select_torrents(
                    config,
                    db,
                    mam,
                    [torrent].into_iter(),
                    &Default::default(),
                    cost,
                    None,
                    None,
                    None,
                    false,
                    u64::MAX,
                    None,
                )
                .await
                .unwrap()
                .selected
            }
        };
        put(&db, selection.clone());
        assert_eq!(select(crate::config::Cost::Ratio, torrent.clone()).await, 0);
        drop_selection(&db, &selection, &no_longer_free())
            .await
            .unwrap();
        assert_eq!(select(crate::config::Cost::Free, torrent.clone()).await, 0);
        assert_eq!(select(crate::config::Cost::Ratio, torrent.clone()).await, 1);
        let selections = stored::<SelectedTorrent>(&db);
        assert_eq!(selections.len(), 1);
        assert_eq!(selections[0].cost, TorrentCost::Ratio);

        // So can another upload of the same book, which the dropped selection kept out
        let db = std::sync::Arc::new(database());
        let mam = MaM::offline(db.clone());
        put(&db, selection.clone());
        let other = MaMTorrent {
            id: 2,
            dl: Some("dl2".to_owned()),
            ..torrent.clone()
        };
        let filter = crate::config::TorrentFilter::default();
        let select_other = || {
            crate::autograbber::select_torrents(
                &config,
                &db,
                &mam,
                [other.clone()].into_iter(),
                &filter,
                crate::config::Cost::Ratio,
                None,
                None,
                None,
                false,
                u64::MAX,
                None,
            )
        };
        assert_eq!(select_other().await.unwrap().selected, 0);
        drop_selection(&db, &selection, &no_longer_free())
            .await
            .unwrap();
        assert_eq!(select_other().await.unwrap().selected, 1);

        // A stale dl link, or a selection made by hand, is kept as removed, and so are the list
        // items that selected it
        for (err, by_hand) in [
            (anyhow::Error::new(StaleDlLink), false),
            (no_longer_free(), true),
            (anyhow::Error::new(NotGrabbed::NotOnMam), true),
        ] {
            let db = database();
            let selection = SelectedTorrent {
                by_hand,
                ..selection.clone()
            };
            put(&db, selection.clone());
            put(&db, list_item("a", found(1, TorrentStatus::Selected), None));
            drop_selection(&db, &selection, &err).await.unwrap();
            let selections = stored::<SelectedTorrent>(&db);
            assert_eq!(selections.len(), 1);
            assert!(selections[0].removed_at.is_some());
            assert_eq!(
                status(&stored::<mlm_db::ListItem>(&db), "a"),
                [Some((1, TorrentStatus::Selected)), None]
            );
        }
    }
}
