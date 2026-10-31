use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Error, Result, bail};
use bytes::Bytes;
use cookie::Cookie;
use mlm_db::DatabaseExt as _;
use native_db::Database;
use reqwest::Url;
use reqwest_cookie_store::CookieStoreRwLock;
use time::{OffsetDateTime, UtcDateTime};
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace, warn};

use crate::{
    // config::{Config, SnatchlistType},
    enums::SnatchlistType,
    search::{MaMTorrent, SearchError, SearchFields, SearchQuery, SearchResult, Tor},
    user_data::UserResponse,
    user_torrent::UserDetailsTorrentResponse,
};

#[derive(thiserror::Error, Debug)]
#[error("Account is blocked or unsat limit reached (HTTP 406 Not Acceptable)")]
pub struct AccountBlockedError;

#[derive(thiserror::Error, Debug)]
#[error("Hit MaM rate limit")]
pub struct RateLimitError;

/// A request that wasn't sent, because requests to MaM are paused
#[derive(thiserror::Error, Debug)]
#[error("Requests to MaM are paused for {} more minutes after {reason}", minutes(*.remaining))]
pub struct RequestsPausedError {
    pub reason: PauseReason,
    pub remaining: Duration,
}

/// Whether MaM refused a request, by rate limiting it or blocking the account, or the request
/// wasn't sent because requests are paused after such a refusal
pub fn refused_or_paused(err: &anyhow::Error) -> bool {
    err.is::<RateLimitError>() || err.is::<AccountBlockedError>() || err.is::<RequestsPausedError>()
}

fn minutes(duration: Duration) -> u64 {
    duration.as_secs().div_ceil(60)
}

/// What started a pause
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseReason {
    RateLimited,
    AccountBlocked,
}

impl fmt::Display for PauseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PauseReason::RateLimited => write!(f, "an HTTP 429 (rate limit)"),
            PauseReason::AccountBlocked => {
                write!(f, "an HTTP 406 (account blocked or unsat limit reached)")
            }
        }
    }
}

/// After MaM rate limits a request or blocks the account, no request is sent to MaM for a
/// while: 30 minutes at first, then twice as long each time MaM refuses again before a request
/// succeeds, up to a day. The first request that succeeds after a pause starts over. This is
/// only kept in memory, so a restart ends the pause
#[derive(Debug, Default)]
struct RequestPause {
    refusals: u32,
    until: Option<(Instant, PauseReason)>,
}

impl RequestPause {
    const FIRST_WAIT: Duration = Duration::from_secs(30 * 60);
    const MAX_WAIT: Duration = Duration::from_secs(24 * 60 * 60);

    /// The error for a request that isn't sent, while paused
    pub fn paused(&self, now: Instant) -> Option<RequestsPausedError> {
        self.until
            .filter(|(until, _)| now < *until)
            .map(|(until, reason)| RequestsPausedError {
                reason,
                remaining: until - now,
            })
    }

    /// Records that MaM refused a request. Requests sent before a pause started can still come
    /// back refused while it lasts, and don't make it longer
    pub fn refused(&mut self, reason: PauseReason, now: Instant) -> Option<Duration> {
        if self.paused(now).is_some() {
            return None;
        }
        self.refusals = self.refusals.saturating_add(1);
        let wait = Self::FIRST_WAIT
            .saturating_mul(2u32.saturating_pow(self.refusals - 1))
            .min(Self::MAX_WAIT);
        self.until = Some((now + wait, reason));
        Some(wait)
    }

    /// Records that MaM answered a request with a success status. Requests sent before a pause
    /// started can still succeed while it lasts, and don't end it
    pub fn succeeded(&mut self, now: Instant) {
        if self.paused(now).is_none() {
            self.refusals = 0;
            self.until = None;
        }
    }

    /// Records the status MaM answered a request with. A rate limit (429) or blocked account
    /// (406) is returned as an error and starts a pause, a success status after a pause starts
    /// the doubling over, even if reading the body fails later, and any other status is left to
    /// the caller
    pub fn answered(&mut self, status: reqwest::StatusCode, now: Instant) -> Result<()> {
        let reason = match status.as_u16() {
            429 => PauseReason::RateLimited,
            406 => PauseReason::AccountBlocked,
            _ => {
                if status.is_success() {
                    self.succeeded(now);
                }
                return Ok(());
            }
        };
        if let Some(wait) = self.refused(reason, now) {
            warn!(
                "MaM refused a request with {reason}, pausing all requests to MaM for {} minutes",
                minutes(wait)
            );
        }
        Err(match reason {
            PauseReason::RateLimited => anyhow::Error::new(RateLimitError),
            PauseReason::AccountBlocked => anyhow::Error::new(AccountBlockedError),
        })
    }
}

pub struct MaM<'a> {
    jar: Arc<CookieStoreRwLock>,
    client: reqwest::Client,
    db: Arc<Database<'a>>,
    pub user: Arc<Mutex<Option<(SystemTime, UserResponse)>>>,
    pause: std::sync::Mutex<RequestPause>,
}

impl<'a> MaM<'a> {
    pub async fn new(mam_id: &str, db: Arc<Database<'a>>, version: &str) -> Result<MaM<'a>> {
        let jar: CookieStoreRwLock = Default::default();
        let url = "https://www.myanonamouse.net/json".parse::<Url>().unwrap();

        let stored_mam_id = db
            .r_transaction()
            .and_then(|r| r.get().primary::<mlm_db::Config>("mam_id"))
            .ok()
            .flatten()
            .map(|c| c.value);
        if stored_mam_id.is_some() {
            info!("Restoring mam_id");
        } else {
            info!("Using mam_id from config");
        }
        let has_stored_mam_id = stored_mam_id.is_some();
        let cookie = Cookie::build(("mam_id", stored_mam_id.unwrap_or(mam_id.to_owned())))
            .expires(OffsetDateTime::now_local()? + Duration::from_mins(10))
            .build();
        jar.write()
            .unwrap()
            .store_response_cookies([cookie].into_iter(), &url);

        let jar = Arc::new(jar);
        let client = reqwest::Client::builder()
            .cookie_provider(jar.clone())
            .user_agent(format!("MLM/{version}"))
            .timeout(Duration::from_secs(20))
            .build()?;

        let mam = MaM {
            jar,
            client,
            db,
            user: Default::default(),
            pause: Default::default(),
        };
        if let Err(err) = mam.user_info().await {
            if falls_back_to_config(has_stored_mam_id, &err) {
                warn!("Stored mam_id failed with {err}, falling back to config value");
                let cookie = Cookie::build(("mam_id", mam_id.to_owned()))
                    .expires(OffsetDateTime::now_local()? + Duration::from_mins(10))
                    .build();
                mam.jar
                    .write()
                    .unwrap()
                    .store_response_cookies([cookie].into_iter(), &url);

                // Retry with user_info()
                mam.user_info().await?;
            } else {
                return Err(err);
            }
        }

        Ok(mam)
    }

    /// The pause in effect, if requests to MaM are paused
    pub fn requests_paused(&self) -> Option<RequestsPausedError> {
        self.pause.lock().unwrap().paused(Instant::now())
    }

    /// When the pause in effect ends, if requests to MaM are paused
    pub fn paused_until(&self) -> Option<Instant> {
        let now = Instant::now();
        self.pause
            .lock()
            .unwrap()
            .paused(now)
            .map(|paused| now + paused.remaining)
    }

    /// Sends a request unless requests are paused. A rate limit (429) or blocked account (406)
    /// starts a pause, and any other status is left to the caller
    async fn send(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        if let Some(paused) = self.requests_paused() {
            return Err(paused.into());
        }
        let resp = request.send().await?;
        self.pause
            .lock()
            .unwrap()
            .answered(resp.status(), Instant::now())?;
        Ok(resp)
    }

    pub async fn check_mam_id(&self) -> Result<()> {
        let resp = self
            .send(
                self.client
                    .get("https://www.myanonamouse.net/json/checkCookie.php"),
            )
            .await?;

        let status = resp.status();
        let text = resp.text().await?;
        info!("checked mam_id: {text:?}");

        if status.is_client_error() || status.is_server_error() {
            bail!("Bad status for checkCookie: {}", status)
        }

        // Check if the body contains "Success":true
        if !text.contains("\"Success\":true") {
            bail!(
                "Session check failed (Success: false in response): {}",
                text
            )
        }

        self.store_cookies().await;
        Ok(())
    }

    pub async fn user_info(&self) -> Result<UserResponse> {
        let mut cache = self.user.lock().await;
        if let Some((at, user_info)) = cache.as_ref()
            && SystemTime::now()
                .duration_since(*at)
                .is_ok_and(|d| d < Duration::from_secs(60))
        {
            return Ok(user_info.clone());
        }
        let resp: UserResponse = self
            .send(
                self.client
                    .get("https://www.myanonamouse.net/jsonLoad.php?snatch_summary=true"),
            )
            .await?
            .error_for_status()?
            .json()
            .await?;
        self.store_cookies().await;
        cache.replace((SystemTime::now(), resp.clone()));
        Ok(resp)
    }

    pub async fn cached_user_info(&self) -> Option<UserResponse> {
        self.user
            .lock()
            .await
            .as_ref()
            .map(|(_, user_info)| user_info.clone())
    }

    pub async fn add_unsats(&self, unsats: u64) {
        if let Some((_, user)) = self.user.lock().await.as_mut() {
            user.snatch_summary.unsat.count += unsats;
        }
    }

    pub async fn get_torrent_file(&self, dl_hash: &str, tid: u64, wedge: bool) -> Result<Bytes> {
        let resp = self
            .send(self.client.get(torrent_file_url(dl_hash, tid, wedge)))
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(resp)
    }

    pub async fn get_torrent_info(&self, hash: &str) -> Result<Option<MaMTorrent>> {
        let mut resp = self
            .search(&SearchQuery {
                fields: SearchFields {
                    description: true,
                    isbn: true,
                    ..Default::default()
                },
                tor: Tor {
                    hash: hash.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?;
        Ok(resp.data.pop().map(|mut t| {
            t.fix();
            t
        }))
    }
    pub async fn get_torrent_info_by_id(&self, mam_id: u64) -> Result<Option<MaMTorrent>> {
        let mut resp = self
            .search(&SearchQuery {
                fields: SearchFields {
                    description: true,
                    media_info: true,
                    isbn: true,
                    dl_link: true,
                    ..Default::default()
                },
                tor: Tor {
                    id: mam_id,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?;
        Ok(resp.data.pop().map(|mut t| {
            t.fix();
            t
        }))
    }

    pub async fn search(&self, query: &SearchQuery) -> Result<SearchResult> {
        debug!("search: {}", serde_json::to_string_pretty(query)?);
        let resp = self
            .send(
                self.client
                    .post("https://www.myanonamouse.net/tor/js/loadSearchJSONbasic.php")
                    .json(query),
            )
            .await?
            .error_for_status()?
            .text()
            .await?;
        if let Ok(resp) = serde_json::from_str::<SearchError>(&resp) {
            if resp.error == "Nothing returned, out of 0" {
                return Ok(SearchResult::default());
            } else {
                return Err(Error::msg(resp.error));
            }
        };
        let mut resp: SearchResult = serde_json::from_str(&resp).map_err(|err| {
            error!("Error parsing mam response: {err}\nResponse: {resp}");
            err
        })?;
        for t in &mut resp.data {
            t.fix();
        }
        self.store_cookies().await;
        Ok(resp)
    }

    pub async fn snatchlist(
        &self,
        kind: SnatchlistType,
        page: u64,
        timestamp: UtcDateTime,
    ) -> Result<UserDetailsTorrentResponse> {
        debug!("snatchlist: {:?} {}", kind, page);
        let user = self.user_info().await?;
        let resp = self
            .send(
                self.client
                    .get("https://cdn.myanonamouse.net/json/loadUserDetailsTorrents.php")
                    .query(&(
                        ("uid", user.uid.to_string()),
                        ("iteration", page.to_string()),
                        ("type", kind.to_string()),
                        ("cacheTime", timestamp.unix_timestamp().to_string()),
                    )),
            )
            .await?
            .error_for_status()?
            .text()
            .await?;
        let resp: UserDetailsTorrentResponse = serde_json::from_str(&resp).map_err(|err| {
            error!("Error parsing mam response: {err}\nResponse: {resp}");
            err
        })?;
        self.store_cookies().await;
        Ok(resp)
    }

    async fn store_cookies(&self) {
        let Ok(jar) = self.jar.read() else {
            return;
        };
        let Some(cookie) = jar.get("myanonamouse.net", "/", "mam_id") else {
            return;
        };
        let value = cookie.value().to_string();
        drop(jar);
        let Ok((_guard, rw)) = self.db.rw_try() else {
            return;
        };
        let ok = rw
            .upsert(mlm_db::Config {
                key: "mam_id".to_string(),
                value,
            })
            .and_then(|_| rw.commit())
            .is_ok();

        if ok {
            trace!("stored new mam_id");
        }
    }
}

/// Whether the value from the config is tried at startup after the stored one failed. After a
/// rate limit or blocked account, requests are paused, so it couldn't be tried anyway, and the
/// error MaM answered with is the one to show
fn falls_back_to_config(has_stored_value: bool, err: &anyhow::Error) -> bool {
    has_stored_value && !refused_or_paused(err)
}

/// Build the download URL for a torrent.
///
/// `tid` is required. The API docs give the dl hash form as
/// `/tor/download.php/<hash>?tid=<id>`; without the `tid` argument the request
/// is invalid and the site rejects it.
///
/// `wedge` adds the `fl` flag, which spends a personal freeleech wedge on the
/// torrent as part of the download. It replaces the bonusBuy.php endpoint,
/// which the site no longer exposes for wedges.
fn torrent_file_url(dl_hash: &str, tid: u64, wedge: bool) -> String {
    let fl = if wedge { "&fl" } else { "" };
    format!("https://www.myanonamouse.net/tor/download.php/{dl_hash}?tid={tid}{fl}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_torrent_file_url_includes_tid() {
        assert_eq!(
            torrent_file_url("abc123", 1234567890, false),
            "https://www.myanonamouse.net/tor/download.php/abc123?tid=1234567890"
        );
    }

    #[test]
    fn test_torrent_file_url_wedge_adds_fl() {
        assert_eq!(
            torrent_file_url("abc123", 1234567890, true),
            "https://www.myanonamouse.net/tor/download.php/abc123?tid=1234567890&fl"
        );
    }

    const MINUTE: Duration = Duration::from_secs(60);

    #[test]
    fn test_pause_starts_on_rate_limit_and_blocked_account() {
        for reason in [PauseReason::RateLimited, PauseReason::AccountBlocked] {
            let mut pause = RequestPause::default();
            let start = Instant::now();
            assert!(pause.paused(start).is_none());

            assert_eq!(pause.refused(reason, start), Some(30 * MINUTE));
            let paused = pause.paused(start + 10 * MINUTE).unwrap();
            assert_eq!(paused.reason, reason);
            assert_eq!(paused.remaining, 20 * MINUTE);
            assert!(
                pause
                    .paused(start + 30 * MINUTE - Duration::from_secs(1))
                    .is_some()
            );
            assert!(pause.paused(start + 30 * MINUTE).is_none());
        }
    }

    #[test]
    fn test_pause_doubles_up_to_a_day() {
        let mut pause = RequestPause::default();
        let start = Instant::now();
        let minutes = |m: u32| start + m * MINUTE;

        // A request every 10 minutes that MaM keeps refusing: only the first one after each
        // pause is sent, after 30 minutes, then 1, 2, 4, 8 and 16 hours, then a day
        let mut sent_at = vec![];
        for m in (0..=60 * 80).step_by(10) {
            if pause.paused(minutes(m)).is_none() {
                pause.refused(PauseReason::RateLimited, minutes(m));
                sent_at.push(m);
            }
        }
        assert_eq!(sent_at, vec![0, 30, 90, 210, 450, 930, 1890, 3330, 4770]);
    }

    #[test]
    fn test_pause_ignores_answers_to_earlier_requests() {
        let mut pause = RequestPause::default();
        let start = Instant::now();

        // Requests sent before the pause started neither make it longer nor end it
        pause.refused(PauseReason::RateLimited, start);
        assert_eq!(
            pause.refused(PauseReason::AccountBlocked, start + MINUTE),
            None
        );
        pause.succeeded(start + 2 * MINUTE);
        let paused = pause.paused(start + 10 * MINUTE).unwrap();
        assert_eq!(paused.reason, PauseReason::RateLimited);
        assert_eq!(paused.remaining, 20 * MINUTE);

        // A second refusal right after the pause doubles it
        assert_eq!(
            pause.refused(PauseReason::RateLimited, start + 30 * MINUTE),
            Some(60 * MINUTE)
        );
    }

    #[test]
    fn test_pause_ends_on_success() {
        let mut pause = RequestPause::default();
        let start = Instant::now();
        let minutes = |m: u32| start + m * MINUTE;

        pause.refused(PauseReason::RateLimited, minutes(0));
        pause.refused(PauseReason::RateLimited, minutes(30));
        assert!(pause.paused(minutes(89)).is_some());

        // The first request after the pause succeeds, so the next refusal starts over at 30
        // minutes
        pause.succeeded(minutes(90));
        assert!(pause.paused(minutes(90)).is_none());
        assert_eq!(
            pause.refused(PauseReason::AccountBlocked, minutes(100)),
            Some(30 * MINUTE)
        );
        assert!(pause.paused(minutes(129)).is_some());
        assert!(pause.paused(minutes(130)).is_none());
    }

    #[test]
    fn test_pause_from_status() {
        use reqwest::StatusCode;

        let mut pause = RequestPause::default();
        let start = Instant::now();
        let minutes = |m: u32| start + m * MINUTE;

        // Other errors are left to the caller, and don't start a pause
        for status in [StatusCode::FORBIDDEN, StatusCode::INTERNAL_SERVER_ERROR] {
            assert!(pause.answered(status, minutes(0)).is_ok());
            assert!(pause.paused(minutes(0)).is_none());
        }

        let err = pause
            .answered(StatusCode::TOO_MANY_REQUESTS, minutes(0))
            .unwrap_err();
        assert!(err.is::<RateLimitError>());
        assert_eq!(
            pause.paused(minutes(0)).unwrap().reason,
            PauseReason::RateLimited
        );

        // Another error after the pause doesn't start over, so the next refusal doubles it
        assert!(
            pause
                .answered(StatusCode::INTERNAL_SERVER_ERROR, minutes(30))
                .is_ok()
        );
        let err = pause
            .answered(StatusCode::NOT_ACCEPTABLE, minutes(40))
            .unwrap_err();
        assert!(err.is::<AccountBlockedError>());
        let paused = pause.paused(minutes(40)).unwrap();
        assert_eq!(paused.reason, PauseReason::AccountBlocked);
        assert_eq!(paused.remaining, 60 * MINUTE);

        // A success after the pause starts over
        assert!(pause.answered(StatusCode::OK, minutes(100)).is_ok());
        pause
            .answered(StatusCode::TOO_MANY_REQUESTS, minutes(110))
            .unwrap_err();
        assert_eq!(pause.paused(minutes(110)).unwrap().remaining, 30 * MINUTE);
    }

    #[tokio::test]
    async fn test_paused_requests_are_not_sent() {
        // Every request goes to a closed local port instead, so this can never reach MaM, even
        // if the pause stopped working
        let client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all("http://127.0.0.1:9").unwrap())
            .build()
            .unwrap();
        let db = native_db::Builder::new()
            .create_in_memory(&mlm_db::MODELS)
            .unwrap();
        let mam = MaM {
            jar: Default::default(),
            client,
            db: Arc::new(db),
            user: Default::default(),
            pause: Default::default(),
        };
        assert!(mam.requests_paused().is_none());
        assert!(mam.paused_until().is_none());
        let start = Instant::now();
        mam.pause
            .lock()
            .unwrap()
            .refused(PauseReason::AccountBlocked, start);
        assert_eq!(
            mam.requests_paused().unwrap().reason,
            PauseReason::AccountBlocked
        );
        assert_eq!(mam.paused_until(), Some(start + 30 * MINUTE));

        let paused = |result: Result<()>| result.unwrap_err().is::<RequestsPausedError>();
        assert!(paused(mam.check_mam_id().await));
        assert!(paused(mam.user_info().await.map(|_| ())));
        assert!(paused(
            mam.get_torrent_file("abc123", 1, true).await.map(|_| ())
        ));
        assert!(paused(
            mam.search(&SearchQuery::default()).await.map(|_| ())
        ));
        assert!(paused(
            mam.snatchlist(SnatchlistType::Unsat, 0, UtcDateTime::now())
                .await
                .map(|_| ())
        ));
    }

    #[test]
    fn test_falls_back_to_config() {
        let other = || anyhow::Error::msg("other");
        assert!(falls_back_to_config(true, &other()));
        assert!(!falls_back_to_config(false, &other()));
        // A refusal starts a pause, so the config value isn't tried
        assert!(!falls_back_to_config(
            true,
            &anyhow::Error::new(RateLimitError)
        ));
        assert!(!falls_back_to_config(
            true,
            &anyhow::Error::new(AccountBlockedError)
        ));
    }

    #[test]
    fn test_refused_or_paused() {
        assert!(refused_or_paused(&anyhow::Error::new(RateLimitError)));
        assert!(refused_or_paused(&anyhow::Error::new(AccountBlockedError)));
        assert!(refused_or_paused(&anyhow::Error::new(
            RequestsPausedError {
                reason: PauseReason::RateLimited,
                remaining: MINUTE,
            }
        )));
        assert!(!refused_or_paused(&anyhow::Error::msg("other")));
        // Context added on the way up doesn't hide the refusal
        assert!(refused_or_paused(
            &anyhow::Error::new(RateLimitError).context("search")
        ));
    }

    #[test]
    fn test_paused_error_message() {
        let err = RequestsPausedError {
            reason: PauseReason::RateLimited,
            remaining: 20 * MINUTE + Duration::from_secs(1),
        };
        assert_eq!(
            err.to_string(),
            "Requests to MaM are paused for 21 more minutes after an HTTP 429 (rate limit)"
        );
    }
}
