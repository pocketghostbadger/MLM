use std::{
    collections::BTreeSet,
    env::VarError,
    fs, io,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use anyhow::{Result, bail};
use time::{Date, Month, OffsetDateTime};
use tracing::{info, warn};
use tracing_appender::rolling::{RollingFileAppender, Rotation};

const PREFIX: &str = "mlm";
const SUFFIX: &str = "log";

/// The appender for the log files, which starts a new `mlm.YYYY-MM-DD.log` each day (UTC)
pub fn appender(dir: &Path) -> Result<RollingFileAppender> {
    Ok(RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(PREFIX)
        .filename_suffix(SUFFIX)
        .build(dir)?)
}

/// The log file's filter: `MLM_LOG_FILE` when it is set and not blank, otherwise `MLM_LOG`, the one stderr uses.
pub fn file_filter(log_file: Option<String>, log: Option<String>) -> String {
    log_file
        .filter(|value| !value.trim().is_empty())
        .or(log)
        .unwrap_or_default()
}

/// Parses `MLM_LOG_MAX_FILES`. Unset or empty keeps every log file.
pub fn parse_max_files(value: Result<String, VarError>) -> Result<Option<usize>> {
    let value = match value {
        Ok(value) => value,
        Err(VarError::NotPresent) => return Ok(None),
        Err(VarError::NotUnicode(value)) => {
            bail!("MLM_LOG_MAX_FILES must be a whole number of at least 1, got {value:?}")
        }
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    match value.parse::<usize>() {
        Ok(0) | Err(_) => {
            bail!("MLM_LOG_MAX_FILES must be a whole number of at least 1, got {value:?}")
        }
        Ok(max_files) => Ok(Some(max_files)),
    }
}

/// Deletes the oldest daily log files in `dir` so that at most `max_files` remain, and returns each file it
/// tried to delete with the result. A file that can't be deleted doesn't stop the others.
///
/// Only files named like the ones MLM writes (`mlm.YYYY-MM-DD.log`, with a real date) are counted or deleted.
/// They are ordered by the date in their name rather than by creation time, which some file systems (such as
/// Unraid's user shares) don't report. The file for `today` (UTC, the one being written) is never deleted, and files dated later,
/// as left by a clock that was ahead, are neither deleted nor counted.
pub fn prune_log_files(
    dir: &Path,
    max_files: usize,
    today: Date,
) -> io::Result<Vec<(PathBuf, io::Result<()>)>> {
    let mut logs = vec![];
    for entry in fs::read_dir(dir)?.flatten() {
        if !entry.file_type().is_ok_and(|t| t.is_file()) {
            continue;
        }
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if let Some(date) = log_date(&name)
            && date <= today
        {
            logs.push((date, name));
        }
    }
    logs.sort_unstable();
    let excess = logs.len().saturating_sub(max_files);
    Ok(logs
        .into_iter()
        .take(excess)
        .map(|(_, name)| {
            let path = dir.join(name);
            let result = fs::remove_file(&path);
            (path, result)
        })
        .collect())
}

/// Starts a thread that prunes the log files at once and then every hour, so the count stays close to the cap
/// after each daily rotation.
///
/// Not the appender's own max_log_files: it orders files by creation time and skips files without one, which
/// Unraid's user shares don't report, so it would never delete anything there.
pub fn spawn_pruner(dir: PathBuf, max_files: usize) -> io::Result<()> {
    thread::Builder::new()
        .name("log-pruner".to_string())
        .spawn(move || {
            // A failure that keeps happening is only warned about the first time
            let mut failures = BTreeSet::new();
            loop {
                let mut new_failures = BTreeSet::new();
                match prune_log_files(&dir, max_files, OffsetDateTime::now_utc().date()) {
                    Ok(results) => {
                        for (path, result) in results {
                            match result {
                                Ok(()) => info!("Removed old log file {}", path.display()),
                                Err(err) => {
                                    new_failures.insert(format!(
                                        "Failed to remove old log file {}: {err}",
                                        path.display()
                                    ));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        new_failures.insert(format!(
                            "Failed to read the log folder {}: {err}",
                            dir.display()
                        ));
                    }
                }
                for failure in new_failures.difference(&failures) {
                    warn!("{failure}");
                }
                failures = new_failures;
                thread::sleep(Duration::from_secs(60 * 60));
            }
        })?;
    Ok(())
}

fn daily_log_name(date: Date) -> String {
    format!(
        "{PREFIX}.{:04}-{:02}-{:02}.{SUFFIX}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

/// The date of a file named like the appender's, or None for any other name, including impossible dates
fn log_date(name: &str) -> Option<Date> {
    let date = name
        .strip_prefix(PREFIX)?
        .strip_prefix('.')?
        .strip_suffix(SUFFIX)?
        .strip_suffix('.')?;
    let mut parts = date.split('-');
    let (year, month, day) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let month = Month::try_from(month.parse::<u8>().ok()?).ok()?;
    let date = Date::from_calendar_date(year.parse().ok()?, month, day.parse().ok()?).ok()?;
    // Only the appender's own spelling, zero-padded and without signs
    (daily_log_name(date) == name).then_some(date)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use time::macros::date;

    use super::*;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("mlm-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn files(&self) -> Vec<String> {
            let mut names = fs::read_dir(&self.0)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect::<Vec<_>>();
            names.sort();
            names
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).ok();
        }
    }

    fn removed(results: Vec<(PathBuf, io::Result<()>)>) -> Vec<String> {
        results
            .into_iter()
            .map(|(path, result)| {
                result.unwrap();
                path.file_name().unwrap().to_str().unwrap().to_owned()
            })
            .collect()
    }

    #[test]
    fn file_filter_falls_back_to_mlm_log() {
        let filter = |log_file: Option<&str>, log: Option<&str>| {
            file_filter(log_file.map(str::to_owned), log.map(str::to_owned))
        };
        assert_eq!(filter(None, Some("mlm=info")), "mlm=info");
        assert_eq!(filter(Some(""), Some("mlm=info")), "mlm=info");
        assert_eq!(filter(Some(" "), Some("mlm=info")), "mlm=info");
        assert_eq!(filter(Some("mlm=debug"), Some("mlm=info")), "mlm=debug");
        assert_eq!(filter(Some("mlm=debug"), None), "mlm=debug");
        assert_eq!(filter(None, None), "");
    }

    #[test]
    fn max_files_parsing() {
        let parse = |value: &str| parse_max_files(Ok(value.to_owned()));
        assert_eq!(parse_max_files(Err(VarError::NotPresent)).unwrap(), None);
        assert_eq!(parse("").unwrap(), None);
        assert_eq!(parse(" 14 ").unwrap(), Some(14));
        assert!(parse("0").is_err());
        assert!(parse("-1").is_err());
        assert!(parse("two").is_err());
        assert!(parse_max_files(Err(VarError::NotUnicode("14".into()))).is_err());
    }

    #[test]
    fn appender_writes_daily_log_names() {
        let dir = TestDir::new();
        let before = OffsetDateTime::now_utc().date();
        let mut appender = appender(&dir.0).unwrap();
        appender.write_all(b"test\n").unwrap();
        let after = OffsetDateTime::now_utc().date();
        let files = dir.files();
        assert_eq!(files.len(), 1);
        let date = log_date(&files[0]).unwrap();
        assert!(date == before || date == after);
    }

    #[test]
    fn daily_log_names() {
        assert_eq!(daily_log_name(date!(2026 - 01 - 02)), "mlm.2026-01-02.log");
        assert_eq!(log_date("mlm.2026-11-01.log"), Some(date!(2026 - 11 - 01)));
        assert_eq!(log_date("mlm.2024-02-29.log"), Some(date!(2024 - 02 - 29)));
        for name in [
            "mlm.log",
            "mlm.2026-11-01.log.gz",
            "mlm.2026-11-01-12.log",
            "other.2026-11-01.log",
            "mlm.2026_11_01.log",
            "mlm.2026-1-01.log",
            "mlm.+2026-11-01.log",
            "mlm.2026-00-00.log",
            "mlm.2026-10-99.log",
            "mlm.2026-19-01.log",
            "mlm.2026-02-29.log",
        ] {
            assert_eq!(log_date(name), None, "{name}");
        }
    }

    #[test]
    fn prune_ignores_impossible_dates() {
        let dir = TestDir::new();
        for name in [
            "mlm.2026-10-31.log",
            "mlm.2026-10-99.log",
            "mlm.2026-11-01.log",
        ] {
            fs::write(dir.0.join(name), "").unwrap();
        }

        assert!(
            prune_log_files(&dir.0, 2, date!(2026 - 11 - 01))
                .unwrap()
                .is_empty()
        );
        assert_eq!(dir.files().len(), 3);
    }

    #[test]
    fn prune_keeps_newest_daily_logs() {
        let dir = TestDir::new();
        fs::create_dir_all(dir.0.join("mlm.2026-01-01.log")).unwrap();
        for name in [
            "mlm.2026-10-30.log",
            "mlm.2026-11-01.log",
            "mlm.2026-10-31.log",
            "mlm.2025-12-31.log",
            "config.toml",
            "mlm.log",
        ] {
            fs::write(dir.0.join(name), "").unwrap();
        }

        let today = date!(2026 - 11 - 01);
        assert_eq!(
            removed(prune_log_files(&dir.0, 2, today).unwrap()),
            vec!["mlm.2025-12-31.log", "mlm.2026-10-30.log"]
        );
        assert_eq!(
            dir.files(),
            vec![
                "config.toml",
                "mlm.2026-01-01.log",
                "mlm.2026-10-31.log",
                "mlm.2026-11-01.log",
                "mlm.log",
            ]
        );
        assert!(prune_log_files(&dir.0, 2, today).unwrap().is_empty());
    }

    #[test]
    fn prune_ignores_later_files() {
        let dir = TestDir::new();
        for name in [
            "mlm.2026-10-30.log",
            "mlm.2026-10-31.log",
            "mlm.2026-11-01.log",
            "mlm.2026-11-02.log",
        ] {
            fs::write(dir.0.join(name), "").unwrap();
        }

        assert_eq!(
            removed(prune_log_files(&dir.0, 2, date!(2026 - 11 - 01)).unwrap()),
            vec!["mlm.2026-10-30.log"]
        );
        assert_eq!(
            dir.files(),
            vec![
                "mlm.2026-10-31.log",
                "mlm.2026-11-01.log",
                "mlm.2026-11-02.log"
            ]
        );
    }
}
