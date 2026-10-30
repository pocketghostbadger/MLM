use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

/// Things that keep failing are tried less often: each failure doubles the wait before the next
/// try, from `first_wait` up to `max_wait`. This is only kept in memory, so after a restart
/// everything is tried again
pub struct Backoff<K> {
    first_wait: Duration,
    max_wait: Duration,
    misses: BTreeMap<K, (u32, Instant)>,
}

impl<K: Ord + Clone> Backoff<K> {
    pub const fn new(first_wait: Duration, max_wait: Duration) -> Self {
        Self {
            first_wait,
            max_wait,
            misses: BTreeMap::new(),
        }
    }

    /// Whether something is still waiting after failures
    pub fn waiting(&self, key: &K, now: Instant) -> bool {
        self.misses
            .get(key)
            .is_some_and(|(_, retry_at)| now < *retry_at)
    }

    /// Records whether a try succeeded
    pub fn record(&mut self, key: &K, ok: bool, now: Instant) {
        if ok {
            self.misses.remove(key);
            return;
        }
        let misses = self.misses.get(key).map_or(0, |(misses, _)| *misses) + 1;
        let wait = self
            .first_wait
            .saturating_mul(2u32.saturating_pow(misses - 1))
            .min(self.max_wait);
        self.misses.insert(key.clone(), (misses, now + wait));
    }

    /// Ends every wait, so everything is tried again
    pub fn clear(&mut self) {
        self.misses.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(60 * 60);

    #[test]
    fn test_backoff() {
        let mut backoff = Backoff::new(2 * HOUR, 24 * HOUR);
        let book = ("list".to_owned(), "book".to_owned());
        let other = ("list".to_owned(), "other".to_owned());
        let start = Instant::now();
        let hours = |h: u32| start + h * HOUR;
        assert!(!backoff.waiting(&book, start));

        // Hourly runs: each failure doubles the wait, 2, 4, 8, 16 hours
        let mut tried_at = vec![];
        for h in 0..=30 {
            if !backoff.waiting(&book, hours(h)) {
                backoff.record(&book, false, hours(h));
                tried_at.push(h);
            }
        }
        assert_eq!(tried_at, vec![0, 2, 6, 14, 30]);
        assert!(!backoff.waiting(&other, hours(30)));

        // Then a day at most
        assert!(backoff.waiting(&book, hours(53)));
        assert!(!backoff.waiting(&book, hours(54)));
        backoff.record(&book, false, hours(54));
        assert!(backoff.waiting(&book, hours(77)));
        assert!(!backoff.waiting(&book, hours(78)));

        // Success ends the wait and starts over
        backoff.record(&book, true, hours(78));
        assert!(!backoff.waiting(&book, hours(78)));
        backoff.record(&book, false, hours(79));
        assert!(backoff.waiting(&book, hours(80)));
        assert!(!backoff.waiting(&book, hours(81)));
    }

    #[test]
    fn test_backoff_short_first_wait() {
        const MINUTE: Duration = Duration::from_secs(60);
        let mut backoff = Backoff::new(30 * MINUTE, 24 * HOUR);
        let torrent = "hash".to_owned();
        let start = Instant::now();
        let minutes = |m: u32| start + m * MINUTE;

        // Runs every 10 minutes: 30 minutes, then 1, 2, 4, 8 and 16 hours, then a day
        let mut tried_at = vec![];
        for m in (0..=60 * 80).step_by(10) {
            if !backoff.waiting(&torrent, minutes(m)) {
                backoff.record(&torrent, false, minutes(m));
                tried_at.push(m);
            }
        }
        assert_eq!(tried_at, vec![0, 30, 90, 210, 450, 930, 1890, 3330, 4770]);

        // Clearing ends every wait
        assert!(backoff.waiting(&torrent, minutes(60 * 80)));
        backoff.clear();
        assert!(!backoff.waiting(&torrent, minutes(60 * 80)));
    }
}
