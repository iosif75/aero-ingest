//! Process-wide counters and gauges, rendered in Prometheus text format.
//!
//! Hand-rolled with atomics: the daemon only needs a dozen series, and this
//! keeps the dependency tree small.

use std::fmt::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::{SystemTime, UNIX_EPOCH};

/// Sentinel for "no value yet" in signed gauges.
const UNSET: i64 = i64::MIN;

pub struct Metrics {
    started_at: i64,
    pub polls_ok: AtomicU64,
    pub polls_failed: AtomicU64,
    pub polls_rate_limited: AtomicU64,
    pub fetch_retries: AtomicU64,
    pub rows_inserted: AtomicU64,
    pub rows_duplicate: AtomicU64,
    pub db_errors: AtomicU64,
    pub aircraft_last_poll: AtomicU64,
    pub stale_last_poll: AtomicU64,
    credits_remaining: AtomicI64,
    last_success_unix: AtomicI64,
    rate_limited_until_unix: AtomicI64,
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(now_unix())
    }
}

impl Metrics {
    pub fn new(started_at: i64) -> Self {
        Self {
            started_at,
            polls_ok: AtomicU64::new(0),
            polls_failed: AtomicU64::new(0),
            polls_rate_limited: AtomicU64::new(0),
            fetch_retries: AtomicU64::new(0),
            rows_inserted: AtomicU64::new(0),
            rows_duplicate: AtomicU64::new(0),
            db_errors: AtomicU64::new(0),
            aircraft_last_poll: AtomicU64::new(0),
            stale_last_poll: AtomicU64::new(0),
            credits_remaining: AtomicI64::new(UNSET),
            last_success_unix: AtomicI64::new(UNSET),
            rate_limited_until_unix: AtomicI64::new(UNSET),
        }
    }

    pub fn started_at(&self) -> i64 {
        self.started_at
    }

    pub fn record_success(&self, at: i64, aircraft: usize, stale: usize, credits: Option<i64>) {
        self.polls_ok.fetch_add(1, Relaxed);
        self.aircraft_last_poll.store(aircraft as u64, Relaxed);
        self.stale_last_poll.store(stale as u64, Relaxed);
        if let Some(c) = credits {
            self.credits_remaining.store(c, Relaxed);
        }
        self.last_success_unix.store(at, Relaxed);
        self.rate_limited_until_unix.store(UNSET, Relaxed);
    }

    pub fn record_rate_limited(&self, until: i64) {
        self.polls_rate_limited.fetch_add(1, Relaxed);
        self.rate_limited_until_unix.store(until, Relaxed);
    }

    pub fn last_success(&self) -> Option<i64> {
        get(&self.last_success_unix)
    }

    pub fn rate_limited_until(&self) -> Option<i64> {
        get(&self.rate_limited_until_unix)
    }

    pub fn credits_remaining(&self) -> Option<i64> {
        get(&self.credits_remaining)
    }

    /// Prometheus text exposition format (version 0.0.4).
    pub fn render(&self) -> String {
        let mut out = String::new();
        let mut counter = |name: &str, help: &str, v: u64| {
            let _ = writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}"
            );
        };
        counter(
            "aero_polls_ok_total",
            "Successful /states/all polls.",
            self.polls_ok.load(Relaxed),
        );
        counter(
            "aero_polls_failed_total",
            "Polls that failed after all retries.",
            self.polls_failed.load(Relaxed),
        );
        counter(
            "aero_polls_rate_limited_total",
            "Polls answered with HTTP 429.",
            self.polls_rate_limited.load(Relaxed),
        );
        counter(
            "aero_fetch_retries_total",
            "Retries after transient fetch errors.",
            self.fetch_retries.load(Relaxed),
        );
        counter(
            "aero_rows_inserted_total",
            "State vectors written to Postgres.",
            self.rows_inserted.load(Relaxed),
        );
        counter(
            "aero_rows_duplicate_total",
            "State vectors skipped as already stored.",
            self.rows_duplicate.load(Relaxed),
        );
        counter(
            "aero_db_errors_total",
            "Failed snapshot writes.",
            self.db_errors.load(Relaxed),
        );

        let mut gauge = |name: &str, help: &str, v: Option<i64>| {
            if let Some(v) = v {
                let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}");
            }
        };
        gauge(
            "aero_aircraft_last_poll",
            "Aircraft in the box on the last successful poll.",
            Some(self.aircraft_last_poll.load(Relaxed) as i64),
        );
        gauge(
            "aero_stale_positions_last_poll",
            "Positions older than 60 s on the last successful poll.",
            Some(self.stale_last_poll.load(Relaxed) as i64),
        );
        gauge(
            "aero_credits_remaining",
            "OpenSky credits left, from X-Rate-Limit-Remaining.",
            self.credits_remaining(),
        );
        gauge(
            "aero_last_success_timestamp_seconds",
            "Unix time of the last successful poll.",
            self.last_success(),
        );
        gauge(
            "aero_start_time_seconds",
            "Unix time the daemon started.",
            Some(self.started_at),
        );
        out
    }
}

fn get(a: &AtomicI64) -> Option<i64> {
    match a.load(Relaxed) {
        UNSET => None,
        v => Some(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_counters_and_skips_unset_gauges() {
        let m = Metrics::new(100);
        m.rows_inserted.fetch_add(7, Relaxed);
        let text = m.render();
        assert!(text.contains("aero_rows_inserted_total 7\n"));
        assert!(text.contains("# TYPE aero_polls_ok_total counter"));
        assert!(!text.contains("aero_credits_remaining"));
        assert!(!text.contains("aero_last_success_timestamp_seconds"));
    }

    #[test]
    fn success_clears_rate_limit_and_sets_gauges() {
        let m = Metrics::new(100);
        m.record_rate_limited(500);
        assert_eq!(m.rate_limited_until(), Some(500));
        m.record_success(200, 42, 3, Some(3990));
        assert_eq!(m.rate_limited_until(), None);
        let text = m.render();
        assert!(text.contains("aero_aircraft_last_poll 42\n"));
        assert!(text.contains("aero_credits_remaining 3990\n"));
        assert!(text.contains("aero_last_success_timestamp_seconds 200\n"));
    }
}
