//! The poll loop: one `/states/all` call per tick, logged.

use crate::client::{FetchError, OpenSkyClient, Snapshot};
use crate::config::BoundingBox;
use crate::metrics::{now_unix, Metrics};
use crate::model::StateVector;
use crate::storage::Store;
use std::future::Future;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio::time::{self, MissedTickBehavior};

/// A position older than this (relative to the snapshot time) counts as stale.
pub const STALE_AFTER_SECS: i64 = 60;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PollSummary {
    pub total: usize,
    pub with_position: usize,
    pub stale: usize,
    pub on_ground: usize,
}

pub fn summarize(time: i64, states: &[StateVector]) -> PollSummary {
    let mut s = PollSummary {
        total: states.len(),
        ..Default::default()
    };
    for v in states {
        if v.has_position() {
            s.with_position += 1;
            if v.position_age(time).is_some_and(|a| a > STALE_AFTER_SECS) {
                s.stale += 1;
            }
        }
        if v.on_ground {
            s.on_ground += 1;
        }
    }
    s
}

/// Why the loop stopped.
#[derive(Debug)]
pub enum Exit {
    Shutdown,
    Fatal(String),
}

/// Waits before each retry of a transient failure within one tick.
///
/// Doubles from 2 s, and stops once the total wait would pass half the poll
/// interval, so retries never run into the next tick.
pub fn retry_delays(interval: Duration) -> Vec<Duration> {
    let budget = interval / 2;
    let mut out = Vec::new();
    let mut total = Duration::ZERO;
    let mut next = Duration::from_secs(2);
    while out.len() < MAX_RETRIES && total + next <= budget {
        total += next;
        out.push(next);
        next *= 2;
    }
    out
}

const MAX_RETRIES: usize = 3;

/// Poll until `shutdown` resolves or a fatal error occurs.
pub async fn run(
    client: &OpenSkyClient,
    store: Option<&Store>,
    metrics: &Metrics,
    bbox: BoundingBox,
    every: Duration,
    shutdown: impl Future<Output = ()>,
) -> Exit {
    tokio::pin!(shutdown);
    let mut ticker = time::interval(every);
    // After a long pause (rate limit, slow request), don't fire a burst of catch-up polls.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let delays = retry_delays(every);

    loop {
        tokio::select! {
            _ = &mut shutdown => return Exit::Shutdown,
            _ = ticker.tick() => {}
        }

        let mut attempt = 0;
        let result = loop {
            match client.fetch_states(&bbox).await {
                Err(FetchError::Transient(msg)) if attempt < delays.len() => {
                    let wait = delays[attempt];
                    attempt += 1;
                    metrics.fetch_retries.fetch_add(1, Relaxed);
                    tracing::warn!(error = %msg, attempt, retry_in_s = wait.as_secs(), "poll failed, retrying");
                    tokio::select! {
                        _ = &mut shutdown => return Exit::Shutdown,
                        _ = time::sleep(wait) => {}
                    }
                }
                other => break other,
            }
        };

        match result {
            Ok(snap) => {
                let s = log_snapshot(&snap);
                metrics.record_success(now_unix(), s.total, s.stale, snap.credits_remaining);
                if let Some(store) = store {
                    persist(store, metrics, &snap).await;
                }
            }
            Err(FetchError::Fatal(msg)) => return Exit::Fatal(msg),
            Err(FetchError::RateLimited { retry_after }) => {
                metrics.record_rate_limited(now_unix() + retry_after.as_secs() as i64);
                tracing::warn!(
                    retry_after_s = retry_after.as_secs(),
                    "out of OpenSky credits, pausing"
                );
                tokio::select! {
                    _ = &mut shutdown => return Exit::Shutdown,
                    _ = time::sleep(retry_after) => {}
                }
                ticker.reset();
            }
            Err(FetchError::Transient(msg)) => {
                metrics.polls_failed.fetch_add(1, Relaxed);
                tracing::error!(error = %msg, "poll failed after retries, skipping this tick");
            }
        }
    }
}

/// Write a snapshot. A database error is logged and the poll loop carries on;
/// that snapshot is lost, which is acceptable for a 60 s sampled feed.
async fn persist(store: &Store, metrics: &Metrics, snap: &Snapshot) {
    let r = &snap.response;
    match store.insert_snapshot(r.time, &r.states).await {
        Ok(w) => {
            let dup = w.candidates as u64 - w.inserted;
            metrics.rows_inserted.fetch_add(w.inserted, Relaxed);
            metrics.rows_duplicate.fetch_add(dup, Relaxed);
            tracing::info!(
                candidates = w.candidates,
                inserted = w.inserted,
                duplicates = dup,
                "stored snapshot"
            );
        }
        Err(e) => {
            metrics.db_errors.fetch_add(1, Relaxed);
            tracing::error!(error = %e, "failed to store snapshot");
        }
    }
}

fn log_snapshot(snap: &Snapshot) -> PollSummary {
    let r = &snap.response;
    let s = summarize(r.time, &r.states);
    tracing::info!(
        snapshot_time = r.time,
        aircraft = s.total,
        with_position = s.with_position,
        stale = s.stale,
        on_ground = s.on_ground,
        credits_remaining = ?snap.credits_remaining,
        "poll ok"
    );
    for v in &r.states {
        tracing::debug!(
            icao24 = %v.icao24,
            callsign = v.callsign.as_deref().unwrap_or("-"),
            lat = ?v.latitude,
            lon = ?v.longitude,
            baro_alt_m = ?v.baro_altitude,
            velocity_ms = ?v.velocity,
            on_ground = v.on_ground,
            age_s = ?v.position_age(r.time),
            "aircraft"
        );
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::StatesResponse;

    #[test]
    fn retry_delays_fit_in_half_the_interval() {
        let secs = |v: Vec<Duration>| v.iter().map(|d| d.as_secs()).collect::<Vec<_>>();
        assert_eq!(secs(retry_delays(Duration::from_secs(60))), vec![2, 4, 8]);
        assert_eq!(secs(retry_delays(Duration::from_secs(10))), vec![2]);
        assert_eq!(secs(retry_delays(Duration::from_secs(5))), vec![2]);
        assert!(retry_delays(Duration::from_secs(3)).is_empty());
    }

    #[test]
    fn counts_positions_stale_and_ground() {
        let r: StatesResponse = serde_json::from_str(
            r#"{"time":1000,"states":[
                ["a",null,"ES",995,999,2.0,41.0,null,false,null,null,null,null,null,null,false,0],
                ["b",null,"ES",900,999,2.0,41.0,null,true,null,null,null,null,null,null,false,0],
                ["c",null,"ES",null,999,null,null,null,true,null,null,null,null,null,null,false,0]
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            summarize(r.time, &r.states),
            PollSummary {
                total: 3,
                with_position: 2,
                stale: 1,
                on_ground: 2
            }
        );
    }
}
