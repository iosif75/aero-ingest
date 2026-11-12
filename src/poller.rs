//! The poll loop: one `/states/all` call per tick, logged.

use crate::client::{FetchError, OpenSkyClient, Snapshot};
use crate::config::BoundingBox;
use crate::model::StateVector;
use std::future::Future;
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

/// Poll until `shutdown` resolves or a fatal error occurs.
pub async fn run(
    client: &OpenSkyClient,
    bbox: BoundingBox,
    every: Duration,
    shutdown: impl Future<Output = ()>,
) -> Exit {
    tokio::pin!(shutdown);
    let mut ticker = time::interval(every);
    // After a long pause (rate limit, slow request), don't fire a burst of catch-up polls.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = &mut shutdown => return Exit::Shutdown,
            _ = ticker.tick() => {}
        }

        match client.fetch_states(&bbox).await {
            Ok(snap) => log_snapshot(&snap),
            Err(FetchError::Fatal(msg)) => return Exit::Fatal(msg),
            Err(FetchError::RateLimited { retry_after }) => {
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
                tracing::warn!(error = %msg, "poll failed, will retry next tick");
            }
        }
    }
}

fn log_snapshot(snap: &Snapshot) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::StatesResponse;

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
