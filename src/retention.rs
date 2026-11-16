//! Keeps the `state_vectors` table from growing forever.
//!
//! Once at startup and then every hour, rows older than the retention period
//! are deleted in batches. A failed prune is logged and retried next round.

use crate::metrics::{now_unix, Metrics};
use crate::storage::Store;
use std::future::Future;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio::time::{self, MissedTickBehavior};

pub const PRUNE_EVERY: Duration = Duration::from_secs(3600);
pub const BATCH: i64 = 10_000;

/// Run one prune pass. Returns rows deleted, or `None` on error.
pub async fn prune_once(store: &Store, metrics: &Metrics, keep: Duration) -> Option<u64> {
    let cutoff = now_unix() - keep.as_secs() as i64;
    match store.prune_older_than(cutoff, BATCH).await {
        Ok(n) => {
            metrics.rows_pruned.fetch_add(n, Relaxed);
            if n > 0 {
                tracing::info!(deleted = n, cutoff, "pruned old positions");
            } else {
                tracing::debug!(cutoff, "nothing to prune");
            }
            Some(n)
        }
        Err(e) => {
            metrics.prune_errors.fetch_add(1, Relaxed);
            tracing::error!(error = %e, "prune failed, will retry next round");
            None
        }
    }
}

/// Prune at startup and every `every` until `shutdown` resolves.
pub async fn run(
    store: Store,
    metrics: &Metrics,
    keep: Duration,
    every: Duration,
    shutdown: impl Future<Output = ()>,
) {
    tokio::pin!(shutdown);
    let mut ticker = time::interval(every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = ticker.tick() => {}
        }
        tokio::select! {
            _ = &mut shutdown => return,
            _ = prune_once(&store, metrics, keep) => {}
        }
    }
}
