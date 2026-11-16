//! Process-wide shutdown signal shared by the poll loop, pruner and HTTP server.

use tokio::sync::watch;

/// A cloneable handle that resolves once shutdown is requested.
#[derive(Clone)]
pub struct Shutdown(watch::Receiver<bool>);

impl Shutdown {
    /// Listen for Ctrl+C or SIGTERM (`docker stop`) in the background.
    pub fn on_signal() -> Self {
        let (tx, rx) = watch::channel(false);
        tokio::spawn(async move {
            let signal = wait_for_signal().await;
            tracing::info!(signal, "shutdown requested");
            let _ = tx.send(true);
        });
        Self(rx)
    }

    /// A handle plus a trigger, for tests and embedding.
    pub fn manual() -> (watch::Sender<bool>, Self) {
        let (tx, rx) = watch::channel(false);
        (tx, Self(rx))
    }

    /// Resolve once shutdown has been requested.
    pub async fn wait(mut self) {
        let _ = self.0.wait_for(|stop| *stop).await;
    }
}

/// Resolve on the first shutdown signal and return its name.
async fn wait_for_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = tokio::signal::ctrl_c() => "SIGINT",
                _ = term.recv() => "SIGTERM",
            },
            Err(e) => {
                tracing::warn!("cannot listen for SIGTERM: {e}");
                let _ = tokio::signal::ctrl_c().await;
                "SIGINT"
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "Ctrl+C"
    }
}

#[cfg(test)]
mod tests {
    use super::Shutdown;
    use std::time::Duration;

    #[tokio::test]
    async fn every_clone_sees_the_trigger() {
        let (tx, a) = Shutdown::manual();
        let b = a.clone();
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            a.wait().await;
            b.wait().await;
        })
        .await
        .expect("both handles resolve");
    }
}
