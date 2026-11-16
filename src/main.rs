use aero_ingest::auth::TokenManager;
use aero_ingest::client::OpenSkyClient;
use aero_ingest::config::Config;
use aero_ingest::http::{self as api, AppState};
use aero_ingest::metrics::Metrics;
use aero_ingest::poller::{self, Exit};
use aero_ingest::retention;
use aero_ingest::storage::Store;
use std::future::IntoFuture;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    // .env is optional; real env vars win.
    let _ = dotenvy::dotenv();

    // `aero-ingest healthcheck`: probe a running instance's /health, for
    // container health checks (the runtime image has no curl).
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return healthcheck().await;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("aero_ingest=info")),
        )
        .init();

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("configuration error: {e}");
            return ExitCode::from(2);
        }
    };

    tracing::info!(
        bbox = ?config.bbox,
        interval_s = config.poll_interval.as_secs(),
        credits_per_request = config.bbox.credits_per_request(),
        est_daily_credits = config.estimated_daily_credits(),
        "starting aero-ingest"
    );
    if config.estimated_daily_credits() > 4000 {
        tracing::warn!("estimated use exceeds the 4,000 credits/day of a standard account");
    }

    let http = match reqwest::Client::builder()
        .user_agent(concat!("aero-ingest/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("could not build HTTP client: {e}");
            return ExitCode::FAILURE;
        }
    };

    let store = match config.database_url.as_deref() {
        Some(url) => match Store::connect(url).await {
            Ok(s) => {
                tracing::info!("connected to Postgres, migrations applied");
                Some(s)
            }
            Err(e) => {
                tracing::error!("could not connect to Postgres: {e}");
                return ExitCode::from(3);
            }
        },
        None => {
            tracing::warn!("DATABASE_URL not set, running in log-only mode");
            None
        }
    };

    let auth = TokenManager::new(http.clone(), &config.token_url, config.credentials.clone());
    let client = OpenSkyClient::new(http, &config.api_base, auth);

    let metrics = Arc::new(Metrics::default());
    let listener = match tokio::net::TcpListener::bind(config.http_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("could not listen on {}: {e}", config.http_addr);
            return ExitCode::from(4);
        }
    };
    tracing::info!(addr = %config.http_addr, "serving /health and /metrics");

    // Ctrl+C or SIGTERM (`docker stop`) stops the poll loop, pruner and HTTP server.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let signal = shutdown_signal().await;
        tracing::info!(signal, "shutdown requested");
        let _ = stop_tx.send(true);
    });
    let stopped = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|v| *v).await;
    };

    let app = api::router(AppState {
        metrics: metrics.clone(),
        max_silence: config.max_silence(),
        store: store.clone(),
    });
    let server = tokio::spawn(
        axum::serve(listener, app)
            .with_graceful_shutdown(stopped(stop_rx.clone()))
            .into_future(),
    );

    let pruner = match (store.clone(), config.retention) {
        (Some(store), Some(keep)) => {
            tracing::info!(
                retention_days = keep.as_secs() / 86_400,
                "retention enabled"
            );
            let metrics = metrics.clone();
            let stop = stopped(stop_rx.clone());
            Some(tokio::spawn(async move {
                retention::run(store, &metrics, keep, retention::PRUNE_EVERY, stop).await
            }))
        }
        (Some(_), None) => {
            tracing::info!("retention disabled, keeping all positions");
            None
        }
        (None, _) => None,
    };

    let exit = poller::run(
        &client,
        store.as_ref(),
        &metrics,
        config.bbox,
        config.poll_interval,
        stopped(stop_rx),
    )
    .await;
    server.abort();
    if let Some(p) = pruner {
        p.abort();
    }

    match exit {
        Exit::Shutdown => ExitCode::SUCCESS,
        Exit::Fatal(msg) => {
            tracing::error!("stopping: {msg}");
            ExitCode::FAILURE
        }
    }
}

/// Exit 0 if the local `/health` answers 200, 1 otherwise.
async fn healthcheck() -> ExitCode {
    let addr = std::env::var("AERO_HTTP_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".into());
    let mut addr: std::net::SocketAddr = match addr.trim().parse() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("invalid AERO_HTTP_ADDR {addr:?}");
            return ExitCode::FAILURE;
        }
    };
    // A server listening on all interfaces is reachable on loopback.
    if addr.ip().is_unspecified() {
        addr.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    }
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("could not build HTTP client: {e}");
            return ExitCode::FAILURE;
        }
    };
    match client.get(format!("http://{addr}/health")).send().await {
        Ok(r) if r.status().is_success() => ExitCode::SUCCESS,
        Ok(r) => {
            eprintln!("unhealthy: HTTP {}", r.status());
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("health check failed: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Resolve on the first shutdown signal and return its name.
async fn shutdown_signal() -> &'static str {
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
