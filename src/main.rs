use aero_ingest::auth::TokenManager;
use aero_ingest::client::OpenSkyClient;
use aero_ingest::config::{default_http_addr, Config, ConfigError};
use aero_ingest::http::{self as api, AppState};
use aero_ingest::metrics::Metrics;
use aero_ingest::poller::{self, Exit};
use aero_ingest::retention;
use aero_ingest::shutdown::Shutdown;
use aero_ingest::storage::Store;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

/// How long open HTTP requests may take to finish at shutdown.
const SERVER_DRAIN: Duration = Duration::from_secs(5);

/// Standard accounts get this many OpenSky credits a day.
const DAILY_CREDIT_BUDGET: u64 = 4_000;

/// Failures before the poll loop starts, each with its own exit code.
#[derive(Debug, Error)]
enum StartupError {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    #[error("could not build HTTP client: {0}")]
    HttpClient(reqwest::Error),
    #[error("could not connect to Postgres: {0}")]
    Database(sqlx::Error),
    #[error("could not listen on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

impl StartupError {
    fn exit_code(&self) -> ExitCode {
        match self {
            Self::Config(_) => ExitCode::from(2),
            Self::HttpClient(_) => ExitCode::FAILURE,
            Self::Database(_) => ExitCode::from(3),
            Self::Bind { .. } => ExitCode::from(4),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // .env is optional; real env vars win.
    let _ = dotenvy::dotenv();

    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return healthcheck().await;
    }

    init_tracing();
    match run().await {
        Ok(Exit::Shutdown) => ExitCode::SUCCESS,
        Ok(Exit::Fatal(msg)) => {
            tracing::error!("stopping: {msg}");
            ExitCode::FAILURE
        }
        Err(e) => {
            tracing::error!("{e}");
            e.exit_code()
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("aero_ingest=info")),
        )
        .init();
}

/// Start everything, poll until shutdown or a fatal error, then stop.
async fn run() -> Result<Exit, StartupError> {
    let config = Config::from_env()?;
    log_startup(&config);

    let http = build_http_client().map_err(StartupError::HttpClient)?;
    let store = connect_store(&config).await?;
    let auth = TokenManager::new(http.clone(), &config.token_url, config.credentials.clone());
    let client = OpenSkyClient::new(http, &config.api_base, auth);
    let metrics = Arc::new(Metrics::default());

    let listener = TcpListener::bind(config.http_addr)
        .await
        .map_err(|source| StartupError::Bind {
            addr: config.http_addr,
            source,
        })?;
    tracing::info!(addr = %config.http_addr, "serving HTTP API");

    let shutdown = Shutdown::on_signal();
    let app = api::router(AppState {
        metrics: metrics.clone(),
        max_silence: config.max_silence(),
        store: store.clone(),
    });
    let server = tokio::spawn(
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.clone().wait())
            .into_future(),
    );
    let pruner = spawn_pruner(&config, store.clone(), metrics.clone(), shutdown.clone());

    let exit = poller::run(
        &client,
        store.as_ref(),
        &metrics,
        config.bbox,
        config.poll_interval,
        shutdown.wait(),
    )
    .await;

    if let Some(p) = pruner {
        p.abort();
    }
    match exit {
        // Let in-flight API requests finish; the server already saw the signal.
        Exit::Shutdown => {
            if tokio::time::timeout(SERVER_DRAIN, server).await.is_err() {
                tracing::warn!("HTTP server did not finish draining in time");
            }
        }
        // A fatal error doesn't trigger the shutdown signal, so stop it directly.
        Exit::Fatal(_) => server.abort(),
    }
    Ok(exit)
}

fn log_startup(config: &Config) {
    let daily = config.estimated_daily_credits();
    tracing::info!(
        bbox = ?config.bbox,
        interval_s = config.poll_interval.as_secs(),
        credits_per_request = config.bbox.credits_per_request(),
        est_daily_credits = daily,
        "starting aero-ingest"
    );
    if daily > DAILY_CREDIT_BUDGET {
        tracing::warn!("estimated use exceeds the 4,000 credits/day of a standard account");
    }
}

fn build_http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(concat!("aero-ingest/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
}

/// Connect and migrate if `DATABASE_URL` is set; otherwise run log-only.
async fn connect_store(config: &Config) -> Result<Option<Store>, StartupError> {
    let Some(url) = config.database_url.as_deref() else {
        tracing::warn!("DATABASE_URL not set, running in log-only mode");
        return Ok(None);
    };
    let store = Store::connect(url).await.map_err(StartupError::Database)?;
    tracing::info!("connected to Postgres, migrations applied");
    Ok(Some(store))
}

/// Start the retention task when there is storage and retention is enabled.
fn spawn_pruner(
    config: &Config,
    store: Option<Store>,
    metrics: Arc<Metrics>,
    shutdown: Shutdown,
) -> Option<JoinHandle<()>> {
    let store = store?;
    let Some(keep) = config.retention else {
        tracing::info!("retention disabled, keeping all positions");
        return None;
    };
    tracing::info!(
        retention_days = keep.as_secs() / 86_400,
        "retention enabled"
    );
    Some(tokio::spawn(async move {
        retention::run(
            store,
            &metrics,
            keep,
            retention::PRUNE_EVERY,
            shutdown.wait(),
        )
        .await
    }))
}

/// `aero-ingest healthcheck`: exit 0 if the local `/health` answers 200.
async fn healthcheck() -> ExitCode {
    let addr = match std::env::var("AERO_HTTP_ADDR") {
        Err(_) => default_http_addr(),
        Ok(raw) => match raw.trim().parse() {
            Ok(a) => a,
            Err(_) => {
                eprintln!("invalid AERO_HTTP_ADDR {raw:?}");
                return ExitCode::FAILURE;
            }
        },
    };
    match api::probe(addr).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("{msg}");
            ExitCode::FAILURE
        }
    }
}
