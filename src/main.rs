use aero_ingest::auth::TokenManager;
use aero_ingest::client::OpenSkyClient;
use aero_ingest::config::Config;
use aero_ingest::poller::{self, Exit};
use std::process::ExitCode;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    // .env is optional; real env vars win.
    let _ = dotenvy::dotenv();
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

    let auth = TokenManager::new(http.clone(), &config.token_url, config.credentials.clone());
    let client = OpenSkyClient::new(http, &config.api_base, auth);

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown requested");
    };

    match poller::run(&client, config.bbox, config.poll_interval, shutdown).await {
        Exit::Shutdown => ExitCode::SUCCESS,
        Exit::Fatal(msg) => {
            tracing::error!("stopping: {msg}");
            ExitCode::FAILURE
        }
    }
}
