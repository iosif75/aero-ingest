//! Helpers shared by the integration tests.
//!
//! Each test binary uses a different subset, hence the `dead_code` allowance.
#![allow(dead_code)]

use aero_ingest::auth::TokenManager;
use aero_ingest::client::OpenSkyClient;
use aero_ingest::config::Credentials;
use aero_ingest::model::StatesResponse;
use aero_ingest::storage::{Store, MIGRATOR};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::Executor;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One aircraft over El Prat, as `/states/all` returns it.
pub const STATES: &str = r#"{"time":1731400000,"states":[["3423c4","VLG12AB ","Spain",1731399995,1731399999,2.0785,41.2971,1150.6,false,92.5,248.1,-4.2,null,1188.7,"1000",false,0,4]]}"#;

pub fn snapshot(json: &str) -> StatesResponse {
    serde_json::from_str(json).unwrap()
}

// ---- fake HTTP server ----

/// A canned HTTP response from the fake server.
pub struct Reply {
    status: &'static str,
    headers: String,
    body: String,
}

impl Reply {
    pub fn ok(body: &str) -> Self {
        Self::status("200 OK").body(body)
    }

    pub fn status(status: &'static str) -> Self {
        Self {
            status,
            headers: String::new(),
            body: String::new(),
        }
    }

    pub fn body(mut self, body: &str) -> Self {
        self.body = body.to_string();
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push_str(&format!("{name}: {value}\r\n"));
        self
    }

    /// The OAuth token endpoint's answer.
    pub fn token() -> Self {
        Self::ok(r#"{"access_token":"tok","expires_in":1800}"#)
    }

    fn to_http(&self) -> String {
        format!(
            "HTTP/1.1 {}\r\nContent-Type: application/json\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.status,
            self.headers,
            self.body.len(),
            self.body
        )
    }
}

/// Serve on a random localhost port, answering each request with `handler`,
/// which gets the raw request text. Returns the base URL.
pub async fn fake_server<F>(handler: F) -> String
where
    F: Fn(&str) -> Reply + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handler = Arc::new(handler);
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let handler = handler.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let reply = handler(&req);
                let _ = sock.write_all(reply.to_http().as_bytes()).await;
            });
        }
    });
    base
}

/// An OpenSky client pointed at a fake server's `/token` and `/api`.
pub fn opensky_client(base: &str) -> OpenSkyClient {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let creds = Credentials {
        client_id: "id".into(),
        client_secret: "secret".into(),
    };
    let auth = TokenManager::new(http.clone(), format!("{base}/token"), creds);
    OpenSkyClient::new(http, format!("{base}/api"), auth)
}

// ---- Postgres ----

static SCHEMA_SEQ: AtomicU32 = AtomicU32::new(0);

/// A migrated store in a fresh schema of `TEST_DATABASE_URL`, or `None` when
/// the variable is unset (the caller then skips the test).
pub async fn test_store() -> Option<Store> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
    let schema = format!(
        "aero_test_{}_{}",
        std::process::id(),
        SCHEMA_SEQ.fetch_add(1, Ordering::SeqCst)
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    admin
        .execute(format!("DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}").as_str())
        .await
        .unwrap();
    let opts = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .unwrap();
    MIGRATOR.run(&pool).await.unwrap();
    Some(Store::from_pool(pool))
}
