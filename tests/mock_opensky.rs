//! End-to-end tests against a tiny fake OpenSky server on localhost.

use aero_ingest::auth::TokenManager;
use aero_ingest::client::{FetchError, OpenSkyClient};
use aero_ingest::config::{BoundingBox, Credentials};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const STATES: &str = r#"{"time":1731400000,"states":[["3423c4","VLG12AB ","Spain",1731399995,1731399999,2.0785,41.2971,1150.6,false,92.5,248.1,-4.2,null,1188.7,"1000",false,0,4]]}"#;

#[derive(Clone, Copy)]
enum StatesMode {
    Ok,
    UnauthorizedOnce,
    AlwaysUnauthorized,
    RateLimited,
}

struct Counters {
    token_calls: AtomicUsize,
    state_calls: AtomicUsize,
}

async fn spawn(mode: StatesMode) -> (String, Arc<Counters>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let c = Arc::new(Counters {
        token_calls: AtomicUsize::new(0),
        state_calls: AtomicUsize::new(0),
    });
    let c2 = c.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let c = c2.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let line = req.lines().next().unwrap_or("");
                let (status, extra, body) = if line.starts_with("POST /token") {
                    c.token_calls.fetch_add(1, Ordering::SeqCst);
                    assert!(req.contains("grant_type=client_credentials"));
                    (
                        "200 OK",
                        "",
                        r#"{"access_token":"tok","expires_in":1800}"#.to_string(),
                    )
                } else if line.starts_with("GET /api/states/all?") {
                    let i = c.state_calls.fetch_add(1, Ordering::SeqCst);
                    assert!(req
                        .to_ascii_lowercase()
                        .contains("authorization: bearer tok"));
                    assert!(line.contains("extended=1") && line.contains("lamin=40.9"));
                    match mode {
                        StatesMode::Ok => {
                            ("200 OK", "X-Rate-Limit-Remaining: 3999\r\n", STATES.into())
                        }
                        StatesMode::UnauthorizedOnce if i == 0 => {
                            ("401 Unauthorized", "", String::new())
                        }
                        StatesMode::UnauthorizedOnce => ("200 OK", "", STATES.into()),
                        StatesMode::AlwaysUnauthorized => ("401 Unauthorized", "", String::new()),
                        StatesMode::RateLimited => (
                            "429 Too Many Requests",
                            "X-Rate-Limit-Retry-After-Seconds: 777\r\n",
                            String::new(),
                        ),
                    }
                } else {
                    ("404 Not Found", "", String::new())
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (base, c)
}

fn client(base: &str) -> OpenSkyClient {
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

#[tokio::test]
async fn fetches_and_caches_token() {
    let (base, c) = spawn(StatesMode::Ok).await;
    let cl = client(&base);
    for _ in 0..3 {
        let snap = cl.fetch_states(&BoundingBox::EL_PRAT).await.unwrap();
        assert_eq!(snap.response.states.len(), 1);
        assert_eq!(snap.response.states[0].callsign.as_deref(), Some("VLG12AB"));
        assert_eq!(snap.credits_remaining, Some(3999));
    }
    assert_eq!(
        c.token_calls.load(Ordering::SeqCst),
        1,
        "token should be cached"
    );
    assert_eq!(c.state_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn refreshes_token_once_on_401() {
    let (base, c) = spawn(StatesMode::UnauthorizedOnce).await;
    let snap = client(&base)
        .fetch_states(&BoundingBox::EL_PRAT)
        .await
        .unwrap();
    assert_eq!(snap.response.states.len(), 1);
    assert_eq!(c.token_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn second_401_is_fatal() {
    let (base, c) = spawn(StatesMode::AlwaysUnauthorized).await;
    let err = client(&base)
        .fetch_states(&BoundingBox::EL_PRAT)
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Fatal(_)), "{err:?}");
    assert_eq!(c.state_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn rate_limit_reads_retry_header() {
    let (base, _) = spawn(StatesMode::RateLimited).await;
    let err = client(&base)
        .fetch_states(&BoundingBox::EL_PRAT)
        .await
        .unwrap_err();
    match err {
        FetchError::RateLimited { retry_after } => {
            assert_eq!(retry_after, Duration::from_secs(777))
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
}

#[tokio::test]
async fn connection_refused_is_transient() {
    let err = client("http://127.0.0.1:1")
        .fetch_states(&BoundingBox::EL_PRAT)
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Transient(_)), "{err:?}");
}
