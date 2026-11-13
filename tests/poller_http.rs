//! Poll loop retries and the /health and /metrics endpoints, end to end.

use aero_ingest::auth::TokenManager;
use aero_ingest::client::OpenSkyClient;
use aero_ingest::config::{BoundingBox, Credentials};
use aero_ingest::http::{router, AppState};
use aero_ingest::metrics::Metrics;
use aero_ingest::poller::{self, Exit};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const STATES: &str = r#"{"time":1731400000,"states":[["3423c4","VLG12AB ","Spain",1731399995,1731399999,2.0785,41.2971,1150.6,false,92.5,248.1,-4.2,null,1188.7,"1000",false,0,4]]}"#;

/// Fake OpenSky: `/states/all` answers 503 for the first `failures` calls, then 200.
async fn flaky_server(failures: usize) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let c = c.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let (status, extra, body) = if req.starts_with("POST /token") {
                    ("200 OK", "", r#"{"access_token":"tok","expires_in":1800}"#)
                } else if c.fetch_add(1, Ordering::SeqCst) < failures {
                    ("503 Service Unavailable", "", "")
                } else {
                    ("200 OK", "X-Rate-Limit-Remaining: 3990\r\n", STATES)
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (base, calls)
}

fn client(base: &str) -> OpenSkyClient {
    let http = reqwest::Client::new();
    let creds = Credentials {
        client_id: "id".into(),
        client_secret: "secret".into(),
    };
    let auth = TokenManager::new(http.clone(), format!("{base}/token"), creds);
    OpenSkyClient::new(http, format!("{base}/api"), auth)
}

#[tokio::test]
async fn retries_transient_failure_within_the_tick() {
    let (base, calls) = flaky_server(1).await;
    let cl = client(&base);
    let metrics = Metrics::default();

    // 10 s interval allows one 2 s retry. Stop once the first success lands.
    let m = &metrics;
    let stop = async {
        while m.last_success().is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let exit = tokio::time::timeout(
        Duration::from_secs(8),
        poller::run(
            &cl,
            None,
            m,
            BoundingBox::EL_PRAT,
            Duration::from_secs(10),
            stop,
        ),
    )
    .await
    .expect("poller should succeed on the retry");

    assert!(matches!(exit, Exit::Shutdown));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.fetch_retries.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.polls_ok.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.credits_remaining(), Some(3990));
}

#[tokio::test]
async fn health_and_metrics_endpoints() {
    let metrics = Arc::new(Metrics::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState {
        metrics: metrics.clone(),
        max_silence: Duration::from_secs(180),
        store: None,
    });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let http = reqwest::Client::new();
    let get = |path: &str| http.get(format!("http://{addr}{path}")).send();

    let r = get("/health").await.unwrap();
    assert_eq!(r.status(), 200);
    let j: serde_json::Value = r.json().await.unwrap();
    assert_eq!(j["status"], "starting");

    metrics.record_success(aero_ingest::metrics::now_unix(), 12, 1, Some(3000));
    let j: serde_json::Value = get("/health").await.unwrap().json().await.unwrap();
    assert_eq!(j["status"], "ok");
    assert_eq!(j["credits_remaining"], 3000);

    let r = get("/metrics").await.unwrap();
    assert!(r.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    let text = r.text().await.unwrap();
    assert!(text.contains("aero_polls_ok_total 1\n"));
    assert!(text.contains("aero_aircraft_last_poll 12\n"));
}
