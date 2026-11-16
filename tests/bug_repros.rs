//! Regression tests for bugs found in review. Each one failed before its fix.

mod common;

use aero_ingest::client::FetchError;
use aero_ingest::config::BoundingBox;
use aero_ingest::http::{router, AppState};
use aero_ingest::metrics::Metrics;
use aero_ingest::model::StatesResponse;
use aero_ingest::poller;
use common::{fake_server, opensky_client, Reply, STATES};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

/// A 429 from the token endpoint is the auth server throttling us, not a
/// rejected secret. It must not stop the daemon.
#[tokio::test]
async fn token_endpoint_429_is_not_fatal() {
    let base = fake_server(|req| {
        if req.starts_with("POST /token") {
            Reply::status("429 Too Many Requests")
        } else {
            Reply::ok(STATES)
        }
    })
    .await;
    let err = opensky_client(&base)
        .fetch_states(&BoundingBox::EL_PRAT)
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Transient(_)), "{err:?}");
}

/// A bad client secret (Keycloak answers 401 invalid_client) is still fatal.
#[tokio::test]
async fn token_endpoint_401_is_still_fatal() {
    let base =
        fake_server(|_| Reply::status("401 Unauthorized").body(r#"{"error":"invalid_client"}"#))
            .await;
    let err = opensky_client(&base)
        .fetch_states(&BoundingBox::EL_PRAT)
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Fatal(_)), "{err:?}");
}

/// An absurd `expires_in` must not panic on Instant overflow.
#[tokio::test]
async fn huge_token_lifetime_does_not_panic() {
    let base = fake_server(|req| {
        if req.starts_with("POST /token") {
            Reply::ok(r#"{"access_token":"tok","expires_in":18446744073709551615}"#)
        } else {
            Reply::ok(STATES)
        }
    })
    .await;
    let snap = opensky_client(&base)
        .fetch_states(&BoundingBox::EL_PRAT)
        .await
        .unwrap();
    assert_eq!(snap.response.states.len(), 1);
}

/// An absurd retry header must not overflow when the pause end is computed.
#[test]
fn huge_retry_after_is_capped() {
    let mut h = reqwest::header::HeaderMap::new();
    h.insert(
        "X-Rate-Limit-Retry-After-Seconds",
        "9223372036854775807".parse().unwrap(),
    );
    let d = aero_ingest::client::retry_after(&h);
    assert!(d <= Duration::from_secs(86_400), "{d:?}");
}

/// One malformed row must not throw away every other aircraft in the poll.
#[test]
fn one_bad_row_does_not_drop_the_snapshot() {
    let json = r#"{"time":1000,"states":[
        ["3423c4","VLG12AB ","Spain",995,999,2.0,41.0,null,false,null,null,null,null,null,null,false,0],
        ["badbad","X","Spain",995,null,2.0,41.0,null,false,null,null,null,null,null,null,false,0],
        ["4ca7b5","RYR1","Ireland",990,999,2.1,41.3,null,false,null,null,null,null,null,null,false,0]
    ]}"#;
    let r: StatesResponse = serde_json::from_str(json).expect("snapshot should still parse");
    let ids: Vec<&str> = r.states.iter().map(|s| s.icao24.as_str()).collect();
    assert_eq!(ids, vec!["3423c4", "4ca7b5"]);
    assert_eq!(r.skipped, 1);
}

/// After waiting out a 429, the next poll should happen right away rather
/// than one more full interval later.
#[tokio::test]
async fn polls_again_right_after_rate_limit_pause() {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let base = fake_server(move |req| {
        if req.starts_with("POST /token") {
            return Reply::token();
        }
        if c.fetch_add(1, Ordering::SeqCst) == 0 {
            Reply::status("429 Too Many Requests").header("X-Rate-Limit-Retry-After-Seconds", "1")
        } else {
            Reply::ok(STATES)
        }
    })
    .await;
    let client = opensky_client(&base);
    let metrics = Metrics::default();
    let m = &metrics;
    let started = Instant::now();
    let stop = async {
        while m.last_success().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    // 60 s interval: with the bug, the second poll comes 1 s + 60 s later.
    tokio::time::timeout(
        Duration::from_secs(5),
        poller::run(
            &client,
            None,
            m,
            BoundingBox::EL_PRAT,
            Duration::from_secs(60),
            stop,
        ),
    )
    .await
    .expect("second poll should follow the 1 s pause");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

async fn serve_no_store() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState {
        metrics: Arc::new(Metrics::default()),
        max_silence: Duration::from_secs(180),
        store: None,
    });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

/// Extreme `from`/`to` values must be rejected with 400, not overflow
/// (a panic in debug builds, a wrapped window that passes the check in release).
#[tokio::test]
async fn extreme_times_are_rejected() {
    let addr = serve_no_store().await;
    for q in [
        "from=-9223372036854775808&to=0",
        "to=-9223372036854775808",
        "from=0&to=9223372036854775807",
        "from=99999999999999&to=99999999999999",
    ] {
        let r = reqwest::get(format!("http://{addr}/aircraft/3423c4/track?{q}"))
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{q}");
    }
}
