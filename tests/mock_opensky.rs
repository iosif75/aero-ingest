//! End-to-end tests against a tiny fake OpenSky server on localhost.

mod common;

use aero_ingest::client::FetchError;
use aero_ingest::config::BoundingBox;
use common::{fake_server, opensky_client as client, Reply, STATES};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy)]
enum StatesMode {
    Ok,
    UnauthorizedOnce,
    AlwaysUnauthorized,
    RateLimited,
}

#[derive(Default)]
struct Counters {
    token_calls: AtomicUsize,
    state_calls: AtomicUsize,
}

/// Fake OpenSky that checks each request and answers `/states/all` per `mode`.
async fn spawn(mode: StatesMode) -> (String, Arc<Counters>) {
    let c = Arc::new(Counters::default());
    let counters = c.clone();
    let base = fake_server(move |req| {
        let line = req.lines().next().unwrap_or("");
        if line.starts_with("POST /token") {
            counters.token_calls.fetch_add(1, Ordering::SeqCst);
            assert!(req.contains("grant_type=client_credentials"));
            return Reply::token();
        }
        if !line.starts_with("GET /api/states/all?") {
            return Reply::status("404 Not Found");
        }
        let i = counters.state_calls.fetch_add(1, Ordering::SeqCst);
        assert!(req
            .to_ascii_lowercase()
            .contains("authorization: bearer tok"));
        assert!(line.contains("extended=1") && line.contains("lamin=40.9"));
        match mode {
            StatesMode::Ok => Reply::ok(STATES).header("X-Rate-Limit-Remaining", "3999"),
            StatesMode::UnauthorizedOnce if i == 0 => Reply::status("401 Unauthorized"),
            StatesMode::UnauthorizedOnce => Reply::ok(STATES),
            StatesMode::AlwaysUnauthorized => Reply::status("401 Unauthorized"),
            StatesMode::RateLimited => Reply::status("429 Too Many Requests")
                .header("X-Rate-Limit-Retry-After-Seconds", "777"),
        }
    })
    .await;
    (base, c)
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
