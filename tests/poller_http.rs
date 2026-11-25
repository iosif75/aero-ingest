//! Poll loop retries and the /health and /metrics endpoints, end to end.

mod common;

use aero_ingest::config::BoundingBox;
use aero_ingest::http::{probe, router, AppState};
use aero_ingest::metrics::Metrics;
use aero_ingest::poller::{self, Exit};
use common::{fake_server, opensky_client as client, Reply, STATES};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// Fake OpenSky: `/states/all` answers 503 for the first `failures` calls, then 200.
async fn flaky_server(failures: usize) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let base = fake_server(move |req| {
        if req.starts_with("POST /token") {
            Reply::token()
        } else if c.fetch_add(1, Ordering::SeqCst) < failures {
            Reply::status("503 Service Unavailable")
        } else {
            Reply::ok(STATES).header("X-Rate-Limit-Remaining", "3990")
        }
    })
    .await;
    (base, calls)
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
async fn each_poll_lands_in_the_feed() {
    let Some(store) = common::test_store().await else {
        eprintln!("TEST_DATABASE_URL not set, skipping");
        return;
    };
    let (base, _) = flaky_server(0).await;
    let cl = client(&base);
    let metrics = Metrics::default();
    let m = &metrics;
    let stop = async {
        while m.rows_inserted.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        poller::run(
            &cl,
            Some(&store),
            m,
            BoundingBox::AEROSCAN,
            Duration::from_secs(60),
            stop,
        ),
    )
    .await
    .expect("first poll should be stored");

    let polls = store.polls(None, 10).await.unwrap();
    assert_eq!(polls.len(), 1);
    let p = &polls[0];
    assert_eq!(p.snapshot_time, 1731400000);
    assert_eq!(p.credits_remaining, Some(3990));
    assert_eq!(p.bbox, BoundingBox::AEROSCAN);
    assert_eq!(p.positions.len(), 1);
    assert_eq!(p.positions[0].icao24, "3423c4");
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

    // The healthcheck subcommand's probe accepts the running server...
    probe(addr).await.unwrap();
    // ...and fails cleanly when nothing listens.
    assert!(probe("127.0.0.1:1".parse().unwrap()).await.is_err());

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
