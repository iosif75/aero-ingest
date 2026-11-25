//! Query endpoints end to end: Postgres, axum router, HTTP client.
//!
//! The database tests need `TEST_DATABASE_URL` (see tests/storage_pg.rs) and
//! are skipped without it. Validation and the no-storage case always run.

mod common;

use aero_ingest::config::BoundingBox;
use aero_ingest::http::{router, AppState};
use aero_ingest::metrics::Metrics;
use aero_ingest::storage::{NewPoll, Store};
use common::{snapshot, test_store};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

async fn serve(store: Option<Store>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState {
        metrics: Arc::new(Metrics::default()),
        max_silence: Duration::from_secs(180),
        store,
    });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn get(addr: SocketAddr, path: &str) -> (u16, Value) {
    let r = reqwest::get(format!("http://{addr}{path}")).await.unwrap();
    let status = r.status().as_u16();
    (status, r.json().await.unwrap())
}

// Two aircraft over three snapshots. 4ca7b5 is outside the small query box.
const SNAPSHOTS: [&str; 3] = [
    r#"{"time":1000,"states":[
        ["3423c4","VLG12AB ","Spain",1000,1000,2.07,41.29,1000.0,false,90.0,248.0,-4.0,null,1050.0,"1000",false,0,4],
        ["4ca7b5","RYR1","Ireland",1000,1000,2.60,41.60,3000.0,false,180.0,90.0,5.0,null,null,null,false,2,null]]}"#,
    r#"{"time":1060,"states":[
        ["3423c4","VLG12AB ","Spain",1060,1060,2.05,41.28,800.0,false,85.0,248.0,-4.0,null,850.0,"1000",false,0,4]]}"#,
    r#"{"time":1120,"states":[
        ["3423c4","VLG12AB ","Spain",1120,1120,2.03,41.27,600.0,false,80.0,248.0,-4.0,null,650.0,"1000",false,0,4]]}"#,
];

#[tokio::test]
async fn track_and_box_queries() {
    let Some(store) = test_store().await else {
        eprintln!("TEST_DATABASE_URL not set, skipping");
        return;
    };
    for s in SNAPSHOTS {
        let r = snapshot(s);
        store.insert_snapshot(r.time, &r.states).await.unwrap();
    }
    let addr = serve(Some(store)).await;

    // Full track, oldest first; uppercase icao24 is accepted.
    let (st, j) = get(addr, "/aircraft/3423C4/track?from=0&to=2000").await;
    assert_eq!(st, 200, "{j}");
    assert_eq!(j["count"], 3);
    assert_eq!(j["truncated"], false);
    let times: Vec<i64> = j["positions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["time"].as_i64().unwrap())
        .collect();
    assert_eq!(times, vec![1000, 1060, 1120]);
    assert_eq!(j["positions"][0]["callsign"], "VLG12AB");
    assert_eq!(j["positions"][0]["category"], 4);

    // Window bounds are inclusive.
    let (_, j) = get(addr, "/aircraft/3423c4/track?from=1060&to=1120").await;
    assert_eq!(j["count"], 2);

    // Limit truncates and says so.
    let (_, j) = get(addr, "/aircraft/3423c4/track?from=0&to=2000&limit=2").await;
    assert_eq!(j["count"], 2);
    assert_eq!(j["truncated"], true);

    // Box around the airport excludes 4ca7b5.
    let (st, j) = get(
        addr,
        "/positions?lamin=41.2&lomin=2.0&lamax=41.4&lomax=2.2&from=0&to=2000",
    )
    .await;
    assert_eq!(st, 200, "{j}");
    assert_eq!(j["count"], 3);
    assert!(j["positions"]
        .as_array()
        .unwrap()
        .iter()
        .all(|p| p["icao24"] == "3423c4"));

    // Whole default box at t=1000 has both aircraft.
    let (_, j) = get(
        addr,
        "/positions?lamin=40.9&lomin=1.5&lamax=41.7&lomax=2.7&from=1000&to=1000",
    )
    .await;
    assert_eq!(j["count"], 2);

    // Unknown aircraft is an empty result, not an error.
    let (st, j) = get(addr, "/aircraft/aaaaaa/track?from=0&to=2000").await;
    assert_eq!((st, j["count"].as_i64()), (200, Some(0)));
}

#[tokio::test]
async fn poll_feed_reads_every_poll_once() {
    let Some(store) = test_store().await else {
        eprintln!("TEST_DATABASE_URL not set, skipping");
        return;
    };
    let addr = serve(Some(store.clone())).await;

    // Nothing stored yet: an empty feed, and `next_after` echoes the cursor.
    let (st, j) = get(addr, "/polls").await;
    assert_eq!(st, 200, "{j}");
    assert_eq!(
        (j["count"].as_i64(), j["more"].as_bool()),
        (Some(0), Some(false))
    );
    assert!(j["next_after"].is_null());
    let (_, j) = get(addr, "/polls?after=7").await;
    assert_eq!(j["next_after"], 7);

    for s in SNAPSHOTS {
        let r = snapshot(s);
        let poll = NewPoll {
            snapshot_time: r.time,
            credits_remaining: Some(4000 - r.time / 60),
            bbox: BoundingBox::AEROSCAN,
        };
        store.store_poll(&poll, &r.states).await.unwrap();
    }

    // Latest poll only, as a reader starting up would ask.
    let (_, j) = get(addr, "/polls?limit=1").await;
    assert_eq!(j["count"], 1);
    assert_eq!(j["more"], false);
    assert_eq!(j["polls"][0]["snapshot_time"], 1120);
    assert_eq!(j["polls"][0]["bbox"]["lamin"], 39.3);

    // From the start, two at a time, following next_after.
    let mut after = 0;
    let mut seen = Vec::new();
    loop {
        let (st, j) = get(addr, &format!("/polls?after={after}&limit=2")).await;
        assert_eq!(st, 200, "{j}");
        for p in j["polls"].as_array().unwrap() {
            let n = p["positions"].as_array().unwrap().len();
            seen.push((p["snapshot_time"].as_i64().unwrap(), n));
        }
        after = j["next_after"].as_i64().unwrap();
        if !j["more"].as_bool().unwrap() {
            break;
        }
    }
    assert_eq!(seen, vec![(1000, 2), (1060, 1), (1120, 1)]);
    let (_, j) = get(addr, &format!("/polls?after={after}")).await;
    assert_eq!(
        (j["count"].as_i64(), j["next_after"].as_i64()),
        (Some(0), Some(after))
    );

    let (_, j) = get(addr, "/polls?limit=1").await;
    let p = &j["polls"][0]["positions"][0];
    assert_eq!(p["icao24"], "3423c4");
    assert_eq!(p["baro_altitude"], 600.0);
}

#[tokio::test]
async fn bad_parameters_are_400() {
    let addr = serve(None).await;
    for path in [
        "/aircraft/xyz/track",
        "/aircraft/3423c4/track?from=10&to=5",
        "/aircraft/3423c4/track?from=0&to=999999",
        "/aircraft/3423c4/track?limit=0",
        "/positions?lamin=42&lomin=1.5&lamax=41&lomax=2.7",
        "/polls?after=-1",
        "/polls?limit=0",
        "/polls?limit=61",
    ] {
        let (st, j) = get(addr, path).await;
        assert_eq!(st, 400, "{path}: {j}");
        assert!(j["error"].is_string(), "{path}: {j}");
    }
}

#[tokio::test]
async fn without_storage_queries_are_503() {
    let addr = serve(None).await;
    for path in ["/aircraft/3423c4/track", "/polls"] {
        let (st, j) = get(addr, path).await;
        assert_eq!(st, 503, "{path}");
        assert!(j["error"].as_str().unwrap().contains("DATABASE_URL"));
    }
}
