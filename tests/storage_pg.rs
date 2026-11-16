//! Storage tests against a real Postgres.
//!
//! Set `TEST_DATABASE_URL` to run them, e.g. after `docker compose up -d`:
//! `TEST_DATABASE_URL=postgres://aero:aero@localhost:5432/aero cargo test`.
//! Without it they are skipped. Each test works in its own throwaway schema.

mod common;

use aero_ingest::storage::{Store, WriteStats, MIGRATOR};
use common::snapshot;
use sqlx::Row;

async fn store() -> Option<Store> {
    common::test_store().await
}

const FIRST: &str = r#"{"time":1731400000,"states":[
    ["3423c4","VLG12AB ","Spain",1731399995,1731399999,2.0785,41.2971,1150.6,false,92.5,248.1,-4.2,null,1188.7,"1000",false,0,4],
    ["4ca7b5","RYR1  ","Ireland",1731399990,1731399999,2.1,41.3,3000.0,false,180.0,90.0,5.0,null,null,null,false,2,null],
    ["abcdef",null,"Spain",null,1731399999,null,null,null,true,0,null,null,null,null,null,false,0]
]}"#;

// One minute later: 3423c4 has a new position, 4ca7b5 repeats its old one.
const SECOND: &str = r#"{"time":1731400060,"states":[
    ["3423c4","VLG12AB ","Spain",1731400055,1731400059,2.05,41.29,900.0,false,85.0,248.0,-4.0,null,940.0,"1000",false,0,4],
    ["4ca7b5","RYR1  ","Ireland",1731399990,1731400050,2.1,41.3,3000.0,false,180.0,90.0,5.0,null,null,null,false,2,null]
]}"#;

#[tokio::test]
async fn inserts_positions_and_drops_duplicates() {
    let Some(store) = store().await else {
        eprintln!("TEST_DATABASE_URL not set, skipping");
        return;
    };

    let a = snapshot(FIRST);
    let w = store.insert_snapshot(a.time, &a.states).await.unwrap();
    assert_eq!(
        w,
        WriteStats {
            candidates: 2,
            inserted: 2
        },
        "row without position is skipped"
    );

    let b = snapshot(SECOND);
    let w = store.insert_snapshot(b.time, &b.states).await.unwrap();
    assert_eq!(
        w,
        WriteStats {
            candidates: 2,
            inserted: 1
        },
        "repeated report is a duplicate"
    );

    // Re-sending the same snapshot inserts nothing.
    let w = store.insert_snapshot(b.time, &b.states).await.unwrap();
    assert_eq!(w.inserted, 0);

    let rows = sqlx::query(
        "SELECT icao24, extract(epoch FROM time_position)::bigint AS tp, callsign, category, \
                position_source, geo_altitude, extract(epoch FROM snapshot_time)::bigint AS st \
         FROM state_vectors ORDER BY icao24, time_position",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 3);

    let r = &rows[0];
    assert_eq!(r.get::<String, _>("icao24"), "3423c4");
    assert_eq!(r.get::<i64, _>("tp"), 1731399995);
    assert_eq!(
        r.get::<Option<String>, _>("callsign").as_deref(),
        Some("VLG12AB")
    );
    assert_eq!(r.get::<Option<i16>, _>("category"), Some(4));
    assert_eq!(r.get::<i64, _>("st"), 1731400000);

    let r = &rows[2];
    assert_eq!(r.get::<String, _>("icao24"), "4ca7b5");
    assert_eq!(r.get::<Option<i16>, _>("position_source"), Some(2));
    assert_eq!(r.get::<Option<f64>, _>("geo_altitude"), None);
}

#[tokio::test]
async fn empty_snapshot_is_a_no_op() {
    let Some(store) = store().await else {
        return;
    };
    let w = store.insert_snapshot(1, &[]).await.unwrap();
    assert_eq!(w, WriteStats::default());
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let Some(store) = store().await else {
        return;
    };
    MIGRATOR.run(store.pool()).await.unwrap();
}

#[tokio::test]
async fn prune_deletes_only_old_rows_in_batches() {
    let Some(store) = store().await else {
        return;
    };
    // Five old positions (t=100..104) and two recent ones (t=1000, 1001).
    let row = |icao: &str, t: i64| {
        format!(
            r#"["{icao}",null,"ES",{t},{t},2.0,41.0,null,false,null,null,null,null,null,null,false,0]"#
        )
    };
    let mut rows: Vec<String> = (0..5).map(|i| row("aaaaaa", 100 + i)).collect();
    rows.push(row("bbbbbb", 1000));
    rows.push(row("bbbbbb", 1001));
    let snap = snapshot(&format!(r#"{{"time":1001,"states":[{}]}}"#, rows.join(",")));
    store
        .insert_snapshot(snap.time, &snap.states)
        .await
        .unwrap();

    // Batch of 2 forces three delete statements for the five old rows.
    let deleted = store.prune_older_than(1000, 2).await.unwrap();
    assert_eq!(deleted, 5);

    let left: Vec<(String, i64)> = sqlx::query_as(
        "SELECT icao24, extract(epoch FROM time_position)::bigint FROM state_vectors ORDER BY 2",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(
        left,
        vec![("bbbbbb".to_string(), 1000), ("bbbbbb".to_string(), 1001)],
        "cutoff is exclusive"
    );

    // Nothing left to prune.
    assert_eq!(store.prune_older_than(1000, 2).await.unwrap(), 0);
}

#[tokio::test]
async fn prune_once_counts_in_metrics() {
    use aero_ingest::metrics::Metrics;
    use aero_ingest::retention::prune_once;
    use std::time::Duration;

    let Some(store) = store().await else {
        return;
    };
    let snap = snapshot(FIRST); // positions from 2024, far older than 30 days
    store
        .insert_snapshot(snap.time, &snap.states)
        .await
        .unwrap();

    let m = Metrics::default();
    let n = prune_once(&store, &m, Duration::from_secs(30 * 86_400)).await;
    assert_eq!(n, Some(2));
    assert!(m.render().contains("aero_rows_pruned_total 2\n"));
}

#[tokio::test]
async fn duplicate_rows_within_one_snapshot_insert_once() {
    let Some(store) = store().await else {
        return;
    };
    let row = r#"["3423c4","VLG12AB","Spain",1000,1000,2.0,41.0,null,false,null,null,null,null,null,null,false,0]"#;
    let snap = snapshot(&format!(r#"{{"time":1000,"states":[{row},{row}]}}"#));
    let w = store
        .insert_snapshot(snap.time, &snap.states)
        .await
        .unwrap();
    assert_eq!(
        w,
        WriteStats {
            candidates: 2,
            inserted: 1
        }
    );
}

#[tokio::test]
async fn missing_position_source_is_stored_as_null() {
    let Some(store) = store().await else {
        return;
    };
    let snap = snapshot(
        r#"{"time":1000,"states":[["3423c4","X","ES",1000,1000,2.0,41.0,null,false,null,null,null,null,null,null,false,null]]}"#,
    );
    store
        .insert_snapshot(snap.time, &snap.states)
        .await
        .unwrap();
    let ps: Option<i16> = sqlx::query_scalar("SELECT position_source FROM state_vectors")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(ps, None);
}
