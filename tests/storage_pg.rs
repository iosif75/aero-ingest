//! Storage tests against a real Postgres.
//!
//! Set `TEST_DATABASE_URL` to run them, e.g. after `docker compose up -d`:
//! `TEST_DATABASE_URL=postgres://aero:aero@localhost:5432/aero cargo test`.
//! Without it they are skipped. Each test works in its own throwaway schema.

use aero_ingest::model::StatesResponse;
use aero_ingest::storage::{Store, WriteStats, MIGRATOR};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, Row};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};

static SEQ: AtomicU32 = AtomicU32::new(0);

async fn store() -> Option<(Store, String)> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
    let schema = format!(
        "aero_test_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
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
    Some((Store::from_pool(pool), schema))
}

fn snapshot(json: &str) -> StatesResponse {
    serde_json::from_str(json).unwrap()
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
    let Some((store, _)) = store().await else {
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
    assert_eq!(r.get::<i16, _>("position_source"), 2);
    assert_eq!(r.get::<Option<f64>, _>("geo_altitude"), None);
}

#[tokio::test]
async fn empty_snapshot_is_a_no_op() {
    let Some((store, _)) = store().await else {
        return;
    };
    let w = store.insert_snapshot(1, &[]).await.unwrap();
    assert_eq!(w, WriteStats::default());
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let Some((store, _)) = store().await else {
        return;
    };
    MIGRATOR.run(store.pool()).await.unwrap();
}
