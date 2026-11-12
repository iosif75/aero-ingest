//! Postgres storage for state vectors.

use crate::model::StateVector;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

/// Migrations embedded at compile time from `migrations/`.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

/// Result of writing one snapshot.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Rows offered with a usable position.
    pub candidates: usize,
    /// Rows actually inserted (the rest were already stored).
    pub inserted: u64,
}

impl Store {
    /// Connect and apply pending migrations.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(url)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Insert every state with a position in one statement. Rows already
    /// stored under the same `(icao24, time_position)` are skipped.
    pub async fn insert_snapshot(
        &self,
        snapshot_time: i64,
        states: &[StateVector],
    ) -> Result<WriteStats, sqlx::Error> {
        let rows: Vec<&StateVector> = states.iter().filter(|s| s.has_position()).collect();
        if rows.is_empty() {
            return Ok(WriteStats::default());
        }

        let mut c = Columns::with_capacity(rows.len());
        for s in &rows {
            c.push(s);
        }

        let result = sqlx::query(
            r#"
            INSERT INTO state_vectors (
                icao24, time_position, last_contact, snapshot_time, callsign, origin_country,
                latitude, longitude, baro_altitude, geo_altitude, velocity, true_track,
                vertical_rate, on_ground, squawk, spi, position_source, category
            )
            SELECT icao24, to_timestamp(tp), to_timestamp(lc), to_timestamp($4), callsign, origin_country,
                   latitude, longitude, baro_altitude, geo_altitude, velocity, true_track,
                   vertical_rate, on_ground, squawk, spi, position_source, category
            FROM UNNEST(
                $1::text[], $2::bigint[], $3::bigint[], $5::text[], $6::text[],
                $7::float8[], $8::float8[], $9::float8[], $10::float8[], $11::float8[], $12::float8[],
                $13::float8[], $14::bool[], $15::text[], $16::bool[], $17::int2[], $18::int2[]
            ) AS t(icao24, tp, lc, callsign, origin_country,
                   latitude, longitude, baro_altitude, geo_altitude, velocity, true_track,
                   vertical_rate, on_ground, squawk, spi, position_source, category)
            ON CONFLICT (icao24, time_position) DO NOTHING
            "#,
        )
        .bind(&c.icao24)
        .bind(&c.time_position)
        .bind(&c.last_contact)
        .bind(snapshot_time as f64)
        .bind(&c.callsign)
        .bind(&c.origin_country)
        .bind(&c.latitude)
        .bind(&c.longitude)
        .bind(&c.baro_altitude)
        .bind(&c.geo_altitude)
        .bind(&c.velocity)
        .bind(&c.true_track)
        .bind(&c.vertical_rate)
        .bind(&c.on_ground)
        .bind(&c.squawk)
        .bind(&c.spi)
        .bind(&c.position_source)
        .bind(&c.category)
        .execute(&self.pool)
        .await?;

        Ok(WriteStats {
            candidates: rows.len(),
            inserted: result.rows_affected(),
        })
    }
}

/// Column-wise buffers for the UNNEST insert.
struct Columns {
    icao24: Vec<String>,
    time_position: Vec<i64>,
    last_contact: Vec<i64>,
    callsign: Vec<Option<String>>,
    origin_country: Vec<String>,
    latitude: Vec<f64>,
    longitude: Vec<f64>,
    baro_altitude: Vec<Option<f64>>,
    geo_altitude: Vec<Option<f64>>,
    velocity: Vec<Option<f64>>,
    true_track: Vec<Option<f64>>,
    vertical_rate: Vec<Option<f64>>,
    on_ground: Vec<bool>,
    squawk: Vec<Option<String>>,
    spi: Vec<bool>,
    position_source: Vec<i16>,
    category: Vec<Option<i16>>,
}

impl Columns {
    fn with_capacity(n: usize) -> Self {
        Self {
            icao24: Vec::with_capacity(n),
            time_position: Vec::with_capacity(n),
            last_contact: Vec::with_capacity(n),
            callsign: Vec::with_capacity(n),
            origin_country: Vec::with_capacity(n),
            latitude: Vec::with_capacity(n),
            longitude: Vec::with_capacity(n),
            baro_altitude: Vec::with_capacity(n),
            geo_altitude: Vec::with_capacity(n),
            velocity: Vec::with_capacity(n),
            true_track: Vec::with_capacity(n),
            vertical_rate: Vec::with_capacity(n),
            on_ground: Vec::with_capacity(n),
            squawk: Vec::with_capacity(n),
            spi: Vec::with_capacity(n),
            position_source: Vec::with_capacity(n),
            category: Vec::with_capacity(n),
        }
    }

    /// Caller guarantees `s.has_position()`.
    fn push(&mut self, s: &StateVector) {
        self.icao24.push(s.icao24.clone());
        self.time_position
            .push(s.time_position.expect("filtered on has_position"));
        self.last_contact.push(s.last_contact);
        self.callsign.push(s.callsign.clone());
        self.origin_country.push(s.origin_country.clone());
        self.latitude
            .push(s.latitude.expect("filtered on has_position"));
        self.longitude
            .push(s.longitude.expect("filtered on has_position"));
        self.baro_altitude.push(s.baro_altitude);
        self.geo_altitude.push(s.geo_altitude);
        self.velocity.push(s.velocity);
        self.true_track.push(s.true_track);
        self.vertical_rate.push(s.vertical_rate);
        self.on_ground.push(s.on_ground);
        self.squawk.push(s.squawk.clone());
        self.spi.push(s.spi);
        self.position_source.push(s.position_source.code());
        self.category.push(s.category.map(i16::from));
    }
}
