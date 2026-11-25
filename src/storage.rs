//! Postgres storage for state vectors.

use crate::config::BoundingBox;
use crate::model::{PositionSource, StateVector};
use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions};
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
    ///
    /// This does not add the poll to the feed; the poller uses [`Store::store_poll`].
    pub async fn insert_snapshot(
        &self,
        snapshot_time: i64,
        states: &[StateVector],
    ) -> Result<WriteStats, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        insert_states(&mut conn, snapshot_time, states).await
    }

    /// Store a poll's positions and its `polls` row in one transaction, so a
    /// feed reader never sees a poll whose positions are not there yet.
    /// A poll whose snapshot time is already stored adds no second row.
    pub async fn store_poll(
        &self,
        poll: &NewPoll,
        states: &[StateVector],
    ) -> Result<WriteStats, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let w = insert_states(&mut tx, poll.snapshot_time, states).await?;
        sqlx::query(
            "INSERT INTO polls (snapshot_time, aircraft, credits_remaining, lamin, lomin, lamax, lomax) \
             VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (snapshot_time) DO NOTHING",
        )
        .bind(poll.snapshot_time as f64)
        .bind(i32::try_from(states.len()).unwrap_or(i32::MAX))
        .bind(poll.credits_remaining.map(|c| c.clamp(0, i64::from(i32::MAX)) as i32))
        .bind(poll.bbox.lamin)
        .bind(poll.bbox.lomin)
        .bind(poll.bbox.lamax)
        .bind(poll.bbox.lomax)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(w)
    }
}

/// What the poller knows about a poll besides its state vectors.
#[derive(Debug, Clone, Copy)]
pub struct NewPoll {
    /// OpenSky's `time` for the snapshot, unix seconds.
    pub snapshot_time: i64,
    pub credits_remaining: Option<i64>,
    pub bbox: BoundingBox,
}

async fn insert_states(
    conn: &mut PgConnection,
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
    .execute(conn)
    .await?;

    Ok(WriteStats {
        candidates: rows.len(),
        inserted: result.rows_affected(),
    })
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
    position_source: Vec<Option<i16>>,
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
        self.position_source
            .push(s.position_source.map(PositionSource::code));
        self.category.push(s.category.map(i16::from));
    }
}

/// One stored position, as returned by the query API.
#[derive(Debug, Clone, PartialEq, serde::Serialize, sqlx::FromRow)]
pub struct Position {
    pub icao24: String,
    /// Unix seconds.
    pub time: i64,
    pub callsign: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub baro_altitude: Option<f64>,
    pub geo_altitude: Option<f64>,
    pub velocity: Option<f64>,
    pub true_track: Option<f64>,
    pub vertical_rate: Option<f64>,
    pub on_ground: bool,
    pub squawk: Option<String>,
    pub category: Option<i16>,
}

const POSITION_COLUMNS: &str =
    "icao24, extract(epoch FROM time_position)::bigint AS time, callsign, \
     latitude, longitude, baro_altitude, geo_altitude, velocity, true_track, vertical_rate, \
     on_ground, squawk, category";

impl Store {
    /// Positions of one aircraft in `[from, to]` (unix seconds), oldest first.
    /// Fetches up to `limit + 1` rows so the caller can tell if it was cut off.
    pub async fn track(
        &self,
        icao24: &str,
        from: i64,
        to: i64,
        limit: i64,
    ) -> Result<Vec<Position>, sqlx::Error> {
        sqlx::query_as::<_, Position>(&format!(
            "SELECT {POSITION_COLUMNS} FROM state_vectors \
             WHERE icao24 = $1 AND time_position BETWEEN to_timestamp($2) AND to_timestamp($3) \
             ORDER BY time_position LIMIT $4"
        ))
        .bind(icao24)
        .bind(from as f64)
        .bind(to as f64)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await
    }

    /// Every position inside the box in `[from, to]`, ordered by time then aircraft.
    /// Fetches up to `limit + 1` rows so the caller can tell if it was cut off.
    pub async fn in_box(
        &self,
        bbox: &BoundingBox,
        from: i64,
        to: i64,
        limit: i64,
    ) -> Result<Vec<Position>, sqlx::Error> {
        sqlx::query_as::<_, Position>(&format!(
            "SELECT {POSITION_COLUMNS} FROM state_vectors \
             WHERE time_position BETWEEN to_timestamp($1) AND to_timestamp($2) \
               AND latitude BETWEEN $3 AND $4 AND longitude BETWEEN $5 AND $6 \
             ORDER BY time_position, icao24 LIMIT $7"
        ))
        .bind(from as f64)
        .bind(to as f64)
        .bind(bbox.lamin)
        .bind(bbox.lamax)
        .bind(bbox.lomin)
        .bind(bbox.lomax)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await
    }
}

/// One completed poll from the feed, with the positions it brought in.
///
/// `positions` holds the reports first stored by this poll. A report OpenSky
/// repeated from an earlier poll belongs to that earlier poll, so a consumer
/// reading the feed in order sees every stored report exactly once.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Poll {
    pub id: i64,
    /// OpenSky's snapshot time, unix seconds.
    pub snapshot_time: i64,
    /// State vectors in the response, including ones without a position.
    pub aircraft: i32,
    pub credits_remaining: Option<i32>,
    pub bbox: BoundingBox,
    pub positions: Vec<Position>,
}

#[derive(sqlx::FromRow)]
struct PollRow {
    id: i64,
    snapshot_time: i64,
    aircraft: i32,
    credits_remaining: Option<i32>,
    lamin: f64,
    lomin: f64,
    lamax: f64,
    lomax: f64,
}

#[derive(sqlx::FromRow)]
struct PositionInPoll {
    snapshot: i64,
    #[sqlx(flatten)]
    position: Position,
}

const POLL_COLUMNS: &str =
    "id, extract(epoch FROM snapshot_time)::bigint AS snapshot_time, aircraft, \
     credits_remaining, lamin, lomin, lamax, lomax";

impl Store {
    /// Polls with `id > after`, oldest first, or the latest `limit` polls
    /// (still oldest first) when `after` is `None`. Fetches up to `limit + 1`
    /// polls after `after`, so the caller can tell if more are waiting.
    pub async fn polls(&self, after: Option<i64>, limit: i64) -> Result<Vec<Poll>, sqlx::Error> {
        let rows: Vec<PollRow> = match after {
            Some(after) => {
                sqlx::query_as(&format!(
                    "SELECT {POLL_COLUMNS} FROM polls WHERE id > $1 ORDER BY id LIMIT $2"
                ))
                .bind(after)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(&format!(
                    "SELECT * FROM (SELECT {POLL_COLUMNS} FROM polls ORDER BY id DESC LIMIT $1) p \
                     ORDER BY id"
                ))
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        };
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return Ok(Vec::new());
        };
        let (lo, hi) = rows
            .iter()
            .fold((first.snapshot_time, last.snapshot_time), |(lo, hi), r| {
                (lo.min(r.snapshot_time), hi.max(r.snapshot_time))
            });
        let positions: Vec<PositionInPoll> = sqlx::query_as(&format!(
            "SELECT extract(epoch FROM snapshot_time)::bigint AS snapshot, {POSITION_COLUMNS} \
             FROM state_vectors \
             WHERE snapshot_time BETWEEN to_timestamp($1) AND to_timestamp($2) \
             ORDER BY snapshot_time, icao24"
        ))
        .bind(lo as f64)
        .bind(hi as f64)
        .fetch_all(&self.pool)
        .await?;

        let mut by_time: std::collections::HashMap<i64, Vec<Position>> =
            std::collections::HashMap::new();
        for p in positions {
            by_time.entry(p.snapshot).or_default().push(p.position);
        }
        Ok(rows
            .into_iter()
            .map(|r| Poll {
                id: r.id,
                snapshot_time: r.snapshot_time,
                aircraft: r.aircraft,
                credits_remaining: r.credits_remaining,
                bbox: BoundingBox {
                    lamin: r.lamin,
                    lomin: r.lomin,
                    lamax: r.lamax,
                    lomax: r.lomax,
                },
                positions: by_time.remove(&r.snapshot_time).unwrap_or_default(),
            })
            .collect())
    }
}

impl Store {
    /// Delete positions with `time_position` before `cutoff` (unix seconds),
    /// `batch` rows per statement so no single delete holds locks for long.
    /// Polls from before `cutoff` are dropped from the feed as well.
    /// Returns the number of position rows deleted.
    pub async fn prune_older_than(&self, cutoff: i64, batch: i64) -> Result<u64, sqlx::Error> {
        let mut total = 0;
        loop {
            let n = sqlx::query(
                "DELETE FROM state_vectors WHERE ctid IN ( \
                     SELECT ctid FROM state_vectors \
                     WHERE time_position < to_timestamp($1) LIMIT $2)",
            )
            .bind(cutoff as f64)
            .bind(batch)
            .execute(&self.pool)
            .await?
            .rows_affected();
            total += n;
            if (n as i64) < batch {
                break;
            }
        }
        // Poll rows are tiny (one a minute), so one statement is enough.
        sqlx::query("DELETE FROM polls WHERE snapshot_time < to_timestamp($1)")
            .bind(cutoff as f64)
            .execute(&self.pool)
            .await?;
        Ok(total)
    }
}
