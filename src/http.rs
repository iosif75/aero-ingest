//! HTTP API: `/health`, `/metrics`, and the read-only query endpoints.
//!
//! There is no authentication, so keep it bound to localhost (the default)
//! or behind a reverse proxy.

use crate::metrics::{now_unix, Metrics};
use crate::query::{parse_icao24, BoxParams, Window, WindowParams};
use crate::storage::{Position, Store};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub metrics: Arc<Metrics>,
    /// Report unhealthy when the last success is older than this.
    pub max_silence: Duration,
    /// `None` in log-only mode; query endpoints then answer 503.
    pub store: Option<Store>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/aircraft/{icao24}/track", get(track))
        .route("/positions", get(positions))
        .with_state(state)
}

/// A JSON error body with a status code.
pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

fn bad_request(msg: String) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg)
}

fn store(s: &AppState) -> Result<&Store, ApiError> {
    s.store.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage is disabled (DATABASE_URL not set)".into(),
        )
    })
}

fn db_error(e: sqlx::Error) -> ApiError {
    tracing::error!(error = %e, "query failed");
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "database query failed".into(),
    )
}

#[derive(Debug, Serialize)]
pub struct PositionsResponse {
    pub from: i64,
    pub to: i64,
    pub count: usize,
    /// True if more rows matched than `limit`; narrow the window to get the rest.
    pub truncated: bool,
    pub positions: Vec<Position>,
}

impl PositionsResponse {
    fn new(w: Window, mut rows: Vec<Position>) -> Self {
        let truncated = rows.len() as i64 > w.limit;
        rows.truncate(w.limit as usize);
        Self {
            from: w.from,
            to: w.to,
            count: rows.len(),
            truncated,
            positions: rows,
        }
    }
}

/// `GET /aircraft/{icao24}/track?from=&to=&limit=`
async fn track(
    State(s): State<AppState>,
    Path(icao24): Path<String>,
    Query(p): Query<WindowParams>,
) -> Result<Json<PositionsResponse>, ApiError> {
    let icao24 = parse_icao24(&icao24).map_err(bad_request)?;
    let w = p.resolve(now_unix()).map_err(bad_request)?;
    let rows = store(&s)?
        .track(&icao24, w.from, w.to, w.limit)
        .await
        .map_err(db_error)?;
    Ok(Json(PositionsResponse::new(w, rows)))
}

/// `GET /positions?lamin=&lomin=&lamax=&lomax=&from=&to=&limit=`
async fn positions(
    State(s): State<AppState>,
    Query(p): Query<BoxParams>,
) -> Result<Json<PositionsResponse>, ApiError> {
    let bbox = p.bbox().map_err(bad_request)?;
    let w = p.window().resolve(now_unix()).map_err(bad_request)?;
    let rows = store(&s)?
        .in_box(&bbox, w.from, w.to, w.limit)
        .await
        .map_err(db_error)?;
    Ok(Json(PositionsResponse::new(w, rows)))
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Polling normally.
    Ok,
    /// Started recently, no successful poll yet.
    Starting,
    /// Out of credits and waiting on OpenSky; expected, not a fault.
    RateLimited,
    /// No successful poll for longer than `max_silence`.
    Stale,
}

#[derive(Debug, Serialize)]
pub struct HealthReport {
    pub status: Status,
    pub last_success_unix: Option<i64>,
    pub seconds_since_success: Option<i64>,
    pub rate_limited_until_unix: Option<i64>,
    pub credits_remaining: Option<i64>,
    pub uptime_seconds: i64,
}

impl HealthReport {
    pub fn http_status(&self) -> StatusCode {
        match self.status {
            Status::Ok | Status::Starting | Status::RateLimited => StatusCode::OK,
            Status::Stale => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// Decide health from the metrics at time `now`.
pub fn assess(m: &Metrics, max_silence: Duration, now: i64) -> HealthReport {
    let max = max_silence.as_secs() as i64;
    let last = m.last_success();
    let since = last.map(|t| now - t);
    let limited = m.rate_limited_until().filter(|&until| until > now);
    let uptime = now - m.started_at();

    let status = match (since, limited) {
        (Some(s), _) if s <= max => Status::Ok,
        (_, Some(_)) => Status::RateLimited,
        (None, None) if uptime <= max => Status::Starting,
        _ => Status::Stale,
    };

    HealthReport {
        status,
        last_success_unix: last,
        seconds_since_success: since,
        rate_limited_until_unix: limited,
        credits_remaining: m.credits_remaining(),
        uptime_seconds: uptime,
    }
}

async fn health(State(s): State<AppState>) -> impl IntoResponse {
    let report = assess(&s.metrics, s.max_silence, now_unix());
    (report.http_status(), Json(report))
}

async fn metrics(State(s): State<AppState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        s.metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: Duration = Duration::from_secs(180);

    #[test]
    fn starting_then_stale_without_success() {
        let m = Metrics::new(1000);
        assert_eq!(assess(&m, MAX, 1100).status, Status::Starting);
        assert_eq!(assess(&m, MAX, 1181).status, Status::Stale);
    }

    #[test]
    fn ok_after_recent_success() {
        let m = Metrics::new(1000);
        m.record_success(1050, 10, 0, None);
        let r = assess(&m, MAX, 1200);
        assert_eq!(r.status, Status::Ok);
        assert_eq!(r.seconds_since_success, Some(150));
        assert_eq!(r.http_status(), StatusCode::OK);
    }

    #[test]
    fn rate_limit_is_healthy_until_it_expires() {
        let m = Metrics::new(1000);
        m.record_success(1050, 10, 0, None);
        m.record_rate_limited(5000);
        assert_eq!(assess(&m, MAX, 2000).status, Status::RateLimited);
        let r = assess(&m, MAX, 6000);
        assert_eq!(r.status, Status::Stale);
        assert_eq!(r.http_status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
