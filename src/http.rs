//! `/health` and `/metrics` endpoints.

use crate::metrics::{now_unix, Metrics};
use axum::extract::State;
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
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(state)
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
