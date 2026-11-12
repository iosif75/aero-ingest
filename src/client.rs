//! OpenSky REST client for `/states/all`.

use crate::auth::{truncate, AuthError, TokenManager};
use crate::config::BoundingBox;
use crate::model::StatesResponse;
use reqwest::{header::HeaderMap, StatusCode};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FetchError {
    /// Credentials are wrong or revoked; the daemon should stop.
    #[error("authentication failed: {0}")]
    Fatal(String),
    /// Out of credits. Wait `retry_after` before calling again.
    #[error("rate limited by OpenSky, retry after {retry_after:?}")]
    RateLimited { retry_after: Duration },
    /// Network error, timeout, 5xx or unparseable body. Try again next tick.
    #[error("transient error: {0}")]
    Transient(String),
}

impl From<AuthError> for FetchError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::Rejected { .. } => FetchError::Fatal(e.to_string()),
            AuthError::Transient(m) => FetchError::Transient(m),
        }
    }
}

/// A successful poll.
#[derive(Debug)]
pub struct Snapshot {
    pub response: StatesResponse,
    /// From `X-Rate-Limit-Remaining`, if OpenSky sent it.
    pub credits_remaining: Option<i64>,
}

pub struct OpenSkyClient {
    http: reqwest::Client,
    api_base: String,
    auth: TokenManager,
}

/// Used when a 429 arrives without a usable retry header.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(300);

pub fn retry_after(headers: &HeaderMap) -> Duration {
    header_i64(headers, "x-rate-limit-retry-after-seconds")
        .filter(|s| *s >= 0)
        .map(|s| Duration::from_secs(s as u64))
        .unwrap_or(DEFAULT_RETRY_AFTER)
}

fn header_i64(headers: &HeaderMap, name: &str) -> Option<i64> {
    headers.get(name)?.to_str().ok()?.trim().parse().ok()
}

impl OpenSkyClient {
    pub fn new(http: reqwest::Client, api_base: impl Into<String>, auth: TokenManager) -> Self {
        Self {
            http,
            api_base: api_base.into().trim_end_matches('/').to_string(),
            auth,
        }
    }

    /// Fetch current state vectors inside `bbox`.
    ///
    /// On a 401 the token is refreshed and the call retried once; a second
    /// 401 is fatal.
    pub async fn fetch_states(&self, bbox: &BoundingBox) -> Result<Snapshot, FetchError> {
        for attempt in 0..2 {
            let token = self.auth.token().await?;
            let resp = self
                .http
                .get(format!("{}/states/all", self.api_base))
                .bearer_auth(&token)
                .query(&[
                    ("lamin", bbox.lamin.to_string()),
                    ("lomin", bbox.lomin.to_string()),
                    ("lamax", bbox.lamax.to_string()),
                    ("lomax", bbox.lomax.to_string()),
                    ("extended", "1".to_string()),
                ])
                .send()
                .await
                .map_err(|e| FetchError::Transient(e.to_string()))?;

            let status = resp.status();
            let headers = resp.headers().clone();
            match status {
                s if s.is_success() => {
                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| FetchError::Transient(e.to_string()))?;
                    let response: StatesResponse = serde_json::from_slice(&body).map_err(|e| {
                        FetchError::Transient(format!(
                            "could not parse states response: {e}; body starts: {}",
                            truncate(&String::from_utf8_lossy(&body), 200)
                        ))
                    })?;
                    return Ok(Snapshot {
                        response,
                        credits_remaining: header_i64(&headers, "x-rate-limit-remaining"),
                    });
                }
                StatusCode::UNAUTHORIZED if attempt == 0 => {
                    tracing::warn!("got 401 from OpenSky, refreshing token and retrying once");
                    self.auth.invalidate().await;
                }
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    return Err(FetchError::Fatal(format!("HTTP {status} from /states/all")));
                }
                StatusCode::TOO_MANY_REQUESTS => {
                    return Err(FetchError::RateLimited {
                        retry_after: retry_after(&headers),
                    });
                }
                s => {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(FetchError::Transient(format!(
                        "HTTP {s}: {}",
                        truncate(&body, 200)
                    )));
                }
            }
        }
        unreachable!("loop returns on the second attempt")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn reads_retry_after_header() {
        let mut h = HeaderMap::new();
        h.insert(
            "X-Rate-Limit-Retry-After-Seconds",
            HeaderValue::from_static("1234"),
        );
        assert_eq!(retry_after(&h), Duration::from_secs(1234));
    }

    #[test]
    fn missing_retry_after_uses_default() {
        assert_eq!(retry_after(&HeaderMap::new()), DEFAULT_RETRY_AFTER);
    }
}
