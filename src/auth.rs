//! OAuth2 client-credentials token manager for OpenSky.
//!
//! Fetches a bearer token, caches it, and refreshes it shortly before it
//! expires. Accounts created since March 2025 can't use basic auth, so this
//! is the only supported login.

use crate::config::Credentials;
use serde::Deserialize;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Mutex;

/// Refresh this long before the server-side expiry.
pub const REFRESH_MARGIN: Duration = Duration::from_secs(60);
/// Used when the token response has no `expires_in`.
const FALLBACK_LIFETIME: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Error)]
pub enum AuthError {
    /// The server refused our client id/secret. Retrying won't help.
    #[error("OpenSky rejected the client credentials (HTTP {status}): {body}")]
    Rejected { status: u16, body: String },
    /// Network trouble or a 5xx; worth retrying later.
    #[error("token request failed: {0}")]
    Transient(String),
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

struct CachedToken {
    value: String,
    refresh_at: Instant,
}

pub struct TokenManager {
    http: reqwest::Client,
    token_url: String,
    creds: Credentials,
    cached: Mutex<Option<CachedToken>>,
}

/// When to refresh a token with `lifetime`, issued at `now`.
pub fn refresh_deadline(now: Instant, lifetime: Duration) -> Instant {
    // For very short lifetimes, refresh at half-life instead of going negative.
    let margin = REFRESH_MARGIN.min(lifetime / 2);
    now + lifetime - margin
}

impl TokenManager {
    pub fn new(http: reqwest::Client, token_url: impl Into<String>, creds: Credentials) -> Self {
        Self {
            http,
            token_url: token_url.into(),
            creds,
            cached: Mutex::new(None),
        }
    }

    /// A valid bearer token, fetching a new one if needed.
    pub async fn token(&self) -> Result<String, AuthError> {
        let mut guard = self.cached.lock().await;
        if let Some(t) = guard.as_ref() {
            if Instant::now() < t.refresh_at {
                return Ok(t.value.clone());
            }
            tracing::debug!("access token due for refresh");
        }
        let fresh = self.fetch().await?;
        let value = fresh.value.clone();
        *guard = Some(fresh);
        Ok(value)
    }

    /// Drop the cached token so the next call fetches a new one (after a 401).
    pub async fn invalidate(&self) {
        *self.cached.lock().await = None;
    }

    async fn fetch(&self) -> Result<CachedToken, AuthError> {
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.creds.client_id.as_str()),
                ("client_secret", self.creds.client_secret.as_str()),
            ])
            .send()
            .await
            .map_err(|e| AuthError::Transient(e.to_string()))?;

        let status = resp.status();
        if status.is_client_error() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::Rejected {
                status: status.as_u16(),
                body: truncate(&body, 300),
            });
        }
        if !status.is_success() {
            return Err(AuthError::Transient(format!("HTTP {status}")));
        }
        let tr: TokenResponse = resp
            .json()
            .await
            .map_err(|e| AuthError::Transient(format!("bad token JSON: {e}")))?;
        let lifetime = tr
            .expires_in
            .map(Duration::from_secs)
            .unwrap_or(FALLBACK_LIFETIME);
        tracing::info!(
            expires_in_s = lifetime.as_secs(),
            "obtained OpenSky access token"
        );
        Ok(CachedToken {
            value: tr.access_token,
            refresh_at: refresh_deadline(Instant::now(), lifetime),
        })
    }
}

pub(crate) fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refreshes_a_minute_early() {
        let now = Instant::now();
        assert_eq!(
            refresh_deadline(now, Duration::from_secs(1800)),
            now + Duration::from_secs(1740)
        );
    }

    #[test]
    fn short_lifetime_uses_half_life() {
        let now = Instant::now();
        assert_eq!(
            refresh_deadline(now, Duration::from_secs(30)),
            now + Duration::from_secs(15)
        );
    }
}
