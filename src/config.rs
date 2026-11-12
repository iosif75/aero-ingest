//! Configuration from environment variables (and an optional `.env`).

use serde::Deserialize;
use std::fmt;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read credentials file {path}: {source}")]
    CredentialsRead {
        path: String,
        source: std::io::Error,
    },
    #[error("could not parse credentials file {path}: {source} (expected keys clientId/clientSecret or client_id/client_secret)")]
    CredentialsParse {
        path: String,
        source: serde_json::Error,
    },
    #[error("no credentials: set OPENSKY_CREDENTIALS_FILE, or OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET")]
    MissingCredentials,
    #[error("invalid value for {name}: {value:?}")]
    Invalid { name: &'static str, value: String },
    #[error("invalid bounding box: {0}")]
    BadBox(String),
}

/// OAuth2 client credentials. `Debug` never prints the secret.
#[derive(Clone, Deserialize)]
pub struct Credentials {
    #[serde(alias = "clientId", alias = "client_id")]
    pub client_id: String,
    #[serde(alias = "clientSecret", alias = "client_secret")]
    pub client_secret: String,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .finish()
    }
}

impl Credentials {
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let p = path.display().to_string();
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::CredentialsRead {
            path: p.clone(),
            source,
        })?;
        Self::from_json(&raw).map_err(|source| ConfigError::CredentialsParse { path: p, source })
    }

    pub fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }
}

/// Geographic box in decimal degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    pub lamin: f64,
    pub lomin: f64,
    pub lamax: f64,
    pub lomax: f64,
}

impl BoundingBox {
    /// Roughly 0.8 x 1.2 degrees around El Prat (BCN).
    pub const EL_PRAT: Self = Self {
        lamin: 40.9,
        lomin: 1.5,
        lamax: 41.7,
        lomax: 2.7,
    };

    pub fn validate(&self) -> Result<(), ConfigError> {
        let Self {
            lamin,
            lomin,
            lamax,
            lomax,
        } = *self;
        if !(-90.0..=90.0).contains(&lamin) || !(-90.0..=90.0).contains(&lamax) {
            return Err(ConfigError::BadBox("latitude out of [-90, 90]".into()));
        }
        if !(-180.0..=180.0).contains(&lomin) || !(-180.0..=180.0).contains(&lomax) {
            return Err(ConfigError::BadBox("longitude out of [-180, 180]".into()));
        }
        if lamin >= lamax || lomin >= lomax {
            return Err(ConfigError::BadBox("min must be below max".into()));
        }
        Ok(())
    }

    pub fn area_sq_deg(&self) -> f64 {
        (self.lamax - self.lamin) * (self.lomax - self.lomin)
    }

    /// Credits per `/states/all` call, per the OpenSky docs' area tiers.
    pub fn credits_per_request(&self) -> u32 {
        match self.area_sq_deg() {
            a if a <= 25.0 => 1,
            a if a <= 100.0 => 2,
            a if a <= 400.0 => 3,
            _ => 4,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub credentials: Credentials,
    pub bbox: BoundingBox,
    pub poll_interval: Duration,
    pub api_base: String,
    pub token_url: String,
}

pub const DEFAULT_API_BASE: &str = "https://opensky-network.org/api";
pub const DEFAULT_TOKEN_URL: &str =
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token";

impl Config {
    /// Build from the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Build from any key lookup (lets tests avoid touching the real env).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let credentials = match get("OPENSKY_CREDENTIALS_FILE").filter(|s| !s.is_empty()) {
            Some(path) => Credentials::from_file(Path::new(&path))?,
            None => match (get("OPENSKY_CLIENT_ID"), get("OPENSKY_CLIENT_SECRET")) {
                (Some(id), Some(secret)) if !id.is_empty() && !secret.is_empty() => Credentials {
                    client_id: id,
                    client_secret: secret,
                },
                _ => return Err(ConfigError::MissingCredentials),
            },
        };

        let f = |name: &'static str, default: f64| -> Result<f64, ConfigError> {
            match get(name) {
                None => Ok(default),
                Some(v) => v
                    .trim()
                    .parse()
                    .map_err(|_| ConfigError::Invalid { name, value: v }),
            }
        };
        let d = BoundingBox::EL_PRAT;
        let bbox = BoundingBox {
            lamin: f("AERO_LAMIN", d.lamin)?,
            lomin: f("AERO_LOMIN", d.lomin)?,
            lamax: f("AERO_LAMAX", d.lamax)?,
            lomax: f("AERO_LOMAX", d.lomax)?,
        };
        bbox.validate()?;

        let secs: u64 = match get("AERO_POLL_INTERVAL_SECS") {
            None => 60,
            Some(v) => v
                .trim()
                .parse()
                .ok()
                .filter(|s| *s >= 5)
                .ok_or(ConfigError::Invalid {
                    name: "AERO_POLL_INTERVAL_SECS (min 5)",
                    value: v,
                })?,
        };

        Ok(Config {
            credentials,
            bbox,
            poll_interval: Duration::from_secs(secs),
            api_base: get("OPENSKY_API_BASE").unwrap_or_else(|| DEFAULT_API_BASE.into()),
            token_url: get("OPENSKY_TOKEN_URL").unwrap_or_else(|| DEFAULT_TOKEN_URL.into()),
        })
    }

    /// Credits used per day at the configured interval.
    pub fn estimated_daily_credits(&self) -> u64 {
        let polls = 86_400 / self.poll_interval.as_secs().max(1);
        polls * u64::from(self.bbox.credits_per_request())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn credentials_accept_both_key_styles() {
        let a = Credentials::from_json(r#"{"clientId":"a","clientSecret":"s"}"#).unwrap();
        let b = Credentials::from_json(r#"{"client_id":"a","client_secret":"s"}"#).unwrap();
        assert_eq!(a.client_id, b.client_id);
    }

    #[test]
    fn debug_hides_secret() {
        let c = Credentials {
            client_id: "id".into(),
            client_secret: "hunter2".into(),
        };
        assert!(!format!("{c:?}").contains("hunter2"));
    }

    #[test]
    fn defaults_give_1440_credits_a_day() {
        let c = Config::from_lookup(lookup(&[
            ("OPENSKY_CLIENT_ID", "id"),
            ("OPENSKY_CLIENT_SECRET", "s"),
        ]))
        .unwrap();
        assert_eq!(c.bbox, BoundingBox::EL_PRAT);
        assert_eq!(c.estimated_daily_credits(), 1440);
    }

    #[test]
    fn missing_credentials_is_an_error() {
        assert!(matches!(
            Config::from_lookup(lookup(&[])),
            Err(ConfigError::MissingCredentials)
        ));
    }

    #[test]
    fn rejects_inverted_box() {
        let r = Config::from_lookup(lookup(&[
            ("OPENSKY_CLIENT_ID", "id"),
            ("OPENSKY_CLIENT_SECRET", "s"),
            ("AERO_LAMIN", "42"),
        ]));
        assert!(matches!(r, Err(ConfigError::BadBox(_))));
    }

    #[test]
    fn credit_tiers() {
        let big = BoundingBox {
            lamin: 30.0,
            lomin: 0.0,
            lamax: 40.0,
            lomax: 10.0,
        };
        assert_eq!(big.credits_per_request(), 2);
    }
}
