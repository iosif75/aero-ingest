//! Parameter parsing and validation for the query API.
//!
//! Kept separate from the handlers so the rules are unit-testable.

use crate::config::BoundingBox;
use serde::Deserialize;

/// Longest time window one request may cover.
pub const MAX_WINDOW_SECS: i64 = 24 * 3600;
/// Window used when `from` is omitted.
pub const DEFAULT_WINDOW_SECS: i64 = 2 * 3600;
pub const DEFAULT_LIMIT: i64 = 5_000;
pub const MAX_LIMIT: i64 = 50_000;

#[derive(Debug, Default, Deserialize)]
pub struct WindowParams {
    /// Unix seconds; defaults to `to - 2h`.
    pub from: Option<i64>,
    /// Unix seconds; defaults to now.
    pub to: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct BoxParams {
    pub lamin: f64,
    pub lomin: f64,
    pub lamax: f64,
    pub lomax: f64,
    // Not `#[serde(flatten)]`: flattening breaks number parsing in query strings.
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub from: i64,
    pub to: i64,
    pub limit: i64,
}

impl WindowParams {
    pub fn resolve(&self, now: i64) -> Result<Window, String> {
        let to = self.to.unwrap_or(now);
        let from = self.from.unwrap_or(to - DEFAULT_WINDOW_SECS);
        if from > to {
            return Err("`from` must not be after `to`".into());
        }
        if to - from > MAX_WINDOW_SECS {
            return Err(format!("window too long: at most {MAX_WINDOW_SECS} s"));
        }
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT);
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Err(format!("`limit` must be between 1 and {MAX_LIMIT}"));
        }
        Ok(Window { from, to, limit })
    }
}

impl BoxParams {
    pub fn window(&self) -> WindowParams {
        WindowParams {
            from: self.from,
            to: self.to,
            limit: self.limit,
        }
    }

    pub fn bbox(&self) -> Result<BoundingBox, String> {
        let b = BoundingBox {
            lamin: self.lamin,
            lomin: self.lomin,
            lamax: self.lamax,
            lomax: self.lomax,
        };
        b.validate().map_err(|e| e.to_string())?;
        Ok(b)
    }
}

/// Normalise and check an ICAO 24-bit address: six hex digits, lowercased.
pub fn parse_icao24(raw: &str) -> Result<String, String> {
    let s = raw.trim().to_ascii_lowercase();
    if s.len() == 6 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(s)
    } else {
        Err(format!("invalid icao24 {raw:?}: expected 6 hex digits"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_defaults_to_last_two_hours() {
        let w = WindowParams::default().resolve(10_000).unwrap();
        assert_eq!(
            w,
            Window {
                from: 10_000 - 7200,
                to: 10_000,
                limit: DEFAULT_LIMIT
            }
        );
    }

    #[test]
    fn window_rejects_bad_ranges() {
        let p = |from, to, limit| WindowParams { from, to, limit };
        assert!(p(Some(10), Some(5), None).resolve(0).is_err());
        assert!(p(Some(0), Some(MAX_WINDOW_SECS + 1), None)
            .resolve(0)
            .is_err());
        assert!(p(Some(0), Some(MAX_WINDOW_SECS), None).resolve(0).is_ok());
        assert!(p(None, None, Some(0)).resolve(0).is_err());
        assert!(p(None, None, Some(MAX_LIMIT + 1)).resolve(0).is_err());
    }

    #[test]
    fn icao24_is_normalised_and_checked() {
        assert_eq!(parse_icao24(" 3423C4 ").unwrap(), "3423c4");
        assert!(parse_icao24("3423c").is_err());
        assert!(parse_icao24("3423cz").is_err());
    }
}
