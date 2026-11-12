//! OpenSky `/states/all` response types.
//!
//! Each state vector arrives as a JSON array read by position, not an object:
//! `[icao24, callsign, origin_country, time_position, last_contact, lon, lat,
//!   baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors,
//!   geo_altitude, squawk, spi, position_source, category?]`
//! Many fields can be null, and `category` only appears with `extended=1`.

use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::Deserialize;
use std::fmt;

/// Top-level response of `GET /states/all`.
#[derive(Debug, Deserialize)]
pub struct StatesResponse {
    /// Unix time (seconds) the snapshot refers to.
    pub time: i64,
    /// `null` when no aircraft are in the box.
    #[serde(default, deserialize_with = "null_as_empty")]
    pub states: Vec<StateVector>,
}

fn null_as_empty<'de, D>(d: D) -> Result<Vec<StateVector>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<Vec<StateVector>>::deserialize(d)?.unwrap_or_default())
}

/// Origin of a position report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSource {
    AdsB,
    Asterix,
    Mlat,
    Flarm,
    Unknown(u8),
}

impl From<u8> for PositionSource {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::AdsB,
            1 => Self::Asterix,
            2 => Self::Mlat,
            3 => Self::Flarm,
            other => Self::Unknown(other),
        }
    }
}

impl PositionSource {
    /// The numeric code OpenSky uses.
    pub fn code(self) -> i16 {
        match self {
            Self::AdsB => 0,
            Self::Asterix => 1,
            Self::Mlat => 2,
            Self::Flarm => 3,
            Self::Unknown(v) => i16::from(v),
        }
    }
}

/// One aircraft's state, with OpenSky's nulls kept as `Option`.
#[derive(Debug, Clone, PartialEq)]
pub struct StateVector {
    /// ICAO 24-bit address, lowercase hex. Always present.
    pub icao24: String,
    /// Callsign with OpenSky's space padding trimmed; `None` if empty.
    pub callsign: Option<String>,
    pub origin_country: String,
    /// Unix seconds of the last position update.
    pub time_position: Option<i64>,
    /// Unix seconds of the last message of any kind.
    pub last_contact: i64,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    /// Metres.
    pub baro_altitude: Option<f64>,
    pub on_ground: bool,
    /// m/s over ground.
    pub velocity: Option<f64>,
    /// Degrees clockwise from north.
    pub true_track: Option<f64>,
    /// m/s, positive is climbing.
    pub vertical_rate: Option<f64>,
    pub sensors: Option<Vec<i64>>,
    /// Metres.
    pub geo_altitude: Option<f64>,
    pub squawk: Option<String>,
    pub spi: bool,
    pub position_source: PositionSource,
    /// Aircraft category (only with `extended=1`).
    pub category: Option<u8>,
}

impl StateVector {
    /// True if the row carries a usable position.
    pub fn has_position(&self) -> bool {
        self.latitude.is_some() && self.longitude.is_some() && self.time_position.is_some()
    }

    /// Seconds between the last position fix and `now`; `None` without a fix.
    pub fn position_age(&self, now: i64) -> Option<i64> {
        self.time_position.map(|t| now - t)
    }
}

impl<'de> Deserialize<'de> for StateVector {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_seq(StateVectorVisitor)
    }
}

struct StateVectorVisitor;

/// Pull the next element or fail with the field name.
fn next<'de, A, T>(seq: &mut A, idx: usize, name: &'static str) -> Result<T, A::Error>
where
    A: SeqAccess<'de>,
    T: Deserialize<'de>,
{
    seq.next_element()?.ok_or_else(|| {
        de::Error::custom(format_args!(
            "state vector too short: missing [{idx}] {name}"
        ))
    })
}

/// Accept JSON numbers that may be integers or floats.
fn num<'de, A>(seq: &mut A, idx: usize, name: &'static str) -> Result<Option<f64>, A::Error>
where
    A: SeqAccess<'de>,
{
    next::<A, Option<f64>>(seq, idx, name)
}

impl<'de> Visitor<'de> for StateVectorVisitor {
    type Value = StateVector;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an OpenSky state vector array of at least 17 elements")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<StateVector, A::Error> {
        let icao24: String = next(&mut seq, 0, "icao24")?;
        let callsign: Option<String> = next(&mut seq, 1, "callsign")?;
        let origin_country: Option<String> = next(&mut seq, 2, "origin_country")?;
        let time_position: Option<i64> = next(&mut seq, 3, "time_position")?;
        let last_contact: i64 = next(&mut seq, 4, "last_contact")?;
        let longitude = num(&mut seq, 5, "longitude")?;
        let latitude = num(&mut seq, 6, "latitude")?;
        let baro_altitude = num(&mut seq, 7, "baro_altitude")?;
        let on_ground: Option<bool> = next(&mut seq, 8, "on_ground")?;
        let velocity = num(&mut seq, 9, "velocity")?;
        let true_track = num(&mut seq, 10, "true_track")?;
        let vertical_rate = num(&mut seq, 11, "vertical_rate")?;
        let sensors: Option<Vec<i64>> = next(&mut seq, 12, "sensors")?;
        let geo_altitude = num(&mut seq, 13, "geo_altitude")?;
        let squawk: Option<String> = next(&mut seq, 14, "squawk")?;
        let spi: Option<bool> = next(&mut seq, 15, "spi")?;
        let position_source: Option<u8> = next(&mut seq, 16, "position_source")?;
        // Optional 18th element with extended=1.
        let category: Option<u8> = seq.next_element::<Option<u8>>()?.flatten();
        // Ignore anything OpenSky adds later rather than failing the whole poll.
        while seq.next_element::<de::IgnoredAny>()?.is_some() {}

        Ok(StateVector {
            icao24: icao24.trim().to_ascii_lowercase(),
            callsign: callsign
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty()),
            origin_country: origin_country.unwrap_or_default(),
            time_position,
            last_contact,
            longitude,
            latitude,
            baro_altitude,
            on_ground: on_ground.unwrap_or(false),
            velocity,
            true_track,
            vertical_rate,
            sensors,
            geo_altitude,
            squawk: squawk
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            spi: spi.unwrap_or(false),
            position_source: PositionSource::from(position_source.unwrap_or(0)),
            category,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "time": 1731400000,
        "states": [
            ["3423c4","VLG12AB ","Spain",1731399995,1731399999,2.0785,41.2971,1150.62,false,92.5,248.1,-4.23,null,1188.72,"1000",false,0,4],
            ["4ca7b5","        ","Ireland",null,1731399990,null,null,null,true,0,null,null,null,null,null,false,0]
        ]
    }"#;

    #[test]
    fn parses_full_row() {
        let r: StatesResponse = serde_json::from_str(SAMPLE).unwrap();
        assert_eq!(r.time, 1731400000);
        let s = &r.states[0];
        assert_eq!(s.icao24, "3423c4");
        assert_eq!(s.callsign.as_deref(), Some("VLG12AB"));
        assert_eq!(s.latitude, Some(41.2971));
        assert_eq!(s.category, Some(4));
        assert_eq!(s.position_source, PositionSource::AdsB);
        assert!(s.has_position());
        assert_eq!(s.position_age(1731400000), Some(5));
    }

    #[test]
    fn handles_nulls_and_blank_callsign() {
        let r: StatesResponse = serde_json::from_str(SAMPLE).unwrap();
        let s = &r.states[1];
        assert_eq!(s.callsign, None);
        assert_eq!(s.time_position, None);
        assert!(s.on_ground);
        assert_eq!(s.category, None);
        assert!(!s.has_position());
        assert_eq!(s.velocity, Some(0.0));
    }

    #[test]
    fn null_states_is_empty() {
        let r: StatesResponse = serde_json::from_str(r#"{"time":1,"states":null}"#).unwrap();
        assert!(r.states.is_empty());
    }

    #[test]
    fn tolerates_extra_trailing_fields() {
        let json = r#"["abc123","X",null,1,2,1.0,2.0,null,false,null,null,null,[1,2],null,null,null,2,null,"future",42]"#;
        let s: StateVector = serde_json::from_str(json).unwrap();
        assert_eq!(s.sensors, Some(vec![1, 2]));
        assert_eq!(s.position_source, PositionSource::Mlat);
        assert_eq!(s.origin_country, "");
    }

    #[test]
    fn rejects_short_row() {
        let err = serde_json::from_str::<StateVector>(r#"["abc123","X","Spain",1,2]"#).unwrap_err();
        assert!(err.to_string().contains("too short"), "{err}");
    }
}
