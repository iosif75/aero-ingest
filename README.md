# aero-ingest

A small Rust daemon that polls the [OpenSky Network](https://opensky-network.org) REST API for aircraft
around Barcelona–El Prat and logs what it sees. It is the ingestion layer for Aeroscan.

Status: steps 1–2 of the plan (OAuth2 token manager, poll loop, state-vector parsing). Storage, health/metrics
and the query API come next.

## Setup

1. Create an API client on your OpenSky account page and download `credentials.json`.
   Keep it **outside** this repo (e.g. `%USERPROFILE%\.config\aero-ingest\credentials.json`).
2. `cp .env.example .env` and set `OPENSKY_CREDENTIALS_FILE` to that path
   (or set `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`).
3. `cargo run --release`

`RUST_LOG=aero_ingest=debug` logs every aircraft on each poll instead of just the counts.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OPENSKY_CREDENTIALS_FILE` | – | Path to `credentials.json` (`clientId`/`clientSecret` or snake_case keys) |
| `OPENSKY_CLIENT_ID`, `OPENSKY_CLIENT_SECRET` | – | Used if no file is set |
| `AERO_LAMIN`, `AERO_LOMIN`, `AERO_LAMAX`, `AERO_LOMAX` | 40.9, 1.5, 41.7, 2.7 | Bounding box (degrees) |
| `AERO_POLL_INTERVAL_SECS` | 60 | Poll interval, minimum 5 |

At startup it logs the estimated daily credit use (1,440 with the defaults; a registered account has 4,000/day).

## Behaviour

- **Auth** (`src/auth.rs`): OAuth2 client credentials. The token is cached and refreshed 60 s before it expires.
- **Client** (`src/client.rs`): `GET /states/all?…&extended=1`. A 401 refreshes the token and retries once; a
  second 401/403 stops the daemon. A 429 pauses for `X-Rate-Limit-Retry-After-Seconds` (300 s if missing).
  Network errors, timeouts, 5xx and bad JSON are logged and retried on the next tick.
- **Parsing** (`src/model.rs`): hand-written deserialiser for the positional state-vector arrays. Nulls become
  `None`, callsign padding is trimmed, extra trailing fields are ignored.
- **Poll loop** (`src/poller.rs`): logs aircraft count, how many have a position, how many positions are stale
  (> 60 s old), how many are on the ground, and credits remaining. Ctrl+C exits cleanly.

Exit codes: `0` clean shutdown, `1` fatal auth error, `2` bad configuration.

## Tests

`cargo test` runs unit tests plus integration tests against a fake OpenSky server on localhost
(token caching, 401 retry, fatal 401, 429 retry header, connection errors). They don't touch the real API.
