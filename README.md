# aero-ingest

A small Rust daemon that polls the [OpenSky Network](https://opensky-network.org) REST API for aircraft
around Barcelona–El Prat and logs what it sees. It is the ingestion layer for Aeroscan.

Status: steps 1–5 of the plan: OAuth2 token manager, poll loop, state-vector parsing, Postgres storage,
retries, `/health`, Prometheus metrics, and a read-only query API.

## Setup

1. Create an API client on your OpenSky account page and download `credentials.json`.
   Keep it **outside** this repo (e.g. `%USERPROFILE%\.config\aero-ingest\credentials.json`).
2. `cp .env.example .env` and set `OPENSKY_CREDENTIALS_FILE` to that path
   (or set `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`).
3. Start Postgres: `docker compose up -d` (and keep `DATABASE_URL` from `.env.example`).
   Leave `DATABASE_URL` unset to run in log-only mode.
4. `cargo run --release`. Migrations run automatically at startup.

`RUST_LOG=aero_ingest=debug` logs every aircraft on each poll instead of just the counts.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OPENSKY_CREDENTIALS_FILE` | – | Path to `credentials.json` (`clientId`/`clientSecret` or snake_case keys) |
| `OPENSKY_CLIENT_ID`, `OPENSKY_CLIENT_SECRET` | – | Used if no file is set |
| `AERO_LAMIN`, `AERO_LOMIN`, `AERO_LAMAX`, `AERO_LOMAX` | 40.9, 1.5, 41.7, 2.7 | Bounding box (degrees) |
| `AERO_POLL_INTERVAL_SECS` | 60 | Poll interval, minimum 5 |
| `DATABASE_URL` | – | Postgres URL; unset means log-only |
| `AERO_HTTP_ADDR` | 127.0.0.1:9090 | Address for `/health` and `/metrics` |

At startup it logs the estimated daily credit use (1,440 with the defaults; a registered account has 4,000/day).

## Behaviour

- **Auth** (`src/auth.rs`): OAuth2 client credentials. The token is cached and refreshed 60 s before it expires.
- **Client** (`src/client.rs`): `GET /states/all?…&extended=1`. A 401 refreshes the token and retries once; a
  second 401/403 stops the daemon. A 429 pauses for `X-Rate-Limit-Retry-After-Seconds` (300 s if missing).
  Network errors, timeouts, 5xx and bad JSON are logged and retried on the next tick.
- **Parsing** (`src/model.rs`): hand-written deserialiser for the positional state-vector arrays. Nulls become
  `None`, callsign padding is trimmed, extra trailing fields are ignored.
- **Storage** (`src/storage.rs`, `migrations/`): table `state_vectors` with primary key `(icao24, time_position)`.
  Each snapshot is one `INSERT … SELECT FROM UNNEST(…) ON CONFLICT DO NOTHING`, so the repeated reports OpenSky
  returns until an aircraft sends a new position are dropped for free. Rows without a position are not stored.
  A database error is logged and that snapshot is skipped; the loop keeps running.
- **Retries** (`src/poller.rs`): a transient failure is retried within the same tick after 2 s, 4 s and 8 s,
  as long as the total wait stays under half the poll interval. If every retry fails, that tick is skipped.
- **Health** (`GET /health`, `src/http.rs`): JSON with `status` and timings. `ok`, `starting` (no poll yet, within
  the grace period) and `rate_limited` (waiting out a 429) return 200. `stale` returns 503: no successful poll for
  3× the interval (at least 180 s).
- **Metrics** (`GET /metrics`, `src/metrics.rs`): Prometheus text format. Counters for polls ok/failed/rate-limited,
  retries, rows inserted/duplicate and DB errors; gauges for aircraft and stale positions on the last poll, credits
  remaining, last success time and start time.
- **Query API** (`src/http.rs`, `src/query.rs`): see below.
- **Poll loop** (`src/poller.rs`): logs aircraft count, how many have a position, how many positions are stale
  (> 60 s old), how many are on the ground, and credits remaining. Ctrl+C exits cleanly.

Exit codes: `0` clean shutdown, `1` fatal auth error, `2` bad configuration, `3` can't reach Postgres at startup,
`4` can't bind the HTTP address.

## Query API

Times are Unix seconds and both bounds are inclusive. `to` defaults to now, `from` to `to` minus 2 hours. One
request may cover at most 24 hours. `limit` defaults to 5,000 (max 50,000). If more rows match, the response
has `"truncated": true`, so narrow the window to get the rest.

```
GET /aircraft/{icao24}/track?from=&to=&limit=
GET /positions?lamin=&lomin=&lamax=&lomax=&from=&to=&limit=
```

Example: `curl "localhost:9090/aircraft/3423c4/track"` for the last two hours of one aircraft.

```json
{
  "from": 1731392800, "to": 1731400000, "count": 1, "truncated": false,
  "positions": [
    { "icao24": "3423c4", "time": 1731399995, "callsign": "VLG12AB", "latitude": 41.2971, "longitude": 2.0785,
      "baro_altitude": 1150.6, "geo_altitude": 1188.7, "velocity": 92.5, "true_track": 248.1,
      "vertical_rate": -4.2, "on_ground": false, "squawk": "1000", "category": 4 }
  ]
}
```

Track results are oldest first. Box results are ordered by time, then by aircraft. Bad parameters return 400 with
`{"error": "..."}`, and with no `DATABASE_URL` the query endpoints return 503. The API has no authentication, so
keep it on localhost (the default) or behind a reverse proxy.

## Tests

`cargo test` runs unit tests plus integration tests against a fake OpenSky server on localhost
(token caching, 401 retry, fatal 401, 429 retry header, connection errors, retry after a 503, the `/health`
and `/metrics` endpoints, and query parameter validation). They don't touch the real API.

Storage and query API tests need a Postgres and are skipped otherwise:

```
docker compose up -d
TEST_DATABASE_URL=postgres://aero:aero@localhost:5432/aero cargo test
```

Each one runs in its own throwaway schema (`aero_test_*`), so they won't touch the daemon's data.
