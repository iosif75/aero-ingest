# aero-ingest

[![CI](https://github.com/iosif75/aero-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/iosif75/aero-ingest/actions/workflows/ci.yml)

A small Rust daemon that polls the [OpenSky Network](https://opensky-network.org) REST API for aircraft
around Barcelona–El Prat and stores what it sees. It is the ingestion layer for
[Aero-Scan](https://github.com/iosif75/aero-scan), which reads it through the [poll feed](#poll-feed).

Status: steps 1–5 of the plan: OAuth2 token manager, poll loop, state-vector parsing, Postgres storage,
retries, `/health`, Prometheus metrics, a read-only query API, storage retention, a Grafana dashboard, and the
poll feed for Aero-Scan.

## Setup

1. Create an API client on your OpenSky account page and download `credentials.json`.
   Keep it **outside** this repo (e.g. `%USERPROFILE%\.config\aero-ingest\credentials.json`).
2. `cp .env.example .env` and set `OPENSKY_CREDENTIALS_FILE` to that path
   (or set `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`).
3. Start Postgres: `docker compose up -d` (and keep `DATABASE_URL` from `.env.example`).
   Leave `DATABASE_URL` unset to run in log-only mode.
4. `cargo run --release`. Migrations run automatically at startup.

`RUST_LOG=aero_ingest=debug` logs every aircraft on each poll instead of just the counts.

## Running in Docker

The daemon has its own image and an opt-in compose service, so plain `docker compose up -d` still starts only
Postgres.

```
# .env must set OPENSKY_CREDENTIALS_FILE to the host path of credentials.json
docker compose --profile daemon up -d --build
docker compose logs -f aero-ingest
curl localhost:9090/health
```

- Credentials are mounted as a compose secret at `/run/secrets/opensky_credentials`, so they stay out of the image
  and out of `docker inspect`.
- On Linux the secret keeps the host file's owner and mode. If `credentials.json` is `chmod 600`, set `AERO_UID` and
  `AERO_GID` in `.env` to its owner (`id -u`, `id -g`) so the container can read it. Docker Desktop on Windows and
  macOS doesn't need this.
- The container talks to Postgres at `db:5432`. `AERO_RETENTION_DAYS`, `AERO_POLL_INTERVAL_SECS` and `RUST_LOG`
  are passed through from `.env`.
- The API is published on `127.0.0.1:9090` only.
- The image runs as a non-root user and has a built-in health check (`aero-ingest healthcheck` calls `/health`,
  since the image has no curl). `docker ps` shows it as healthy or unhealthy.
- Multi-stage build: about 130 MB, with only the binary and TLS roots on top of `debian:bookworm-slim`.

## Dashboard

Prometheus and Grafana come as another opt-in compose profile, with the data sources and an `aero-ingest`
dashboard provisioned from `monitoring/`.

```
docker compose --profile daemon --profile monitoring up -d --build
```

Then open <http://localhost:3000>. The dashboard is the home page and can be viewed without logging in; log in as
`admin` (password `admin`, or `GRAFANA_ADMIN_PASSWORD` from `.env`) to edit. Prometheus is on
<http://localhost:9091>, since 9090 is the daemon's. Both are bound to localhost only.

- **Status**: whether Prometheus can scrape the daemon, time since the last good poll (red at 180 s, when `/health`
  goes `stale`), DB write failures in a row (red at 3, when it goes `db_failing`), credits remaining, aircraft in
  the box and uptime.
- **Polling**: poll outcomes (ok, failed, 429, retries), aircraft and stale positions per poll, credits remaining
  and credits used per hour.
- **Storage**: rows inserted, duplicate and skipped as malformed, DB and retention errors, rows pruned, and the
  table's size on disk and estimated row count.
- **Traffic**, straight from `state_vectors`: a map of each aircraft's latest position in the last 10 minutes,
  coloured by altitude and pointing along its track, the most reported aircraft in the time range, and stored
  positions and distinct aircraft over time.

Prometheus keeps 30 days of metrics (`PROMETHEUS_RETENTION` in `.env` to change it). To scrape a daemon started
with `cargo run` instead of the compose service, change the target in `monitoring/prometheus.yml` to
`host.docker.internal:9090` and run the daemon with `AERO_HTTP_ADDR=0.0.0.0:9090`. The dashboard is provisioned
read-only, so to change it, edit a copy in Grafana, export the JSON and replace
`monitoring/grafana/dashboards/aero-ingest.json`. The map's base layer is loaded from the internet by your browser.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OPENSKY_CREDENTIALS_FILE` | – | Path to `credentials.json` (`clientId`/`clientSecret` or snake_case keys) |
| `OPENSKY_CLIENT_ID`, `OPENSKY_CLIENT_SECRET` | – | Used if no file is set |
| `AERO_LAMIN`, `AERO_LOMIN`, `AERO_LAMAX`, `AERO_LOMAX` | 39.3, 0, 43.3, 5 | Bounding box (degrees): Aero-Scan's region |
| `AERO_POLL_INTERVAL_SECS` | 60 | Poll interval, minimum 5 |
| `DATABASE_URL` | – | Postgres URL; unset means log-only |
| `AERO_HTTP_ADDR` | 127.0.0.1:9090 | Address for `/health`, `/metrics`, the query API and `/polls` |
| `AERO_RETENTION_DAYS` | 30 | Delete positions older than this many days; `0` keeps everything |

The default box is Aero-Scan's monitored region: El Prat, Girona, Reus, Sabadell and Palma. At 20 square degrees it
is still 1 credit per poll, so at startup the daemon logs an estimated 1,440 credits a day with the defaults (a
registered account has 4,000/day). Aero-Scan should then stop polling OpenSky itself, so the credits are only spent
once.

## Behaviour

- **Auth** (`src/auth.rs`): OAuth2 client credentials. The token is cached and refreshed 60 s before it expires.
- **Client** (`src/client.rs`): `GET /states/all?…&extended=1`. A 401 refreshes the token and retries once; a
  second 401/403 stops the daemon. A 429 pauses for `X-Rate-Limit-Retry-After-Seconds` (300 s if missing).
  Network errors, timeouts, 5xx and bad JSON are logged and retried on the next tick.
- **Parsing** (`src/model.rs`): hand-written deserialiser for the positional state-vector arrays. Nulls become
  `None`, callsign padding is trimmed, extra trailing fields are ignored. A malformed row is logged and skipped
  (`aero_rows_skipped_total`) instead of failing the whole poll.
- **Storage** (`src/storage.rs`, `migrations/`): table `state_vectors` with primary key `(icao24, time_position)`.
  Each snapshot is one `INSERT … SELECT FROM UNNEST(…) ON CONFLICT DO NOTHING`, so the repeated reports OpenSky
  returns until an aircraft sends a new position are dropped for free. Rows without a position are not stored.
  A database error is logged and that snapshot is skipped; the loop keeps running. Each poll also gets a row in
  `polls`, written in the same transaction as its positions, which is what the [poll feed](#poll-feed) follows.
- **Retries** (`src/poller.rs`): a transient failure is retried within the same tick after 2 s, 4 s and 8 s,
  as long as the total wait stays under half the poll interval. If every retry fails, that tick is skipped.
- **Health** (`GET /health`, `src/http.rs`): JSON with `status` and timings. `ok`, `starting` (no poll yet, within
  the grace period) and `rate_limited` (waiting out a 429) return 200. `stale` returns 503: no successful poll for
  3× the interval (at least 180 s). `db_failing` also returns 503: polling
  works but the last 3 snapshot writes failed. It clears on the next successful write.
- **Metrics** (`GET /metrics`, `src/metrics.rs`): Prometheus text format. Counters for polls ok/failed/rate-limited,
  retries, rows inserted/duplicate/pruned/skipped, DB errors and prune errors; gauges for aircraft and stale positions on the last poll, consecutive DB write failures, credits
  remaining, last success time and start time.
- **Query API** (`src/http.rs`, `src/query.rs`): see below.
- **Retention** (`src/retention.rs`): at startup and then hourly, positions and polls older than
  `AERO_RETENTION_DAYS` are deleted in batches of 10,000 rows so no single statement holds locks for long. A failed pass is logged and
  retried next hour. Progress shows up as `aero_rows_pruned_total` and `aero_prune_errors_total` in `/metrics`.
- **Poll loop** (`src/poller.rs`): logs aircraft count, how many have a position, how many positions are stale
  (> 60 s old), how many are on the ground, and credits remaining. Ctrl+C or SIGTERM (`docker stop`) exits cleanly.

`aero-ingest healthcheck` exits 0 if `/health` on `AERO_HTTP_ADDR` answers 200, otherwise 1.

Exit codes: `0` clean shutdown, `1` fatal auth error, `2` bad configuration, `3` can't reach Postgres at startup,
`4` can't bind the HTTP address.

## Query API

Times are Unix seconds between 0 and 253402300799 (year 9999), and both bounds are inclusive. `to` defaults to now, `from` to `to` minus 2 hours. One
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

## Poll feed

```
GET /polls?after=&limit=
```

The feed Aero-Scan reads instead of calling OpenSky: one entry per successful poll, oldest first, each with the
positions that poll brought in. Without `after` it returns the latest `limit` polls; with `after` it returns the
polls stored after that `id`. `limit` defaults to 10 (max 60, an hour of polls). Pass `next_after` back as
`after` to read the next ones, and ask again straight away while `more` is `true`.

```json
{
  "count": 1, "more": false, "next_after": 4213,
  "polls": [
    { "id": 4213, "snapshot_time": 1731400000, "aircraft": 64, "credits_remaining": 3421,
      "bbox": { "lamin": 39.3, "lomin": 0.0, "lamax": 43.3, "lomax": 5.0 },
      "positions": [
        { "icao24": "3423c4", "time": 1731399995, "callsign": "VLG12AB", "latitude": 41.2971, "longitude": 2.0785,
          "baro_altitude": 1150.6, "geo_altitude": 1188.7, "velocity": 92.5, "true_track": 248.1,
          "vertical_rate": -4.2, "on_ground": false, "squawk": "1000", "category": 4 }
      ] }
  ]
}
```

- **Complete polls only.** A poll's row and its positions are committed together, so a poll in the feed never gains
  positions later. Following `next_after` reads every poll exactly once, in order.
- **Each report once.** `positions` holds the reports first stored by that poll. A report OpenSky repeats because
  the aircraft hasn't sent a new position belongs to the earlier poll, so it isn't repeated. Positions without
  coordinates are never stored; `aircraft` counts every state vector in the response.
- **Gaps are real.** A failed or rate-limited poll, or one whose write failed, has no entry. Compare
  `snapshot_time` values to spot them. Polls older than the retention period drop out of the feed.
- **Units are OpenSky's**: metres, m/s and degrees, as in the query API. Converting to feet and knots is up to the
  reader.

## Tests

`cargo test` runs unit tests plus integration tests against a fake OpenSky server on localhost
(token caching, 401 retry, fatal 401, 429 retry header, connection errors, retry after a 503, the `/health`
and `/metrics` endpoints, and query and feed parameter validation). They don't touch the real API.

Storage and query API tests need a Postgres and are skipped otherwise:

```
docker compose up -d
TEST_DATABASE_URL=postgres://aero:aero@localhost:5432/aero cargo test
```

Each one runs in its own throwaway schema (`aero_test_*`), so they won't touch the daemon's data.
