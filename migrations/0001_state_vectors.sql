-- One row per position report. OpenSky repeats the same report across polls
-- until the aircraft sends a new position, so (icao24, time_position) is the
-- natural key and duplicates are dropped with ON CONFLICT DO NOTHING.
CREATE TABLE state_vectors (
    icao24          TEXT             NOT NULL,
    time_position   TIMESTAMPTZ      NOT NULL,
    last_contact    TIMESTAMPTZ      NOT NULL,
    snapshot_time   TIMESTAMPTZ      NOT NULL,
    callsign        TEXT,
    origin_country  TEXT             NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    baro_altitude   DOUBLE PRECISION,
    geo_altitude    DOUBLE PRECISION,
    velocity        DOUBLE PRECISION,
    true_track      DOUBLE PRECISION,
    vertical_rate   DOUBLE PRECISION,
    on_ground       BOOLEAN          NOT NULL,
    squawk          TEXT,
    spi             BOOLEAN          NOT NULL,
    position_source SMALLINT         NOT NULL,
    category        SMALLINT,
    ingested_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (icao24, time_position)
);

-- Time-window queries over the whole box ("everything between t1 and t2").
-- BRIN stays tiny because rows arrive roughly in time order.
CREATE INDEX state_vectors_time_brin ON state_vectors USING BRIN (time_position);
