-- One row per successful poll, written in the same transaction as the poll's
-- positions. Consumers such as Aero-Scan follow this table by `id` to read
-- the feed poll by poll: a row here means every position the poll brought in
-- is already stored, and a poll with no aircraft still gets a row.
CREATE TABLE polls (
    id                BIGSERIAL        PRIMARY KEY,
    snapshot_time     TIMESTAMPTZ      NOT NULL UNIQUE,
    -- State vectors in the response, with or without a position.
    aircraft          INTEGER          NOT NULL,
    credits_remaining INTEGER,
    lamin             DOUBLE PRECISION NOT NULL,
    lomin             DOUBLE PRECISION NOT NULL,
    lamax             DOUBLE PRECISION NOT NULL,
    lomax             DOUBLE PRECISION NOT NULL,
    recorded_at       TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- A poll's positions are the rows it inserted, found by snapshot_time.
-- Rows arrive in snapshot order, so BRIN fits here too.
CREATE INDEX state_vectors_snapshot_brin ON state_vectors USING BRIN (snapshot_time);
