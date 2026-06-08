-- 029: engine order-book snapshots (T4) — bounded journal replay.
--
-- The engine periodically serializes its resting book + sequence
-- counters here, tagged with the journal position consumed at snapshot
-- time. On restart it restores the latest snapshot for the symbol and
-- replays ONLY the journal AFTER that position, instead of from genesis.
-- Recordings strictly older than the snapshot can then be truncated.
--
-- One row per (symbol, snapshot_seq); the engine keeps the latest and
-- prunes older rows. payload is a self-describing little-endian blob
-- (see matching::snapshot::serialize).
CREATE TABLE IF NOT EXISTS engine_snapshots (
    id              BIGSERIAL   PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    snapshot_seq    BIGINT      NOT NULL,
    journal_recording_id BIGINT NOT NULL,
    journal_position BIGINT     NOT NULL,
    order_count     INTEGER     NOT NULL,
    payload         BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_engine_snapshots_latest
    ON engine_snapshots(symbol, snapshot_seq DESC);
