-- Consumer checkpoints for the persist stream.
--
-- Each persist frame carries (publisher_id, seq) assigned at the desk's
-- single drain point. pg-writer records "applied up to seq" per publisher
-- IN THE SAME TRANSACTION as the data flush, giving exactly-once apply:
-- on restart or Aeron replay, frames at or below the checkpoint are
-- discarded as duplicates instead of being re-applied.

CREATE TABLE IF NOT EXISTS persist_checkpoints (
    publisher_id INTEGER PRIMARY KEY,
    last_seq     BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
