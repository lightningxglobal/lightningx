-- Leader election + fencing for single-writer roles (the matching engine).
--
-- One row per role. epoch is the FENCING TOKEN: it increments on every
-- ownership CHANGE (not on renewal), and consumers reject output stamped
-- with an epoch lower than the highest they have seen — a zombie
-- ex-leader that wakes up after its lease expired cannot corrupt anything
-- even if it still manages to publish.

CREATE TABLE IF NOT EXISTS leader_lease (
    role       TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    epoch      BIGINT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL
);
