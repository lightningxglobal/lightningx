-- 025: funding-rate state and history — S3.3.
--
-- funding_state holds the per-symbol settlement schedule; pg-writer
-- advances next_settlement_at IN THE SAME TRANSACTION that applies the
-- settlement's account deltas (driven by a FundingSettled persist frame,
-- exactly-once via the checkpoint floor). The desk reads this table at
-- startup to resume the schedule — a restart can therefore neither skip
-- nor repeat a period.
CREATE TABLE IF NOT EXISTS funding_state (
    symbol             TEXT        NOT NULL PRIMARY KEY,
    next_settlement_at TIMESTAMPTZ NOT NULL,
    last_rate_e9       BIGINT      NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only settlement log. Totals are computed by pg-writer at apply
-- time from the positions table; residue is the per-position truncation
-- dust swept into the insurance fund (long_paid = short_received +
-- residue ... signs depending on rate direction; the invariant test
-- asserts Σ(account deltas) + residue == 0 exactly).
CREATE TABLE IF NOT EXISTS funding_history (
    id                    BIGSERIAL   PRIMARY KEY,
    symbol                TEXT        NOT NULL,
    rate_e9               BIGINT      NOT NULL,
    mark_price_ticks      BIGINT      NOT NULL CHECK (mark_price_ticks > 0),
    settled_at            TIMESTAMPTZ NOT NULL,
    long_paid_atoms       BIGINT      NOT NULL,
    short_received_atoms  BIGINT      NOT NULL,
    fund_residue_atoms    BIGINT      NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_funding_history_symbol_time
    ON funding_history(symbol, settled_at DESC);
