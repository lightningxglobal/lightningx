-- 031: withdrawal requests — two-phase, idempotent.
--
-- The symmetric half of deposits. Flow (each step is one transaction
-- with the balance side, so funds and state never diverge):
--   request   → freeze the amount (incl. fee), insert row 'pending'
--   approve   → 'approved'  (risk/manual gate; balance unchanged)
--   broadcast → 'broadcast' + tx_hash (the chain service signed & sent)
--   confirm   → DEBIT the frozen amount (frozen -= amount), 'confirmed'
--   fail      → RELEASE the freeze (frozen -= amount, back to spendable),
--               'failed'
-- A confirmed/failed row is terminal; confirm/fail are idempotent on the
-- row's own state (only a non-terminal row transitions), so a chain
-- service that re-delivers cannot double-debit or double-release.
CREATE TABLE IF NOT EXISTS withdrawals (
    id            BIGSERIAL   PRIMARY KEY,
    user_id       BIGINT      NOT NULL REFERENCES users(id),
    asset         TEXT        NOT NULL,
    chain         TEXT        NOT NULL,
    to_address    TEXT        NOT NULL,
    amount_atoms  BIGINT      NOT NULL CHECK (amount_atoms > 0),
    fee_atoms     BIGINT      NOT NULL DEFAULT 0 CHECK (fee_atoms >= 0),
    status        TEXT        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','approved','broadcast','confirmed','failed','cancelled')),
    tx_hash       TEXT,
    fail_reason   TEXT,
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_withdrawals_user ON withdrawals(user_id, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status) WHERE status IN ('approved','broadcast');
