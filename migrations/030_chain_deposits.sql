-- 030: on-chain deposit ledger → virtual sub-account credit.
--
-- The exchange-level custody model: users send USDT to the exchange's
-- chain wallet (a per-user derived address or a shared address + memo);
-- the off-chain deposit watcher, after N confirmations, calls the credit
-- endpoint ONCE per on-chain transfer. This table is the idempotency
-- anchor: a (chain, tx_hash, log_index) is credited at most once, so a
-- watcher that re-delivers (restart, re-org rescan) cannot double-credit.
--
-- The matching engine never sees this — credit is a single AccountSet
-- on the existing atoms ledger, audited in fund_audit, all in one
-- transaction with the deposit row (exactly-once).
CREATE TABLE IF NOT EXISTS chain_deposits (
    id            BIGSERIAL   PRIMARY KEY,
    chain         TEXT        NOT NULL,         -- e.g. 'TRON', 'ETH'
    tx_hash       TEXT        NOT NULL,
    log_index     INTEGER     NOT NULL DEFAULT 0, -- vout / log position
    user_id       BIGINT      NOT NULL REFERENCES users(id),
    asset         TEXT        NOT NULL,
    amount_atoms  BIGINT      NOT NULL CHECK (amount_atoms > 0),
    from_address  TEXT,
    to_address    TEXT,
    credited_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- one credit per on-chain transfer output
    UNIQUE (chain, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_chain_deposits_user
    ON chain_deposits(user_id, credited_at DESC);
