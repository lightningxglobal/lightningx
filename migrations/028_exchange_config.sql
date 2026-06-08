-- 028: runtime exchange controls (testnet ops console) — T2.
--
-- Per-symbol controls the desk reads at startup and an admin endpoint
-- mutates WITHOUT a restart. The desk keeps an in-memory mirror
-- (DashMap, lock-free point reads on the hot path) and refreshes the row
-- it just wrote on every admin change.
--
-- NULL fee columns mean "fall through to the env/SymbolRules default",
-- so an operator can override one symbol's fees without pinning all of
-- them. trading_halted gates order ENTRY only — existing orders, cancels
-- and liquidations always proceed (you must be able to de-risk a halted
-- market).
CREATE TABLE IF NOT EXISTS exchange_config (
    symbol          TEXT        NOT NULL PRIMARY KEY,
    trading_halted  BOOLEAN     NOT NULL DEFAULT FALSE,
    maker_fee_bps   BIGINT,
    taker_fee_bps   BIGINT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by      TEXT
);
