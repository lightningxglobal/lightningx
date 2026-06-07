-- 022: durable state for the swap (perpetual) margin engine — S1.1.
--
-- The in-memory risk engine (src/desk/risk/) tracks accounts, positions
-- and the insurance fund in DashMaps; until now a desk restart lost all
-- of it. These tables are the durable mirror, written by pg-writer from
-- PersistFrames (S1.2/S1.3) and read back on desk startup (S1.4).
--
-- Unit conventions (deliberate, see checklist S2):
--   * money columns are ATOMS (1e-8 USDT) — the ledger-wide unit the
--     risk engine is converging to; the cents→atoms conversion happens
--     once at the frame-publish boundary until S2 lands;
--   * prices are TICKS and quantities are LOTS — matching-engine native
--     integers, not money;
--   * derived/volatile values (mark price, unrealized PnL, maintenance
--     margin) are NOT persisted: they are recomputed from the mark-price
--     feed after hydrate, persisting them would only create drift.

-- ── Open positions, one row per (user, symbol) ─────────────────────────
-- One-way position mode (matches PositionRiskState): a user holds at
-- most one position per symbol, long or short. A closed position is
-- DELETEd, not zeroed — "no row" is the canonical flat state, so the
-- qty_lots > 0 CHECK can stay strict.
CREATE TABLE IF NOT EXISTS positions (
    user_id            BIGINT      NOT NULL REFERENCES users(id),
    symbol             TEXT        NOT NULL,
    side               TEXT        NOT NULL CHECK (side IN ('long', 'short')),
    qty_lots           BIGINT      NOT NULL CHECK (qty_lots > 0),
    entry_price_ticks  BIGINT      NOT NULL CHECK (entry_price_ticks > 0),
    leverage           SMALLINT    NOT NULL CHECK (leverage BETWEEN 1 AND 125),
    -- Margin actually locked into this position (atoms). Snapshot of the
    -- engine's accounting, not recomputed; non-negative by construction.
    used_margin_atoms  BIGINT      NOT NULL CHECK (used_margin_atoms >= 0),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, symbol)
);

-- The risk tick scans per symbol; reconcile sums per symbol.
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);

-- ── Margin-account state, one row per user ─────────────────────────────
-- equity may go NEGATIVE transiently (bankruptcy before the insurance
-- fund absorbs the loss) — deliberately no non-negative CHECK there.
-- order_margin/used_margin are reservations and can never be negative.
CREATE TABLE IF NOT EXISTS risk_accounts (
    user_id            BIGINT      NOT NULL PRIMARY KEY REFERENCES users(id),
    equity_atoms       BIGINT      NOT NULL,
    used_margin_atoms  BIGINT      NOT NULL CHECK (used_margin_atoms  >= 0),
    order_margin_atoms BIGINT      NOT NULL CHECK (order_margin_atoms >= 0),
    -- Mirrors RiskStatus; the hydrate path refuses unknown values early
    -- rather than guessing, hence the strict CHECK.
    status             TEXT        NOT NULL DEFAULT 'normal'
        CHECK (status IN ('normal', 'margin_call', 'liquidation_pending',
                          'liquidating', 'liquidated', 'bankruptcy')),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Insurance fund: a single global row ────────────────────────────────
-- id is pinned to 1 so concurrent writers can only ever UPSERT the same
-- row; the CHECK makes "accidentally a second fund" a constraint error
-- instead of silent double accounting.
CREATE TABLE IF NOT EXISTS insurance_fund (
    id            SMALLINT    NOT NULL PRIMARY KEY CHECK (id = 1),
    balance_atoms BIGINT      NOT NULL DEFAULT 0 CHECK (balance_atoms >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO insurance_fund (id, balance_atoms)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
