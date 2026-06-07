-- 026: trigger (stop / take-profit) orders — S5.
--
-- A trigger order is DESK state, not engine state: it rests outside the
-- book and is converted into a regular order when the MARK price (S4 —
-- manipulation-clamped, never the raw mid) crosses trigger_price in the
-- armed direction.
--
-- State machine, enforced by the atomic UPDATE ... WHERE status='pending':
--   pending → triggered   (mark crossed; exactly-once: only the row's
--                          single winning UPDATE injects an order)
--   pending → cancelled   (user cancel, or margin check failed at fire
--                          time — recorded in cancel_reason)
-- 'triggered' rows carry the pre-allocated order id they injected;
-- restart recovery re-injects only when that id is provably absent from
-- both orders and matching_events (see desk::trigger::needs_reinjection).
CREATE TABLE IF NOT EXISTS trigger_orders (
    id                  BIGINT      NOT NULL PRIMARY KEY,
    user_id             BIGINT      NOT NULL REFERENCES users(id),
    symbol              TEXT        NOT NULL,
    side                TEXT        NOT NULL CHECK (side IN ('buy', 'sell')),
    order_type          TEXT        NOT NULL CHECK (order_type IN ('limit', 'market')),
    trigger_price_ticks BIGINT      NOT NULL CHECK (trigger_price_ticks > 0),
    -- 'rising': fire when mark >= trigger (buy-stop / short take-profit);
    -- 'falling': fire when mark <= trigger (sell-stop / long take-profit).
    trigger_when        TEXT        NOT NULL CHECK (trigger_when IN ('rising', 'falling')),
    -- Limit price for order_type='limit'; NULL for market.
    price_ticks         BIGINT      CHECK (price_ticks IS NULL OR price_ticks > 0),
    qty_lots            BIGINT      NOT NULL CHECK (qty_lots > 0),
    status              TEXT        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'triggered', 'cancelled')),
    cancel_reason       TEXT,
    triggered_order_id  BIGINT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    triggered_at        TIMESTAMPTZ
);

-- The desk hydrates pending triggers per symbol at startup.
CREATE INDEX IF NOT EXISTS idx_trigger_orders_pending
    ON trigger_orders(symbol, status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_trigger_orders_user
    ON trigger_orders(user_id, created_at DESC);
