-- Extend fixed-point (atoms, 1e-8) accounting from accounts to orders and
-- trades. Same playbook as migration 012: add integer twin columns, backfill
-- from the float8 columns once, keep dual-write during the compatibility
-- window. The atoms columns are authoritative; the float8 columns are
-- display-only legacy and will be dropped when the window closes
-- (criteria: reconcile drift alarms quiet for 30 days AND no reader
-- depends on float8 columns).

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS price_atoms    BIGINT,             -- NULL for market orders
    ADD COLUMN IF NOT EXISTS quantity_atoms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS filled_atoms   BIGINT NOT NULL DEFAULT 0;

UPDATE orders
SET price_atoms = ROUND(price * 100000000)::BIGINT
WHERE price_atoms IS NULL
  AND price IS NOT NULL
  AND price <= 92233720368.54775807;

UPDATE orders
SET quantity_atoms = ROUND(quantity * 100000000)::BIGINT
WHERE quantity_atoms = 0
  AND quantity <> 0
  AND quantity <= 92233720368.54775807;

UPDATE orders
SET filled_atoms = ROUND(filled * 100000000)::BIGINT
WHERE filled_atoms = 0
  AND filled <> 0
  AND filled <= 92233720368.54775807;

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS price_atoms    BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS quantity_atoms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS buy_fee_atoms  BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sell_fee_atoms BIGINT NOT NULL DEFAULT 0;

UPDATE trades
SET price_atoms = ROUND(price * 100000000)::BIGINT
WHERE price_atoms = 0
  AND price <> 0
  AND price <= 92233720368.54775807;

UPDATE trades
SET quantity_atoms = ROUND(quantity * 100000000)::BIGINT
WHERE quantity_atoms = 0
  AND quantity <> 0
  AND quantity <= 92233720368.54775807;

UPDATE trades
SET buy_fee_atoms = ROUND(buy_fee * 100000000)::BIGINT
WHERE buy_fee_atoms = 0
  AND buy_fee <> 0
  AND buy_fee <= 92233720368.54775807;

UPDATE trades
SET sell_fee_atoms = ROUND(sell_fee * 100000000)::BIGINT
WHERE sell_fee_atoms = 0
  AND sell_fee <> 0
  AND sell_fee <= 92233720368.54775807;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'orders_quantity_atoms_nonnegative'
    ) THEN
        ALTER TABLE orders
            ADD CONSTRAINT orders_quantity_atoms_nonnegative CHECK (quantity_atoms >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'orders_filled_atoms_nonnegative'
    ) THEN
        -- NOTE: filled <= quantity is intentionally NOT a constraint during
        -- the dual-write window (a writer racing the migration could trip it
        -- and lose the batch). The over-fill invariant is monitored by the
        -- reconcile sweep instead; promote to a CHECK once legacy float
        -- columns are dropped.
        ALTER TABLE orders
            ADD CONSTRAINT orders_filled_atoms_nonnegative CHECK (filled_atoms >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'trades_atoms_nonnegative'
    ) THEN
        ALTER TABLE trades
            ADD CONSTRAINT trades_atoms_nonnegative
            CHECK (price_atoms >= 0 AND quantity_atoms >= 0
                   AND buy_fee_atoms >= 0 AND sell_fee_atoms >= 0);
    END IF;
END $$;
