-- P1 final: orders/trades monetary storage is atoms-ONLY.
--
-- The float8 columns become GENERATED ALWAYS AS (atoms / 1e8) STORED:
--   * every reader (including SELECT *) keeps working unchanged;
--   * every writer is FORCED off the float columns by PostgreSQL itself
--     (inserting into a generated column is an error) — the float value
--     physically cannot diverge from the atoms truth.
-- freeze_price never had an atoms twin: add + backfill it first.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS freeze_price_atoms BIGINT NOT NULL DEFAULT 0;

UPDATE orders
SET freeze_price_atoms = ROUND(freeze_price * 100000000)::BIGINT
WHERE freeze_price_atoms = 0
  AND freeze_price <> 0
  AND freeze_price <= 92233720368.54775807;

ALTER TABLE orders
    DROP COLUMN IF EXISTS price,
    DROP COLUMN IF EXISTS quantity,
    DROP COLUMN IF EXISTS filled,
    DROP COLUMN IF EXISTS freeze_price;

ALTER TABLE orders
    ADD COLUMN price        float8 GENERATED ALWAYS AS (price_atoms::float8 / 100000000.0) STORED,
    ADD COLUMN quantity     float8 GENERATED ALWAYS AS (quantity_atoms::float8 / 100000000.0) STORED,
    ADD COLUMN filled       float8 GENERATED ALWAYS AS (filled_atoms::float8 / 100000000.0) STORED,
    ADD COLUMN freeze_price float8 GENERATED ALWAYS AS (freeze_price_atoms::float8 / 100000000.0) STORED;

ALTER TABLE trades
    DROP COLUMN IF EXISTS price,
    DROP COLUMN IF EXISTS quantity,
    DROP COLUMN IF EXISTS buy_fee,
    DROP COLUMN IF EXISTS sell_fee;

-- Fee atoms twins were added in migration 014; price/quantity twins too.
ALTER TABLE trades
    ADD COLUMN price    float8 GENERATED ALWAYS AS (price_atoms::float8 / 100000000.0) STORED,
    ADD COLUMN quantity float8 GENERATED ALWAYS AS (quantity_atoms::float8 / 100000000.0) STORED,
    ADD COLUMN buy_fee  float8 GENERATED ALWAYS AS (buy_fee_atoms::float8 / 100000000.0) STORED,
    ADD COLUMN sell_fee float8 GENERATED ALWAYS AS (sell_fee_atoms::float8 / 100000000.0) STORED;
