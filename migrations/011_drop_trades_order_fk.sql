-- Drop the trades.buy/sell_order_id → orders(id) foreign keys so that the
-- desk-server can DELETE orders once they reach a terminal state
-- (CANCELED / FILLED / REJECTED). Without this, EC2 accumulates millions of
-- dead-state CANCELED rows and INSERT throughput collapses.
--
-- The trades table keeps buy_order_id / sell_order_id as nullable bigints
-- (per migration 002) — purely informational, no referential integrity.
ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_buy_order_id_fkey;
ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_sell_order_id_fkey;
