-- Allow NULL for maker-side order ID when taker's counterparty is unknown
-- (MVP: only taker DB order ID is tracked; maker tracking requires full Aeron integration)
ALTER TABLE trades ALTER COLUMN buy_order_id DROP NOT NULL;
ALTER TABLE trades ALTER COLUMN sell_order_id DROP NOT NULL;
