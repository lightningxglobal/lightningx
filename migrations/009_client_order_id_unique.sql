-- Idempotency key for client retries.
-- NULL client_order_id values are ignored so legacy/API callers without a key
-- can continue to submit multiple orders.
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_client_order_id
    ON orders(user_id, client_order_id)
    WHERE client_order_id IS NOT NULL;
