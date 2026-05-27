-- Deduplicate existing rows before adding unique constraint.
-- Keep the highest id per (user_id, client_order_id), null out the rest.
WITH dupes AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY user_id, client_order_id ORDER BY id DESC) AS rn
    FROM orders
    WHERE client_order_id IS NOT NULL
)
UPDATE orders
SET client_order_id = NULL
FROM dupes
WHERE dupes.id = orders.id AND dupes.rn > 1;

-- Idempotency key for client retries.
-- NULL client_order_id values are ignored so legacy/API callers without a key
-- can continue to submit multiple orders.
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_client_order_id
    ON orders(user_id, client_order_id)
    WHERE client_order_id IS NOT NULL;
