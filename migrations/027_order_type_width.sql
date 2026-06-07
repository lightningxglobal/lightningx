-- 027: order_type was varchar(10); injected trigger orders write
-- 'trigger-market' (14 chars) / 'trigger-limit' (13). Width 16 matches
-- the wire-format pack_str16 bound exactly.
ALTER TABLE orders ALTER COLUMN order_type TYPE VARCHAR(16);
