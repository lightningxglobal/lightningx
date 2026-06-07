-- Pre-launch cleanup: the system has never served production traffic, so
-- the float8 compatibility window is unnecessary. Atoms columns become the
-- ONLY representation of money on accounts.
--
-- Also promotes the over-fill invariant to a hard constraint: the
-- dual-write race that justified deferring it (migration 014) cannot occur
-- on a pre-launch database.

ALTER TABLE accounts
    DROP COLUMN IF EXISTS balance,
    DROP COLUMN IF EXISTS frozen;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'orders_filled_atoms_range'
    ) THEN
        ALTER TABLE orders
            ADD CONSTRAINT orders_filled_atoms_range
            CHECK (filled_atoms >= 0 AND filled_atoms <= quantity_atoms);
    END IF;
END $$;
