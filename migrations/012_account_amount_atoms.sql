-- Add integer amount columns for production account accounting.
-- Legacy float8 balance/frozen columns are kept during the compatibility
-- window for existing API projections.

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS balance_atoms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS frozen_atoms  BIGINT NOT NULL DEFAULT 0;

UPDATE accounts
SET balance_atoms = ROUND(balance * 100000000)::BIGINT
WHERE balance_atoms = 0
  AND balance <> 0
  AND balance <= 92233720368.54775807;

UPDATE accounts
SET frozen_atoms = ROUND(frozen * 100000000)::BIGINT
WHERE frozen_atoms = 0
  AND frozen <> 0
  AND frozen <= 92233720368.54775807;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'accounts_balance_atoms_nonnegative'
    ) THEN
        ALTER TABLE accounts
            ADD CONSTRAINT accounts_balance_atoms_nonnegative CHECK (balance_atoms >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'accounts_frozen_atoms_nonnegative'
    ) THEN
        ALTER TABLE accounts
            ADD CONSTRAINT accounts_frozen_atoms_nonnegative CHECK (frozen_atoms >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'accounts_frozen_atoms_lte_balance_atoms'
    ) THEN
        ALTER TABLE accounts
            ADD CONSTRAINT accounts_frozen_atoms_lte_balance_atoms CHECK (frozen_atoms <= balance_atoms);
    END IF;
END $$;
