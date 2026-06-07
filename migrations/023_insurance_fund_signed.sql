-- 023: the insurance fund CAN be negative — fix over-strict 022 CHECK.
--
-- Engine semantics (risk/engine.rs insurance_fund_cents): positive =
-- surplus absorbed from profitable liquidations; NEGATIVE = fund debt
-- after socialised losses (bankruptcies the fund could not cover).
-- The 022 non-negative CHECK would make pg-writer's flush ERROR at the
-- exact moment the system most needs its books written down. Drop it.
ALTER TABLE insurance_fund DROP CONSTRAINT IF EXISTS insurance_fund_balance_atoms_check;
