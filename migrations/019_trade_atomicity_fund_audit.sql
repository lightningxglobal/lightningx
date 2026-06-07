-- P3: settlement atomicity + fund audit trail.
--
-- 1. UNIQUE(buy_order_id, sell_order_id): one fill between a taker/maker
--    order pair settles exactly once. This makes the trade INSERT
--    idempotent across BOTH write paths (embedded-engine settle_trade and
--    the pg-writer persist flush), so running them together can never
--    double-record a fill. Pre-existing duplicates (dev data) are removed
--    keeping the lowest id.
--
-- 2. fund_audit: append-only ledger of every balance-affecting operation
--    (freeze / release / settle legs), written IN THE SAME TRANSACTION as
--    the operation itself. UPDATE/DELETE raise — reuses the audit_log
--    immutability trigger function (migration 017).

DELETE FROM trades a USING trades b
 WHERE a.id > b.id
   AND a.buy_order_id = b.buy_order_id
   AND a.sell_order_id = b.sell_order_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_order_pair
    ON trades(buy_order_id, sell_order_id);

CREATE TABLE IF NOT EXISTS fund_audit (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    asset        VARCHAR(16) NOT NULL,
    kind         VARCHAR(16) NOT NULL,  -- freeze|release|settle_debit|settle_credit
    amount_atoms BIGINT NOT NULL,
    ref_id       BIGINT NOT NULL DEFAULT 0,  -- order/trade reference when known
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fund_audit_user ON fund_audit(user_id, created_at DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'fund_audit_no_mutate'
    ) THEN
        CREATE TRIGGER fund_audit_no_mutate
            BEFORE UPDATE OR DELETE ON fund_audit
            FOR EACH ROW EXECUTE FUNCTION audit_log_immutable();
    END IF;
END $$;
