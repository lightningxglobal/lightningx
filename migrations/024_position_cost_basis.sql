-- 024: cost-basis accounting for positions — S2.4.
--
-- The zero-sum conservation test proved that PnL computed from the
-- whole-tick VWAP entry price MINTS money (the truncated entry differs
-- from the true average cost by up to half a tick × quantity). The
-- engine now books PnL against the exact cumulative open cost in atoms;
-- entry_price_ticks remains a display / liquidation-price reference.
--
-- DEFAULT 0 backfill: any pre-024 row hydrates with cost 0 and would
-- realize phantom profit on close — acceptable ONLY because the system
-- is pre-launch with no standing positions. Post-launch this would need
-- a backfill from entry_price_ticks instead.
ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS cost_atoms BIGINT NOT NULL DEFAULT 0
    CHECK (cost_atoms >= 0);
