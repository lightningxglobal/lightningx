-- D1: track whether the Aeron publish step completed after the orders-table
-- INSERT so that a crash between INSERT and publish can be detected on
-- restart and the order re-injected (needs_reinjection/is_pending_submission).
--
-- submitted        (default) — Aeron publish completed; fully injected.
-- pending_submission          — INSERT done but publish not confirmed yet;
--                              treat the same as "no footprint" on recovery.
ALTER TABLE trigger_orders
    ADD COLUMN IF NOT EXISTS submission_status TEXT NOT NULL DEFAULT 'submitted';
