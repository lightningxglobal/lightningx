-- Append-only audit log for security-relevant actions (login, register,
-- API-key operations; withdrawal hooks land with the wallet system).
--
-- Tamper-resistance: UPDATE and DELETE raise via trigger — the table can
-- only grow. (Hash-chaining rows is a later enhancement; it requires
-- serialized inserts.)

CREATE TABLE IF NOT EXISTS audit_log (
    id            BIGSERIAL PRIMARY KEY,
    actor_user_id BIGINT,                 -- NULL for failed/anonymous attempts
    action        VARCHAR(40) NOT NULL,   -- e.g. 'register', 'login_ok', 'login_failed'
    ip            TEXT,
    detail        JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_actor   ON audit_log(actor_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_action  ON audit_log(action, created_at DESC);

CREATE OR REPLACE FUNCTION audit_log_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only';
END
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'audit_log_no_mutate'
    ) THEN
        CREATE TRIGGER audit_log_no_mutate
            BEFORE UPDATE OR DELETE ON audit_log
            FOR EACH ROW EXECUTE FUNCTION audit_log_immutable();
    END IF;
END $$;
