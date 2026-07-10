-- HOR-334: gateway read-only DB user. The inference-gateway reads control-plane
-- views directly as a dedicated `gateway` role (least privilege). This adds the
-- api-keys view it reads through + grants the role SELECT on the views + USAGE
-- on the schemas, and defaults future views/tables to be readable.
--
-- The `gateway` role is created by the bootstrap Postgres subchart's init script
-- (prod). In BYO/test setups it may not exist, so the grants are conditional
-- (no-op if absent) — the control-plane migrate must not fail when the role is
-- missing.

-- Active API keys the gateway may resolve (key_hash -> identity_id), without
-- exposing the rest of the row.
CREATE OR REPLACE VIEW identity.active_api_keys AS
    SELECT key_hash, identity_id
    FROM identity.api_keys
    WHERE revoked_at IS NULL AND (expires_at IS NULL OR expires_at > now());

DO $$
DECLARE
    cur_user text := current_user;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gateway') THEN
        GRANT USAGE ON SCHEMA identity, permissions, catalog TO gateway;
        GRANT SELECT
            ON identity.active_api_keys,
               permissions.effective_capabilities,
               permissions.effective_rate_limits,
               catalog.effective_catalog
            TO gateway;
        EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA identity    GRANT SELECT ON TABLES TO gateway', cur_user);
        EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA permissions GRANT SELECT ON TABLES TO gateway', cur_user);
        EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA catalog     GRANT SELECT ON TABLES TO gateway', cur_user);
    END IF;
END $$;
