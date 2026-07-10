-- HOR-334: reverse the gateway read-only grants + the active_api_keys view.
-- DEFAULT PRIVILEGES are not reversed (harmless if the role lingers).

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gateway') THEN
        REVOKE SELECT ON identity.active_api_keys, permissions.effective_capabilities, permissions.effective_rate_limits, catalog.effective_catalog FROM gateway;
        REVOKE USAGE ON SCHEMA identity, permissions, catalog FROM gateway;
    END IF;
END $$;

DROP VIEW IF EXISTS identity.active_api_keys;
