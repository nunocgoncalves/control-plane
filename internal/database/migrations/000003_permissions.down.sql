-- HOR-243: roll back the permissions store.

-- Cross-schema trigger on HOR-242's table (must drop while the table exists).
DROP TRIGGER IF EXISTS identities_permissions_notify ON identity.identities;

-- permissions.policies triggers (explicit; the table drop would also cascade).
DROP TRIGGER IF EXISTS policies_notify ON permissions.policies;
DROP TRIGGER IF EXISTS policies_updated ON permissions.policies;

DROP FUNCTION IF EXISTS permissions.notify_change();
DROP FUNCTION IF EXISTS permissions.set_updated_at();

DROP VIEW IF EXISTS permissions.effective_capabilities;
DROP TABLE IF EXISTS permissions.policies;
