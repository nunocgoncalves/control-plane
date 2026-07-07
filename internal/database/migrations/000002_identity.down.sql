-- HOR-242: roll back the identity store.

DROP TRIGGER IF EXISTS local_users_updated ON identity.local_users;
DROP TRIGGER IF EXISTS ident_updated ON identity.identities;
DROP FUNCTION IF EXISTS identity.set_updated_at();

DROP TABLE IF EXISTS identity.api_keys;
DROP TABLE IF EXISTS identity.local_users;
DROP TABLE IF EXISTS identity.external_mappings;
DROP TABLE IF EXISTS identity.identities;
