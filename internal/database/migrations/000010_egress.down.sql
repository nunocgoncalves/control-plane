-- HOR-244: reverse 000010_egress.

DROP VIEW IF EXISTS egress.effective_routes;
DROP TRIGGER IF EXISTS credentials_updated ON egress.credentials;
DROP FUNCTION IF EXISTS egress.set_updated_at();
DROP TABLE IF EXISTS egress.credentials;
DROP SCHEMA IF EXISTS egress;
