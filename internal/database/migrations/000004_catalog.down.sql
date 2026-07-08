-- HOR-306: reverse catalog.backends. The catalog.notify_change() function is
-- owned here (HOR-268's 000005 reuses it for catalog.models; migrate-down runs
-- 000005 first, dropping the models trigger, before this drops the function).

DROP TRIGGER IF EXISTS backends_notify ON catalog.backends;
DROP TRIGGER IF EXISTS backends_updated ON catalog.backends;
DROP FUNCTION IF EXISTS catalog.notify_change();
DROP FUNCTION IF EXISTS catalog.set_updated_at();
DROP TABLE IF EXISTS catalog.backends;
DROP SCHEMA IF EXISTS catalog;
