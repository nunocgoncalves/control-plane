-- HOR-268: reverse catalog.models + the effective_catalog view. The catalog
-- schema + catalog.notify_change()/set_updated_at() functions are owned by
-- 000004 (HOR-306) and are not dropped here.

DROP VIEW IF EXISTS catalog.effective_catalog;
DROP TRIGGER IF EXISTS models_notify ON catalog.models;
DROP TRIGGER IF EXISTS models_updated ON catalog.models;
DROP TABLE IF EXISTS catalog.models;
