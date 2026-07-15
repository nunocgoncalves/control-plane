-- HOR-246: reverse the durable turn runtime schema. Runtime rows are history
-- (never soft-deleted in v1), so a plain schema drop is the correct rollback.

DROP SCHEMA IF EXISTS runtime CASCADE;
