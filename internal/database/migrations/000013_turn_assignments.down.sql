-- HOR-249: reverse 000013_turn_assignments — drop the active-assignment
-- context + worker/generation binding. Rollback-aware.

DROP TRIGGER IF EXISTS turn_assignments_updated ON runtime.turn_assignments;

DROP TABLE IF EXISTS runtime.turn_assignments;
