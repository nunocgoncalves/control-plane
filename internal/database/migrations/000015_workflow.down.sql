-- HOR-252: rollback the workflow definition model + trigger registration.

DROP TRIGGER IF EXISTS trigger_bindings_updated ON workflow.trigger_bindings;
DROP TRIGGER IF EXISTS definitions_updated ON workflow.definitions;
DROP FUNCTION IF EXISTS workflow.set_updated_at();

DROP TABLE IF EXISTS workflow.trigger_bindings;
DROP TABLE IF EXISTS workflow.definitions;

DROP SCHEMA IF EXISTS workflow;
