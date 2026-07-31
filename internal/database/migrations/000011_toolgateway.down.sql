-- HOR-392: reverse tool-gateway schema migration (rollback-aware).

DROP VIEW IF EXISTS toolgateway.available_tool_versions;

DROP TABLE IF EXISTS toolgateway.attempt_tool_pins;
DROP TABLE IF EXISTS runtime.run_pool_assignments;

DROP TRIGGER IF EXISTS invocations_updated ON toolgateway.invocations;
DROP TRIGGER IF EXISTS approved_runners_updated ON toolgateway.approved_runners;
DROP TRIGGER IF EXISTS workflow_pool_bindings_updated ON toolgateway.workflow_pool_bindings;
DROP TRIGGER IF EXISTS credential_bindings_updated ON toolgateway.credential_bindings;
DROP TRIGGER IF EXISTS pool_grants_updated ON toolgateway.pool_grants;
DROP TRIGGER IF EXISTS pools_updated ON toolgateway.pools;
DROP TRIGGER IF EXISTS runner_registrations_updated ON toolgateway.runner_registrations;
DROP TRIGGER IF EXISTS tool_versions_updated ON toolgateway.tool_versions;
DROP FUNCTION IF EXISTS toolgateway.set_updated_at();

DROP TABLE IF EXISTS toolgateway.invocations;
DROP TABLE IF EXISTS toolgateway.approved_runners;
DROP TABLE IF EXISTS toolgateway.workflow_pool_bindings;
DROP TABLE IF EXISTS toolgateway.credential_bindings;
DROP TABLE IF EXISTS toolgateway.pool_grants;
DROP TABLE IF EXISTS toolgateway.pools;
DROP TABLE IF EXISTS toolgateway.runner_registrations;
DROP TABLE IF EXISTS toolgateway.tool_versions;

DROP SCHEMA IF EXISTS toolgateway;
