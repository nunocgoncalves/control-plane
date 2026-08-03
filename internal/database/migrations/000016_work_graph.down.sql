ALTER TABLE identity.api_keys DROP CONSTRAINT IF EXISTS api_keys_scope_check;
ALTER TABLE identity.api_keys ADD CONSTRAINT api_keys_scope_check
    CHECK (scope IN ('admin', 'token', 'gateway'));

DROP VIEW IF EXISTS work.current_work_items;

DROP TRIGGER IF EXISTS timeline_events_notify ON work.timeline_events;
DROP TRIGGER IF EXISTS timeline_events_append_only ON work.timeline_events;
DROP TRIGGER IF EXISTS value_ledger_append_only ON work.value_ledger;
DROP TRIGGER IF EXISTS work_items_updated ON work.work_items;
DROP TRIGGER IF EXISTS node_executions_updated ON runtime.node_executions;

DROP TABLE IF EXISTS work.timeline_events;
DROP TABLE IF EXISTS work.value_ledger;
DROP TABLE IF EXISTS work.artifact_links;
DROP TABLE IF EXISTS work.feedback;
DROP TABLE IF EXISTS work.blockers;

ALTER TABLE runtime.turn_assignments
    DROP COLUMN IF EXISTS node_execution_id,
    DROP COLUMN IF EXISTS work_item_id;
ALTER TABLE runtime.events DROP COLUMN IF EXISTS node_execution_id;
ALTER TABLE runtime.turns DROP COLUMN IF EXISTS node_execution_id;

DROP TABLE IF EXISTS runtime.graph_transitions;
DROP TABLE IF EXISTS runtime.node_executions;

ALTER TABLE work.work_items DROP CONSTRAINT IF EXISTS work_items_current_attempt_fk;
DROP TABLE IF EXISTS work.attempts;
DROP TABLE IF EXISTS work.work_items;
DROP TABLE IF EXISTS work.value_models;

DROP FUNCTION IF EXISTS work.notify_timeline_event();
DROP FUNCTION IF EXISTS work.reject_history_mutation();
DROP SCHEMA IF EXISTS work;

ALTER TABLE runtime.run_steps DROP CONSTRAINT IF EXISTS run_steps_kind_check;
ALTER TABLE runtime.run_steps ADD CONSTRAINT run_steps_kind_check
    CHECK (kind IN ('agent_task', 'tool_call', 'approval_gate'));
ALTER TABLE runtime.run_steps DROP CONSTRAINT IF EXISTS run_steps_state_check;
ALTER TABLE runtime.run_steps ADD CONSTRAINT run_steps_state_check
    CHECK (state IN ('pending', 'running', 'pending_approval', 'succeeded', 'failed', 'skipped'));
