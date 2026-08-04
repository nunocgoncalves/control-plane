-- HOR-254: graph workflow runtime + durable customer work domain.
--
-- `runtime` remains authoritative for execution state. `work` owns the stable
-- customer work item, attempt relationship, actionable blockers, feedback,
-- business-safe timeline, artifact-reference trace, and estimated-value ledger.
-- Customer state is projected from the current attempt's runtime state plus an
-- active actionable blocker; it is never duplicated on work_items.

CREATE SCHEMA IF NOT EXISTS work;

-- The pre-release run_steps workflow plan is superseded. The table remains only
-- for chat's degenerate single agent task; graph workflows use node_executions.
ALTER TABLE runtime.run_steps DROP CONSTRAINT run_steps_kind_check;
ALTER TABLE runtime.run_steps ADD CONSTRAINT run_steps_kind_check CHECK (kind = 'agent_task');
ALTER TABLE runtime.run_steps DROP CONSTRAINT run_steps_state_check;
ALTER TABLE runtime.run_steps ADD CONSTRAINT run_steps_state_check
    CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'skipped'));

ALTER TABLE identity.api_keys DROP CONSTRAINT api_keys_scope_check;
ALTER TABLE identity.api_keys ADD CONSTRAINT api_keys_scope_check
    CHECK (scope IN ('admin', 'token', 'gateway', 'work'));

-- Immutable, versioned transparent value configuration. V1 intentionally has
-- one evidence-backed formula; absent configuration means unconfigured.
CREATE TABLE work.value_models (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ref                 text NOT NULL,
    version             text NOT NULL,
    formula             text NOT NULL DEFAULT 'labor_time_saved'
                        CHECK (formula = 'labor_time_saved'),
    currency            text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    baseline_seconds    bigint NOT NULL CHECK (baseline_seconds > 0),
    loaded_hourly_cost  numeric(20,6) NOT NULL CHECK (loaded_hourly_cost > 0),
    assumptions         jsonb NOT NULL DEFAULT '{}'::jsonb,
    explanation         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (ref, version)
);

CREATE TABLE work.work_items (
    id                         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_key               text NOT NULL,
    scope_identity_id          uuid NOT NULL REFERENCES identity.identities(id),
    title                      text NOT NULL,
    source                     jsonb NOT NULL DEFAULT '{}'::jsonb,
    start_identity_id          uuid NOT NULL REFERENCES identity.identities(id),
    start_idempotency_key      text NOT NULL,
    start_payload_hash         text NOT NULL,
    current_attempt_id         uuid,
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now(),
    UNIQUE (start_identity_id, workflow_key, start_idempotency_key)
);

-- One attempt is one runtime workflow_run and deliberately shares its UUID with
-- that run, preserving the existing dispatch/gateway attempt_id contract.
CREATE TABLE work.attempts (
    id                         uuid PRIMARY KEY REFERENCES runtime.workflow_runs(id) ON DELETE RESTRICT,
    work_item_id               uuid NOT NULL REFERENCES work.work_items(id) ON DELETE RESTRICT,
    number                     integer NOT NULL CHECK (number > 0),
    definition_id              uuid NOT NULL REFERENCES workflow.definitions(id),
    definition_key             text NOT NULL,
    definition_version         text NOT NULL,
    definition_digest          text NOT NULL,
    graph_snapshot             jsonb NOT NULL,
    skills_snapshot            jsonb NOT NULL DEFAULT '[]'::jsonb,
    capabilities_snapshot      jsonb NOT NULL DEFAULT '[]'::jsonb,
    models_snapshot            jsonb NOT NULL DEFAULT '{}'::jsonb,
    value_model_id             uuid REFERENCES work.value_models(id),
    value_model_snapshot       jsonb,
    revised_from_attempt_id    uuid REFERENCES work.attempts(id),
    actionable_guidance        text,
    consequence_confirmation  jsonb NOT NULL DEFAULT '[]'::jsonb,
    customer_failure_summary   jsonb,
    operator_failure_detail    jsonb,
    created_at                 timestamptz NOT NULL DEFAULT now(),
    finished_at                timestamptz,
    UNIQUE (work_item_id, number)
);

ALTER TABLE work.work_items
    ADD CONSTRAINT work_items_current_attempt_fk
    FOREIGN KEY (current_attempt_id) REFERENCES work.attempts(id) DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX idx_attempts_work_item ON work.attempts (work_item_id, number);

-- Append-only node visits. A cycle inserts a new row with the next visit and
-- execution_seq; it never resets an earlier visit.
CREATE TABLE runtime.node_executions (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    attempt_id            uuid NOT NULL REFERENCES work.attempts(id) ON DELETE RESTRICT,
    node_key              text NOT NULL,
    visit                 integer NOT NULL CHECK (visit > 0),
    execution_seq         integer NOT NULL CHECK (execution_seq > 0),
    kind                  text NOT NULL CHECK (kind IN ('agent_task', 'human_gate')),
    prompt                text,
    context               jsonb NOT NULL DEFAULT '{}'::jsonb,
    model_snapshot        jsonb,
    skills_snapshot       jsonb NOT NULL DEFAULT '[]'::jsonb,
    capabilities_snapshot jsonb NOT NULL DEFAULT '[]'::jsonb,
    workspace_tools       boolean NOT NULL DEFAULT false,
    timeout_ms            bigint CHECK (timeout_ms IS NULL OR timeout_ms > 0),
    state                 text NOT NULL DEFAULT 'pending' CHECK (state IN
                              ('pending', 'running', 'blocked', 'succeeded', 'failed')),
    completion_outcome    text,
    completion_summary    text,
    output                jsonb,
    artifact_refs         jsonb NOT NULL DEFAULT '[]'::jsonb,
    completion_reported_at timestamptz,
    started_at            timestamptz,
    finished_at           timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, node_key, visit),
    UNIQUE (attempt_id, execution_seq)
);

CREATE UNIQUE INDEX idx_node_executions_one_active
    ON runtime.node_executions (attempt_id)
    WHERE state IN ('pending', 'running', 'blocked');
CREATE INDEX idx_node_executions_attempt ON runtime.node_executions (attempt_id, execution_seq);

CREATE TABLE runtime.graph_transitions (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    attempt_id          uuid NOT NULL REFERENCES work.attempts(id) ON DELETE RESTRICT,
    from_execution_id   uuid NOT NULL REFERENCES runtime.node_executions(id) ON DELETE RESTRICT,
    outcome             text NOT NULL,
    to_execution_id     uuid REFERENCES runtime.node_executions(id) ON DELETE RESTRICT,
    terminal            boolean NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now(),
    CHECK ((terminal AND to_execution_id IS NULL) OR (NOT terminal AND to_execution_id IS NOT NULL)),
    UNIQUE (from_execution_id)
);

CREATE INDEX idx_graph_transitions_attempt ON runtime.graph_transitions (attempt_id, created_at);

-- Bind turns/events/assignments to the exact graph-node visit while preserving
-- the existing run/step fields used by the separate chat runtime.
ALTER TABLE runtime.turns
    ADD COLUMN node_execution_id uuid REFERENCES runtime.node_executions(id) ON DELETE RESTRICT;
CREATE UNIQUE INDEX idx_turns_node_execution ON runtime.turns (node_execution_id)
    WHERE node_execution_id IS NOT NULL;

ALTER TABLE runtime.events
    ADD COLUMN node_execution_id uuid REFERENCES runtime.node_executions(id) ON DELETE RESTRICT;
CREATE INDEX idx_events_node_execution ON runtime.events (node_execution_id)
    WHERE node_execution_id IS NOT NULL;

ALTER TABLE runtime.turn_assignments
    ADD COLUMN work_item_id uuid REFERENCES work.work_items(id) ON DELETE RESTRICT,
    ADD COLUMN node_execution_id uuid REFERENCES runtime.node_executions(id) ON DELETE RESTRICT;
CREATE INDEX idx_turn_assignments_work_item ON runtime.turn_assignments (work_item_id)
    WHERE work_item_id IS NOT NULL;

-- One active actionable blocker per work item. Human gates and generated
-- consequence-confirmation gates use the same durable response contract.
CREATE TABLE work.blockers (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    work_item_id          uuid NOT NULL REFERENCES work.work_items(id) ON DELETE RESTRICT,
    attempt_id            uuid NOT NULL REFERENCES work.attempts(id) ON DELETE RESTRICT,
    node_execution_id     uuid REFERENCES runtime.node_executions(id) ON DELETE RESTRICT,
    kind                  text NOT NULL CHECK (kind IN
                              ('information', 'decision', 'approval', 'artifact', 'consequence_confirmation')),
    title                 jsonb NOT NULL,
    description           jsonb NOT NULL,
    response_schema       jsonb NOT NULL DEFAULT '{}'::jsonb,
    allowed_outcomes      jsonb NOT NULL DEFAULT '[]'::jsonb,
    required_consequences jsonb NOT NULL DEFAULT '[]'::jsonb,
    state                 text NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'resolved')),
    response_outcome      text,
    response              jsonb,
    responded_by          uuid REFERENCES identity.identities(id),
    created_at            timestamptz NOT NULL DEFAULT now(),
    resolved_at           timestamptz
);

CREATE UNIQUE INDEX idx_blockers_one_open_per_item ON work.blockers (work_item_id) WHERE state = 'open';
CREATE INDEX idx_blockers_attempt ON work.blockers (attempt_id, created_at);

CREATE TABLE work.feedback (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    work_item_id          uuid NOT NULL REFERENCES work.work_items(id) ON DELETE RESTRICT,
    attempt_id            uuid NOT NULL REFERENCES work.attempts(id) ON DELETE RESTRICT,
    category              text NOT NULL,
    explanation           text,
    corrected_result      jsonb,
    created_by            uuid NOT NULL REFERENCES identity.identities(id),
    created_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_feedback_item ON work.feedback (work_item_id, created_at);
CREATE INDEX idx_feedback_attempt ON work.feedback (attempt_id, created_at);

-- The revised attempt points back to the exact feedback that requested it, so
-- the customer read projection can reconstruct original attempt -> feedback ->
-- revised attempt without interpreting timeline JSON.
ALTER TABLE work.attempts
    ADD COLUMN revision_feedback_id uuid REFERENCES work.feedback(id) ON DELETE RESTRICT;
CREATE UNIQUE INDEX idx_attempts_revision_feedback ON work.attempts (revision_feedback_id)
    WHERE revision_feedback_id IS NOT NULL;

-- HOR-399 owns artifact metadata/bytes. This table records immutable trace
-- relationships to opaque artifact IDs without pre-implementing that service.
CREATE TABLE work.artifact_links (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    artifact_id           text NOT NULL,
    work_item_id          uuid NOT NULL REFERENCES work.work_items(id) ON DELETE RESTRICT,
    attempt_id            uuid NOT NULL REFERENCES work.attempts(id) ON DELETE RESTRICT,
    node_execution_id     uuid REFERENCES runtime.node_executions(id) ON DELETE RESTRICT,
    role                  text NOT NULL CHECK (role IN
                              ('source', 'input', 'output', 'corrected_result', 'evidence')),
    metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (artifact_id, work_item_id, attempt_id, node_execution_id, role)
);
CREATE INDEX idx_artifact_links_item ON work.artifact_links (work_item_id, created_at);

-- Global monotonic cursor supports resumable SSE Last-Event-ID. Events contain
-- customer-safe semantic codes/parameters only; technical runtime events never
-- enter this table.
CREATE TABLE work.timeline_events (
    cursor                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id                    uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    work_item_id          uuid NOT NULL REFERENCES work.work_items(id) ON DELETE RESTRICT,
    attempt_id            uuid REFERENCES work.attempts(id) ON DELETE RESTRICT,
    node_execution_id     uuid REFERENCES runtime.node_executions(id) ON DELETE RESTRICT,
    code                  text NOT NULL,
    params                jsonb NOT NULL DEFAULT '{}'::jsonb,
    artifact_refs         jsonb NOT NULL DEFAULT '[]'::jsonb,
    actor_identity_id     uuid REFERENCES identity.identities(id),
    created_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_timeline_item_cursor ON work.timeline_events (work_item_id, cursor);

-- Append-only estimated value history. Credit is positive; deduction is
-- negative. V1 permits at most one of each per item and revised attempts never
-- add another credit.
CREATE TABLE work.value_ledger (
    cursor                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id                    uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    work_item_id          uuid NOT NULL REFERENCES work.work_items(id) ON DELETE RESTRICT,
    attempt_id            uuid NOT NULL REFERENCES work.attempts(id) ON DELETE RESTRICT,
    feedback_id           uuid REFERENCES work.feedback(id) ON DELETE RESTRICT,
    value_model_id        uuid NOT NULL REFERENCES work.value_models(id),
    kind                  text NOT NULL CHECK (kind IN ('completion_credit', 'feedback_deduction')),
    amount                numeric(20,6) NOT NULL,
    currency              text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    formula_snapshot      jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'completion_credit' AND amount > 0 AND feedback_id IS NULL)
        OR (kind = 'feedback_deduction' AND amount < 0 AND feedback_id IS NOT NULL))
);
CREATE UNIQUE INDEX idx_value_one_credit ON work.value_ledger (work_item_id)
    WHERE kind = 'completion_credit';
CREATE UNIQUE INDEX idx_value_one_deduction ON work.value_ledger (work_item_id)
    WHERE kind = 'feedback_deduction';
CREATE INDEX idx_value_ledger_created ON work.value_ledger (created_at, cursor);

-- Current customer state is a projection, not duplicated mutable state.
CREATE VIEW work.current_work_items AS
SELECT wi.*,
       CASE
           WHEN wr.state IN ('failed', 'aborted') THEN 'failed'
           WHEN EXISTS (SELECT 1 FROM work.blockers b WHERE b.work_item_id = wi.id AND b.state = 'open') THEN 'blocked'
           WHEN wr.state = 'pending' THEN 'todo'
           WHEN wr.state IN ('running', 'awaiting_approval') THEN 'in_progress'
           WHEN wr.state = 'succeeded' THEN 'done'
       END AS customer_state,
       wr.state AS runtime_state,
       wr.started_at,
       wr.finished_at
FROM work.work_items wi
JOIN runtime.workflow_runs wr ON wr.id = wi.current_attempt_id;

-- updated_at maintenance.
CREATE TRIGGER work_items_updated BEFORE UPDATE ON work.work_items
    FOR EACH ROW EXECUTE FUNCTION runtime.set_updated_at();
CREATE TRIGGER node_executions_updated BEFORE UPDATE ON runtime.node_executions
    FOR EACH ROW EXECUTE FUNCTION runtime.set_updated_at();

-- Timeline/value history is immutable. The database rejects accidental UPDATE
-- or DELETE regardless of application code.
CREATE OR REPLACE FUNCTION work.reject_history_mutation() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;
CREATE TRIGGER timeline_events_append_only BEFORE UPDATE OR DELETE ON work.timeline_events
    FOR EACH ROW EXECUTE FUNCTION work.reject_history_mutation();
CREATE TRIGGER value_ledger_append_only BEFORE UPDATE OR DELETE ON work.value_ledger
    FOR EACH ROW EXECUTE FUNCTION work.reject_history_mutation();

-- Wake the in-process SSE broadcaster after commit; the payload is only a
-- cursor, never customer data.
CREATE OR REPLACE FUNCTION work.notify_timeline_event() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('work_timeline_events', NEW.cursor::text);
    RETURN NEW;
END;
$$;
CREATE TRIGGER timeline_events_notify AFTER INSERT ON work.timeline_events
    FOR EACH ROW EXECUTE FUNCTION work.notify_timeline_event();
