-- HOR-246: durable turn runtime.
--
-- The durable engine's data layer: a turn/workflow state machine + an
-- append-only event/audit log + orchestration state, all on Postgres. NOT a
-- transcript store -- the conversation transcript is the ephemeral pi session
-- on a PVC (S9); this schema holds the durable *control* state + history.
--
-- One runtime (S4/S6): a workflow_run composes agent tasks (a deterministic
-- plan -> steps = agent_task | tool_call | approval_gate); chat is a degenerate
-- run (kind=chat, one freeform agent_task step). One run = one pi session
-- (session_id/session_dir); all of a run's agent-task steps (turns) share that
-- session. The control-plane owns the workflow/chat -> session.id mapping and
-- directs each harness pod to its session.id (HOR-249); the harness resumes-or-
-- creates that session (HOR-351).
--
-- State columns are authoritative current state; runtime.events is the
-- append-only history (audit/replay/usage). Each state transition is recorded
-- atomically with its audit event by the Go store (internal/runtime). Replay =
-- fold events in per-run `seq` order.
--
-- The approval-gate step type is supported in schema now (pending_approval
-- state + approval_requested/approval_resolved events); its *execution* (HITL)
-- is deferred. Usage (token/cost) rides in event payloads; rollup to the
-- `usage` schema is post-v1 (S8).
--
-- Mirrors the identity (HOR-242) / permissions (HOR-243) / catalog (HOR-306)
-- stores: pgxpool store + ErrNotFound/ErrInvalidTransition, no soft-delete
-- (runtime rows are history, retained), no pg_notify (the runtime is internal;
-- the orchestrator pushes replies to surfaces itself).

CREATE SCHEMA IF NOT EXISTS runtime;

-- workflow_runs: an execution instance. kind=chat (freeform, definition_key
-- NULL) or kind=workflow (references a Workflow definition key, HOR-252; no FK
-- here -- HOR-252 adds it when its definitions table lands). scope_identity_id
-- is the identity under whose permission/credential scope the run executes
-- (chat -> the human caller; triggered workflow -> the workflow's own identity,
-- a kind=workflow identity). trigger is the opaque source descriptor (webhook/
-- mention) for audit. One run = one session (session_id/session_dir).
CREATE TABLE runtime.workflow_runs (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind              text NOT NULL CHECK (kind IN ('chat', 'workflow')),
    definition_key    text,                       -- HOR-252 Workflow key; NULL for chat
    scope_identity_id uuid NOT NULL REFERENCES identity.identities(id),
    session_id        text NOT NULL,              -- pi session.id (HOR-249 generates)
    session_dir       text NOT NULL,              -- pi session.dir
    trigger           jsonb NOT NULL DEFAULT '{}'::jsonb,  -- opaque source descriptor
    state             text NOT NULL DEFAULT 'pending' CHECK (state IN
                        ('pending', 'running', 'awaiting_approval', 'succeeded', 'failed', 'aborted')),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,                -- set on pending->running
    finished_at       timestamptz                 -- set on any terminal transition
);

CREATE INDEX idx_workflow_runs_state ON runtime.workflow_runs (state) WHERE finished_at IS NULL;
CREATE INDEX idx_workflow_runs_scope_identity ON runtime.workflow_runs (scope_identity_id);

-- run_steps: the snapshotted plan (one row per step). chat = one freeform
-- agent_task step. config is opaque JSONB (prompt, tool allow-list, branch
-- condition, approver scope) -- HOR-252 defines the shape, HOR-249 evaluates it;
-- HOR-246 stores but never interprets it. `seq` is the plan order within the run.
CREATE TABLE runtime.run_steps (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      uuid NOT NULL REFERENCES runtime.workflow_runs(id) ON DELETE CASCADE,
    seq         integer NOT NULL,                 -- plan order within the run
    kind        text NOT NULL CHECK (kind IN ('agent_task', 'tool_call', 'approval_gate')),
    config      jsonb NOT NULL DEFAULT '{}'::jsonb,  -- opaque
    state       text NOT NULL DEFAULT 'pending' CHECK (state IN
                    ('pending', 'running', 'pending_approval', 'succeeded', 'failed', 'skipped')),
    started_at  timestamptz,                      -- set on pending->running
    finished_at timestamptz,                      -- set on any terminal transition
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE(run_id, seq)
);

-- One running step per run: the deterministic plan executes steps sequentially.
CREATE UNIQUE INDEX idx_run_steps_one_running ON runtime.run_steps (run_id) WHERE state = 'running';
CREATE INDEX idx_run_steps_run ON runtime.run_steps (run_id);

-- turns: one agent invocation (one Prompt->Settled cycle). Linked to a run +
-- the agent_task step it fulfils; a step may have several turns (retries).
-- session_id is denormalized from the run so the one-active-turn-per-session
-- invariant (the physical truth: one pod = one session = one active Prompt,
-- HOR-351) is enforced by a partial unique index. state maps 1:1 to the harness
-- Settled.Reason: completed->succeeded, failed->failed, aborted->aborted.
CREATE TABLE runtime.turns (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id     uuid NOT NULL REFERENCES runtime.workflow_runs(id) ON DELETE CASCADE,
    step_id    uuid REFERENCES runtime.run_steps(id) ON DELETE CASCADE,
    session_id text NOT NULL,                     -- denormalized from run for the unique index
    model      text,
    state      text NOT NULL DEFAULT 'pending' CHECK (state IN
                   ('pending', 'running', 'succeeded', 'failed', 'aborted')),
    started_at timestamptz,                       -- set on pending->running
    settled_at timestamptz,                       -- set on any terminal transition
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One active turn per session (pending or running). A second StartTurn on the
-- same session violates this -> the Go store surfaces a conflict to HOR-249
-- (the run is already being driven).
CREATE UNIQUE INDEX idx_turns_one_active_per_session ON runtime.turns (session_id) WHERE state IN ('pending', 'running');
CREATE INDEX idx_turns_run ON runtime.turns (run_id);

-- events: append-only audit/replay log. `seq` is gapless per-run, computed
-- in-transaction (COALESCE(MAX(seq),0)+1) by the Go store; UNIQUE(run_id, seq)
-- is the collision backstop. kind mirrors the harness Event set (turn_started,
-- assistant_message, tool_result, error, settled) plus workflow/step-level
-- kinds (run_started, run_succeeded, ..., step_started, approval_requested,
-- approval_resolved). payload is opaque JSONB. Immutable (no updated_at).
CREATE TABLE runtime.events (
    id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id  uuid NOT NULL REFERENCES runtime.workflow_runs(id) ON DELETE CASCADE,
    turn_id uuid REFERENCES runtime.turns(id) ON DELETE CASCADE,
    step_id uuid REFERENCES runtime.run_steps(id) ON DELETE CASCADE,
    seq     integer NOT NULL,
    kind    text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    ts      timestamptz NOT NULL DEFAULT now(),
    UNIQUE(run_id, seq)
);

CREATE INDEX idx_events_run_seq ON runtime.events (run_id, seq);
CREATE INDEX idx_events_turn ON runtime.events (turn_id) WHERE turn_id IS NOT NULL;

-- updated_at maintenance (mirrors identity/catalog). events is immutable.
CREATE OR REPLACE FUNCTION runtime.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER workflow_runs_updated BEFORE UPDATE ON runtime.workflow_runs
    FOR EACH ROW EXECUTE FUNCTION runtime.set_updated_at();
CREATE TRIGGER run_steps_updated BEFORE UPDATE ON runtime.run_steps
    FOR EACH ROW EXECUTE FUNCTION runtime.set_updated_at();
CREATE TRIGGER turns_updated BEFORE UPDATE ON runtime.turns
    FOR EACH ROW EXECUTE FUNCTION runtime.set_updated_at();
