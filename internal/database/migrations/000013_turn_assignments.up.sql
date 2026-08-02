-- HOR-249: durable dispatch + active assignment context + worker fencing.
--
-- The active-assignment context: the durable record that a turn is assigned to
-- exactly one eligible warm worker (verified cert SAN pod name) at a specific
-- connection-generation, under one AgentPool's scope, with the immutable
-- attempt/model/capability/tool-version snapshot the gateways authorize against
-- (ARCH-004/006/007/010/014/018). It is the missing link that lets the tool
-- gateway AND the inference gateway (HOR-398, separate repo) bind authorization
-- to the specific verified worker + current fencing generation for a running
-- turn — closing the HOR-398 / DEC-041 interim residual.
--
-- Writer: HOR-249 dispatch, on AssignTurn (one row per active turn). Readers:
-- the tool gateway's ResolveTurnScope (this repo) and the inference gateway's
-- ResolveTurnScope (cross-repo), both fail-closed. A fenced/old-generation or
-- terminalized assignment denies authorization.
--
-- `runtime.run_pool_assignments` (migration 000011) remains the run -> pool
-- binding; this table is the per-turn worker/generation binding the acceptance
-- criterion requires ("a worker/generation binding in run_pool_assignments or
-- equivalent"). Per-turn is the correct granularity: a turn is assigned to one
-- worker at one generation, and a reconnect fences the prior generation.
--
-- `highest_applied_sequence` is the cumulative dedup/ACK watermark for the
-- worker's per-turn monotonic TurnEvent sequence: the CP applies events with
-- worker sequence > this value and ACKs through it after committing them to
-- runtime.events. On reconnect replay the worker resends the unacked tail; the
-- CP dedups (sequence <= highest_applied -> skip). Monotonicity makes a high
-- watermark sufficient (no per-event idempotency table needed).

CREATE TABLE runtime.turn_assignments (
    turn_id                uuid PRIMARY KEY REFERENCES runtime.turns(id) ON DELETE CASCADE,
    run_id                 uuid NOT NULL REFERENCES runtime.workflow_runs(id) ON DELETE CASCADE,
    pool_id                uuid NOT NULL,                 -- toolgateway.pools.id (validated in Go; no cross-schema FK)
    worker_id              text NOT NULL,                 -- verified cert SAN pod name (stable warm-worker slot)
    fencing_generation     bigint NOT NULL,               -- CP-assigned, monotonic per (pool, worker); fences reconnects
    attempt_id             text NOT NULL,                 -- run id (v1 attempt identity; HOR-254 may add a first-class attempts table)
    scope_identity_id      uuid NOT NULL,                 -- customer/workflow identity the run executes under
    agent_pool_key         text NOT NULL,                 -- AgentPool identity (CR "<ns>/<name>"); audit/trace
    model_permission       jsonb NOT NULL DEFAULT '{}'::jsonb,    -- exact model config snapshot granted for the turn
    capability_request     jsonb NOT NULL DEFAULT '[]'::jsonb,    -- gateway capability request (permitted tools) for the attempt
    tool_version_snapshot  jsonb NOT NULL DEFAULT '[]'::jsonb,    -- pinned gateway-tool version digest refs (attempt snapshot)
    state                  text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'terminal', 'fenced')),
    highest_applied_sequence bigint NOT NULL DEFAULT 0,   -- cumulative worker-event dedup/ACK watermark
    assigned_at            timestamptz NOT NULL DEFAULT now(),
    terminalized_at        timestamptz,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);

-- Authoritative active assignment for a turn (gateways read this fail-closed).
CREATE UNIQUE INDEX idx_turn_assignments_active
    ON runtime.turn_assignments (turn_id) WHERE state = 'active';

-- Worker/generation binding lookup: is this (pool, worker, generation) the
-- current active assignment? Used by the Work server to fence stale generations
-- and by gateway authorization to deny a fenced/old-generation caller.
CREATE INDEX idx_turn_assignments_worker_gen
    ON runtime.turn_assignments (pool_id, worker_id, fencing_generation)
    WHERE state = 'active';

CREATE INDEX idx_turn_assignments_run ON runtime.turn_assignments (run_id);

-- runtime.set_updated_at() is defined in migration 000009; reuse it.
CREATE TRIGGER turn_assignments_updated BEFORE UPDATE ON runtime.turn_assignments
    FOR EACH ROW EXECUTE FUNCTION runtime.set_updated_at();
