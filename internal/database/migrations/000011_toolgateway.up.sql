-- HOR-392: tool gateway registry, authorization, and durable invocation ledger.
--
-- The control-plane tool-gateway data layer. The gateway owns tool
-- registration (runners self-register immutable descriptors over mTLS,
-- ARCH-015), effective-tool discovery (registry ∩ AgentPool grants ∩ workflow
-- request ∩ pool-level identity scope, ARCH-006/016/018), credential-slot
-- resolution to K8s Secret references (ARCH-008/018), and the durable
-- at-most-once invocation ledger committed before the side-effect boundary
-- (ARCH-014).
--
-- Schema is `toolgateway` (not `gateway`) to avoid colliding with the existing
-- `gateway` Postgres ROLE -- the inference-gateway read-only DB user (HOR-334).
-- The Go package is internal/gateway; schema/package names need not match.
--
-- Mirrors the identity (HOR-242) / permissions (HOR-243) / catalog (HOR-306) /
-- egress (HOR-244) / runtime (HOR-246) stores: pgxpool store + ErrNotFound,
-- soft-delete on operator-owned config tables, set_updated_at triggers, no
-- pg_notify (the gateway is the sole writer; runners push over streams).
--
-- Several tables are operator-seeded now and populated by later tickets via the
-- Git->DB bridge pattern, exactly like EgressRoute/IdentityMapping:
--   * pools / pool_grants / credential_bindings  <- AgentPool CRD (HOR-245)
--   * workflow_pool_bindings                     <- Workflow definitions (HOR-252)
--   * approved_runners                           <- runner materializer (HOR-397/245)
-- The gateway contract (columns + resolution) is stable; later tickets write
-- into these tables without reshaping them.

CREATE SCHEMA IF NOT EXISTS toolgateway;

-- ---------------------------------------------------------------------------
-- tool_versions: immutable published descriptors (ARCH-006/007).
--
-- A logical tool name may have multiple immutable versions coexisting for
-- pinned attempts. `digest` is the exact implementation identity retained on
-- every audit/ledger record; (name, digest) is unique. Publishing an update
-- creates a NEW row (a new version) rather than mutating an existing one; old
-- versions remain routable while any active attempt pins them.
--
-- Populated by runner self-registration (RunnerService.RegisterRunner) on first
-- sight of a (name, version, digest); re-registration of the same digest is
-- idempotent.
-- ---------------------------------------------------------------------------
-- NOTE: tool_versions has NO updated_at column and NO set_updated_at trigger.
-- Descriptors are immutable (ARCH-007): re-registration of the same (name,
-- digest) is a validated no-op (the store uses INSERT ... ON CONFLICT DO
-- NOTHING then SELECT); no descriptor field is ever mutated. A different digest
-- for the same (name, version) is rejected by the store's immutability guard.
CREATE TABLE toolgateway.tool_versions (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 text NOT NULL,
    version              text NOT NULL,
    digest               text NOT NULL,
    description          text NOT NULL DEFAULT '',
    input_schema         jsonb NOT NULL DEFAULT '{}'::jsonb,   -- JSON Schema for arguments
    effect_class         text NOT NULL CHECK (effect_class IN
                           ('read_only', 'idempotent_write', 'non_idempotent_write')),
    credential_slots     jsonb NOT NULL DEFAULT '[]'::jsonb,    -- [{name, scheme, binding_schema, required}]
    artifact_capabilities jsonb NOT NULL DEFAULT '{}'::jsonb,   -- {reads, writes, accepted_mime_types}
    timeout_ms           bigint NOT NULL,                       -- per-invocation timeout (google.protobuf.Duration -> ms)
    idempotency_proof    jsonb,                                 -- required when effect_class = idempotent_write
    created_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (name, version),
    UNIQUE (name, digest)
);

-- ---------------------------------------------------------------------------
-- runner_registrations: one row per (runner, tool version) a trusted runner
-- has registered over its live mTLS stream (ARCH-015). `active` tracks the
-- stream: set true on Register/Heartbeat, false on stream close or fencing.
-- A version is available for NEW attempt resolution when at least one active
-- registration with a fresh heartbeat exists. Pinned active attempts keep
-- their pin even when no active registration remains (ARCH-007) -- the gateway
-- fails rather than substitutes.
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.runner_registrations (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    runner_id         text NOT NULL,                -- stable runner instance id
    spiffe_id         text NOT NULL,                -- verified URI SAN
    namespace         text NOT NULL,                -- permitted tool namespace (ARCH-015)
    tool_name         text NOT NULL,
    tool_version      text NOT NULL,
    tool_digest       text NOT NULL,
    fencing_generation bigint NOT NULL,             -- stream generation (fencing on reconnect)
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    active            boolean NOT NULL DEFAULT true,
    registered_at     timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- One active registration per (runner, tool version): a reconnect fences the
-- previous generation for that (runner_id, tool_name, tool_version).
CREATE UNIQUE INDEX idx_runner_reg_active_unique
    ON toolgateway.runner_registrations (runner_id, tool_name, tool_version)
    WHERE active;

CREATE INDEX idx_runner_reg_available
    ON toolgateway.runner_registrations (tool_name, tool_digest)
    WHERE active;

-- ---------------------------------------------------------------------------
-- pools: the AgentPool registry (ARCH-016/018). Operator-seeded now; the
-- AgentPool CRD reconciler (HOR-245) populates this via the Git->DB bridge.
-- `spiffe_id_prefix` is the trust prefix a supervisor runner's URI SAN must
-- match to bind to this pool (e.g. spiffe://iterabase.local/pools/<pool-uid>).
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.pools (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key              text NOT NULL,                 -- stable natural key; CR "<ns>/<name>"
    name             text NOT NULL,                 -- AgentPool name
    spiffe_id_prefix text NOT NULL,                 -- pool identity prefix (SAN match)
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    deleted_at       timestamptz,
    UNIQUE (key)
);

-- ---------------------------------------------------------------------------
-- pool_grants: the action-scoped deny-by-default gate (ARCH-016/018; REQ-010/
-- SCN-009). A pool -> (tool_name, max effect class, allowed actions) row.
-- ABSENCE = denied (the gateway returns an attributable denial, never a
-- broader fallback). `allowed_actions` is an opaque JSONB allow-list for
-- action-specific narrowing beyond tool-name + effect class (stored, enforced
-- from v1). Operator-seeded; AgentPool CRD (HOR-245) populates.
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.pool_grants (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pool_id         uuid NOT NULL REFERENCES toolgateway.pools(id) ON DELETE CASCADE,
    tool_name       text NOT NULL,
    max_effect_class text NOT NULL CHECK (max_effect_class IN
                      ('read_only', 'idempotent_write', 'non_idempotent_write')),
    allowed_actions jsonb NOT NULL DEFAULT '[]'::jsonb,  -- opaque action allow-list
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    deleted_at      timestamptz,
    UNIQUE (pool_id, tool_name)
);

CREATE INDEX idx_pool_grants_active ON toolgateway.pool_grants (pool_id) WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- credential_bindings: logical credential slot -> K8s Secret reference +
-- non-secret resource constraints (ARCH-008/018). The credential VALUE never
-- lives here -- only the scheme-dependent `secret_ref` JSONB spec, which the
-- gateway resolves to values via the in-cluster K8s API at invocation time and
-- hands as a short-lived CredentialContext to the trusted runner. `scheme`
-- selects how `secret_ref` is parsed:
--   bearer:                   {"value_ref": {name, key}}
--   oauth_client_credentials: {"client_id", "client_secret_ref": {name,key},
--                              "token_url", "scope"}
-- Resource constraints (tenant/site/mailbox) are non-secret. Operator-seeded;
-- AgentPool CRD (HOR-245) populates + validates against the runner-declared
-- slot schema.
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.credential_bindings (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pool_id               uuid NOT NULL REFERENCES toolgateway.pools(id) ON DELETE CASCADE,
    tool_name             text NOT NULL,
    slot_name             text NOT NULL,
    scheme                text NOT NULL CHECK (scheme IN ('bearer', 'oauth_client_credentials')),
    secret_ref            jsonb NOT NULL,                 -- {name, key} K8s Secret reference
    resource_constraints  jsonb NOT NULL DEFAULT '{}'::jsonb,  -- non-secret scope (tenant/site/...)
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    deleted_at            timestamptz,
    UNIQUE (pool_id, tool_name, slot_name)
);

CREATE INDEX idx_credential_bindings_active
    ON toolgateway.credential_bindings (pool_id, tool_name) WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- workflow_pool_bindings: workflow definition -> pool + workflow-requested
-- permitted tools (ARCH-012/018). Lets the control-plane workflow-step caller
-- path resolve pool + permitted tool set without HOR-252's definitions table
-- built. Operator-seeded; Workflow definitions (HOR-252) populate. The
-- workflow-requested tools intersect with pool_grants + the registry at
-- discovery time.
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.workflow_pool_bindings (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_definition_key text NOT NULL,
    pool_id               uuid NOT NULL REFERENCES toolgateway.pools(id) ON DELETE CASCADE,
    permitted_tools       jsonb NOT NULL DEFAULT '[]'::jsonb,  -- tool names the workflow requests
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    deleted_at            timestamptz,
    UNIQUE (workflow_definition_key)
);

-- ---------------------------------------------------------------------------
-- approved_runners: deny-by-default runner identity approval (ARCH-015). A
-- runner's SPIFFE identity + namespace must be approved before registration is
-- accepted. Operator-seeded; the runner materializer (HOR-397/245) populates.
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.approved_runners (
    id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace                   text NOT NULL,
    runner_id                   text NOT NULL,
    spiffe_id                   text NOT NULL,
    allowed_tool_namespaces     text[] NOT NULL DEFAULT '{}',
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now(),
    deleted_at                  timestamptz,
    UNIQUE (spiffe_id)
);

CREATE INDEX idx_approved_runners_active ON toolgateway.approved_runners (namespace) WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- invocations: the durable at-most-once ledger (ARCH-014).
--
-- Uniquely keyed by (attempt_id, caller_scope, caller_scope_id, tool_call_id,
-- tool_version_digest, idempotency_key). The gateway commits a row in state
-- 'dispatching' BEFORE dispatching over the runner stream (the side-effect
-- boundary). ON CONFLICT is the idempotency backstop: a duplicate completed
-- invocation returns its committed result/artifact refs; a duplicate while
-- dispatching/running reports the existing invocation (in_progress); a
-- possible effect with no committed result becomes outcome_unknown and is
-- never automatically repeated.
--
-- `attempt_id` has no FK -- attempts are a HOR-254 concept; the gateway stores
-- the reference now and HOR-254 may add the FK later (mirrors
-- runtime.workflow_runs.definition_key, which has no FK until HOR-252).
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.invocations (
    id                        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    attempt_id                text NOT NULL,
    caller_scope              text NOT NULL CHECK (caller_scope IN ('turn', 'workflow_step')),
    caller_scope_id           text NOT NULL,             -- turn_id or run_step_id
    tool_call_id              text NOT NULL,
    tool_name                 text NOT NULL,
    tool_version_digest       text NOT NULL,             -- the pinned immutable version (resolved from attempt_tool_pins)
    idempotency_key           text NOT NULL DEFAULT '',  -- '' for read_only; required for non_idempotent_write
    effect_class              text NOT NULL CHECK (effect_class IN
                                ('read_only', 'idempotent_write', 'non_idempotent_write')),
    pool_id                   uuid REFERENCES toolgateway.pools(id) ON DELETE SET NULL,
    runner_id                 text,                      -- the runner that executed (audit)
    arguments_json            jsonb NOT NULL DEFAULT '{}'::jsonb,
    state                     text NOT NULL DEFAULT 'dispatching' CHECK (state IN
                                ('dispatching', 'running', 'succeeded', 'failed', 'outcome_unknown')),
    result_json               jsonb,                     -- committed structured result (when succeeded)
    artifact_output_refs      jsonb NOT NULL DEFAULT '[]'::jsonb,  -- committed ArtifactRef list
    error                     jsonb,                     -- {code, message, retryability, details_json} when failed/outcome_unknown
    -- Crash-recovery lease (ARCH-014, SCN-008). Set at dispatch; a row whose
    -- lease has expired while still non-terminal is swept at gateway start (and
    -- by a background ticker): read_only -> failed, writes -> outcome_unknown.
    dispatch_lease_expires_at timestamptz,
    gateway_instance_id       text,                      -- the process that owns the in-flight dispatch
    dispatching_at            timestamptz NOT NULL DEFAULT now(),
    running_at                timestamptz,
    finished_at               timestamptz,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now()
);

-- The at-most-once uniqueness backstop (ARCH-014). A second InvokeTool with the
-- same key hits this and is routed to duplicate handling.
CREATE UNIQUE INDEX idx_invocations_uniq
    ON toolgateway.invocations (attempt_id, caller_scope, caller_scope_id, tool_call_id, tool_version_digest, idempotency_key);

CREATE INDEX idx_invocations_attempt ON toolgateway.invocations (attempt_id);
CREATE INDEX idx_invocations_state ON toolgateway.invocations (state) WHERE finished_at IS NULL;
-- Crash-recovery sweep index: non-terminal rows with an expired lease.
CREATE INDEX idx_invocations_recoverable
    ON toolgateway.invocations (dispatch_lease_expires_at)
    WHERE state IN ('dispatching', 'running') AND finished_at IS NULL;

-- ---------------------------------------------------------------------------
-- attempt_tool_pins: the attempt's immutable tool-version snapshot (ARCH-007).
-- At attempt creation, each permitted logical gateway tool resolves to one
-- exact immutable (name, digest); every turn/invocation in that attempt uses
-- that snapshot. The gateway resolves the digest from here and IGNORES any
-- caller-supplied digest. Absence of a pin for (attempt, tool) => fail closed.
--
-- `attempt_id` is the runtime run id (text; v1 treats a run as its attempt
-- until HOR-252 introduces a first-class attempts table). Populated by
-- SnapshotAttemptTools at attempt creation (HOR-254 / workflow runtime are the
-- production callers; tests call it directly).
-- ---------------------------------------------------------------------------
CREATE TABLE toolgateway.attempt_tool_pins (
    attempt_id          text NOT NULL,
    tool_name           text NOT NULL,
    tool_version_digest text NOT NULL,            -- the pinned immutable digest
    pinned_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (attempt_id, tool_name)
);

CREATE INDEX idx_attempt_tool_pins_digest ON toolgateway.attempt_tool_pins (tool_version_digest);

-- ---------------------------------------------------------------------------
-- run_pool_assignments: durable run -> pool binding (ARCH-004).
--
-- The gateway resolves the caller's pool from durable state, not from
-- agent-supplied scope. A supervisor's SPIFFE id encodes the pool, but the
-- gateway must ALSO prove the turn's run is actually assigned to that pool
-- before trusting the turn context. HOR-249 (dispatch) writes this row when a
-- turn is assigned to a pool/worker; the gateway reads it fail-closed. Cross-
-- schema (runtime); pool_id is validated in Go (no cross-schema FK, mirroring
-- the existing definition_key/attempt_id no-FK-until-later pattern).
-- ---------------------------------------------------------------------------
CREATE TABLE runtime.run_pool_assignments (
    run_id      uuid PRIMARY KEY REFERENCES runtime.workflow_runs(id) ON DELETE CASCADE,
    pool_id     uuid NOT NULL,                 -- toolgateway.pools.id (validated in Go)
    assigned_at timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- available_tool_versions: a version is discoverable for NEW attempts when at
-- least one active runner registration with a fresh heartbeat exists. Pinned
-- active attempts bypass this (they keep their pin regardless). Freshness is
-- enforced here by the heartbeat lease; the gateway's lease interval (default
-- 30s) must match the server config. A reaper marks stale rows active=false as
-- a backstop, but this view is the authoritative availability filter.
-- ---------------------------------------------------------------------------
CREATE VIEW toolgateway.available_tool_versions AS
    SELECT DISTINCT tv.*
    FROM toolgateway.tool_versions tv
    JOIN toolgateway.runner_registrations rr
      ON rr.tool_name = tv.name AND rr.tool_digest = tv.digest
    WHERE rr.active
      AND rr.last_heartbeat_at > now() - interval '30 seconds';

-- ---------------------------------------------------------------------------
-- set_updated_at trigger (mirrors identity/permissions/catalog/egress/runtime).
-- invocations participates (state transitions touch updated_at); the append
-- semantics are on the row's state machine, not a separate event log (the
-- runtime.events log is the cross-cutting audit; the gateway ledger is the
-- tool-call audit).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION toolgateway.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER runner_registrations_updated BEFORE UPDATE ON toolgateway.runner_registrations
    FOR EACH ROW EXECUTE FUNCTION toolgateway.set_updated_at();
CREATE TRIGGER pools_updated BEFORE UPDATE ON toolgateway.pools
    FOR EACH ROW EXECUTE FUNCTION toolgateway.set_updated_at();
CREATE TRIGGER pool_grants_updated BEFORE UPDATE ON toolgateway.pool_grants
    FOR EACH ROW EXECUTE FUNCTION toolgateway.set_updated_at();
CREATE TRIGGER credential_bindings_updated BEFORE UPDATE ON toolgateway.credential_bindings
    FOR EACH ROW EXECUTE FUNCTION toolgateway.set_updated_at();
CREATE TRIGGER workflow_pool_bindings_updated BEFORE UPDATE ON toolgateway.workflow_pool_bindings
    FOR EACH ROW EXECUTE FUNCTION toolgateway.set_updated_at();
CREATE TRIGGER approved_runners_updated BEFORE UPDATE ON toolgateway.approved_runners
    FOR EACH ROW EXECUTE FUNCTION toolgateway.set_updated_at();
CREATE TRIGGER invocations_updated BEFORE UPDATE ON toolgateway.invocations
    FOR EACH ROW EXECUTE FUNCTION toolgateway.set_updated_at();
