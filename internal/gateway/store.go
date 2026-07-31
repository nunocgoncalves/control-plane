// Package gateway implements the control-plane tool gateway (HOR-392): the
// registry of trusted tool-runner registrations, effective-tool discovery,
// credential-slot resolution, and the durable at-most-once invocation ledger.
//
// It is the authorized boundary for customer-system and externally
// side-effecting tool execution (ARCH-001..ARCH-018). The gateway owns a new
// gRPC/Connect contract (iterabase.gateway.v1) served by cmd/gateway: a bidi
// RunnerService.RegisterRunner stream (runners self-register over mTLS and
// receive invocations to execute; they expose no inbound endpoint — ARCH-015)
// and a caller-facing GatewayService (DiscoverEffectiveTools + ledger-gated
// InvokeTool + CancelInvocation).
//
// This file is the Postgres store (schema `toolgateway`, migration 000011).
// It mirrors the identity/permissions/catalog/egress/runtime stores: pgxpool +
// ErrNotFound, soft-delete on operator-owned config tables, no pg_notify (the
// gateway is the sole writer; runners push over streams).
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors.
var (
	// ErrNotFound is returned when no row matches.
	ErrNotFound = errors.New("gateway: not found")
	// ErrInvalidTransition is returned when an invocation state transition is
	// illegal (e.g. finishing an already-terminal invocation).
	ErrInvalidTransition = errors.New("gateway: invalid invocation transition")
)

// EffectClass classifies a tool's external effect (ARCH-014). It drives the
// retry/ledger policy: read_only may bounded-retry; idempotent_write may retry
// only when the descriptor proves a stable strategy; non_idempotent_write never
// auto-retries after execution begins.
type EffectClass string

const (
	EffectReadOnly           EffectClass = "read_only"
	EffectIdempotentWrite    EffectClass = "idempotent_write"
	EffectNonIdempotentWrite EffectClass = "non_idempotent_write"
)

// CallerScope identifies the invocation origin (ARCH-012): an active turn
// (supervisor) or an active workflow step (control-plane runtime).
type CallerScope string

const (
	CallerScopeTurn         CallerScope = "turn"
	CallerScopeWorkflowStep CallerScope = "workflow_step"
)

// InvocationState is the ledger lifecycle (ARCH-014):
// dispatching->running->succeeded|failed|outcome_unknown.
type InvocationState string

const (
	InvocationDispatching    InvocationState = "dispatching"
	InvocationRunning        InvocationState = "running"
	InvocationSucceeded      InvocationState = "succeeded"
	InvocationFailed         InvocationState = "failed"
	InvocationOutcomeUnknown InvocationState = "outcome_unknown"
)

// CredentialScheme mirrors the proto CredentialScheme, stored as text.
type CredentialScheme string

const (
	CredBearer                 CredentialScheme = "bearer"
	CredOAuthClientCredentials CredentialScheme = "oauth_client_credentials" //nolint:gosec // G101: scheme constant, not a credential
)

// ToolVersion is a row from toolgateway.tool_versions: an immutable published
// descriptor (ARCH-006/007).
type ToolVersion struct {
	ID               string
	Name             string
	Version          string
	Digest           string
	Description      string
	InputSchema      []byte // JSON Schema
	EffectClass      EffectClass
	CredentialSlots  []byte // JSONB [{name, scheme, binding_schema, required}]
	ArtifactCapabs   []byte // JSONB {reads, writes, accepted_mime_types}
	TimeoutMS        int64
	IdempotencyProof []byte // JSONB; required when EffectClass == idempotent_write
	CreatedAt        time.Time
}

// RunnerRegistration is a row from toolgateway.runner_registrations.
type RunnerRegistration struct {
	ID                string
	RunnerID          string
	SpiffeID          string
	Namespace         string
	ToolName          string
	ToolVersion       string
	ToolDigest        string
	FencingGeneration int64
	LastHeartbeatAt   time.Time
	Active            bool
	RegisteredAt      time.Time
}

// Pool is a row from toolgateway.pools (the AgentPool registry, ARCH-016/018).
type Pool struct {
	ID             string
	Key            string
	Name           string
	SpiffeIDPrefix string
}

// PoolGrant is a row from toolgateway.pool_grants — the action-scoped
// deny-by-default gate (ARCH-016/018; REQ-010/SCN-009). Absence = denied.
type PoolGrant struct {
	ID             string
	PoolID         string
	ToolName       string
	MaxEffectClass EffectClass
	AllowedActions []byte // JSONB opaque action allow-list
}

// CredentialBinding is a row from toolgateway.credential_bindings: a logical
// slot -> K8s SecretRef + non-secret resource constraints (ARCH-008/018). The
// credential VALUE is never here — only the SecretRef.
type CredentialBinding struct {
	ID                  string
	PoolID              string
	ToolName            string
	SlotName            string
	Scheme              CredentialScheme
	SecretRef           []byte // JSONB {name, key}
	ResourceConstraints []byte // JSONB non-secret scope
}

// SecretRef names a key within a K8s Secret. The gateway reads the value from
// the named Secret via the in-cluster API at invocation time.
type SecretRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// WorkflowPoolBinding is a row from toolgateway.workflow_pool_bindings:
// workflow definition -> pool + workflow-requested permitted tools.
type WorkflowPoolBinding struct {
	ID                    string
	WorkflowDefinitionKey string
	PoolID                string
	PermittedTools        []string // from JSONB array
}

// ApprovedRunner is a row from toolgateway.approved_runners — deny-by-default
// runner identity approval (ARCH-015).
type ApprovedRunner struct {
	ID                    string
	Namespace             string
	RunnerID              string
	SpiffeID              string
	AllowedToolNamespaces []string
}

// Invocation is a row from toolgateway.invocations — the durable at-most-once
// ledger (ARCH-014).
type Invocation struct {
	ID                 string
	AttemptID          string
	CallerScope        CallerScope
	CallerScopeID      string
	ToolCallID         string
	ToolName           string
	ToolVersionDigest  string
	IdempotencyKey     string
	EffectClass        EffectClass
	PoolID             *string
	RunnerID           *string
	ArgumentsJSON      []byte
	State              InvocationState
	ResultJSON         []byte
	ArtifactOutputRefs []byte // JSONB []ArtifactRef
	Error              []byte // JSONB {code, message, retryability, details_json}
	DispatchingAt      time.Time
	RunningAt          *time.Time
	FinishedAt         *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Store reads and writes the toolgateway schema via a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool for tool-gateway operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// RegisterToolVersion inserts an immutable descriptor on first sight of a
// (name, digest); re-registration of the same digest is idempotent (returns
// the existing row). A different digest for the same (name, version) violates
// the immutability contract and is rejected (ARCH-007).
func (s *Store) RegisterToolVersion(ctx context.Context, tv ToolVersion) (ToolVersion, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO toolgateway.tool_versions
			(name, version, digest, description, input_schema, effect_class,
			 credential_slots, artifact_capabilities, timeout_ms, idempotency_proof)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (name, digest) DO UPDATE
			SET description = EXCLUDED.description
		RETURNING id, name, version, digest, description, input_schema, effect_class,
		          credential_slots, artifact_capabilities, timeout_ms, idempotency_proof, created_at`,
		tv.Name, tv.Version, tv.Digest, tv.Description, tv.InputSchema, tv.EffectClass,
		tv.CredentialSlots, tv.ArtifactCapabs, tv.TimeoutMS, tv.IdempotencyProof)
	out, err := scanToolVersion(row)
	if err != nil {
		return ToolVersion{}, fmt.Errorf("register tool version: %w", err)
	}
	// Immutability guard: same (name, version) must map to one digest.
	var existingDigest string
	err = s.pool.QueryRow(ctx, `
		SELECT digest FROM toolgateway.tool_versions
		WHERE name = $1 AND version = $2 AND digest <> $3`,
		tv.Name, tv.Version, tv.Digest).Scan(&existingDigest)
	if err == nil {
		return ToolVersion{}, fmt.Errorf(
			"tool %s version %s already published with digest %s; immutable versions cannot be mutated (ARCH-007)",
			tv.Name, tv.Version, existingDigest)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ToolVersion{}, fmt.Errorf("immutability guard: %w", err)
	}
	return out, nil
}

// GetToolVersion fetches a descriptor by (name, digest) — the pinned identity.
func (s *Store) GetToolVersion(ctx context.Context, name, digest string) (ToolVersion, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, version, digest, description, input_schema, effect_class,
		       credential_slots, artifact_capabilities, timeout_ms, idempotency_proof, created_at
		FROM toolgateway.tool_versions WHERE name = $1 AND digest = $2`, name, digest)
	tv, err := scanToolVersion(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ToolVersion{}, ErrNotFound
	}
	return tv, err
}

// UpsertRunnerRegistration fences any previous active registration for the same
// (runner_id, tool_name, tool_version) and inserts a fresh active one. Called
// on Register over a new stream; the fencing generation distinguishes
// reconnects (ARCH-015).
func (s *Store) UpsertRunnerRegistration(ctx context.Context, r RunnerRegistration) (RunnerRegistration, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RunnerRegistration{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.runner_registrations SET active = false
		WHERE runner_id = $1 AND tool_name = $2 AND tool_version = $3 AND active`,
		r.RunnerID, r.ToolName, r.ToolVersion); err != nil {
		return RunnerRegistration{}, fmt.Errorf("fence previous: %w", err)
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO toolgateway.runner_registrations
			(runner_id, spiffe_id, namespace, tool_name, tool_version, tool_digest, fencing_generation)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, runner_id, spiffe_id, namespace, tool_name, tool_version, tool_digest,
		          fencing_generation, last_heartbeat_at, active, registered_at`,
		r.RunnerID, r.SpiffeID, r.Namespace, r.ToolName, r.ToolVersion, r.ToolDigest, r.FencingGeneration)
	out, err := scanRunnerRegistration(row)
	if err != nil {
		return RunnerRegistration{}, fmt.Errorf("insert registration: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RunnerRegistration{}, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}

// Heartbeat renews the lease for an active registration.
func (s *Store) Heartbeat(ctx context.Context, runnerID, toolName, toolVersion string, gen int64) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.runner_registrations
		SET last_heartbeat_at = now()
		WHERE runner_id = $1 AND tool_name = $2 AND tool_version = $3
		  AND fencing_generation = $4 AND active`,
		runnerID, toolName, toolVersion, gen)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// HeartbeatRunner renews the lease for ALL of a runner's active registrations
// (by runner_id + generation), without per-tool lookups. Used by the bidi loop
// on a Heartbeat message.
func (s *Store) HeartbeatRunner(ctx context.Context, runnerID string, gen int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.runner_registrations
		SET last_heartbeat_at = now()
		WHERE runner_id = $1 AND fencing_generation = $2 AND active`,
		runnerID, gen)
	if err != nil {
		return fmt.Errorf("heartbeat runner: %w", err)
	}
	return nil
}

// DeactivateRunnerStream marks all of a runner's active registrations inactive
// (stream close / fencing). Existing pinned attempts keep their pin; the
// versions just become unavailable for NEW attempt resolution.
func (s *Store) DeactivateRunnerStream(ctx context.Context, runnerID string, gen int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.runner_registrations SET active = false
		WHERE runner_id = $1 AND fencing_generation = $2 AND active`,
		runnerID, gen)
	if err != nil {
		return fmt.Errorf("deactivate runner stream: %w", err)
	}
	return nil
}

// IsApprovedRunner returns the approval record for a SPIFFE id, or
// ErrNotFound. Deny-by-default (ARCH-015): an unapproved identity is rejected
// at registration.
func (s *Store) IsApprovedRunner(ctx context.Context, spiffeID string) (ApprovedRunner, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, namespace, runner_id, spiffe_id, allowed_tool_namespaces
		FROM toolgateway.approved_runners
		WHERE spiffe_id = $1 AND deleted_at IS NULL`, spiffeID)
	var a ApprovedRunner
	var ns []string
	if err := row.Scan(&a.ID, &a.Namespace, &a.RunnerID, &a.SpiffeID, &ns); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovedRunner{}, ErrNotFound
		}
		return ApprovedRunner{}, err
	}
	a.AllowedToolNamespaces = ns
	return a, nil
}

// ResolvePoolBySpiffePrefix finds the pool whose spiffe_id_prefix is a prefix of
// the supervisor's verified SPIFFE id (the pool is derived from the workload
// identity, not caller-supplied — ARCH-004/010).
func (s *Store) ResolvePoolBySpiffePrefix(ctx context.Context, spiffeID string) (Pool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, name, spiffe_id_prefix
		FROM toolgateway.pools
		WHERE $1 LIKE spiffe_id_prefix || '%' AND deleted_at IS NULL
		ORDER BY length(spiffe_id_prefix) DESC LIMIT 1`, spiffeID)
	var p Pool
	if err := row.Scan(&p.ID, &p.Key, &p.Name, &p.SpiffeIDPrefix); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Pool{}, ErrNotFound
		}
		return Pool{}, err
	}
	return p, nil
}

// GetWorkflowPoolBinding fetches the pool + permitted tools for a workflow
// definition (the workflow-step caller path, ARCH-012/018).
func (s *Store) GetWorkflowPoolBinding(ctx context.Context, workflowDefinitionKey string) (WorkflowPoolBinding, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, workflow_definition_key, pool_id, permitted_tools
		FROM toolgateway.workflow_pool_bindings
		WHERE workflow_definition_key = $1 AND deleted_at IS NULL`, workflowDefinitionKey)
	var w WorkflowPoolBinding
	var permitted []string
	if err := row.Scan(&w.ID, &w.WorkflowDefinitionKey, &w.PoolID, &permitted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkflowPoolBinding{}, ErrNotFound
		}
		return WorkflowPoolBinding{}, err
	}
	w.PermittedTools = permitted
	return w, nil
}

// DiscoverEffectiveTools returns the descriptors permitted for a pool +
// workflow-requested tool set: available_tool_versions (healthy runners) ∩
// pool_grants ∩ permittedTools (ARCH-006/016/018). poolID + permittedTools are
// resolved by the caller (supervisor SPIFFE -> pool, or workflow binding).
// permittedTools empty means "no workflow narrowing" (all granted tools).
func (s *Store) DiscoverEffectiveTools(ctx context.Context, poolID string, permittedTools []string) ([]ToolVersion, error) {
	if permittedTools == nil {
		permittedTools = []string{}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tv.id, tv.name, tv.version, tv.digest, tv.description, tv.input_schema,
		       tv.effect_class, tv.credential_slots, tv.artifact_capabilities,
		       tv.timeout_ms, tv.idempotency_proof, tv.created_at
		FROM toolgateway.available_tool_versions tv
		JOIN toolgateway.pool_grants pg
		  ON pg.tool_name = tv.name AND pg.deleted_at IS NULL AND pg.pool_id = $1
		WHERE CASE tv.effect_class
		        WHEN 'read_only' THEN 1
		        WHEN 'idempotent_write' THEN 2
		        WHEN 'non_idempotent_write' THEN 3
		      END <= CASE pg.max_effect_class
		        WHEN 'read_only' THEN 1
		        WHEN 'idempotent_write' THEN 2
		        WHEN 'non_idempotent_write' THEN 3
		      END
		  AND ($2::text[] IS NULL OR $2::text[] = '{}' OR tv.name = ANY($2::text[]))
		ORDER BY tv.name, tv.version`, poolID, permittedTools)
	if err != nil {
		return nil, fmt.Errorf("discover effective tools: %w", err)
	}
	defer rows.Close()
	var out []ToolVersion
	for rows.Next() {
		tv, err := scanToolVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tv)
	}
	return out, rows.Err()
}

// ResolveCredentialBindings returns the slot bindings for a pool + tool
// (ARCH-008). The gateway reads the named K8s Secret values via the in-cluster
// API and hands a CredentialContext to the trusted runner.
func (s *Store) ResolveCredentialBindings(ctx context.Context, poolID, toolName string) ([]CredentialBinding, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, pool_id, tool_name, slot_name, scheme, secret_ref, resource_constraints
		FROM toolgateway.credential_bindings
		WHERE pool_id = $1 AND tool_name = $2 AND deleted_at IS NULL
		ORDER BY slot_name`, poolID, toolName)
	if err != nil {
		return nil, fmt.Errorf("resolve credential bindings: %w", err)
	}
	defer rows.Close()
	var out []CredentialBinding
	for rows.Next() {
		var b CredentialBinding
		if err := rows.Scan(&b.ID, &b.PoolID, &b.ToolName, &b.SlotName, &b.Scheme, &b.SecretRef, &b.ResourceConstraints); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// InvocationKey is the at-most-once uniqueness key (ARCH-014).
type InvocationKey struct {
	AttemptID         string
	CallerScope       CallerScope
	CallerScopeID     string
	ToolCallID        string
	ToolVersionDigest string
	IdempotencyKey    string
}

// BeginInvocation commits a ledger row in 'dispatching' state before the
// side-effect boundary. On a unique-key conflict (duplicate caller) it returns
// the existing invocation with inserted=false so the caller can report
// in-progress or replay the committed result (ARCH-014).
func (s *Store) BeginInvocation(ctx context.Context, key InvocationKey, tv ToolVersion, poolID *string, args []byte) (inv Invocation, inserted bool, err error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO toolgateway.invocations
			(attempt_id, caller_scope, caller_scope_id, tool_call_id, tool_name,
			 tool_version_digest, idempotency_key, effect_class, pool_id, arguments_json, state)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'dispatching')
		ON CONFLICT (attempt_id, caller_scope, caller_scope_id, tool_call_id, tool_version_digest, idempotency_key)
		DO NOTHING
		RETURNING id`,
		key.AttemptID, key.CallerScope, key.CallerScopeID, key.ToolCallID, tv.Name,
		key.ToolVersionDigest, key.IdempotencyKey, tv.EffectClass, poolID, args)
	var id string
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Conflict: duplicate. Fetch the existing invocation.
			existing, gerr := s.GetInvocationByKey(ctx, key)
			if gerr != nil {
				return Invocation{}, false, gerr
			}
			return existing, false, nil
		}
		return Invocation{}, false, fmt.Errorf("begin invocation: %w", err)
	}
	inv, err = s.GetInvocation(ctx, id)
	if err != nil {
		return Invocation{}, false, err
	}
	return inv, true, nil
}

// MarkRunning transitions dispatching -> running (dispatched over the runner
// stream), recording the executing runner.
func (s *Store) MarkRunning(ctx context.Context, invocationID, runnerID string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.invocations
		SET state = 'running', running_at = now(), runner_id = $2
		WHERE id = $1 AND state = 'dispatching'`,
		invocationID, runnerID)
	if err != nil {
		return fmt.Errorf("mark running: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

// FinishInvocation transitions a running/dispatching invocation to a terminal
// state (succeeded/failed/outcome_unknown) with the committed result/artifact
// refs/error (ARCH-014).
func (s *Store) FinishInvocation(ctx context.Context, invocationID string, state InvocationState, result, artifactRefs, errDetail []byte) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.invocations
		SET state = $2, result_json = $3, artifact_output_refs = $4, error = $5, finished_at = now()
		WHERE id = $1 AND state IN ('dispatching', 'running')`,
		invocationID, state, result, artifactRefs, errDetail)
	if err != nil {
		return fmt.Errorf("finish invocation: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

// GetInvocation fetches a ledger row by id.
func (s *Store) GetInvocation(ctx context.Context, id string) (Invocation, error) {
	row := s.pool.QueryRow(ctx, invocationSelect+` WHERE id = $1`, id)
	inv, err := scanInvocation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Invocation{}, ErrNotFound
	}
	return inv, err
}

// GetInvocationByKey fetches a ledger row by its at-most-once uniqueness key.
func (s *Store) GetInvocationByKey(ctx context.Context, key InvocationKey) (Invocation, error) {
	row := s.pool.QueryRow(ctx, invocationSelect+`
		WHERE attempt_id = $1 AND caller_scope = $2 AND caller_scope_id = $3
		  AND tool_call_id = $4 AND tool_version_digest = $5 AND idempotency_key = $6`,
		key.AttemptID, key.CallerScope, key.CallerScopeID, key.ToolCallID,
		key.ToolVersionDigest, key.IdempotencyKey)
	inv, err := scanInvocation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Invocation{}, ErrNotFound
	}
	return inv, err
}

// --- Operator-seed methods (test/admin population; HOR-245/252/397 populate via CRD later) ---

// UpsertPool inserts/revives a pool (operator-seed; AgentPool CRD HOR-245 later).
func (s *Store) UpsertPool(ctx context.Context, key, name, spiffePrefix string) (Pool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE
			SET name = EXCLUDED.name, spiffe_id_prefix = EXCLUDED.spiffe_id_prefix,
			    deleted_at = NULL, updated_at = now()
		RETURNING id, key, name, spiffe_id_prefix`,
		key, name, spiffePrefix)
	var p Pool
	if err := row.Scan(&p.ID, &p.Key, &p.Name, &p.SpiffeIDPrefix); err != nil {
		return Pool{}, fmt.Errorf("upsert pool: %w", err)
	}
	return p, nil
}

// UpsertPoolGrant inserts/revives a pool grant (operator-seed; AgentPool CRD HOR-245).
func (s *Store) UpsertPoolGrant(ctx context.Context, poolID, toolName string, maxEffect EffectClass, allowedActions []byte) error {
	if allowedActions == nil {
		allowedActions = []byte("[]")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO toolgateway.pool_grants (pool_id, tool_name, max_effect_class, allowed_actions)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (pool_id, tool_name) DO UPDATE
			SET max_effect_class = EXCLUDED.max_effect_class,
			    allowed_actions = EXCLUDED.allowed_actions,
			    deleted_at = NULL, updated_at = now()`,
		poolID, toolName, maxEffect, allowedActions)
	if err != nil {
		return fmt.Errorf("upsert pool grant: %w", err)
	}
	return nil
}

// UpsertCredentialBinding inserts/revives a credential binding (operator-seed;
// AgentPool CRD HOR-245 validates against the runner-declared slot schema).
// secretSpec is the scheme-dependent JSONB spec stored in secret_ref:
//
//	bearer:                  {"value_ref": {name, key}}
//	oauth_client_credentials: {"client_id", "client_secret_ref": {name,key}, "token_url", "scope"}
//
// The credential VALUE is never stored — only SecretRef pointers the gateway
// resolves at invocation time (ARCH-008).
func (s *Store) UpsertCredentialBinding(ctx context.Context, poolID, toolName, slotName string, scheme CredentialScheme, secretSpec []byte, resourceConstraints []byte) error {
	if secretSpec == nil {
		secretSpec = []byte("{}")
	}
	if resourceConstraints == nil {
		resourceConstraints = []byte("{}")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO toolgateway.credential_bindings
			(pool_id, tool_name, slot_name, scheme, secret_ref, resource_constraints)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (pool_id, tool_name, slot_name) DO UPDATE
			SET scheme = EXCLUDED.scheme, secret_ref = EXCLUDED.secret_ref,
			    resource_constraints = EXCLUDED.resource_constraints,
			    deleted_at = NULL, updated_at = now()`,
		poolID, toolName, slotName, scheme, secretSpec, resourceConstraints)
	if err != nil {
		return fmt.Errorf("upsert credential binding: %w", err)
	}
	return nil
}

// UpsertWorkflowPoolBinding inserts/revives a workflow->pool binding
// (operator-seed; Workflow definitions HOR-252 later).
func (s *Store) UpsertWorkflowPoolBinding(ctx context.Context, workflowKey, poolID string, permittedTools []string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO toolgateway.workflow_pool_bindings (workflow_definition_key, pool_id, permitted_tools)
		VALUES ($1, $2, $3)
		ON CONFLICT (workflow_definition_key) DO UPDATE
			SET pool_id = EXCLUDED.pool_id, permitted_tools = EXCLUDED.permitted_tools,
			    deleted_at = NULL, updated_at = now()`,
		workflowKey, poolID, permittedTools)
	if err != nil {
		return fmt.Errorf("upsert workflow pool binding: %w", err)
	}
	return nil
}

// UpsertApprovedRunner inserts/revives a runner approval (operator-seed;
// runner materializer HOR-397/245 later).
func (s *Store) UpsertApprovedRunner(ctx context.Context, namespace, runnerID, spiffeID string, allowedNs []string) error {
	if allowedNs == nil {
		allowedNs = []string{}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO toolgateway.approved_runners (namespace, runner_id, spiffe_id, allowed_tool_namespaces)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (spiffe_id) DO UPDATE
			SET namespace = EXCLUDED.namespace, runner_id = EXCLUDED.runner_id,
			    allowed_tool_namespaces = EXCLUDED.allowed_tool_namespaces,
			    deleted_at = NULL, updated_at = now()`,
		namespace, runnerID, spiffeID, allowedNs)
	if err != nil {
		return fmt.Errorf("upsert approved runner: %w", err)
	}
	return nil
}

// --- scanners / helpers ---

const invocationSelect = `
	SELECT id, attempt_id, caller_scope, caller_scope_id, tool_call_id, tool_name,
	       tool_version_digest, idempotency_key, effect_class, pool_id, runner_id,
	       arguments_json, state, result_json, artifact_output_refs, error,
	       dispatching_at, running_at, finished_at, created_at, updated_at
	FROM toolgateway.invocations`

func scanToolVersion(row pgx.Row) (ToolVersion, error) {
	var tv ToolVersion
	err := row.Scan(&tv.ID, &tv.Name, &tv.Version, &tv.Digest, &tv.Description,
		&tv.InputSchema, &tv.EffectClass, &tv.CredentialSlots, &tv.ArtifactCapabs,
		&tv.TimeoutMS, &tv.IdempotencyProof, &tv.CreatedAt)
	return tv, err
}

func scanRunnerRegistration(row pgx.Row) (RunnerRegistration, error) {
	var r RunnerRegistration
	err := row.Scan(&r.ID, &r.RunnerID, &r.SpiffeID, &r.Namespace, &r.ToolName,
		&r.ToolVersion, &r.ToolDigest, &r.FencingGeneration, &r.LastHeartbeatAt,
		&r.Active, &r.RegisteredAt)
	return r, err
}

func scanInvocation(row pgx.Row) (Invocation, error) {
	var inv Invocation
	err := row.Scan(&inv.ID, &inv.AttemptID, &inv.CallerScope, &inv.CallerScopeID,
		&inv.ToolCallID, &inv.ToolName, &inv.ToolVersionDigest, &inv.IdempotencyKey,
		&inv.EffectClass, &inv.PoolID, &inv.RunnerID, &inv.ArgumentsJSON, &inv.State,
		&inv.ResultJSON, &inv.ArtifactOutputRefs, &inv.Error, &inv.DispatchingAt,
		&inv.RunningAt, &inv.FinishedAt, &inv.CreatedAt, &inv.UpdatedAt)
	return inv, err
}

// effectRank maps an effect class to its severity rank for grant ceiling
// comparison: read_only (1) < idempotent_write (2) < non_idempotent_write (3).
// A pool grant's max_effect_class is the ceiling; a tool whose effect_class
// exceeds it is denied (ARCH-016).
func effectRank(c EffectClass) int {
	switch c {
	case EffectReadOnly:
		return 1
	case EffectIdempotentWrite:
		return 2
	case EffectNonIdempotentWrite:
		return 3
	}
	return 0
}

func marshalJSON(v any) ([]byte, error) { return json.Marshal(v) }

func unmarshalJSON(b []byte, v any) error {
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, v)
}
