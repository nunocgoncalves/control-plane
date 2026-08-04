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
// It mirrors the identity/permissions/catalog/runtime stores: pgxpool +
// ErrNotFound, soft-delete on operator-owned config tables, no pg_notify (the
// gateway is the sole writer; runners push over streams). The gateway also
// reads the `runtime` schema (workflow_runs/run_steps/turns/run_pool_assignments)
// read-only to resolve caller scope from durable state (ARCH-004).
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
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
	// ErrScopeDenied is returned when the caller's durable scope cannot be
	// resolved or does not match the supplied context (ARCH-004). Fail closed.
	ErrScopeDenied = errors.New("gateway: caller scope not authorized")
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

// IsTerminal reports whether the state is a committed terminal outcome
// (succeeded / failed / outcome_unknown). Non-terminal states (dispatching,
// running) are still in flight and may not be reported as a final result.
func (s InvocationState) IsTerminal() bool {
	switch s {
	case InvocationSucceeded, InvocationFailed, InvocationOutcomeUnknown:
		return true
	}
	return false
}

// CredentialScheme mirrors the proto CredentialScheme, stored as text.
type CredentialScheme string

const (
	CredBearer                 CredentialScheme = "bearer"
	CredOAuthClientCredentials CredentialScheme = "oauth_client_credentials" //nolint:gosec // G101: scheme constant, not a credential
)

// ToolVersion is a row from toolgateway.tool_versions: an immutable published
// descriptor (ARCH-006/007).
type ToolVersion struct {
	ID                  string
	Name                string
	Version             string
	Digest              string
	Description         string
	InputSchema         []byte // JSON Schema
	EffectClass         EffectClass
	CredentialSlots     []byte // JSONB [{name, scheme, binding_schema, required}]
	ArtifactCapabs      []byte // JSONB {reads, writes, accepted_mime_types}
	TimeoutMS           int64
	IdempotencyProof    []byte // JSONB; required when EffectClass == idempotent_write
	ConsequenceTemplate []byte // JSONB; required for writes and rendered before dispatch (ARCH-022)
	CreatedAt           time.Time
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
	AcceptingNew      bool
	RegisteredAt      time.Time
}

// ToolRef is the immutable name+digest identity used by generation draining.
type ToolRef struct {
	Name   string
	Digest string
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
	AllowedActions []string // from JSONB array; empty = effect-class-only (no action narrowing)
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

// Capability is one workflow-requested gateway capability narrowing entry,
// persisted in workflow_pool_bindings.permitted_tools as a JSONB array of
// objects (ARCH-016). The gateway intersects it with deployed registry
// availability and AgentPool grants at discovery/authorization so a workflow
// narrowed to read_only / a subset of actions is not widened back to the pool
// ceiling at runtime (REQ-001/REQ-010). Actions is enforced at discovery and
// invocation using the approved v1 effective-action contract: an undecomposed
// descriptor's action is its tool name; empty means effect-class-only.
type Capability struct {
	Tool           string   `json:"tool"`
	MaxEffectClass string   `json:"maxEffectClass"`
	Actions        []string `json:"actions,omitempty"`
}

// WorkflowPoolBinding is a row from toolgateway.workflow_pool_bindings:
// workflow definition -> pool + workflow-requested permitted capabilities.
type WorkflowPoolBinding struct {
	ID                    string
	WorkflowDefinitionKey string
	PoolID                string
	PermittedCapabilities []Capability // from JSONB array; nil = absent, len==0 = explicitly none (deny all)
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
	ID                     string
	AttemptID              string
	CallerScope            CallerScope
	CallerScopeID          string
	ToolCallID             string
	ToolName               string
	ToolVersionDigest      string
	IdempotencyKey         string
	EffectClass            EffectClass
	PoolID                 *string
	RunnerID               *string
	ArgumentsJSON          []byte
	ArtifactInputRefs      []byte // JSONB exact authorized inputs committed before dispatch
	ConsequenceSummary     []byte // JSONB localized customer-safe text, persisted before dispatch
	State                  InvocationState
	ResultJSON             []byte
	ArtifactOutputRefs     []byte // JSONB []ArtifactRef
	Error                  []byte // JSONB {code, message, retryability, details_json}
	DispatchLeaseExpiresAt *time.Time
	GatewayInstanceID      *string
	DispatchingAt          time.Time
	RunningAt              *time.Time
	FinishedAt             *time.Time
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// CallerResolution is the durable caller scope resolved from runtime state
// (ARCH-004): the pool the call is bound to, the workflow-permitted tool set
// (nil = no narrowing for the turn path; empty slice = explicitly none), the
// attempt id (the runtime run id for v1), and the authoritative caller scope
// + scope id (identity-derived + validated; used for the ledger key, never the
// caller-supplied values).
type CallerResolution struct {
	Pool                  Pool
	PermittedCapabilities []Capability // nil = no workflow narrowing (turn path); len==0 = deny all
	AttemptID             string       // runtime run id (v1 attempt identity)
	CallerScope           CallerScope
	CallerScopeID         string // validated turn_id / run_step_id
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
// (name, digest); re-registration of the same digest is a validated no-op that
// returns the existing row WITHOUT mutating any descriptor field (ARCH-007).
// A different digest for the same (name, version) violates the immutability
// contract and is rejected. effect_class and idempotency_proof are validated
// fail-closed (ARCH-014).
func (s *Store) RegisterToolVersion(ctx context.Context, tv ToolVersion) (ToolVersion, error) {
	if err := validateToolVersion(tv); err != nil {
		return ToolVersion{}, err
	}
	// Idempotent insert: ON CONFLICT DO NOTHING, then fetch the canonical row.
	// No descriptor field is ever updated on re-registration (ARCH-007).
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO toolgateway.tool_versions
			(name, version, digest, description, input_schema, effect_class,
			 credential_slots, artifact_capabilities, timeout_ms, idempotency_proof,
			 consequence_summary_template)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (name, digest) DO NOTHING`,
		tv.Name, tv.Version, tv.Digest, tv.Description, tv.InputSchema, tv.EffectClass,
		tv.CredentialSlots, tv.ArtifactCapabs, tv.TimeoutMS, tv.IdempotencyProof, tv.ConsequenceTemplate); err != nil {
		return ToolVersion{}, fmt.Errorf("register tool version: %w", err)
	}
	out, err := s.GetToolVersion(ctx, tv.Name, tv.Digest)
	if err != nil {
		return ToolVersion{}, err
	}
	// A write version published before consequence templates existed is immutable
	// and cannot be repaired in place. Reject re-registration of that digest so
	// the runner must publish a new descriptor/version identity (ARCH-007).
	if err := validateToolVersion(out); err != nil {
		return ToolVersion{}, fmt.Errorf("tool %s digest %s is incompatible with the current descriptor contract; publish a new version and digest: %w", tv.Name, tv.Digest, err)
	}
	// The digest binds descriptor + implementation (HOR-397). Re-registration
	// must match every immutable descriptor field; returning the canonical row
	// while silently accepting changed metadata would break attribution.
	if !sameToolVersionDescriptor(tv, out) {
		return ToolVersion{}, fmt.Errorf("tool %s digest %s was registered with different immutable descriptor metadata", tv.Name, tv.Digest)
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

// validateToolVersion enforces fail-closed descriptor invariants (ARCH-014):
// a concrete effect class is required, and idempotent_write requires a
// non-empty idempotency proof with a concrete strategy.
func validateToolVersion(tv ToolVersion) error {
	switch tv.EffectClass {
	case EffectReadOnly, EffectIdempotentWrite, EffectNonIdempotentWrite:
	default:
		return fmt.Errorf("tool %s: effect_class must be a concrete class (read_only|idempotent_write|non_idempotent_write), got %q",
			tv.Name, tv.EffectClass)
	}
	if err := validateConsequenceTemplate(tv); err != nil {
		return err
	}
	if tv.EffectClass == EffectIdempotentWrite {
		if len(tv.IdempotencyProof) == 0 || bytes.Equal(tv.IdempotencyProof, []byte("null")) {
			return fmt.Errorf("tool %s: idempotent_write requires a non-empty idempotency_proof", tv.Name)
		}
		var proof struct {
			Strategy string `json:"strategy"`
		}
		if err := json.Unmarshal(tv.IdempotencyProof, &proof); err != nil {
			return fmt.Errorf("tool %s: idempotency_proof is not valid JSON: %w", tv.Name, err)
		}
		// A concrete, gateway-provable strategy is the "proven stable strategy"
		// the retry gate requires (ARCH-014). In v1 only `upstream_key` is
		// accepted: the gateway derives the stable upstream key from the durable
		// invocation id and propagates it to the runner across retries, so the
		// proof is verifiable end-to-end. A `resource_identity`/
		// `deterministic_resource_id` strategy is NOT accepted in v1 because the
		// gateway has no declared resource-identity argument to validate it
		// against (the v1 proto carries no such field); accepting a bare strategy
		// string would let an unprovable descriptor become auto-retryable.
		// Fail-closed: reject anything but `upstream_key` until a later ticket
		// adds a gateway-verifiable resource-identity contract.
		switch proof.Strategy {
		case "upstream_key":
		default:
			return fmt.Errorf("tool %s: idempotent_write requires idempotency_proof.strategy=upstream_key in v1 (gateway-derived stable key); %q is not gateway-provable (ARCH-014 fail-closed)", tv.Name, proof.Strategy)
		}
	}
	return nil
}

func sameToolVersionDescriptor(a, b ToolVersion) bool {
	if a.Name != b.Name || a.Version != b.Version || a.Digest != b.Digest ||
		a.Description != b.Description || a.EffectClass != b.EffectClass || a.TimeoutMS != b.TimeoutMS {
		return false
	}
	return sameJSON(a.InputSchema, b.InputSchema) &&
		sameJSON(a.CredentialSlots, b.CredentialSlots) &&
		sameJSON(a.ArtifactCapabs, b.ArtifactCapabs) &&
		sameJSON(a.IdempotencyProof, b.IdempotencyProof) &&
		sameJSON(a.ConsequenceTemplate, b.ConsequenceTemplate)
}

func sameJSON(a, b []byte) bool {
	if len(a) == 0 || bytes.Equal(a, []byte("null")) {
		a = []byte("null")
	}
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		b = []byte("null")
	}
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return bytes.Equal(a, b)
	}
	return reflect.DeepEqual(av, bv)
}

// GetToolVersion fetches a descriptor by (name, digest) — the pinned identity.
func (s *Store) GetToolVersion(ctx context.Context, name, digest string) (ToolVersion, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, version, digest, description, input_schema, effect_class,
		       credential_slots, artifact_capabilities, timeout_ms, idempotency_proof,
		       consequence_summary_template, created_at
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
// reconnects (ARCH-015). A reconnect preserves the latest registration's
// accepting_new state so a draining version never becomes snapshot-eligible
// during the Register -> BeginDrain replay window.
func (s *Store) UpsertRunnerRegistration(ctx context.Context, r RunnerRegistration) (RunnerRegistration, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RunnerRegistration{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	acceptingNew := true
	if err := tx.QueryRow(ctx, `
		SELECT accepting_new FROM toolgateway.runner_registrations
		WHERE runner_id = $1 AND tool_name = $2 AND tool_version = $3
		ORDER BY registered_at DESC LIMIT 1`,
		r.RunnerID, r.ToolName, r.ToolVersion).Scan(&acceptingNew); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return RunnerRegistration{}, fmt.Errorf("read previous drain state: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.runner_registrations SET active = false
		WHERE runner_id = $1 AND tool_name = $2 AND tool_version = $3 AND active`,
		r.RunnerID, r.ToolName, r.ToolVersion); err != nil {
		return RunnerRegistration{}, fmt.Errorf("fence previous: %w", err)
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO toolgateway.runner_registrations
			(runner_id, spiffe_id, namespace, tool_name, tool_version, tool_digest, fencing_generation, accepting_new)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, runner_id, spiffe_id, namespace, tool_name, tool_version, tool_digest,
		          fencing_generation, last_heartbeat_at, active, accepting_new, registered_at`,
		r.RunnerID, r.SpiffeID, r.Namespace, r.ToolName, r.ToolVersion, r.ToolDigest, r.FencingGeneration, acceptingNew)
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

// BeginDrain removes the runner's superseded versions from new-attempt
// selection while keeping the live stream dispatchable for existing pins.
func (s *Store) BeginDrain(ctx context.Context, runnerID string, gen int64, refs []ToolRef) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin drain: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, ref := range refs {
		if ref.Name == "" || ref.Digest == "" {
			return fmt.Errorf("begin drain: name and digest are required")
		}
		ct, err := tx.Exec(ctx, `
			UPDATE toolgateway.runner_registrations SET accepting_new=false
			WHERE runner_id=$1 AND fencing_generation=$2 AND tool_name=$3
			  AND tool_digest=$4 AND active`, runnerID, gen, ref.Name, ref.Digest)
		if err != nil {
			return fmt.Errorf("begin drain %s: %w", ref.Name, err)
		}
		if ct.RowsAffected() == 0 {
			return fmt.Errorf("begin drain %s: registration not owned by this stream", ref.Name)
		}
	}
	return tx.Commit(ctx)
}

// DrainingStatus partitions this stream's draining registrations by whether an
// unfinished attempt still pins the exact name+digest. Missing work.attempts
// rows are treated conservatively as pinned (legacy/runtime callers).
func (s *Store) DrainingStatus(ctx context.Context, runnerID string, gen int64) (pinned, releasable []ToolRef, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT rr.tool_name, rr.tool_digest,
		       EXISTS (
		         SELECT 1 FROM toolgateway.attempt_tool_pins pin
		         LEFT JOIN work.attempts a ON a.id::text=pin.attempt_id
		         WHERE pin.tool_name=rr.tool_name
		           AND pin.tool_version_digest=rr.tool_digest
		           AND (a.id IS NULL OR a.finished_at IS NULL)
		       ) AS pinned
		FROM toolgateway.runner_registrations rr
		WHERE rr.runner_id=$1 AND rr.fencing_generation=$2
		  AND rr.active AND NOT rr.accepting_new
		ORDER BY rr.tool_name, rr.tool_digest`, runnerID, gen)
	if err != nil {
		return nil, nil, fmt.Errorf("draining status: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref ToolRef
		var isPinned bool
		if err := rows.Scan(&ref.Name, &ref.Digest, &isPinned); err != nil {
			return nil, nil, err
		}
		if isPinned {
			pinned = append(pinned, ref)
		} else {
			releasable = append(releasable, ref)
		}
	}
	return pinned, releasable, rows.Err()
}

// RetireVersions marks versions unloaded by this exact runner stream.
func (s *Store) RetireVersions(ctx context.Context, runnerID string, gen int64, refs []ToolRef) error {
	for _, ref := range refs {
		ct, err := s.pool.Exec(ctx, `
			UPDATE toolgateway.runner_registrations SET active=false, accepting_new=false
			WHERE runner_id=$1 AND fencing_generation=$2 AND tool_name=$3
			  AND tool_digest=$4 AND active AND NOT accepting_new`,
			runnerID, gen, ref.Name, ref.Digest)
		if err != nil {
			return fmt.Errorf("retire %s: %w", ref.Name, err)
		}
		if ct.RowsAffected() == 0 {
			return fmt.Errorf("retire %s: draining registration not owned by this stream", ref.Name)
		}
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
	var raw []byte
	if err := row.Scan(&w.ID, &w.WorkflowDefinitionKey, &w.PoolID, &raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkflowPoolBinding{}, ErrNotFound
		}
		return WorkflowPoolBinding{}, err
	}
	var caps []Capability
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &caps); err != nil {
			return WorkflowPoolBinding{}, fmt.Errorf("decode workflow permitted capabilities: %w", err)
		}
	}
	w.PermittedCapabilities = caps
	return w, nil
}

// --- caller-scope resolution (durable state, ARCH-004) ---

// ResolveTurnScope resolves a supervisor/turn caller against durable runtime
// state. The supervisor's pool (resolved by the service from the verified
// SPIFFE id) is cross-checked: the supplied turn_id (caller_scope_id) and
// attempt_id (the run id) must match an active turn whose run is durably
// assigned to that same pool. The verified worker (cert SAN pod name) must
// match the turn's active assignment (runtime.turn_assignments) — a
// still-valid supervisor cert from a different same-pool worker, or a
// fenced/old-generation caller whose assignment is no longer active, is
// denied (HOR-249 / HOR-398 DEC-041 residual). Fail closed otherwise
// (ARCH-004/010).
func (s *Store) ResolveTurnScope(ctx context.Context, poolID, attemptID, turnID, workerID string, fencingGeneration uint64) (CallerResolution, error) {
	var runID string
	err := s.pool.QueryRow(ctx, `
		SELECT t.run_id::text
		FROM runtime.turns t
		JOIN runtime.run_pool_assignments a ON a.run_id = t.run_id
		WHERE t.id = $1::uuid AND t.state IN ('pending', 'running')
		  AND t.run_id::text = $2 AND a.pool_id = $3::uuid`,
		turnID, attemptID, poolID).Scan(&runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CallerResolution{}, ErrScopeDenied
		}
		return CallerResolution{}, fmt.Errorf("resolve turn scope: %w", err)
	}
	// Active-assignment cross-check (HOR-249 / DEC-041): the turn's active
	// assignment must be bound to this verified worker AND the caller's current
	// fencing generation. A fenced/terminal assignment (no active row), a
	// different same-pool worker, or an old-generation caller is denied.
	var assignedWorker string
	var assignedGen uint64
	var nodeExecutionID *string
	var capabilityRequest []byte
	err = s.pool.QueryRow(ctx, `
		SELECT worker_id, fencing_generation, node_execution_id, capability_request
		FROM runtime.turn_assignments
		WHERE turn_id = $1::uuid AND state = 'active'`, turnID).
		Scan(&assignedWorker, &assignedGen, &nodeExecutionID, &capabilityRequest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CallerResolution{}, ErrScopeDenied
		}
		return CallerResolution{}, fmt.Errorf("resolve turn assignment: %w", err)
	}
	if assignedWorker != workerID || assignedGen != fencingGeneration {
		return CallerResolution{}, ErrScopeDenied
	}
	pool, err := s.getPoolByID(ctx, poolID)
	if err != nil {
		return CallerResolution{}, ErrScopeDenied
	}
	// Graph turns carry an exact node capability subset in the durable active
	// assignment. Empty is explicit deny-all. Legacy chat turns have no graph
	// node and retain nil=no workflow narrowing.
	var permitted []Capability
	if nodeExecutionID != nil {
		permitted = []Capability{}
		if len(capabilityRequest) > 0 {
			if err := json.Unmarshal(capabilityRequest, &permitted); err != nil {
				return CallerResolution{}, ErrScopeDenied
			}
		}
	}
	return CallerResolution{Pool: pool, PermittedCapabilities: permitted, AttemptID: runID, CallerScope: CallerScopeTurn, CallerScopeID: turnID}, nil
}

// ResolveWorkflowStepScope resolves a control-plane workflow-step caller against
// durable runtime state. The run_step (caller_scope_id) must be running and
// belong to the supplied run (attempt_id); the run's definition_key resolves
// the workflow_pool_binding (pool + permitted tools). The run must be durably
// assigned to that binding's pool. Fail closed otherwise.
func (s *Store) ResolveWorkflowStepScope(ctx context.Context, attemptID, runStepID string) (CallerResolution, error) {
	var definitionKey string
	err := s.pool.QueryRow(ctx, `
		SELECT rs.run_id::text, wr.definition_key
		FROM runtime.run_steps rs
		JOIN runtime.workflow_runs wr ON wr.id = rs.run_id
		WHERE rs.id = $1::uuid AND rs.state = 'running' AND rs.run_id::text = $2`,
		runStepID, attemptID).Scan(&attemptID, &definitionKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CallerResolution{}, ErrScopeDenied
		}
		return CallerResolution{}, fmt.Errorf("resolve workflow-step scope: %w", err)
	}
	if definitionKey == "" {
		return CallerResolution{}, ErrScopeDenied // chat runs have no workflow binding
	}
	b, err := s.GetWorkflowPoolBinding(ctx, definitionKey)
	if err != nil {
		return CallerResolution{}, ErrScopeDenied
	}
	// The run must be durably assigned to the binding's pool.
	var assignedPool string
	err = s.pool.QueryRow(ctx, `
		SELECT pool_id::text FROM runtime.run_pool_assignments WHERE run_id::text = $1`, attemptID).Scan(&assignedPool)
	if err != nil || assignedPool != b.PoolID {
		return CallerResolution{}, ErrScopeDenied
	}
	pool, err := s.getPoolByID(ctx, b.PoolID)
	if err != nil {
		return CallerResolution{}, ErrScopeDenied
	}
	// PermittedCapabilities: nil (absent) = no narrowing is not valid for the
	// workflow path — the binding always carries an explicit set. An empty slice
	// = deny all (preserved distinctly from nil). The caller scope is
	// identity-derived (workflow_step) + the validated run_step id.
	permitted := b.PermittedCapabilities
	if permitted == nil {
		permitted = []Capability{}
	}
	return CallerResolution{Pool: pool, PermittedCapabilities: permitted, AttemptID: attemptID, CallerScope: CallerScopeWorkflowStep, CallerScopeID: runStepID}, nil
}

// getPoolByID fetches a non-deleted pool by id.
func (s *Store) getPoolByID(ctx context.Context, poolID string) (Pool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, name, spiffe_id_prefix
		FROM toolgateway.pools WHERE id = $1::uuid AND deleted_at IS NULL`, poolID)
	var p Pool
	if err := row.Scan(&p.ID, &p.Key, &p.Name, &p.SpiffeIDPrefix); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Pool{}, ErrNotFound
		}
		return Pool{}, err
	}
	return p, nil
}

// UpsertRunPoolAssignment records the durable run -> pool assignment (ARCH-004).
// HOR-249 (dispatch) is the production writer; tests call it directly.
func (s *Store) UpsertRunPoolAssignment(ctx context.Context, runID, poolID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO runtime.run_pool_assignments (run_id, pool_id)
		VALUES ($1::uuid, $2::uuid)
		ON CONFLICT (run_id) DO UPDATE SET pool_id = EXCLUDED.pool_id, assigned_at = now()`,
		runID, poolID)
	if err != nil {
		return fmt.Errorf("upsert run pool assignment: %w", err)
	}
	return nil
}

// --- attempt tool-version pinning (ARCH-007) ---

// SnapshotAttemptTools resolves the attempt's immutable tool-version snapshot:
// available_tool_versions (healthy at snapshot time) ∩ pool_grants ∩ permitted,
// and pins each (attempt, tool) to its exact digest. Idempotent (ON CONFLICT
// DO NOTHING — a re-snapshot never mutates an existing pin). HOR-254 applies
// the same selection inside its larger atomic attempt-creation transaction;
// this method remains the gateway-owned helper used by chat/admin paths/tests.
//
//nolint:gocyclo // Snapshotting intentionally keeps resolution, eligibility, and atomic pinning in one fail-closed operation.
func (s *Store) SnapshotAttemptTools(ctx context.Context, attemptID, poolID string, permittedTools []string) error {
	if permittedTools != nil && len(permittedTools) == 0 {
		return nil // explicit workflow deny-all: pin nothing
	}
	unique := map[string]struct{}{}
	if permittedTools == nil {
		// Legacy chat has no workflow capability list: snapshot every currently
		// eligible pool tool, still choosing exactly one deterministic version.
		rows, err := s.pool.Query(ctx, `
			SELECT DISTINCT tv.name FROM toolgateway.available_tool_versions tv
			JOIN toolgateway.pool_grants pg ON pg.tool_name=tv.name AND pg.pool_id=$1 AND pg.deleted_at IS NULL
			WHERE CASE tv.effect_class WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END
			   <= CASE pg.max_effect_class WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END`, poolID)
		if err != nil {
			return fmt.Errorf("snapshot attempt tools: list eligible: %w", err)
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			unique[name] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	} else {
		for _, name := range permittedTools {
			unique[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("snapshot attempt tools: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, name := range names {
		var digest string
		err := tx.QueryRow(ctx, `
			SELECT tv.digest
			FROM toolgateway.available_tool_versions tv
			JOIN toolgateway.pool_grants pg
			  ON pg.tool_name=tv.name AND pg.deleted_at IS NULL AND pg.pool_id=$1
			WHERE tv.name=$2
			  AND CASE tv.effect_class WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END
			      <= CASE pg.max_effect_class WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END
			ORDER BY tv.created_at DESC, tv.id DESC LIMIT 1`, poolID, name).Scan(&digest)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("snapshot attempt tools: requested tool %q has no eligible healthy version", name)
		}
		if err != nil {
			return fmt.Errorf("snapshot attempt tools: resolve %q: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO toolgateway.attempt_tool_pins (attempt_id, tool_name, tool_version_digest)
			VALUES ($1,$2,$3) ON CONFLICT (attempt_id,tool_name) DO NOTHING`, attemptID, name, digest); err != nil {
			return fmt.Errorf("pin attempt tool %s: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("snapshot attempt tools: commit: %w", err)
	}
	return nil
}

// GetAttemptToolPin resolves the pinned digest for an attempt's tool. Absence =
// fail closed (the gateway never substitutes another version, ARCH-007).
func (s *Store) GetAttemptToolPin(ctx context.Context, attemptID, toolName string) (string, error) {
	var digest string
	err := s.pool.QueryRow(ctx, `
		SELECT tool_version_digest FROM toolgateway.attempt_tool_pins
		WHERE attempt_id = $1 AND tool_name = $2`, attemptID, toolName).Scan(&digest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return digest, nil
}

// DiscoverEffectiveTools returns the descriptors permitted for an attempt:
// attempt_tool_pins (the attempt's immutable snapshot) ∩ pool_grants ∩
// permittedTools (ARCH-006/007/016/018). A pinned version is returned even if
// it currently has no live runner — the child still gets the descriptor, and
// invocation fails tool_unavailable rather than substituting (ARCH-007).
//
// permittedTools semantics: nil = no workflow narrowing (turn path; all granted
// + pinned tools); len==0 = explicitly empty workflow set (deny all).
func (s *Store) DiscoverEffectiveTools(ctx context.Context, attemptID, poolID string, permitted []Capability) ([]ToolVersion, error) {
	// Names for the SQL name filter (nil = turn path: no workflow narrowing).
	var names []string
	if permitted != nil {
		names = []string{} // preserve explicit deny-all distinctly from nil
	}
	capByTool := make(map[string]Capability, len(permitted))
	for _, c := range permitted {
		names = append(names, c.Tool)
		capByTool[c.Tool] = c
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tv.id, tv.name, tv.version, tv.digest, tv.description, tv.input_schema,
		       tv.effect_class, tv.credential_slots, tv.artifact_capabilities,
		       tv.timeout_ms, tv.idempotency_proof, tv.consequence_summary_template, tv.created_at
		FROM toolgateway.tool_versions tv
		JOIN toolgateway.attempt_tool_pins pin
		  ON pin.tool_name = tv.name AND pin.tool_version_digest = tv.digest AND pin.attempt_id = $1
		JOIN toolgateway.pool_grants pg
		  ON pg.tool_name = tv.name AND pg.deleted_at IS NULL AND pg.pool_id = $2
		WHERE CASE tv.effect_class
		        WHEN 'read_only' THEN 1
		        WHEN 'idempotent_write' THEN 2
		        WHEN 'non_idempotent_write' THEN 3
		      END <= CASE pg.max_effect_class
		        WHEN 'read_only' THEN 1
		        WHEN 'idempotent_write' THEN 2
		        WHEN 'non_idempotent_write' THEN 3
		      END
		  AND ($3::text[] IS NULL OR tv.name = ANY($3::text[]))
		ORDER BY tv.name, tv.version`, attemptID, poolID, names)
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
		// Workflow-requested effect/action ceiling (ARCH-016): a workflow narrowed
		// to read_only or a subset of actions must not discover a broader tool even
		// when the pool grant ceiling allows it (REQ-001/REQ-010). nil permitted =
		// turn path. V1's undecomposed effective action is the tool name.
		if permitted != nil {
			cap, ok := capByTool[tv.Name]
			if !ok {
				continue
			}
			if cap.MaxEffectClass != "" && effectRank(tv.EffectClass) > effectRank(EffectClass(cap.MaxEffectClass)) {
				continue
			}
			if !capabilityAllowsAction(cap, tv) {
				continue
			}
		}
		out = append(out, tv)
	}
	return out, rows.Err()
}

// GetPoolGrant returns the pool grant for (pool, tool) or ErrNotFound. Used for
// action-specific authorization (ARCH-008/016/018).
func (s *Store) GetPoolGrant(ctx context.Context, poolID, toolName string) (PoolGrant, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, pool_id, tool_name, max_effect_class, allowed_actions
		FROM toolgateway.pool_grants
		WHERE pool_id = $1 AND tool_name = $2 AND deleted_at IS NULL`, poolID, toolName)
	var g PoolGrant
	var actions []byte
	if err := row.Scan(&g.ID, &g.PoolID, &g.ToolName, &g.MaxEffectClass, &actions); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PoolGrant{}, ErrNotFound
		}
		return PoolGrant{}, err
	}
	var acts []string
	if len(actions) > 0 {
		_ = json.Unmarshal(actions, &acts) // stored as JSONB array; tolerate shape
	}
	g.AllowedActions = acts
	return g, nil
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
// side-effect boundary, with a crash-recovery lease. On a unique-key conflict
// (duplicate caller) it returns the existing invocation with inserted=false so
// the caller can report in-progress or replay the committed result (ARCH-014).
func (s *Store) BeginInvocation(ctx context.Context, key InvocationKey, tv ToolVersion, poolID *string, args, artifactInputRefs, consequenceSummary []byte, leaseExpiresAt time.Time, gatewayInstanceID string) (inv Invocation, inserted bool, err error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO toolgateway.invocations
			(attempt_id, caller_scope, caller_scope_id, tool_call_id, tool_name,
			 tool_version_digest, idempotency_key, effect_class, pool_id, arguments_json,
			 artifact_input_refs, consequence_summary, state, dispatch_lease_expires_at, gateway_instance_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'dispatching', $13, $14)
		ON CONFLICT (attempt_id, caller_scope, caller_scope_id, tool_call_id, tool_version_digest, idempotency_key)
		DO NOTHING
		RETURNING id`,
		key.AttemptID, key.CallerScope, key.CallerScopeID, key.ToolCallID, tv.Name,
		key.ToolVersionDigest, key.IdempotencyKey, tv.EffectClass, poolID, args,
		artifactInputRefs, nullableJSONBytes(consequenceSummary), leaseExpiresAt, gatewayInstanceID)
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

// RenewDispatchLease extends the crash-recovery lease for an in-flight
// invocation (called on dispatch/running transitions). It returns
// ErrInvalidTransition when zero rows are affected — i.e. the invocation is
// no longer in a live dispatching/running state (the SCN-008 recovery sweep
// has already terminalized it). Callers MUST treat a non-nil result as "the
// safety lease was not renewed" and fail closed before the effect boundary
// (ARCH-014): never dispatch a retry whose lease could not be re-established.
func (s *Store) RenewDispatchLease(ctx context.Context, invocationID, gatewayInstanceID string, leaseExpiresAt time.Time) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.invocations
		SET dispatch_lease_expires_at = $2, gateway_instance_id = $3
		WHERE id = $1 AND state IN ('dispatching', 'running')`,
		invocationID, leaseExpiresAt, gatewayInstanceID)
	if err != nil {
		return fmt.Errorf("renew dispatch lease: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

// MarkRunning transitions dispatching -> running (dispatched over the runner
// stream), recording the executing runner and renewing the lease.
func (s *Store) MarkRunning(ctx context.Context, invocationID, runnerID string, leaseExpiresAt time.Time, gatewayInstanceID string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.invocations
		SET state = 'running', running_at = now(), runner_id = $2,
		    dispatch_lease_expires_at = $3, gateway_instance_id = $4
		WHERE id = $1 AND state = 'dispatching'`,
		invocationID, runnerID, leaseExpiresAt, gatewayInstanceID)
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
// refs/error (ARCH-014). Returns ErrInvalidTransition if the row is already
// terminal — callers MUST treat a non-nil error as "the terminal result was not
// committed" and NOT report success to the caller.
func (s *Store) FinishInvocation(ctx context.Context, invocationID string, state InvocationState, result, artifactRefs, errDetail []byte) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.invocations
		SET state = $2, result_json = $3, artifact_output_refs = $4, error = $5,
		    finished_at = now(), dispatch_lease_expires_at = NULL
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

// RecoverOrphanedInvocations is the crash-recovery sweep (SCN-008/ARCH-014).
// Non-terminal rows whose lease has expired are terminalized fail-closed:
// read_only -> failed (no effect possible; caller may retry), writes ->
// outcome_unknown (a possible effect with no committed result; never
// auto-repeated). Run once at gateway start and by a background ticker.
func (s *Store) RecoverOrphanedInvocations(ctx context.Context) (recovered int, err error) {
	// read_only -> failed.
	ct, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.invocations
		SET state = 'failed', finished_at = now(),
		    error = jsonb_build_object('code','outcome_unknown',
		                               'message','dispatch lease expired during recovery (read_only)',
		                               'retryability','retryable'),
		    dispatch_lease_expires_at = NULL
		WHERE state IN ('dispatching', 'running')
		  AND finished_at IS NULL
		  AND dispatch_lease_expires_at IS NOT NULL
		  AND dispatch_lease_expires_at < now()
		  AND effect_class = 'read_only'`)
	if err != nil {
		return 0, fmt.Errorf("recover read_only: %w", err)
	}
	recovered += int(ct.RowsAffected())
	// writes -> outcome_unknown.
	ct, err = s.pool.Exec(ctx, `
		UPDATE toolgateway.invocations
		SET state = 'outcome_unknown', finished_at = now(),
		    error = jsonb_build_object('code','outcome_unknown',
		                               'message','dispatch lease expired during recovery (possible effect, no committed result)',
		                               'retryability','unknown'),
		    dispatch_lease_expires_at = NULL
		WHERE state IN ('dispatching', 'running')
		  AND finished_at IS NULL
		  AND dispatch_lease_expires_at IS NOT NULL
		  AND dispatch_lease_expires_at < now()
		  AND effect_class IN ('idempotent_write', 'non_idempotent_write')`)
	if err != nil {
		return recovered, fmt.Errorf("recover writes: %w", err)
	}
	recovered += int(ct.RowsAffected())
	return recovered, nil
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

// InvocationOwnsCaller reports whether the ledger row belongs to the given
// pool (cancel-scope check, REQ-010). The invocation's pool_id must match.
func (s *Store) InvocationOwnsCaller(ctx context.Context, invocationID, poolID string) (bool, error) {
	var ok bool
	err := s.pool.QueryRow(ctx, `
		SELECT (pool_id = $2::uuid) FROM toolgateway.invocations WHERE id = $1`,
		invocationID, poolID).Scan(&ok)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, err
	}
	return ok, nil
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

// PoolGrantInput is a CR-sourced pool grant (HOR-245).
type PoolGrantInput struct {
	ToolName       string
	MaxEffect      EffectClass
	AllowedActions []string
}

// CredentialBindingInput is a CR-sourced credential binding (HOR-245).
// SecretSpec and ResourceConstraints are the JSONB stored in
// toolgateway.credential_bindings (the reconciler marshals them from the CRD's
// scheme-specific fields).
type CredentialBindingInput struct {
	ToolName            string
	SlotName            string
	Scheme              CredentialScheme
	SecretSpec          []byte // scheme-dependent secret_ref JSONB
	ResourceConstraints []byte // non-secret scope JSONB
}

// ReplacePoolGrants reconciles a pool's grants to the desired set (HOR-245):
// it soft-deletes the pool's current grants, then upserts each desired grant
// (reviving a soft-deleted row on conflict). Grants no longer in the desired
// set stay soft-deleted (stale-row removal). The gateway reads only live rows.
func (s *Store) ReplacePoolGrants(ctx context.Context, poolID string, grants []PoolGrantInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("replace pool grants: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.pool_grants SET deleted_at = now()
		WHERE pool_id = $1::uuid AND deleted_at IS NULL`, poolID); err != nil {
		return fmt.Errorf("replace pool grants: clear: %w", err)
	}
	for _, g := range grants {
		actions := g.AllowedActions
		if actions == nil {
			actions = []string{}
		}
		b, err := json.Marshal(actions)
		if err != nil {
			return fmt.Errorf("replace pool grants: marshal actions: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO toolgateway.pool_grants (pool_id, tool_name, max_effect_class, allowed_actions)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (pool_id, tool_name) DO UPDATE
				SET max_effect_class = EXCLUDED.max_effect_class,
				    allowed_actions = EXCLUDED.allowed_actions,
				    deleted_at = NULL, updated_at = now()`,
			poolID, g.ToolName, g.MaxEffect, b); err != nil {
			return fmt.Errorf("replace pool grants: upsert %s: %w", g.ToolName, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("replace pool grants: commit: %w", err)
	}
	return nil
}

// ReplaceCredentialBindings reconciles a pool's credential bindings to the
// desired set (HOR-245), with the same soft-delete + upsert-revive semantics as
// ReplacePoolGrants.
func (s *Store) ReplaceCredentialBindings(ctx context.Context, poolID string, bindings []CredentialBindingInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("replace credential bindings: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.credential_bindings SET deleted_at = now()
		WHERE pool_id = $1::uuid AND deleted_at IS NULL`, poolID); err != nil {
		return fmt.Errorf("replace credential bindings: clear: %w", err)
	}
	for _, b := range bindings {
		spec := b.SecretSpec
		if spec == nil {
			spec = []byte("{}")
		}
		rc := b.ResourceConstraints
		if rc == nil {
			rc = []byte("{}")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO toolgateway.credential_bindings
				(pool_id, tool_name, slot_name, scheme, secret_ref, resource_constraints)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (pool_id, tool_name, slot_name) DO UPDATE
				SET scheme = EXCLUDED.scheme, secret_ref = EXCLUDED.secret_ref,
				    resource_constraints = EXCLUDED.resource_constraints,
				    deleted_at = NULL, updated_at = now()`,
			poolID, b.ToolName, b.SlotName, b.Scheme, spec, rc); err != nil {
			return fmt.Errorf("replace credential bindings: upsert %s/%s: %w", b.ToolName, b.SlotName, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("replace credential bindings: commit: %w", err)
	}
	return nil
}

// SoftDeletePoolByKey soft-deletes a pool and its grants/bindings on AgentPool CR
// deletion (HOR-245). Access is revoked (rows retained for history/audit). A
// no-op if the pool is already soft-deleted or never existed.
func (s *Store) SoftDeletePoolByKey(ctx context.Context, key string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("soft-delete pool: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var poolID string
	err = tx.QueryRow(ctx, `
		UPDATE toolgateway.pools SET deleted_at = now()
		WHERE key = $1 AND deleted_at IS NULL RETURNING id`, key).Scan(&poolID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // already soft-deleted or never existed
		}
		return fmt.Errorf("soft-delete pool: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.pool_grants SET deleted_at = now()
		WHERE pool_id = $1::uuid AND deleted_at IS NULL`, poolID); err != nil {
		return fmt.Errorf("soft-delete pool grants: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.credential_bindings SET deleted_at = now()
		WHERE pool_id = $1::uuid AND deleted_at IS NULL`, poolID); err != nil {
		return fmt.Errorf("soft-delete credential bindings: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("soft-delete pool: commit: %w", err)
	}
	return nil
}

// MaterializePool atomically upserts a pool and replaces its grants +
// credential bindings in a single transaction (HOR-245 Git->DB bridge,
// ARCH-018/migration 000011). Atomicity guarantees the gateway never observes
// mixed old/new authorization for a generation: either all of a generation's
// writes commit or none do. Stale grants/bindings are soft-deleted within the
// same txn, and desired rows are upserted (reviving a soft-deleted row on
// conflict). Idempotent on re-run for the same generation. The caller records
// success (ObservedGeneration) only after this returns nil.
func (s *Store) MaterializePool(ctx context.Context, key, name, spiffePrefix string, grants []PoolGrantInput, bindings []CredentialBindingInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("materialize pool: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var poolID string
	if err := tx.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE
			SET name = EXCLUDED.name, spiffe_id_prefix = EXCLUDED.spiffe_id_prefix,
			    deleted_at = NULL, updated_at = now()
		RETURNING id`, key, name, spiffePrefix).Scan(&poolID); err != nil {
		return fmt.Errorf("materialize pool: upsert pool: %w", err)
	}

	// Replace grants (soft-delete stale + upsert-revive desired) within the txn.
	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.pool_grants SET deleted_at = now()
		WHERE pool_id = $1::uuid AND deleted_at IS NULL`, poolID); err != nil {
		return fmt.Errorf("materialize pool: clear grants: %w", err)
	}
	for _, g := range grants {
		actions := g.AllowedActions
		if actions == nil {
			actions = []string{}
		}
		b, err := json.Marshal(actions)
		if err != nil {
			return fmt.Errorf("materialize pool: marshal actions: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO toolgateway.pool_grants (pool_id, tool_name, max_effect_class, allowed_actions)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (pool_id, tool_name) DO UPDATE
				SET max_effect_class = EXCLUDED.max_effect_class,
			    allowed_actions = EXCLUDED.allowed_actions,
			    deleted_at = NULL, updated_at = now()`,
			poolID, g.ToolName, g.MaxEffect, b); err != nil {
			return fmt.Errorf("materialize pool: upsert grant %s: %w", g.ToolName, err)
		}
	}

	// Replace bindings (soft-delete stale + upsert-revive desired) within the txn.
	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.credential_bindings SET deleted_at = now()
		WHERE pool_id = $1::uuid AND deleted_at IS NULL`, poolID); err != nil {
		return fmt.Errorf("materialize pool: clear bindings: %w", err)
	}
	for _, b := range bindings {
		spec := b.SecretSpec
		if spec == nil {
			spec = []byte("{}")
		}
		rc := b.ResourceConstraints
		if rc == nil {
			rc = []byte("{}")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO toolgateway.credential_bindings
				(pool_id, tool_name, slot_name, scheme, secret_ref, resource_constraints)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (pool_id, tool_name, slot_name) DO UPDATE
				SET scheme = EXCLUDED.scheme, secret_ref = EXCLUDED.secret_ref,
			    resource_constraints = EXCLUDED.resource_constraints,
			    deleted_at = NULL, updated_at = now()`,
			poolID, b.ToolName, b.SlotName, b.Scheme, spec, rc); err != nil {
			return fmt.Errorf("materialize pool: upsert binding %s/%s: %w", b.ToolName, b.SlotName, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("materialize pool: commit: %w", err)
	}
	return nil
}

// UpsertWorkflowPoolBinding inserts/revives a workflow->pool binding
// (operator-seed; Workflow definitions HOR-252 later).
func (s *Store) UpsertWorkflowPoolBinding(ctx context.Context, workflowKey, poolID string, permitted []Capability) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO toolgateway.workflow_pool_bindings (workflow_definition_key, pool_id, permitted_tools)
		VALUES ($1, $2, $3)
		ON CONFLICT (workflow_definition_key) DO UPDATE
			SET pool_id = EXCLUDED.pool_id, permitted_tools = EXCLUDED.permitted_tools,
			    deleted_at = NULL, updated_at = now()`,
		workflowKey, poolID, permitted)
	if err != nil {
		return fmt.Errorf("upsert workflow pool binding: %w", err)
	}
	return nil
}

// GetPoolByKey fetches an active pool by its natural key ("<ns>/<name>").
// Used by the Workflow reconciler (HOR-252) to resolve the pool_id for a
// workflow_pool_binding from the referenced AgentPool's key.
func (s *Store) GetPoolByKey(ctx context.Context, key string) (Pool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, name, spiffe_id_prefix
		FROM toolgateway.pools WHERE key = $1 AND deleted_at IS NULL`, key)
	var p Pool
	if err := row.Scan(&p.ID, &p.Key, &p.Name, &p.SpiffeIDPrefix); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Pool{}, ErrNotFound
		}
		return Pool{}, err
	}
	return p, nil
}

// SoftDeleteWorkflowPoolBindingByKey soft-deletes the workflow_pool_binding
// for a definition key on Workflow CR deletion (HOR-252). Revokes the
// workflow's permitted-tool binding; the row is retained for history. A no-op
// if already soft-deleted or never existed.
func (s *Store) SoftDeleteWorkflowPoolBindingByKey(ctx context.Context, workflowDefinitionKey string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE toolgateway.workflow_pool_bindings SET deleted_at = now()
		WHERE workflow_definition_key = $1 AND deleted_at IS NULL`, workflowDefinitionKey)
	if err != nil {
		return fmt.Errorf("soft-delete workflow pool binding: %w", err)
	}
	return nil
}

// ReconcileStaticApprovedRunners makes gateway deployment configuration the
// source of truth for statically managed runner identities. Operator-managed
// approvals are deliberately untouched.
func (s *Store) ReconcileStaticApprovedRunners(ctx context.Context, configured []ApprovedRunner) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("reconcile static approved runners: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	ids := make([]string, 0, len(configured))
	for _, a := range configured {
		if a.SpiffeID == "" || a.Namespace == "" || a.RunnerID == "" || len(a.AllowedToolNamespaces) == 0 {
			return fmt.Errorf("static approved runner requires spiffe_id, namespace, runner_id, and at least one allowed namespace")
		}
		ids = append(ids, a.SpiffeID)
		if _, err := tx.Exec(ctx, `
			INSERT INTO toolgateway.approved_runners
				(namespace, runner_id, spiffe_id, allowed_tool_namespaces, managed_by)
			VALUES ($1,$2,$3,$4,'static_config')
			ON CONFLICT (spiffe_id) DO UPDATE
			SET namespace=EXCLUDED.namespace, runner_id=EXCLUDED.runner_id,
			    allowed_tool_namespaces=EXCLUDED.allowed_tool_namespaces,
			    managed_by='static_config', deleted_at=NULL, updated_at=now()`,
			a.Namespace, a.RunnerID, a.SpiffeID, a.AllowedToolNamespaces); err != nil {
			return fmt.Errorf("reconcile static approved runner %s: %w", a.SpiffeID, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE toolgateway.approved_runners SET deleted_at=now(), updated_at=now()
		WHERE managed_by='static_config' AND deleted_at IS NULL
		  AND NOT (spiffe_id = ANY($1::text[]))`, ids); err != nil {
		return fmt.Errorf("deactivate removed static approved runners: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("reconcile static approved runners: commit: %w", err)
	}
	return nil
}

// UpsertApprovedRunner inserts/revives an operator-managed runner approval.
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
	       arguments_json, artifact_input_refs, consequence_summary, state, result_json, artifact_output_refs, error,
	       dispatch_lease_expires_at, gateway_instance_id,
	       dispatching_at, running_at, finished_at, created_at, updated_at
	FROM toolgateway.invocations`

func scanToolVersion(row pgx.Row) (ToolVersion, error) {
	var tv ToolVersion
	err := row.Scan(&tv.ID, &tv.Name, &tv.Version, &tv.Digest, &tv.Description,
		&tv.InputSchema, &tv.EffectClass, &tv.CredentialSlots, &tv.ArtifactCapabs,
		&tv.TimeoutMS, &tv.IdempotencyProof, &tv.ConsequenceTemplate, &tv.CreatedAt)
	return tv, err
}

func scanRunnerRegistration(row pgx.Row) (RunnerRegistration, error) {
	var r RunnerRegistration
	err := row.Scan(&r.ID, &r.RunnerID, &r.SpiffeID, &r.Namespace, &r.ToolName,
		&r.ToolVersion, &r.ToolDigest, &r.FencingGeneration, &r.LastHeartbeatAt,
		&r.Active, &r.AcceptingNew, &r.RegisteredAt)
	return r, err
}

func scanInvocation(row pgx.Row) (Invocation, error) {
	var inv Invocation
	err := row.Scan(&inv.ID, &inv.AttemptID, &inv.CallerScope, &inv.CallerScopeID,
		&inv.ToolCallID, &inv.ToolName, &inv.ToolVersionDigest, &inv.IdempotencyKey,
		&inv.EffectClass, &inv.PoolID, &inv.RunnerID, &inv.ArgumentsJSON, &inv.ArtifactInputRefs, &inv.ConsequenceSummary, &inv.State,
		&inv.ResultJSON, &inv.ArtifactOutputRefs, &inv.Error,
		&inv.DispatchLeaseExpiresAt, &inv.GatewayInstanceID,
		&inv.DispatchingAt, &inv.RunningAt, &inv.FinishedAt, &inv.CreatedAt, &inv.UpdatedAt)
	return inv, err
}

// effectRank maps an effect class to its severity rank for grant ceiling
// comparison: read_only (1) < idempotent_write (2) < non_idempotent_write (3).
// A pool grant's max_effect_class is the ceiling; a tool whose effect_class
// exceeds it is denied (ARCH-016).
func nullableJSONBytes(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return v
}

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
