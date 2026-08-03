// Package workflow implements the control-plane workflow definition store
// (HOR-252): the Postgres-backed registry of operator-defined, versioned
// customer workflows and their non-secret trigger bindings.
//
// A definition is immutable per version (ARCH-007): (key, version) is the
// unique immutable identity. Publishing a content change creates a new row
// under a new version; the same (key, version) with different content is
// rejected. Re-registering the same (key, version, digest) is idempotent. Two
// versions may share a content digest and remain independently resolvable by
// their version identity. The definition_key wire format "<key>:<version>"
// (key/version exclude ":") is the stable cross-schema reference stored in
// runtime.workflow_runs.definition_key (HOR-246) and
// toolgateway.workflow_pool_bindings.workflow_definition_key (HOR-392).
//
// Trigger bindings carry ONLY non-secret routing identifiers; customer secret
// values are never stored — they resolve through the AgentPool's
// credentialBindings via the gateway (ARCH-008).
//
// Mirrors the identity (HOR-242) / permissions (HOR-243) / catalog (HOR-306) /
// runtime (HOR-246) / toolgateway (HOR-392) stores: pgxpool store + ErrNotFound,
// soft-delete on operator-owned config tables, set_updated_at triggers.
package workflow

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
	// ErrNotFound is returned when no active definition/binding matches.
	ErrNotFound = errors.New("workflow: not found")
	// ErrImmutableVersion is returned when a content change is published under
	// an already-registered (key, version) (ARCH-007 immutability).
	ErrImmutableVersion = errors.New("workflow: immutable version already registered with different content")
)

// ValidationStatus values (mirror api/v1alpha1).
const (
	ValidationValid   = "valid"
	ValidationInvalid = "invalid"
)

// Definition is a row from workflow.definitions: an immutable versioned
// workflow definition (ARCH-007/REQ-001).
type Definition struct {
	ID               string
	Key              string
	Version          string
	Digest           string
	SpecJSON         []byte // canonical materialized spec
	ValidationStatus string
	ScopeIdentityID  string
	SourceType       string
	PoolKey          string
	Presentation     []byte // JSONB customer-facing labels + persona
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// TriggerBindingRow is a row from workflow.trigger_bindings: a non-secret
// trigger route registration.
type TriggerBindingRow struct {
	ID           string
	DefinitionID string
	Name         string
	SourceType   string
	BindingKey   string
	Config       []byte // JSONB non-secret config
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// TriggerBindingInput is a CR-sourced trigger binding (HOR-252). Config is the
// non-secret JSONB stored in workflow.trigger_bindings.config.
type TriggerBindingInput struct {
	Name       string
	BindingKey string
	Config     []byte
}

// ResolvedDefinition is the exact versioned definition resolved for an attempt
// (HOR-252 scope: "resolve and snapshot exact workflow/skill/config plus
// permitted gateway-tool versions at attempt creation"). It carries the
// definition, its trigger bindings, the scope identity, and the permitted
// gateway tools bound to this definition (from
// toolgateway.workflow_pool_bindings). HOR-254's attempt creation composes this
// with the gateway's SnapshotAttemptTools (which pins exact tool-version
// digests). The workflow store does NOT create attempts — that is HOR-254.
type ResolvedDefinition struct {
	Definition      Definition
	TriggerBindings []TriggerBindingRow
	// PermittedTools are the gateway tool names the workflow is bound to (the
	// validated requested capabilities). nil = no narrowing is not valid for the
	// workflow path; the binding always carries an explicit set (empty = deny
	// all). Read from toolgateway.workflow_pool_bindings (cross-schema).
	PermittedTools []string
	// RequestedCapabilities is the complete workflow-requested capability
	// narrowing (tool + maxEffectClass + actions) parsed from the immutable
	// definition's spec_json. HOR-254's attempt creation enforces this narrowing
	// at the gateway discovery/authorization boundary (ARCH-016: the gateway
	// intersects pool grants with workflow-requested capabilities) so a workflow
	// narrowed to read_only / a subset of actions is not widened back to the pool
	// ceiling at runtime (REQ-001/REQ-010). The tool names in
	// RequestedCapabilities correspond to PermittedTools.
	RequestedCapabilities []CanonicalCapability
}

// Store reads and writes the workflow schema via a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool for workflow operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// DefinitionKey returns the stable cross-schema definition_key wire format
// "<key>:<version>", stored in runtime.workflow_runs.definition_key and
// toolgateway.workflow_pool_bindings.workflow_definition_key.
func DefinitionKey(key, version string) string {
	return key + ":" + version
}

// RegisterDefinition inserts an immutable versioned definition. It is
// idempotent on (key, version, digest): re-registering the same content under
// the same (key, version) returns the canonical row without mutation
// (ARCH-007). A different digest for the same (key, version) is rejected with
// ErrImmutableVersion. A new version is always registered as a distinct
// immutable identity, even when its content digest matches another version's,
// so every published (key, version) is independently resolvable (REQ-001
// acceptance: "immutable version identity"). validation_status is inspectable.
//
// ON CONFLICT DO NOTHING suppresses the (key, version) unique constraint; the
// outcome is then determined by reading the canonical row so an immutability
// violation is surfaced as a typed error rather than a raw SQL unique
// violation.
func (s *Store) RegisterDefinition(ctx context.Context, d Definition) (Definition, error) {
	if d.Key == "" || d.Version == "" || d.Digest == "" {
		return Definition{}, fmt.Errorf("workflow: key, version, and digest are required")
	}
	if d.ScopeIdentityID == "" {
		return Definition{}, fmt.Errorf("workflow: scope_identity_id is required")
	}
	if d.SpecJSON == nil {
		d.SpecJSON = []byte("{}")
	}
	if d.Presentation == nil {
		d.Presentation = []byte("{}")
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO workflow.definitions
			(key, version, digest, spec_json, validation_status, scope_identity_id, source_type, pool_key, presentation)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT DO NOTHING`,
		d.Key, d.Version, d.Digest, d.SpecJSON, d.ValidationStatus, d.ScopeIdentityID,
		d.SourceType, d.PoolKey, d.Presentation); err != nil {
		return Definition{}, fmt.Errorf("register definition: %w", err)
	}
	// Same (key, version) exists?
	byVersion, err := s.GetDefinition(ctx, d.Key, d.Version)
	if err == nil {
		if byVersion.Digest == d.Digest {
			return byVersion, nil // idempotent re-registration of the same version
		}
		return Definition{}, fmt.Errorf("%w: key %s version %s already registered with digest %s (ARCH-007)",
			ErrImmutableVersion, d.Key, d.Version, byVersion.Digest)
	}
	if !errors.Is(err, ErrNotFound) {
		return Definition{}, fmt.Errorf("register definition: read canonical: %w", err)
	}
	// (key, version) not present: the insert succeeded and registered a new
	// immutable version identity (distinct from any same-content version).
	// Re-read the canonical row to return the assigned id/timestamps.
	inserted, err := s.GetDefinition(ctx, d.Key, d.Version)
	if err != nil {
		return Definition{}, fmt.Errorf("register definition: read inserted: %w", err)
	}
	return inserted, nil
}

// GetDefinition fetches an active definition by (key, version).
func (s *Store) GetDefinition(ctx context.Context, key, version string) (Definition, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, version, digest, spec_json, validation_status, scope_identity_id,
		       source_type, pool_key, presentation, created_at, updated_at
		FROM workflow.definitions
		WHERE key = $1 AND version = $2 AND deleted_at IS NULL`, key, version)
	d, err := scanDefinition(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Definition{}, ErrNotFound
	}
	return d, err
}

// GetLatestDefinition fetches the most recently created active definition for a
// key (the version new runs resolve by default when no version is pinned).
func (s *Store) GetLatestDefinition(ctx context.Context, key string) (Definition, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, version, digest, spec_json, validation_status, scope_identity_id,
		       source_type, pool_key, presentation, created_at, updated_at
		FROM workflow.definitions
		WHERE key = $1 AND deleted_at IS NULL
		ORDER BY created_at DESC LIMIT 1`, key)
	d, err := scanDefinition(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Definition{}, ErrNotFound
	}
	return d, err
}

// ListDefinitionsByKey returns every active definition row for a key (all
// published versions). Used by CR-deletion cleanup to revoke the
// workflow_pool_binding for every owned version before finalizer removal
// (REQ-010: no usable gateway authorization may outlive the Workflow CR).
func (s *Store) ListDefinitionsByKey(ctx context.Context, key string) ([]Definition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, key, version, digest, spec_json, validation_status, scope_identity_id,
		       source_type, pool_key, presentation, created_at, updated_at
		FROM workflow.definitions
		WHERE key = $1 AND deleted_at IS NULL
		ORDER BY version`, key)
	if err != nil {
		return nil, fmt.Errorf("list definitions by key: %w", err)
	}
	defer rows.Close()
	var out []Definition
	for rows.Next() {
		d, err := scanDefinition(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ReplaceTriggerBindings atomically replaces a definition's trigger bindings
// with the given set (soft-delete stale + upsert-revive desired within one tx),
// mirroring the gateway store's ReplacePoolGrants semantics. Each binding's
// config is non-secret JSONB (ARCH-008). source_type is denormalized per row.
func (s *Store) ReplaceTriggerBindings(ctx context.Context, definitionID, sourceType string, bindings []TriggerBindingInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("replace trigger bindings: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE workflow.trigger_bindings SET deleted_at = now()
		WHERE definition_id = $1::uuid AND deleted_at IS NULL`, definitionID); err != nil {
		return fmt.Errorf("replace trigger bindings: clear: %w", err)
	}
	seen := make(map[string]bool)
	for _, b := range bindings {
		if b.Name == "" {
			return fmt.Errorf("replace trigger bindings: binding name is required")
		}
		if b.BindingKey == "" {
			return fmt.Errorf("replace trigger bindings: binding %q bindingKey is required", b.Name)
		}
		if seen[b.Name] {
			return fmt.Errorf("replace trigger bindings: binding name %q is duplicated", b.Name)
		}
		seen[b.Name] = true
		cfg := b.Config
		if cfg == nil {
			cfg = []byte("{}")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow.trigger_bindings (definition_id, name, source_type, binding_key, config)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (definition_id, name) DO UPDATE
				SET source_type = EXCLUDED.source_type,
				    binding_key = EXCLUDED.binding_key,
				    config = EXCLUDED.config,
				    deleted_at = NULL, updated_at = now()`,
			definitionID, b.Name, sourceType, b.BindingKey, cfg); err != nil {
			return fmt.Errorf("replace trigger bindings: upsert %s: %w", b.Name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("replace trigger bindings: commit: %w", err)
	}
	return nil
}

// ListTriggerBindings returns a definition's active trigger bindings.
func (s *Store) ListTriggerBindings(ctx context.Context, definitionID string) ([]TriggerBindingRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, definition_id, name, source_type, binding_key, config, created_at, updated_at
		FROM workflow.trigger_bindings
		WHERE definition_id = $1::uuid AND deleted_at IS NULL
		ORDER BY name`, definitionID)
	if err != nil {
		return nil, fmt.Errorf("list trigger bindings: %w", err)
	}
	defer rows.Close()
	var out []TriggerBindingRow
	for rows.Next() {
		var b TriggerBindingRow
		if err := rows.Scan(&b.ID, &b.DefinitionID, &b.Name, &b.SourceType, &b.BindingKey, &b.Config, &b.CreatedAt, &b.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// SoftDeleteDefinitionByKey soft-deletes all versions of a workflow key and
// their trigger bindings on Workflow CR deletion (access revoked, rows retained
// for history/audit). The workflow_pool_binding and scope identity are revoked
// by the reconciler via their owning stores. A no-op if the key is already
// soft-deleted or never existed.
func (s *Store) SoftDeleteDefinitionByKey(ctx context.Context, key string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("soft-delete definition: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE workflow.trigger_bindings SET deleted_at = now()
		WHERE definition_id IN (SELECT id FROM workflow.definitions WHERE key = $1)
		  AND deleted_at IS NULL`, key); err != nil {
		return fmt.Errorf("soft-delete trigger bindings: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflow.definitions SET deleted_at = now()
		WHERE key = $1 AND deleted_at IS NULL`, key); err != nil {
		return fmt.Errorf("soft-delete definitions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("soft-delete definition: commit: %w", err)
	}
	return nil
}

// ResolveForAttempt resolves the exact versioned definition for an attempt
// (HOR-252 scope): the definition, its trigger bindings, the scope identity,
// and the permitted gateway tools bound to this definition (read cross-schema
// from toolgateway.workflow_pool_bindings). HOR-254's attempt creation
// composes this with the gateway's SnapshotAttemptTools (which pins exact
// tool-version digests). The workflow store does NOT create attempts.
//
// When version is empty, the latest active definition for the key is resolved
// (the default a new run binds to). PermittedTools is the explicit set from the
// workflow_pool_binding (nil is normalized to empty = deny all for the workflow
// path; the binding always carries an explicit set).
func (s *Store) ResolveForAttempt(ctx context.Context, key, version string) (ResolvedDefinition, error) {
	var d Definition
	var err error
	if version == "" {
		d, err = s.GetLatestDefinition(ctx, key)
	} else {
		d, err = s.GetDefinition(ctx, key, version)
	}
	if err != nil {
		return ResolvedDefinition{}, err
	}
	if d.ValidationStatus == ValidationInvalid {
		return ResolvedDefinition{}, fmt.Errorf("workflow: definition %s:%s is invalid and cannot start an attempt", d.Key, d.Version)
	}
	bindings, err := s.ListTriggerBindings(ctx, d.ID)
	if err != nil {
		return ResolvedDefinition{}, fmt.Errorf("resolve trigger bindings: %w", err)
	}
	permitted, err := s.readPermittedTools(ctx, DefinitionKey(d.Key, d.Version))
	if err != nil {
		return ResolvedDefinition{}, fmt.Errorf("resolve permitted tools: %w", err)
	}
	if permitted == nil {
		permitted = []string{}
	}
	// Parse the complete workflow-requested capability narrowing (tool +
	// maxEffectClass + actions) from the immutable definition's spec_json so
	// HOR-254 can enforce it at the gateway boundary (ARCH-016) without widening
	// back to the pool ceiling (REQ-001/REQ-010). A malformed spec_json is a
	// data-layer invariant violation, not a runtime authorization decision.
	var spec CanonicalSpec
	var caps []CanonicalCapability
	if err := json.Unmarshal(d.SpecJSON, &spec); err == nil {
		caps = spec.RequestedCapabilities
	}
	return ResolvedDefinition{Definition: d, TriggerBindings: bindings, PermittedTools: permitted, RequestedCapabilities: caps}, nil
}

// readPermittedTools reads the permitted tool set bound to a definition from
// toolgateway.workflow_pool_bindings (cross-schema read, mirroring the gateway
// store's cross-schema runtime reads). Absent binding = ErrNotFound (the
// workflow must be bound to a pool before runs start).
func (s *Store) readPermittedTools(ctx context.Context, definitionKey string) ([]string, error) {
	var permitted []string
	err := s.pool.QueryRow(ctx, `
		SELECT permitted_tools FROM toolgateway.workflow_pool_bindings
		WHERE workflow_definition_key = $1 AND deleted_at IS NULL`, definitionKey).Scan(&permitted)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return permitted, nil
}

// scanDefinition scans a definition row.
func scanDefinition(row pgx.Row) (Definition, error) {
	var d Definition
	err := row.Scan(&d.ID, &d.Key, &d.Version, &d.Digest, &d.SpecJSON, &d.ValidationStatus,
		&d.ScopeIdentityID, &d.SourceType, &d.PoolKey, &d.Presentation, &d.CreatedAt, &d.UpdatedAt)
	return d, err
}

// CanonicalSpec is the deterministic shape marshaled to spec_json and hashed to
// produce the immutable version digest. It captures the workflow definition
// fields that define execution + presentation behavior (everything except the
// version string itself, which is the version identity component). The
// reconciler builds this from the CR spec.
type CanonicalSpec struct {
	Key                   string                `json:"key"`
	Source                CanonicalSource       `json:"source"`
	Steps                 []CanonicalStep       `json:"steps"`
	RequestedCapabilities []CanonicalCapability `json:"requestedCapabilities,omitempty"`
	CompletionRule        CanonicalCompletion   `json:"completionRule"`
	Blocker               *CanonicalBlocker     `json:"blocker,omitempty"`
	ValueModelRef         string                `json:"valueModelRef,omitempty"`
	Presentation          CanonicalPresentation `json:"presentation"`
	PoolRef               string                `json:"poolRef"`
}

// CanonicalSource is the source adapter + trigger bindings.
type CanonicalSource struct {
	Type            string             `json:"type"`
	TriggerBindings []CanonicalTrigger `json:"triggerBindings,omitempty"`
}

// CanonicalTrigger is a non-secret trigger binding.
type CanonicalTrigger struct {
	Name       string          `json:"name"`
	BindingKey string          `json:"bindingKey"`
	Config     json.RawMessage `json:"config,omitempty"`
}

// CanonicalStep is one workflow step.
type CanonicalStep struct {
	Name   string          `json:"name"`
	Kind   string          `json:"kind"`
	Config json.RawMessage `json:"config,omitempty"`
}

// CanonicalCapability is one requested gateway capability.
type CanonicalCapability struct {
	Tool           string   `json:"tool"`
	MaxEffectClass string   `json:"maxEffectClass"`
	Actions        []string `json:"actions,omitempty"`
}

// CanonicalCompletion is the completion rule.
type CanonicalCompletion struct {
	Type string `json:"type"`
	Ref  string `json:"ref,omitempty"`
}

// CanonicalBlocker is the blocker behavior.
type CanonicalBlocker struct {
	Step     string `json:"step"`
	Behavior string `json:"behavior"`
}

// CanonicalPresentation is the customer-facing labels + persona.
type CanonicalPresentation struct {
	WorkflowTitle string `json:"workflowTitle"`
	PersonaName   string `json:"personaName"`
	PersonaAvatar string `json:"personaAvatar,omitempty"`
	Locale        string `json:"locale,omitempty"`
}
