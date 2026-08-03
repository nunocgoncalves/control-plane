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

// TriggerBindingRow is a row from workflow.trigger_bindings: an exact typed,
// non-secret trigger route registration. Source-specific CRD validation maps
// the route to BindingKey; no opaque config/secret persistence path exists.
type TriggerBindingRow struct {
	ID           string
	DefinitionID string
	Name         string
	SourceType   string
	BindingKey   string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// TriggerBindingInput is the validated source-specific trigger route store
// shape (HOR-252). It deliberately has no opaque config field (ARCH-008).
type TriggerBindingInput struct {
	Name       string
	BindingKey string
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
	// Spec is the exact canonical workflow/config snapshot input. It includes
	// step tool semantics, scope identity key, versioned skill references,
	// source bindings, behavior, presentation, and capability narrowing. HOR-254
	// persists this with Definition's immutable key/version/digest at attempt
	// creation (REQ-003/REQ-011).
	Spec CanonicalSpec
	// Skills is the exact immutable skill identity set, surfaced directly for
	// attempt snapshotting in addition to its canonical presence in Spec.
	Skills []CanonicalSkill
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
// mirroring the gateway store's ReplacePoolGrants semantics. The input has no
// opaque config field, so trigger registration cannot persist raw credentials
// (ARCH-008). source_type is denormalized per row.
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
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow.trigger_bindings (definition_id, name, source_type, binding_key)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (definition_id, name) DO UPDATE
				SET source_type = EXCLUDED.source_type,
				    binding_key = EXCLUDED.binding_key,
				    deleted_at = NULL, updated_at = now()`,
			definitionID, b.Name, sourceType, b.BindingKey); err != nil {
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
		SELECT id, definition_id, name, source_type, binding_key, created_at, updated_at
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
		if err := rows.Scan(&b.ID, &b.DefinitionID, &b.Name, &b.SourceType, &b.BindingKey, &b.CreatedAt, &b.UpdatedAt); err != nil {
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
	// Decode the complete immutable workflow/config/skill snapshot contract. A
	// malformed spec_json is a data-layer invariant violation and fails closed;
	// attempt creation must never continue with missing execution semantics.
	var spec CanonicalSpec
	if err := json.Unmarshal(d.SpecJSON, &spec); err != nil {
		return ResolvedDefinition{}, fmt.Errorf("resolve canonical workflow spec: %w", err)
	}
	if err := validateCanonicalSpecForAttempt(d, spec); err != nil {
		return ResolvedDefinition{}, fmt.Errorf("resolve canonical workflow spec: %w", err)
	}
	return ResolvedDefinition{
		Definition: d, TriggerBindings: bindings, Spec: spec, Skills: spec.Skills,
		PermittedTools: permitted, RequestedCapabilities: spec.RequestedCapabilities,
	}, nil
}

// validateCanonicalSpecForAttempt checks the execution identity fields that
// attempt creation must never infer or silently omit. Postgres guarantees valid
// JSON syntax; this closes the semantic fail-open path for incomplete legacy or
// corrupted spec_json.
func validateCanonicalSpecForAttempt(d Definition, spec CanonicalSpec) error {
	if spec.Key == "" || spec.Key != d.Key {
		return fmt.Errorf("key must match definition key %q", d.Key)
	}
	if spec.ScopeIdentityKey == "" {
		return errors.New("scopeIdentityKey is required")
	}
	if spec.Source.Type == "" {
		return errors.New("source.type is required")
	}
	if len(spec.Steps) == 0 {
		return errors.New("steps must not be empty")
	}
	for i, step := range spec.Steps {
		if step.Name == "" || step.Kind == "" {
			return fmt.Errorf("steps[%d] name and kind are required", i)
		}
		if step.Kind == "tool_call" && step.Tool == "" {
			return fmt.Errorf("steps[%d].tool is required for tool_call", i)
		}
	}
	for i, skill := range spec.Skills {
		if skill.Name == "" || skill.Version == "" || skill.Digest == "" {
			return fmt.Errorf("skills[%d] requires name, version, and digest", i)
		}
	}
	return nil
}

// readPermittedTools reads the permitted tool names bound to a definition from
// toolgateway.workflow_pool_bindings (cross-schema read, mirroring the gateway
// store's cross-schema runtime reads). The binding stores full capability
// objects (tool + maxEffectClass + actions); only the tool names are returned
// here — the complete narrowing is available on ResolvedDefinition.
// RequestedCapabilities. Absent binding = ErrNotFound (the workflow must be
// bound to a pool before runs start).
func (s *Store) readPermittedTools(ctx context.Context, definitionKey string) ([]string, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT permitted_tools FROM toolgateway.workflow_pool_bindings
		WHERE workflow_definition_key = $1 AND deleted_at IS NULL`, definitionKey).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var caps []CanonicalCapability
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &caps); err != nil {
			return nil, fmt.Errorf("decode workflow permitted capabilities: %w", err)
		}
	}
	names := make([]string, 0, len(caps))
	for _, c := range caps {
		names = append(names, c.Tool)
	}
	return names, nil
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
	ScopeIdentityKey      string                `json:"scopeIdentityKey"`
	Source                CanonicalSource       `json:"source"`
	Skills                []CanonicalSkill      `json:"skills,omitempty"`
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
	Name       string `json:"name"`
	BindingKey string `json:"bindingKey"`
}

// CanonicalSkill is one exact immutable overlay skill identity.
type CanonicalSkill struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

// CanonicalStep is one workflow step. Tool is included in the immutable digest
// because it defines tool_call execution semantics.
type CanonicalStep struct {
	Name   string          `json:"name"`
	Kind   string          `json:"kind"`
	Tool   string          `json:"tool,omitempty"`
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
