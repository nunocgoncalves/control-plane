// Package workflow implements the control-plane workflow definition store
// (HOR-252): the Postgres-backed registry of operator-defined, versioned
// customer workflows and their non-secret trigger bindings.
//
// A definition is immutable per version (ARCH-007): (key, version) is the
// unique immutable identity. Publishing a content change creates a new row
// under a new version; the same (key, version) with different content is
// rejected. Re-registering the same (key, version, digest) is idempotent. Every
// version of a logical key retains one durable scope-identity owner; a different
// Workflow CR cannot publish another version under that key. Two versions may
// share a content digest and remain independently resolvable by their version
// identity. The definition_key wire format "<key>:<version>"
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
	// ErrDefinitionOwnership is returned when a different Workflow scope
	// identity already owns any version of the same logical definition key.
	ErrDefinitionOwnership = errors.New("workflow: logical definition key is owned by another workflow")
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
	// graph semantics, scope identity key, versioned skill references,
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
// acceptance: "immutable version identity"). All historical and active
// versions of one logical key must retain the same durable scope-identity owner
// so unversioned resolution cannot cross customer/workflow identities
// (REQ-010). validation_status is inspectable.
//
// Registration serializes on the logical key. This makes the owner check and
// insert atomic even when two Workflow CRs concurrently publish the first
// versions of the same key. On conflict, a soft-deleted row is revived only
// when its durable owner and digest match; otherwise the canonical row is read
// so ownership and immutability violations are surfaced as typed errors rather
// than raw SQL unique violations.
func (s *Store) RegisterDefinition(ctx context.Context, d Definition) (Definition, error) {
	if err := prepareDefinition(&d); err != nil {
		return Definition{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Definition{}, fmt.Errorf("register definition: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockAndCheckDefinitionOwner(ctx, tx, d); err != nil {
		return Definition{}, err
	}
	if err := insertDefinition(ctx, tx, d); err != nil {
		return Definition{}, err
	}
	byVersion, err := readRegisteredDefinition(ctx, tx, d)
	if err != nil {
		return Definition{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Definition{}, fmt.Errorf("register definition: commit: %w", err)
	}
	return byVersion, nil
}

func prepareDefinition(d *Definition) error {
	if d.Key == "" || d.Version == "" || d.Digest == "" {
		return fmt.Errorf("workflow: key, version, and digest are required")
	}
	if d.ScopeIdentityID == "" {
		return fmt.Errorf("workflow: scope_identity_id is required")
	}
	if d.SpecJSON == nil {
		d.SpecJSON = []byte("{}")
	}
	if d.Presentation == nil {
		d.Presentation = []byte("{}")
	}
	return nil
}

func lockAndCheckDefinitionOwner(ctx context.Context, tx pgx.Tx, d Definition) error {
	// The transaction-scoped advisory lock closes the first-publish race: only
	// one owner can establish a logical key, even when different versions are
	// registered concurrently. Hash collisions only serialize unrelated keys.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, d.Key); err != nil {
		return fmt.Errorf("register definition: lock logical key: %w", err)
	}
	var ownerID string
	err := tx.QueryRow(ctx, `
		SELECT scope_identity_id
		FROM workflow.definitions
		WHERE key = $1
		ORDER BY created_at, id
		LIMIT 1`, d.Key).Scan(&ownerID)
	if err == nil && ownerID != d.ScopeIdentityID {
		return fmt.Errorf("%w: key %s belongs to scope identity %s, not %s",
			ErrDefinitionOwnership, d.Key, ownerID, d.ScopeIdentityID)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("register definition: read logical key owner: %w", err)
	}
	return nil
}

func insertDefinition(ctx context.Context, tx pgx.Tx, d Definition) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO workflow.definitions
			(key, version, digest, spec_json, validation_status, scope_identity_id, source_type, pool_key, presentation)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (key, version) DO UPDATE
			SET deleted_at = NULL
			WHERE workflow.definitions.scope_identity_id = EXCLUDED.scope_identity_id
			  AND workflow.definitions.digest = EXCLUDED.digest
			  AND workflow.definitions.deleted_at IS NOT NULL`,
		d.Key, d.Version, d.Digest, d.SpecJSON, d.ValidationStatus, d.ScopeIdentityID,
		d.SourceType, d.PoolKey, d.Presentation); err != nil {
		return fmt.Errorf("register definition: insert: %w", err)
	}
	return nil
}

func readRegisteredDefinition(ctx context.Context, tx pgx.Tx, d Definition) (Definition, error) {
	byVersion, err := scanDefinition(tx.QueryRow(ctx, `
		SELECT id, key, version, digest, spec_json, validation_status, scope_identity_id,
		       source_type, pool_key, presentation, created_at, updated_at
		FROM workflow.definitions
		WHERE key = $1 AND version = $2`, d.Key, d.Version))
	if errors.Is(err, pgx.ErrNoRows) {
		return Definition{}, fmt.Errorf("register definition: read canonical: %w", ErrNotFound)
	}
	if err != nil {
		return Definition{}, fmt.Errorf("register definition: read canonical: %w", err)
	}
	if byVersion.ScopeIdentityID != d.ScopeIdentityID {
		return Definition{}, fmt.Errorf("%w: key %s belongs to scope identity %s, not %s",
			ErrDefinitionOwnership, d.Key, byVersion.ScopeIdentityID, d.ScopeIdentityID)
	}
	if byVersion.Digest != d.Digest {
		return Definition{}, fmt.Errorf("%w: key %s version %s already registered with digest %s (ARCH-007)",
			ErrImmutableVersion, d.Key, d.Version, byVersion.Digest)
	}
	return byVersion, nil
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
		ORDER BY created_at DESC, id DESC LIMIT 1`, key)
	d, err := scanDefinition(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Definition{}, ErrNotFound
	}
	return d, err
}

// ListDefinitionsByKey returns every active definition row for a logical key
// across all published versions. Every row has the same durable owner, but
// Workflow finalizer cleanup uses ListDefinitionsByOwner because one CR may
// have published definitions under previous spec.key values.
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

// ListDefinitionsByOwner returns every active definition materialized by the
// Workflow CR identified by its stable scope identity key. Unlike spec.key,
// this owner key is derived from metadata namespace/name and survives spec
// changes, so finalizer cleanup can enumerate every definition the CR created.
func (s *Store) ListDefinitionsByOwner(ctx context.Context, scopeIdentityKey string) ([]Definition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.key, d.version, d.digest, d.spec_json, d.validation_status, d.scope_identity_id,
		       d.source_type, d.pool_key, d.presentation, d.created_at, d.updated_at
		FROM workflow.definitions d
		JOIN identity.identities i ON i.id = d.scope_identity_id
		WHERE i.key = $1 AND d.deleted_at IS NULL
		ORDER BY d.key, d.version`, scopeIdentityKey)
	if err != nil {
		return nil, fmt.Errorf("list definitions by owner: %w", err)
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

// SoftDeleteDefinitionByKey soft-deletes all versions of a logical workflow key
// and their trigger bindings. This is an administrative key-level operation;
// Workflow finalizers use SoftDeleteDefinitionsByOwner so one CR cannot sweep
// another CR's version. A no-op if the key is already deleted or never existed.
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

// SoftDeleteDefinitionsByOwner soft-deletes every definition and trigger
// binding materialized by one Workflow CR, regardless of later spec.key
// changes. The stable owner identity is the cleanup boundary (REQ-010).
func (s *Store) SoftDeleteDefinitionsByOwner(ctx context.Context, scopeIdentityKey string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("soft-delete definitions by owner: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE workflow.trigger_bindings SET deleted_at = now()
		WHERE definition_id IN (
			SELECT d.id FROM workflow.definitions d
			JOIN identity.identities i ON i.id = d.scope_identity_id
			WHERE i.key = $1
		) AND deleted_at IS NULL`, scopeIdentityKey); err != nil {
		return fmt.Errorf("soft-delete trigger bindings by owner: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workflow.definitions d SET deleted_at = now()
		FROM identity.identities i
		WHERE d.scope_identity_id = i.id AND i.key = $1 AND d.deleted_at IS NULL`, scopeIdentityKey); err != nil {
		return fmt.Errorf("soft-delete definitions by owner: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("soft-delete definitions by owner: commit: %w", err)
	}
	return nil
}

// ResolveForAttempt resolves the exact versioned definition for an attempt
// (HOR-252 scope): the definition, its trigger bindings, the scope identity,
// and the permitted gateway tools bound to this definition (read cross-schema
// from toolgateway.workflow_pool_bindings). HOR-254's attempt creation
// composes this atomically with exact latest-healthy tool-version pins. The
// workflow store does NOT create attempts.
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

// validateCanonicalSpecForAttempt checks the immutable execution fields that
// attempt creation must never infer or silently omit. Graph validation is
// repeated here even though the reconciler validates before registration: a
// corrupted or pre-graph spec_json must fail closed.
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
	for i, skill := range spec.Skills {
		if skill.Name == "" || skill.Version == "" || skill.Digest == "" {
			return fmt.Errorf("skills[%d] requires name, version, and digest", i)
		}
	}
	if err := ValidateGraph(spec); err != nil {
		return err
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

// CanonicalSpec is the deterministic immutable workflow snapshot. Version is
// excluded because it is the external immutable identity component.
type CanonicalSpec struct {
	Key                   string                `json:"key"`
	ScopeIdentityKey      string                `json:"scopeIdentityKey"`
	Source                CanonicalSource       `json:"source"`
	Skills                []CanonicalSkill      `json:"skills,omitempty"`
	RequestedCapabilities []CanonicalCapability `json:"requestedCapabilities,omitempty"`
	DefaultModelRef       string                `json:"defaultModelRef,omitempty"`
	Graph                 CanonicalGraph        `json:"graph"`
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

// CanonicalGraph is the single-active-node executable graph (ARCH-019).
type CanonicalGraph struct {
	EntryNode        string                     `json:"entryNode"`
	MaxTransitions   int32                      `json:"maxTransitions"`
	Nodes            []CanonicalNode            `json:"nodes"`
	Edges            []CanonicalEdge            `json:"edges,omitempty"`
	TerminalOutcomes []CanonicalTerminalOutcome `json:"terminalOutcomes"`
}

// CanonicalNode is an immutable agent task or human gate definition.
type CanonicalNode struct {
	Key            string                 `json:"key"`
	Label          CanonicalLocalizedText `json:"label"`
	Kind           string                 `json:"kind"`
	Prompt         string                 `json:"prompt,omitempty"`
	ModelRef       string                 `json:"modelRef,omitempty"`
	Skills         []string               `json:"skills,omitempty"`
	Capabilities   []string               `json:"capabilities,omitempty"`
	WorkspaceTools bool                   `json:"workspaceTools,omitempty"`
	Timeout        string                 `json:"timeout,omitempty"`
	Outcomes       []string               `json:"outcomes"`
	OutputSchema   json.RawMessage        `json:"outputSchema,omitempty"`
	HumanGate      *CanonicalHumanGate    `json:"humanGate,omitempty"`
}

// CanonicalHumanGate is the customer-actionable request contract.
type CanonicalHumanGate struct {
	Type           string                 `json:"type"`
	Title          CanonicalLocalizedText `json:"title"`
	Description    CanonicalLocalizedText `json:"description"`
	ResponseSchema json.RawMessage        `json:"responseSchema,omitempty"`
}

// CanonicalLocalizedText is business copy in approved v1 locales.
type CanonicalLocalizedText struct {
	EN string `json:"en,omitempty"`
	PT string `json:"pt,omitempty"`
}

// CanonicalEdge maps one declared outcome to one next node.
type CanonicalEdge struct {
	From    string `json:"from"`
	Outcome string `json:"outcome"`
	To      string `json:"to"`
}

// CanonicalTerminalOutcome completes an attempt as Done.
type CanonicalTerminalOutcome struct {
	Node    string `json:"node"`
	Outcome string `json:"outcome"`
}

// CanonicalCapability is one requested gateway capability.
type CanonicalCapability struct {
	Tool           string   `json:"tool"`
	MaxEffectClass string   `json:"maxEffectClass"`
	Actions        []string `json:"actions,omitempty"`
}

// CanonicalPresentation is the customer-facing labels + persona.
type CanonicalPresentation struct {
	WorkflowTitle string `json:"workflowTitle"`
	PersonaName   string `json:"personaName"`
	PersonaAvatar string `json:"personaAvatar,omitempty"`
	Locale        string `json:"locale,omitempty"`
}
