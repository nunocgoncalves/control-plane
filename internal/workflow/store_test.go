package workflow_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/testutil"
	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

func newTestStore(t *testing.T) (*workflow.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.NewPostgresPool(t)
	return workflow.NewStore(pool), pool
}

func insertWorkflowIdentity(t *testing.T, pool *pgxpool.Pool, key string) string {
	t.Helper()
	var id string
	require.NoError(t, pool.QueryRow(context.Background(), `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ($1, 'workflow', 'local', $1) RETURNING id`, key).Scan(&id))
	return id
}

// insertPool inserts a toolgateway.pools row and returns its id (for the
// workflow_pool_binding cross-schema read in ResolveForAttempt).
func insertPool(t *testing.T, pool *pgxpool.Pool, key string) string {
	t.Helper()
	var id string
	require.NoError(t, pool.QueryRow(context.Background(), `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ($1, $1, 'spiffe://iterabase.local/pools/test/') RETURNING id`, key).Scan(&id))
	return id
}

func sampleDefinition(key, version, scopeID string) workflow.Definition {
	return workflow.Definition{
		Key:              key,
		Version:          version,
		Digest:           "digest-" + key + "-" + version,
		SpecJSON:         []byte(`{"key":"` + key + `"}`),
		ValidationStatus: workflow.ValidationValid,
		ScopeIdentityID:  scopeID,
		SourceType:       "graph_email",
		PoolKey:          "default/walter-pool",
		Presentation:     []byte(`{"workflowTitle":"Quotation"}`),
	}
}

func TestRegisterDefinition_IdempotentSameDigest(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-1")

	d1, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)
	assert.Equal(t, "digest-walter/quotation-1", d1.Digest)

	// Re-registering the same (key, digest) is a no-op returning the same row.
	d2, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)
	assert.Equal(t, d1.ID, d2.ID)
	assert.Equal(t, d1.Digest, d2.Digest)
}

func TestRegisterDefinition_ImmutableVersionRejectsDifferentContent(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-2")

	_, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)

	// Same (key, version), different digest => rejected (ARCH-007).
	dup := sampleDefinition("walter/quotation", "1", scopeID)
	dup.Digest = "digest-changed"
	_, err = store.RegisterDefinition(ctx, dup)
	require.Error(t, err)
	assert.ErrorIs(t, err, workflow.ErrImmutableVersion)
}

func TestRegisterDefinition_NewVersionSameContentCoexists(t *testing.T) {
	// A new version with identical content must still register as a distinct
	// immutable version identity and remain independently resolvable (the
	// former (key, digest) unique constraint suppressed the insert and returned
	// the existing version's row, leaving the new version unresolvable).
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-same-content")

	d1, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)

	// Version 2 with byte-identical content (same digest).
	same := sampleDefinition("walter/quotation", "1", scopeID)
	same.Version = "2"
	d2, err := store.RegisterDefinition(ctx, same)
	require.NoError(t, err)
	assert.Equal(t, "2", d2.Version)
	assert.Equal(t, d1.Digest, d2.Digest, "same content => same digest")
	assert.NotEqual(t, d1.ID, d2.ID, "distinct version identity")

	// Both versions are independently resolvable.
	got1, err := store.GetDefinition(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, "1", got1.Version)
	got2, err := store.GetDefinition(ctx, "walter/quotation", "2")
	require.NoError(t, err)
	assert.Equal(t, "2", got2.Version)
}

func TestRegisterDefinition_NewVersionCoexists(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-3")

	_, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)
	d2, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "2", scopeID))
	require.NoError(t, err)
	assert.Equal(t, "2", d2.Version)

	// Both versions resolvable; latest is v2.
	got1, err := store.GetDefinition(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, "1", got1.Version)
	latest, err := store.GetLatestDefinition(ctx, "walter/quotation")
	require.NoError(t, err)
	assert.Equal(t, "2", latest.Version)
}

func TestTriggerBindings_ReplaceAndList(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-4")
	def, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)

	bindings := []workflow.TriggerBindingInput{
		{Name: "inbox", BindingKey: "inbox@walter.example", Config: []byte(`{"folder":"Inbox"}`)},
		{Name: "archive", BindingKey: "archive@walter.example"},
	}
	require.NoError(t, store.ReplaceTriggerBindings(ctx, def.ID, "graph_email", bindings))

	got, err := store.ListTriggerBindings(ctx, def.ID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "archive", got[0].Name) // ordered by name
	assert.Equal(t, "inbox@walter.example", got[1].BindingKey)
	var cfg map[string]string
	require.NoError(t, json.Unmarshal(got[1].Config, &cfg))
	assert.Equal(t, "Inbox", cfg["folder"])

	// Replace shrinks to one binding; the other is soft-deleted.
	require.NoError(t, store.ReplaceTriggerBindings(ctx, def.ID, "graph_email",
		[]workflow.TriggerBindingInput{{Name: "inbox", BindingKey: "inbox@walter.example"}}))
	got, err = store.ListTriggerBindings(ctx, def.ID)
	require.NoError(t, err)
	require.Len(t, got, 1, "stale binding must be soft-deleted")
}

func TestTriggerBindings_RejectsDuplicateName(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-5")
	def, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)

	err = store.ReplaceTriggerBindings(ctx, def.ID, "graph_email", []workflow.TriggerBindingInput{
		{Name: "dup", BindingKey: "a"},
		{Name: "dup", BindingKey: "b"},
	})
	require.Error(t, err)
}

func TestResolveForAttempt(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-6")
	def, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)
	require.NoError(t, store.ReplaceTriggerBindings(ctx, def.ID, "graph_email",
		[]workflow.TriggerBindingInput{{Name: "inbox", BindingKey: "inbox@walter.example"}}))

	// Bind the workflow's permitted tools to a pool (toolgateway).
	poolID := insertPool(t, pool, "default/walter-pool")
	defKey := workflow.DefinitionKey(def.Key, def.Version)
	_, err = pool.Exec(ctx, `
		INSERT INTO toolgateway.workflow_pool_bindings (workflow_definition_key, pool_id, permitted_tools)
		VALUES ($1, $2, $3)`, defKey, poolID, []string{"graph.read", "graph.excel.write"})
	require.NoError(t, err)

	// Resolve by exact version.
	resolved, err := store.ResolveForAttempt(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, def.ID, resolved.Definition.ID)
	assert.Equal(t, scopeID, resolved.Definition.ScopeIdentityID)
	require.Len(t, resolved.TriggerBindings, 1)
	assert.Equal(t, []string{"graph.read", "graph.excel.write"}, resolved.PermittedTools)

	// Resolve latest when version empty.
	resolvedLatest, err := store.ResolveForAttempt(ctx, "walter/quotation", "")
	require.NoError(t, err)
	assert.Equal(t, resolved.Definition.ID, resolvedLatest.Definition.ID)

	// Unknown key => ErrNotFound.
	_, err = store.ResolveForAttempt(ctx, "nope/missing", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
}

func TestResolveForAttempt_RejectsInvalidDefinition(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-7")
	invalid := sampleDefinition("walter/quotation", "1", scopeID)
	invalid.ValidationStatus = workflow.ValidationInvalid
	_, err := store.RegisterDefinition(ctx, invalid)
	require.NoError(t, err)

	_, err = store.ResolveForAttempt(ctx, "walter/quotation", "1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
}

func TestSoftDeleteDefinitionByKey(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-8")
	_, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)
	_, err = store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "2", scopeID))
	require.NoError(t, err)

	require.NoError(t, store.SoftDeleteDefinitionByKey(ctx, "walter/quotation"))

	// Both versions are soft-deleted (not resolvable).
	_, err = store.GetDefinition(ctx, "walter/quotation", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
	_, err = store.GetLatestDefinition(ctx, "walter/quotation")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
}

func TestDefinitionKey_WireFormat(t *testing.T) {
	assert.Equal(t, "walter/quotation:1", workflow.DefinitionKey("walter/quotation", "1"))
}

// Ensure the CanonicalSpec marshals deterministically (digest stability).
func TestCanonicalSpec_DeterministicMarshal(t *testing.T) {
	s := workflow.CanonicalSpec{
		Key:            "walter/quotation",
		PoolRef:        "walter-pool",
		Steps:          []workflow.CanonicalStep{{Name: "classify", Kind: "agent_task"}},
		CompletionRule: workflow.CanonicalCompletion{Type: "all_steps"},
		Presentation:   workflow.CanonicalPresentation{WorkflowTitle: "Quotation", PersonaName: "Walter Ops"},
	}
	b1, err := json.Marshal(s)
	require.NoError(t, err)
	b2, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, b1, b2)
}
