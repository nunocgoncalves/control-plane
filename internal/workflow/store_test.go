package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
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
		Key:     key,
		Version: version,
		Digest:  "digest-" + key + "-" + version,
		SpecJSON: []byte(`{"key":"` + key + `","scopeIdentityKey":"workflow:default/walter-quotation",` +
			`"skills":[{"name":"walter-quotation","version":"1.0.0","digest":"sha256:skill-v1"}],` +
			`"requestedCapabilities":[{"tool":"graph.excel.write","maxEffectClass":"idempotent_write"}],` +
			`"defaultModelRef":"model-one","graph":{"entryNode":"write","maxTransitions":10,` +
			`"nodes":[{"key":"write","label":{"en":"Writing result","pt":"A escrever resultado"},"kind":"agent_task","prompt":"write","skills":["walter-quotation"],"capabilities":["graph.excel.write"],"outcomes":["completed"],"resultPresentation":{"outcomes":[{"outcome":"completed","summary":{"en":"Quotation processed","pt":"Pedido de cotação processado"}}]}}],` +
			`"terminalOutcomes":[{"node":"write","outcome":"completed"}]},` +
			`"presentation":{"workflowTitle":"Quotation","personaName":"Walter Ops"},` +
			`"poolRef":"walter-pool","source":{"type":"graph_email"}}`),
		ValidationStatus: workflow.ValidationValid,
		ScopeIdentityID:  scopeID,
		SourceType:       "graph_email",
		PoolKey:          "default/walter-pool",
		Presentation:     []byte(`{"workflowTitle":"Quotation","personaName":"Walter Ops","locale":"en"}`),
	}
}

func TestRegisterDefinition_IdempotentSameDigest(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-1")

	d1, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)
	assert.Equal(t, "digest-walter/quotation-1", d1.Digest)

	// Re-registering the same (key, version, digest) is a no-op returning the same row.
	d2, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)
	assert.Equal(t, d1.ID, d2.ID)
	assert.Equal(t, d1.Digest, d2.Digest)
	assert.Equal(t, d1.UpdatedAt, d2.UpdatedAt, "active idempotent registration must not mutate the row")
}

func TestRegisterDefinition_RevivesSoftDeletedSameVersion(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	ownerKey := "workflow:default/recreated"
	ownerID := insertWorkflowIdentity(t, pool, ownerKey)
	otherOwnerID := insertWorkflowIdentity(t, pool, "workflow:default/other")
	definition := sampleDefinition("walter/quotation", "1", ownerID)

	registered, err := store.RegisterDefinition(ctx, definition)
	require.NoError(t, err)
	require.NoError(t, store.SoftDeleteDefinitionsByOwner(ctx, ownerKey))
	_, err = store.GetDefinition(ctx, definition.Key, definition.Version)
	require.ErrorIs(t, err, workflow.ErrNotFound)

	changed := definition
	changed.Digest = "digest-changed"
	_, err = store.RegisterDefinition(ctx, changed)
	require.ErrorIs(t, err, workflow.ErrImmutableVersion,
		"recreation must not replace an immutable deleted version")

	otherOwner := definition
	otherOwner.ScopeIdentityID = otherOwnerID
	_, err = store.RegisterDefinition(ctx, otherOwner)
	require.ErrorIs(t, err, workflow.ErrDefinitionOwnership,
		"recreation must not transfer a logical workflow key to another owner")

	revived, err := store.RegisterDefinition(ctx, definition)
	require.NoError(t, err)
	assert.Equal(t, registered.ID, revived.ID, "recreation must revive the durable version identity")
	got, err := store.GetDefinition(ctx, definition.Key, definition.Version)
	require.NoError(t, err)
	assert.Equal(t, registered.ID, got.ID)
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

func TestRegisterDefinition_RejectsDifferentOwnerForLogicalKey(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	ownerAKey := "workflow:default/a"
	ownerA := insertWorkflowIdentity(t, pool, ownerAKey)
	ownerB := insertWorkflowIdentity(t, pool, "workflow:default/b")

	_, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", ownerA))
	require.NoError(t, err)
	_, err = store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "2", ownerB))
	require.ErrorIs(t, err, workflow.ErrDefinitionOwnership,
		"a different CR must not publish another version under an owned logical key")
	_, err = store.GetDefinition(ctx, "walter/quotation", "2")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
	latest, err := store.GetLatestDefinition(ctx, "walter/quotation")
	require.NoError(t, err)
	assert.Equal(t, ownerA, latest.ScopeIdentityID,
		"default resolution must remain bound to the logical key's durable owner")

	// The same durable owner may publish the next version, and owner cleanup
	// revokes every version of the logical key.
	_, err = store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "2", ownerA))
	require.NoError(t, err)
	require.NoError(t, store.SoftDeleteDefinitionsByOwner(ctx, ownerAKey))
	_, err = store.GetDefinition(ctx, "walter/quotation", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
	_, err = store.GetDefinition(ctx, "walter/quotation", "2")
	assert.ErrorIs(t, err, workflow.ErrNotFound)

	// Concurrent first publication is serialized by logical key: exactly one
	// owner wins and the other receives the ownership error, even on a different
	// version.
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, d := range []workflow.Definition{
		sampleDefinition("walter/concurrent", "1", ownerA),
		sampleDefinition("walter/concurrent", "2", ownerB),
	} {
		go func() {
			<-start
			_, registerErr := store.RegisterDefinition(ctx, d)
			results <- registerErr
		}()
	}
	close(start)
	var registered, rejected int
	for range 2 {
		registerErr := <-results
		if registerErr == nil {
			registered++
		} else if errors.Is(registerErr, workflow.ErrDefinitionOwnership) {
			rejected++
		}
	}
	assert.Equal(t, 1, registered)
	assert.Equal(t, 1, rejected)
	defs, err := store.ListDefinitionsByKey(ctx, "walter/concurrent")
	require.NoError(t, err)
	assert.Len(t, defs, 1)
}

func TestDefinitionsByOwnerSpanSpecKeys(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	ownerKey := "workflow:default/a"
	ownerID := insertWorkflowIdentity(t, pool, ownerKey)

	_, err := store.RegisterDefinition(ctx, sampleDefinition("walter/original", "1", ownerID))
	require.NoError(t, err)
	_, err = store.RegisterDefinition(ctx, sampleDefinition("walter/renamed", "2", ownerID))
	require.NoError(t, err)

	defs, err := store.ListDefinitionsByOwner(ctx, ownerKey)
	require.NoError(t, err)
	require.Len(t, defs, 2)
	assert.Equal(t, "walter/original", defs[0].Key)
	assert.Equal(t, "walter/renamed", defs[1].Key)

	require.NoError(t, store.SoftDeleteDefinitionsByOwner(ctx, ownerKey))
	_, err = store.GetDefinition(ctx, "walter/original", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
	_, err = store.GetDefinition(ctx, "walter/renamed", "2")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
}

func TestTriggerBindings_ReplaceAndList(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-4")
	def, err := store.RegisterDefinition(ctx, sampleDefinition("walter/quotation", "1", scopeID))
	require.NoError(t, err)

	bindings := []workflow.TriggerBindingInput{
		{Name: "inbox", BindingKey: "inbox@walter.example"},
		{Name: "archive", BindingKey: "archive@walter.example"},
	}
	require.NoError(t, store.ReplaceTriggerBindings(ctx, def.ID, "graph_email", bindings))

	got, err := store.ListTriggerBindings(ctx, def.ID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "archive", got[0].Name) // ordered by name
	assert.Equal(t, "inbox@walter.example", got[1].BindingKey)

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

	// Bind the workflow's permitted tools to a pool (toolgateway). The binding
	// now stores full capability objects (tool + maxEffectClass + actions).
	poolID := insertPool(t, pool, "default/walter-pool")
	defKey := workflow.DefinitionKey(def.Key, def.Version)
	_, err = pool.Exec(ctx, `
		INSERT INTO toolgateway.workflow_pool_bindings (workflow_definition_key, pool_id, permitted_tools)
		VALUES ($1, $2, $3)`, defKey, poolID, []workflow.CanonicalCapability{
		{Tool: "graph.read", MaxEffectClass: "read_only", Actions: []string{"graph.read"}},
		{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
	})
	require.NoError(t, err)

	// Resolve by exact version.
	resolved, err := store.ResolveForAttempt(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, def.ID, resolved.Definition.ID)
	assert.Equal(t, scopeID, resolved.Definition.ScopeIdentityID)
	require.Len(t, resolved.TriggerBindings, 1)
	assert.Equal(t, []string{"graph.read", "graph.excel.write"}, resolved.PermittedTools)
	require.Len(t, resolved.Skills, 1)
	assert.Equal(t, "sha256:skill-v1", resolved.Skills[0].Digest)
	assert.Equal(t, "write", resolved.Spec.Graph.EntryNode)
	assert.Equal(t, []string{"graph.excel.write"}, resolved.Spec.Graph.Nodes[0].Capabilities)
	assert.Equal(t, "workflow:default/walter-quotation", resolved.Spec.ScopeIdentityKey)

	// Resolve latest when version empty.
	resolvedLatest, err := store.ResolveForAttempt(ctx, "walter/quotation", "")
	require.NoError(t, err)
	assert.Equal(t, resolved.Definition.ID, resolvedLatest.Definition.ID)

	// Unknown key => ErrNotFound.
	_, err = store.ResolveForAttempt(ctx, "nope/missing", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound)
}

func TestResolveForAttempt_RejectsIncompleteCanonicalSpec(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	scopeID := insertWorkflowIdentity(t, pool, "wf/test-malformed")
	malformed := sampleDefinition("walter/quotation", "1", scopeID)
	malformed.SpecJSON = []byte(`{}`)
	def, err := store.RegisterDefinition(ctx, malformed)
	require.NoError(t, err)
	poolID := insertPool(t, pool, "default/walter-pool")
	_, err = pool.Exec(ctx, `
		INSERT INTO toolgateway.workflow_pool_bindings (workflow_definition_key, pool_id, permitted_tools)
		VALUES ($1, $2, '[]'::jsonb)`, workflow.DefinitionKey(def.Key, def.Version), poolID)
	require.NoError(t, err)

	_, err = store.ResolveForAttempt(ctx, "walter/quotation", "1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve canonical workflow spec")
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
		Key: "walter/quotation", ScopeIdentityKey: "workflow:default/walter-quotation", PoolRef: "walter-pool", DefaultModelRef: "model-one",
		Skills:                []workflow.CanonicalSkill{{Name: "walter", Version: "1", Digest: "sha256:skill"}},
		RequestedCapabilities: []workflow.CanonicalCapability{{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"}},
		Graph: workflow.CanonicalGraph{EntryNode: "write", MaxTransitions: 10,
			Nodes: []workflow.CanonicalNode{{Key: "write", Label: workflow.CanonicalLocalizedText{EN: "Writing result", PT: "A escrever resultado"}, Kind: workflow.NodeAgentTask, Prompt: "write", Skills: []string{"walter"}, Capabilities: []string{"graph.excel.write"}, Outcomes: []string{"completed"},
				ResultPresentation: &workflow.CanonicalResultPresentation{Outcomes: []workflow.CanonicalResultOutcomePresentation{{Outcome: "completed", Summary: workflow.CanonicalLocalizedText{EN: "Quotation processed", PT: "Pedido de cotação processado"}}}}}},
			TerminalOutcomes: []workflow.CanonicalTerminalOutcome{{Node: "write", Outcome: "completed"}}},
		Presentation: workflow.CanonicalPresentation{WorkflowTitle: "Quotation", PersonaName: "Walter Ops"},
	}
	b1, err := json.Marshal(s)
	require.NoError(t, err)
	b2, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, b1, b2)
}
