package identity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newTestStore returns a Store backed by a fresh migrated Postgres.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(testutil.NewPostgresPool(t))
}

func TestStore_UpsertIdentityAndRevive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Create.
	id1, err := store.UpsertIdentity(ctx, "default/alice", "user", "external", "Alice")
	require.NoError(t, err)
	assert.Equal(t, "default/alice", id1.Key)

	// Soft delete (CR deletion path).
	require.NoError(t, store.SoftDeleteIdentityByKey(ctx, "default/alice"))

	// Resolution should now fail (deleted_at set, mappings removed).
	_, err = store.ResolveByExternalID(ctx, "slack", "user", "U1")
	assert.ErrorIs(t, err, ErrNotFound)

	// Re-create -> revives the same row (same UUID).
	id2, err := store.UpsertIdentity(ctx, "default/alice", "user", "external", "Alice Wong")
	require.NoError(t, err)
	assert.Equal(t, id1.ID, id2.ID, "revive should reuse the existing identity row")
	assert.Equal(t, "Alice Wong", id2.DisplayName)
}

func TestStore_ReplaceExternalMappings(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	id, err := store.UpsertIdentity(ctx, "default/bob", "user", "external", "Bob")
	require.NoError(t, err)

	// Initial bindings.
	require.NoError(t, store.ReplaceExternalMappings(ctx, id.ID, []Binding{
		{Provider: "slack", Type: "user", ExternalID: "U1"},
		{Provider: "teams", Type: "user", ExternalID: "aad:1"},
	}))

	got, err := store.ResolveByExternalID(ctx, "slack", "user", "U1")
	require.NoError(t, err)
	assert.Equal(t, id.ID, got.ID)

	// Replace: drop slack, keep teams, add a duplicate within the batch (deduped).
	require.NoError(t, store.ReplaceExternalMappings(ctx, id.ID, []Binding{
		{Provider: "teams", Type: "user", ExternalID: "aad:1"},
		{Provider: "teams", Type: "user", ExternalID: "aad:2"},
		{Provider: "teams", Type: "user", ExternalID: "aad:2"}, // duplicate
	}))

	// Slack binding is gone.
	_, err = store.ResolveByExternalID(ctx, "slack", "user", "U1")
	assert.ErrorIs(t, err, ErrNotFound)

	// Teams binding still resolves.
	got, err = store.ResolveByExternalID(ctx, "teams", "user", "aad:1")
	require.NoError(t, err)
	assert.Equal(t, id.ID, got.ID)

	// A binding claimed by another identity must error.
	other, err := store.UpsertIdentity(ctx, "default/carol", "user", "external", "Carol")
	require.NoError(t, err)
	err = store.ReplaceExternalMappings(ctx, other.ID, []Binding{
		{Provider: "teams", Type: "user", ExternalID: "aad:1"}, // owned by bob
	})
	assert.Error(t, err, "binding claimed by another identity must conflict")
}

func TestStore_UpsertLocalUserIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	u1, err := store.UpsertLocalUser(ctx, "admin@local", "admin@local", "admin")
	require.NoError(t, err)
	assert.Equal(t, "admin", u1.Role)
	assert.Equal(t, "admin@local", u1.Email)

	// Second call (idempotent) returns the same identity.
	u2, err := store.UpsertLocalUser(ctx, "admin@local", "admin@local", "admin")
	require.NoError(t, err)
	assert.Equal(t, u1.ID, u2.ID)

	users, err := store.ListLocalUsers(ctx)
	require.NoError(t, err)
	assert.Len(t, users, 1)
}

func TestStore_CreateValidateRevokeAPIKey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sa, err := store.UpsertServiceAccount(ctx, "agent-fleet", "Agent Fleet")
	require.NoError(t, err)

	// Create.
	full, key, err := store.CreateAPIKey(ctx, sa.ID, "default", ScopeToken, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, full)
	assert.Equal(t, ScopeToken, key.Scope)

	// Validate.
	k, ident, err := store.ValidateAPIKey(ctx, full)
	require.NoError(t, err)
	assert.Equal(t, key.ID, k.ID)
	assert.Equal(t, sa.ID, ident.ID)

	// Wrong key.
	_, _, err = store.ValidateAPIKey(ctx, "cp-not-a-real-key")
	assert.ErrorIs(t, err, ErrInvalidAPIKey)

	// Revoke -> invalid.
	require.NoError(t, store.RevokeAPIKey(ctx, key.ID))
	_, _, err = store.ValidateAPIKey(ctx, full)
	assert.ErrorIs(t, err, ErrInvalidAPIKey)
}

func TestStore_RevokeAPIKeysForIdentity(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sa, err := store.UpsertServiceAccount(ctx, "agent-fleet", "Agent Fleet")
	require.NoError(t, err)

	full1, _, err := store.CreateAPIKey(ctx, sa.ID, "k1", ScopeToken, nil)
	require.NoError(t, err)
	full2, _, err := store.CreateAPIKey(ctx, sa.ID, "k2", ScopeAdmin, nil)
	require.NoError(t, err)

	// Revoke only token-scoped keys.
	require.NoError(t, store.RevokeAPIKeysForIdentity(ctx, sa.ID, ScopeToken))

	_, _, err = store.ValidateAPIKey(ctx, full1)
	assert.ErrorIs(t, err, ErrInvalidAPIKey, "token key should be revoked")

	_, _, err = store.ValidateAPIKey(ctx, full2)
	assert.NoError(t, err, "admin key should still be valid")
}
