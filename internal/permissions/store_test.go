package permissions

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

func TestStore_UpsertPolicyAndRevive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Create.
	p1, err := store.UpsertPolicy(ctx, "default/alice", "user", "default/alice")
	require.NoError(t, err)
	assert.Equal(t, "default/alice", p1.Key)

	got, err := store.GetPolicyByKey(ctx, "default/alice")
	require.NoError(t, err)
	assert.Equal(t, p1.ID, got.ID)

	// Soft delete (CR deletion path).
	require.NoError(t, store.SoftDeletePolicyByKey(ctx, "default/alice"))
	_, err = store.GetPolicyByKey(ctx, "default/alice")
	assert.ErrorIs(t, err, ErrNotFound)

	// Re-create -> revives the same row (same UUID) with updated subject.
	p2, err := store.UpsertPolicy(ctx, "default/alice", "group", "eng")
	require.NoError(t, err)
	assert.Equal(t, p1.ID, p2.ID, "revive should reuse the existing policy row")
	assert.Equal(t, "group", p2.SubjectKind)
	assert.Equal(t, "eng", p2.SubjectKey)
}

// TestStore_EffectiveCapabilities is the engine's behavioral assertion: the
// view grants broad-default wildcard to active identities, ignores policy rows
// in v1, and denies (no rows) for soft-deleted/unknown identities. Requires
// Docker.
func TestStore_EffectiveCapabilities(t *testing.T) {
	pool := testutil.NewPostgresPool(t)
	store := NewStore(pool)
	ctx := context.Background()

	// Insert an active identity directly (HOR-242's table).
	var aliceID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ('default/alice', 'user', 'external', 'Alice') RETURNING id`).Scan(&aliceID))

	// Active identity -> broad-default wildcard.
	caps, err := store.EffectiveCapabilities(ctx, aliceID)
	require.NoError(t, err)
	require.Len(t, caps, 1)
	assert.Equal(t, Capability{IdentityID: aliceID, Resource: "*", Action: "*"}, caps[0])

	// A policy row does NOT affect the view in v1 (broad-default ignores it).
	_, err = store.UpsertPolicy(ctx, "default/alice", "user", "default/alice")
	require.NoError(t, err)
	caps, err = store.EffectiveCapabilities(ctx, aliceID)
	require.NoError(t, err)
	assert.Len(t, caps, 1, "policy row must not change effective capabilities in v1")

	// Soft-deleting the identity removes its capabilities (revocation).
	_, err = pool.Exec(ctx, `UPDATE identity.identities SET deleted_at = now() WHERE id = $1`, aliceID)
	require.NoError(t, err)
	caps, err = store.EffectiveCapabilities(ctx, aliceID)
	require.NoError(t, err)
	assert.Empty(t, caps, "soft-deleted identity should have no capabilities")

	// Unknown identity -> no capabilities (denied).
	caps, err = store.EffectiveCapabilities(ctx, "00000000-0000-0000-0000-000000000000")
	require.NoError(t, err)
	assert.Empty(t, caps)
}
