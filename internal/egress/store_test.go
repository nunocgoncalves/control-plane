package egress_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/egress"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newStores returns egress + identity stores over a fresh migrated Postgres.
func newStores(t *testing.T) (*egress.Store, *identity.Store) {
	t.Helper()
	pool := testutil.NewPostgresPool(t)
	return egress.NewStore(pool), identity.NewStore(pool)
}

func TestResolveBroadDefault(t *testing.T) {
	egStore, idStore := newStores(t)
	ctx := context.Background()

	// Two active identities; broad-default means both get every route.
	alice, err := idStore.UpsertIdentity(ctx, "default/alice", "user", identity.SourceExternal, "Alice")
	require.NoError(t, err)
	bob, err := idStore.UpsertIdentity(ctx, "default/bob", "user", identity.SourceExternal, "Bob")
	require.NoError(t, err)

	// An oauthClientCredentials route (graph) + a bearer route (legacy-api).
	_, err = egStore.UpsertRoute(ctx, "default/graph", "graph", "default", "https://graph.microsoft.com",
		egress.Auth{
			Scheme:          "oauthClientCredentials",
			TokenURL:        "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
			ClientID:        "app-id",
			ClientSecretRef: &egress.SecretRef{Name: "graph-oauth", Key: "client_secret"},
			Scope:           "https://graph.microsoft.com/.default",
		}, nil)
	require.NoError(t, err)
	_, err = egStore.UpsertRoute(ctx, "default/legacy", "legacy", "default", "https://api.legacy.example",
		egress.Auth{Scheme: "bearer", SecretRef: &egress.SecretRef{Name: "legacy-key", Key: "token"}}, nil)
	require.NoError(t, err)

	// Both identities resolve to both routes (broad-default).
	for _, id := range []string{alice.ID, bob.ID} {
		res, err := egStore.Resolve(ctx, id)
		require.NoError(t, err)
		require.Len(t, res.Routes, 2, "identity %s should get both routes (broad-default)", id)
		assert.Equal(t, "graph", res.Routes[0].RouteID)
		assert.Equal(t, "https://graph.microsoft.com", res.Routes[0].UpstreamBaseURL)
		assert.Equal(t, "oauthClientCredentials", res.Routes[0].Auth.Scheme)
		assert.Equal(t, "legacy", res.Routes[1].RouteID)
		assert.Equal(t, "bearer", res.Routes[1].Auth.Scheme)

		// Distinct secret refs across both routes (graph client_secret + legacy token).
		require.Len(t, res.SecretRefs, 2)
		assert.Equal(t, egress.SecretRef{Name: "graph-oauth", Key: "client_secret"}, res.SecretRefs[0])
		assert.Equal(t, egress.SecretRef{Name: "legacy-key", Key: "token"}, res.SecretRefs[1])
	}
}

func TestResolveInactiveIdentityDenied(t *testing.T) {
	egStore, idStore := newStores(t)
	ctx := context.Background()

	alice, err := idStore.UpsertIdentity(ctx, "default/alice", "user", identity.SourceExternal, "Alice")
	require.NoError(t, err)
	_, err = egStore.UpsertRoute(ctx, "default/graph", "graph", "default",
		"https://graph.microsoft.com", egress.Auth{Scheme: "bearer", SecretRef: &egress.SecretRef{Name: "g", Key: "k"}}, nil)
	require.NoError(t, err)

	// Active identity resolves the route.
	res, err := egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	assert.Len(t, res.Routes, 1)

	// Soft-delete the identity -> it drops out of effective_routes -> denied.
	require.NoError(t, idStore.SoftDeleteIdentityByKey(ctx, "default/alice"))
	res, err = egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	assert.Empty(t, res.Routes, "soft-deleted identity should resolve to no routes")

	// An entirely unknown identity also resolves to nothing (no error).
	res, err = egStore.Resolve(ctx, "00000000-0000-0000-0000-000000000000")
	require.NoError(t, err)
	assert.Empty(t, res.Routes)
}

func TestSoftDeleteRouteRevokesAccess(t *testing.T) {
	egStore, idStore := newStores(t)
	ctx := context.Background()

	alice, err := idStore.UpsertIdentity(ctx, "default/alice", "user", identity.SourceExternal, "Alice")
	require.NoError(t, err)
	_, err = egStore.UpsertRoute(ctx, "default/graph", "graph", "default",
		"https://graph.microsoft.com", egress.Auth{Scheme: "bearer", SecretRef: &egress.SecretRef{Name: "g", Key: "k"}}, nil)
	require.NoError(t, err)

	res, err := egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	assert.Len(t, res.Routes, 1)

	// Soft-delete the route -> it drops out of effective_routes.
	require.NoError(t, egStore.SoftDeleteRouteByKey(ctx, "default/graph"))
	res, err = egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	assert.Empty(t, res.Routes, "soft-deleted route should not resolve")

	// The row is retained (history) but not active.
	_, err = egStore.GetRouteByKey(ctx, "default/graph")
	assert.ErrorIs(t, err, egress.ErrNotFound, "soft-deleted route is not retrievable as active")
}

func TestUpsertRouteReviveOnRecreate(t *testing.T) {
	egStore, idStore := newStores(t)
	ctx := context.Background()

	alice, err := idStore.UpsertIdentity(ctx, "default/alice", "user", identity.SourceExternal, "Alice")
	require.NoError(t, err)
	_, err = egStore.UpsertRoute(ctx, "default/graph", "graph", "default",
		"https://graph.microsoft.com", egress.Auth{Scheme: "bearer", SecretRef: &egress.SecretRef{Name: "g", Key: "k"}}, nil)
	require.NoError(t, err)
	require.NoError(t, egStore.SoftDeleteRouteByKey(ctx, "default/graph"))

	// Recreate (revive) with a new upstream.
	r, err := egStore.UpsertRoute(ctx, "default/graph", "graph", "default",
		"https://graph.microsoft.us", egress.Auth{Scheme: "bearer", SecretRef: &egress.SecretRef{Name: "g", Key: "k"}}, nil)
	require.NoError(t, err)
	assert.Equal(t, "https://graph.microsoft.us", r.UpstreamBaseURL)

	// The revived route is active again.
	res, err := egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	require.Len(t, res.Routes, 1)
	assert.Equal(t, "https://graph.microsoft.us", res.Routes[0].UpstreamBaseURL)
}
