package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/egress"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newEgressStore returns an egress.Store + identity.Store over a fresh migrated
// Postgres.
func newEgressStore(t *testing.T) (*egress.Store, *identity.Store) {
	t.Helper()
	pool := testutil.NewPostgresPool(t)
	return egress.NewStore(pool), identity.NewStore(pool)
}

// TestEgressRouteReconcile exercises the Git->DB bridge UNDER RBAC: creating an
// EgressRoute materializes a route in Postgres (and it resolves for an active
// identity — broad-default); deleting the CR soft-deletes it (access revoked).
// Requires Docker (Postgres) + KUBEBUILDER_ASSETS (envtest).
func TestEgressRouteReconcile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run envtest (make setup-envtest)")
	}

	egStore, idStore := newEgressStore(t)
	ctx := context.Background()

	// Seed an active identity so Resolve (effective_routes) has a row to join.
	alice, err := idStore.UpsertIdentity(ctx, "default/alice", "user", identity.SourceExternal, "Alice")
	require.NoError(t, err)

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	testEnv := &envtest.Environment{
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		},
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = testEnv.Stop() })

	adminClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	saCfg := rbacManagerConfig(t, ctx, cfg, scheme)

	mgr, err := ctrl.NewManager(saCfg, ctrl.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, (&EgressRouteReconciler{
		Client: mgr.GetClient(),
		Scheme: scheme,
		Store:  egStore,
	}).SetupWithManager(mgr))

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(mgrCtx) }()

	// Create an EgressRoute (graph, oauthClientCredentials).
	er := &v1alpha1.EgressRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "graph", Namespace: "default"},
		Spec: v1alpha1.EgressRouteSpec{
			Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://graph.microsoft.com"},
			Auth: v1alpha1.AuthSpec{
				Scheme:          "oauthClientCredentials",
				TokenURL:        "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
				ClientID:        "app-id",
				ClientSecretRef: &v1alpha1.SecretRef{Name: "graph-oauth", Key: "client_secret"},
				Scope:           "https://graph.microsoft.com/.default",
			},
		},
	}
	require.NoError(t, adminClient.Create(ctx, er))

	nn := types.NamespacedName{Name: "graph", Namespace: "default"}

	// Wait for Ready.
	require.Eventually(t, func() bool {
		var got v1alpha1.EgressRoute
		if err := adminClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.Ready
	}, 15*time.Second, 200*time.Millisecond, "EgressRoute should become Ready")

	// The route is materialized + resolves for the active identity (broad-default).
	res, err := egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	require.Len(t, res.Routes, 1)
	assert.Equal(t, "graph", res.Routes[0].RouteID)
	assert.Equal(t, "https://graph.microsoft.com", res.Routes[0].UpstreamBaseURL)
	assert.Equal(t, "oauthClientCredentials", res.Routes[0].Auth.Scheme)
	require.Len(t, res.SecretRefs, 1)
	assert.Equal(t, egress.SecretRef{Name: "graph-oauth", Key: "client_secret"}, res.SecretRefs[0])

	// Delete the CR (finalizer cleanup under RBAC).
	require.NoError(t, adminClient.Delete(ctx, er))

	require.Eventually(t, func() bool {
		var got v1alpha1.EgressRoute
		return errors.IsNotFound(adminClient.Get(ctx, nn, &got))
	}, 15*time.Second, 200*time.Millisecond, "EgressRoute should be deleted after finalizer cleanup")

	// Soft-deleted route drops out of effective_routes (access revoked).
	res, err = egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	assert.Empty(t, res.Routes, "soft-deleted route should not resolve")
}

// TestValidateEgressRoute unit-tests the auth-block validation (pure; no
// manager/cluster). The reconciler surfaces a validate error in status and
// skips materialization.
func TestValidateEgressRoute(t *testing.T) {
	for _, tc := range []struct {
		name    string
		spec    v1alpha1.EgressRouteSpec
		wantSub string
	}{
		{
			name: "bearer missing secretRef",
			spec: v1alpha1.EgressRouteSpec{
				Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://api.example"},
				Auth:     v1alpha1.AuthSpec{Scheme: "bearer"},
			},
			wantSub: "secretRef",
		},
		{
			name: "bearer empty secretRef name",
			spec: v1alpha1.EgressRouteSpec{
				Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://api.example"},
				Auth:     v1alpha1.AuthSpec{Scheme: "bearer", SecretRef: &v1alpha1.SecretRef{Name: "", Key: "k"}},
			},
			wantSub: "secretRef",
		},
		{
			name: "oauth missing tokenURL",
			spec: v1alpha1.EgressRouteSpec{
				Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://api.example"},
				Auth: v1alpha1.AuthSpec{
					Scheme:          "oauthClientCredentials",
					ClientID:        "id",
					ClientSecretRef: &v1alpha1.SecretRef{Name: "s", Key: "k"},
				},
			},
			wantSub: "tokenURL",
		},
		{
			name: "oauth missing clientSecretRef",
			spec: v1alpha1.EgressRouteSpec{
				Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://api.example"},
				Auth: v1alpha1.AuthSpec{
					Scheme:   "oauthClientCredentials",
					TokenURL: "https://token.example",
					ClientID: "id",
				},
			},
			wantSub: "clientSecretRef",
		},
		{
			name: "unknown scheme",
			spec: v1alpha1.EgressRouteSpec{
				Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://api.example"},
				Auth:     v1alpha1.AuthSpec{Scheme: "weird"},
			},
			wantSub: "unknown auth scheme",
		},
		{
			name: "missing upstream baseURL",
			spec: v1alpha1.EgressRouteSpec{
				Auth: v1alpha1.AuthSpec{Scheme: "bearer", SecretRef: &v1alpha1.SecretRef{Name: "s", Key: "k"}},
			},
			wantSub: "upstream.baseURL",
		},
		{
			name: "valid bearer",
			spec: v1alpha1.EgressRouteSpec{
				Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://api.example"},
				Auth:     v1alpha1.AuthSpec{Scheme: "bearer", SecretRef: &v1alpha1.SecretRef{Name: "s", Key: "k"}},
			},
		},
		{
			name: "valid oauth",
			spec: v1alpha1.EgressRouteSpec{
				Upstream: v1alpha1.UpstreamSpec{BaseURL: "https://graph.microsoft.com"},
				Auth: v1alpha1.AuthSpec{
					Scheme:          "oauthClientCredentials",
					TokenURL:        "https://login.microsoftonline.com/t/oauth2/v2.0/token",
					ClientID:        "id",
					ClientSecretRef: &v1alpha1.SecretRef{Name: "s", Key: "k"},
					Scope:           "https://graph.microsoft.com/.default",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			er := &v1alpha1.EgressRoute{Spec: tc.spec}
			err := validateEgressRoute(er)
			if tc.wantSub == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantSub)
		})
	}
}
