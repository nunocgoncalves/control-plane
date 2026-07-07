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
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newPostgresStore returns a Store backed by a fresh migrated Postgres.
func newPostgresStore(t *testing.T) *identity.Store {
	t.Helper()
	return identity.NewStore(testutil.NewPostgresPool(t))
}

// TestIdentityMappingReconcile exercises the full Git->DB bridge: creating a CR
// materializes an identity + bindings in Postgres and reports Ready; deleting
// the CR soft-deletes the identity (access revoked, row retained). Requires
// Docker (Postgres) + KUBEBUILDER_ASSETS (envtest).
func TestIdentityMappingReconcile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run envtest (make setup-envtest)")
	}

	store := newPostgresStore(t)
	ctx := context.Background()

	// envtest: real K8s API server + etcd, with the IdentityMapping CRD installed.
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

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme})
	require.NoError(t, err)

	require.NoError(t, (&IdentityMappingReconciler{
		Client: mgr.GetClient(),
		Scheme: scheme,
		Store:  store,
	}).SetupWithManager(mgr))

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(mgrCtx) }()

	k8sClient := mgr.GetClient()

	// Create an IdentityMapping.
	im := &v1alpha1.IdentityMapping{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec: v1alpha1.IdentityMappingSpec{
			Identity: v1alpha1.IdentitySpec{Kind: "user", DisplayName: "Alice Wong"},
			Bindings: []v1alpha1.Binding{
				{Provider: "teams", Type: "user", ExternalID: "aad:1111"},
				{Provider: "slack", Type: "user", ExternalID: "U012345ABCD"},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, im))

	nn := types.NamespacedName{Name: "alice", Namespace: "default"}

	// Wait for Ready + identityID.
	require.Eventually(t, func() bool {
		var got v1alpha1.IdentityMapping
		if err := k8sClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.Ready && got.Status.IdentityID != ""
	}, 15*time.Second, 200*time.Millisecond, "IdentityMapping should become Ready")

	// The identity + bindings are materialized in Postgres.
	ident, err := store.ResolveByExternalID(ctx, "teams", "user", "aad:1111")
	require.NoError(t, err)
	assert.Equal(t, "default/alice", ident.Key)
	assert.Equal(t, "Alice Wong", ident.DisplayName)

	_, err = store.ResolveByExternalID(ctx, "slack", "user", "U012345ABCD")
	require.NoError(t, err)

	// Delete the CR.
	require.NoError(t, k8sClient.Delete(ctx, im))

	// The CR is removed (finalizer cleanup ran).
	require.Eventually(t, func() bool {
		var got v1alpha1.IdentityMapping
		return errors.IsNotFound(k8sClient.Get(ctx, nn, &got))
	}, 15*time.Second, 200*time.Millisecond, "IdentityMapping should be deleted after finalizer cleanup")

	// The identity is soft-deleted: bindings gone (access revoked).
	_, err = store.ResolveByExternalID(ctx, "teams", "user", "aad:1111")
	assert.ErrorIs(t, err, identity.ErrNotFound, "soft-deleted identity should not resolve")
}
