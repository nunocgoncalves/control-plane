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
	"github.com/nunocgoncalves/control-plane/internal/permissions"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newPermissionsStore returns a Store backed by a fresh migrated Postgres.
func newPermissionsStore(t *testing.T) *permissions.Store {
	t.Helper()
	return permissions.NewStore(testutil.NewPostgresPool(t))
}

// TestPermissionPolicyReconcile exercises the Git->DB bridge UNDER RBAC: the
// reconciler runs as a ServiceAccount bound to the generated role.yaml. Creating
// a CR materializes a policy in Postgres and reports Ready; deleting the CR
// soft-deletes the policy (access revoked, row retained). If role.yaml misses a
// verb the controller needs, the reconciler is forbidden and the test fails.
// Requires Docker (Postgres) + KUBEBUILDER_ASSETS (envtest).
func TestPermissionPolicyReconcile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run envtest (make setup-envtest)")
	}

	store := newPermissionsStore(t)
	ctx := context.Background()

	// envtest: real K8s API server (RBAC enforced) + etcd, CRD installed.
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

	// The admin client drives the test; the manager runs under the RBAC-limited
	// SA config (rbacManagerConfig is shared with the IdentityMapping test) so
	// the reconciler is authorization-bound.
	adminClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	saCfg := rbacManagerConfig(t, ctx, cfg, scheme)

	mgr, err := ctrl.NewManager(saCfg, ctrl.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, (&PermissionPolicyReconciler{
		Client: mgr.GetClient(),
		Scheme: scheme,
		Store:  store,
	}).SetupWithManager(mgr))

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(mgrCtx) }()

	// Create a PermissionPolicy.
	pp := &v1alpha1.PermissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default"},
		Spec: v1alpha1.PermissionPolicySpec{
			Subject: v1alpha1.SubjectSpec{Kind: "user", Key: "default/alice"},
		},
	}
	require.NoError(t, adminClient.Create(ctx, pp))

	nn := types.NamespacedName{Name: "alice", Namespace: "default"}

	// Wait for Ready + policyID (reconciler succeeded under RBAC).
	require.Eventually(t, func() bool {
		var got v1alpha1.PermissionPolicy
		if err := adminClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.Ready && got.Status.PolicyID != ""
	}, 15*time.Second, 200*time.Millisecond, "PermissionPolicy should become Ready")

	// The policy is materialized in Postgres.
	pol, err := store.GetPolicyByKey(ctx, "default/alice")
	require.NoError(t, err)
	assert.Equal(t, "user", pol.SubjectKind)
	assert.Equal(t, "default/alice", pol.SubjectKey)
	assert.Nil(t, pol.RateLimits, "policy without rateLimits is unlimited")

	// Update the CR to add rateLimits -> reconciler re-materializes them.
	var fresh v1alpha1.PermissionPolicy
	require.NoError(t, adminClient.Get(ctx, nn, &fresh))
	fresh.Spec.RateLimits = &v1alpha1.RateLimitsSpec{RPM: 60, TPM: 100000}
	require.NoError(t, adminClient.Update(ctx, &fresh))
	require.Eventually(t, func() bool {
		var got v1alpha1.PermissionPolicy
		if err := adminClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.ObservedGeneration == got.Generation
	}, 15*time.Second, 200*time.Millisecond, "policy should re-reconcile after rateLimits update")
	pol, err = store.GetPolicyByKey(ctx, "default/alice")
	require.NoError(t, err)
	require.NotNil(t, pol.RateLimits)
	assert.Equal(t, 60, pol.RateLimits.RPM)
	assert.Equal(t, 100000, pol.RateLimits.TPM)

	// Delete the CR (finalizer cleanup must also be authorized under RBAC).
	require.NoError(t, adminClient.Delete(ctx, pp))

	require.Eventually(t, func() bool {
		var got v1alpha1.PermissionPolicy
		return errors.IsNotFound(adminClient.Get(ctx, nn, &got))
	}, 15*time.Second, 200*time.Millisecond, "PermissionPolicy should be deleted after finalizer cleanup")

	// The policy is soft-deleted: no longer active.
	_, err = store.GetPolicyByKey(ctx, "default/alice")
	assert.ErrorIs(t, err, permissions.ErrNotFound, "soft-deleted policy should not be active")
}
