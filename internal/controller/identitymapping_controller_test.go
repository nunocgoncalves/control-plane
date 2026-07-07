package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	yaml "sigs.k8s.io/yaml"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newPostgresStore returns a Store backed by a fresh migrated Postgres.
func newPostgresStore(t *testing.T) *identity.Store {
	t.Helper()
	return identity.NewStore(testutil.NewPostgresPool(t))
}

// rbacManagerConfig applies the generated manager-role (config/rbac/role.yaml)
// and returns a rest.Config authenticated as a ServiceAccount bound to it. The
// reconciler runs under this config so the test behaviorally validates that
// role.yaml grants every verb the controller needs. envtest's API server
// enforces RBAC by default; the admin config would bypass it, so this is the
// only way to catch a missing permission.
func rbacManagerConfig(t *testing.T, ctx context.Context, adminCfg *rest.Config, scheme *runtime.Scheme) *rest.Config {
	t.Helper()
	adminClient, err := client.New(adminCfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	// Apply the generated ClusterRole (the artifact under test).
	roleBytes, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(roleBytes, &role))
	require.NoError(t, adminClient.Create(ctx, &role))

	// ServiceAccount + ClusterRoleBinding -> manager-role.
	require.NoError(t, adminClient.Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "cp-test", Namespace: "default"},
	}))
	require.NoError(t, adminClient.Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "cp-test-manager"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "cp-test", Namespace: "default"}},
		RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: "manager-role", APIGroup: "rbac.authorization.k8s.io"},
	}))

	// Mint a short-lived token for the SA (modern K8s; no long-lived secret tokens).
	clientset, err := kubernetes.NewForConfig(adminCfg)
	require.NoError(t, err)
	tr, err := clientset.CoreV1().ServiceAccounts("default").CreateToken(ctx, "cp-test", &authenticationv1.TokenRequest{}, metav1.CreateOptions{})
	require.NoError(t, err)

	// Authenticate as the SA, dropping the admin client cert (keep the CA so the
	// apiserver's TLS cert is still verified).
	saCfg := rest.CopyConfig(adminCfg)
	saCfg.BearerToken = tr.Status.Token
	saCfg.BearerTokenFile = ""
	saCfg.TLSClientConfig.CertData = nil
	saCfg.TLSClientConfig.CertFile = ""
	saCfg.TLSClientConfig.KeyData = nil
	saCfg.TLSClientConfig.KeyFile = ""
	return saCfg
}

// TestIdentityMappingReconcile exercises the full Git->DB bridge UNDER RBAC: the
// reconciler runs as a ServiceAccount bound to the generated role.yaml. Creating
// a CR materializes an identity + bindings in Postgres and reports Ready;
// deleting the CR soft-deletes the identity (access revoked, row retained). If
// role.yaml misses a verb the controller needs, the reconciler is forbidden and
// the test fails. Requires Docker (Postgres) + KUBEBUILDER_ASSETS (envtest).
func TestIdentityMappingReconcile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run envtest (make setup-envtest)")
	}

	store := newPostgresStore(t)
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

	// The admin client drives the test (create/delete/get CR); the manager runs
	// under the RBAC-limited SA config so the reconciler is authorization-bound.
	adminClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	saCfg := rbacManagerConfig(t, ctx, cfg, scheme)

	mgr, err := ctrl.NewManager(saCfg, ctrl.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, (&IdentityMappingReconciler{
		Client: mgr.GetClient(),
		Scheme: scheme,
		Store:  store,
	}).SetupWithManager(mgr))

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(mgrCtx) }()

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
	require.NoError(t, adminClient.Create(ctx, im))

	nn := types.NamespacedName{Name: "alice", Namespace: "default"}

	// Wait for Ready + identityID (reconciler succeeded under RBAC).
	require.Eventually(t, func() bool {
		var got v1alpha1.IdentityMapping
		if err := adminClient.Get(ctx, nn, &got); err != nil {
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

	// Delete the CR (finalizer cleanup must also be authorized under RBAC).
	require.NoError(t, adminClient.Delete(ctx, im))

	require.Eventually(t, func() bool {
		var got v1alpha1.IdentityMapping
		return errors.IsNotFound(adminClient.Get(ctx, nn, &got))
	}, 15*time.Second, 200*time.Millisecond, "IdentityMapping should be deleted after finalizer cleanup")

	// The identity is soft-deleted: bindings gone (access revoked).
	_, err = store.ResolveByExternalID(ctx, "teams", "user", "aad:1111")
	assert.ErrorIs(t, err, identity.ErrNotFound, "soft-deleted identity should not resolve")
}
