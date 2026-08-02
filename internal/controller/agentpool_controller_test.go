package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/gateway"
	"github.com/nunocgoncalves/control-plane/internal/logging"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newAgentPoolTestEnv stands up envtest (real API server, RBAC enforced) with
// the generated CRDs installed and the AgentPoolReconciler running under the
// RBAC-limited manager-role (so role.yaml is behaviorally validated). When
// store is non-nil, the reconciler materializes CRs into it (the Git->DB
// bridge). Returns the admin client + the manager context.
func newAgentPoolTestEnv(t *testing.T, store *gateway.Store) (client.Client, context.Context) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run envtest (make setup-envtest)")
	}
	ctx := context.Background()

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

	mgr, err := ctrl.NewManager(saCfg, ctrl.Options{
		Scheme:     scheme,
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	require.NoError(t, err)
	require.NoError(t, (&AgentPoolReconciler{
		Client:    mgr.GetClient(),
		Scheme:    scheme,
		APIReader: mgr.GetAPIReader(),
		Store:     store,
	}).SetupWithManager(mgr))

	// Surface controller-runtime errors on the test log.
	_, handler := logging.New("debug", "text")
	ctrl.SetLogger(logr.FromSlogHandler(handler))

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() {
		if e := mgr.Start(mgrCtx); e != nil {
			t.Logf("manager start error: %v", e)
		}
	}()
	return adminClient, ctx
}

func validAgentPool(name, ns string) *v1alpha1.AgentPool {
	return &v1alpha1.AgentPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.AgentPoolSpec{
			Replicas:       2,
			WorkerImage:    "control-plane-harness:latest",
			PodSecurity:    "baseline",
			WorkspaceTools: false,
			Identity: v1alpha1.PoolIdentitySpec{
				TrustDomain:   "iterabase.local",
				CASecretRef:   v1alpha1.LocalKeyRef{Name: "platform-ca"},
				CertMountPath: "/etc/harness/tls",
			},
			Sandbox: v1alpha1.SandboxSpec{
				StorageClassName: "rwx-fast",
				AccessMode:       corev1.ReadWriteMany,
				Size:             resource.MustParse("10Gi"),
				MountPath:        "/data/sandboxes",
			},
			Gateways: v1alpha1.PoolGatewaysSpec{
				ControlPlane:     v1alpha1.GatewayEndpoint{URL: "https://control-plane:8443", ServerName: "control-plane", Selector: gwSelector("control-plane")},
				ToolGateway:      v1alpha1.GatewayEndpoint{URL: "https://gateway:8443", ServerName: "tool-gateway", Selector: gwSelector("tool-gateway")},
				InferenceGateway: v1alpha1.GatewayEndpoint{URL: "https://inference-gateway:8443", ServerName: "inference-gateway", Selector: gwSelector("inference-gateway")},
			},
			NetworkPolicy: v1alpha1.NetworkPolicySpec{Egress: "denied"},
			GatewayGrants: []v1alpha1.GatewayGrant{
				{Tool: "graph.read", MaxEffectClass: "read_only", AllowedActions: []string{"read"}},
			},
			CredentialBindings: []v1alpha1.CredentialBinding{
				{ToolName: "graph.read", Slot: "graphToken", Scheme: "bearer", Bearer: &v1alpha1.BearerCredential{ValueSecretRef: v1alpha1.SecretKeyRef{Name: "graph-creds", Key: "token"}}},
			},
		},
	}
}

// gwSelector builds a pod selector matching app.kubernetes.io/name=<app> in the
// pool's own namespace.
func gwSelector(app string) v1alpha1.GatewayPodSelector {
	return v1alpha1.GatewayPodSelector{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": app}}}
}

// TestAgentPoolReconcile exercises the full assembly UNDER RBAC: creating an
// AgentPool materializes the RWX PVC, the deny-by-default NetworkPolicy, the
// per-pod config ConfigMaps, and the warm-worker pods (with the cert-manager
// CSI TLS volume + SPIFFE URI SAN). envtest has no kubelet, so pods never
// become Ready (status stays not-ready) — this asserts object assembly, not
// runtime readiness (real-cluster PSS/CSI/storage validation is the ticket's
// stated real-cluster gate). Requires KUBEBUILDER_ASSETS (envtest).
func TestAgentPoolReconcile(t *testing.T) {
	adminClient, ctx := newAgentPoolTestEnv(t, nil)
	ns := "default"

	// Prerequisite Secrets: the platform CA + a credential-binding Secret.
	require.NoError(t, adminClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-ca", Namespace: ns},
		StringData: map[string]string{"tls.crt": "ca-cert", "tls.key": "ca-key"},
	}))
	require.NoError(t, adminClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "graph-creds", Namespace: ns},
		StringData: map[string]string{"token": "secret-value"},
	}))

	pool := validAgentPool("walter-pool", ns)
	require.NoError(t, adminClient.Create(ctx, pool))

	nn := types.NamespacedName{Name: "walter-pool", Namespace: ns}

	// PVC created as RWX.
	require.Eventually(t, func() bool {
		var pvc corev1.PersistentVolumeClaim
		if err := adminClient.Get(ctx, types.NamespacedName{Name: "walter-pool-sandbox", Namespace: ns}, &pvc); err != nil {
			return false
		}
		access := pvc.Spec.AccessModes
		return len(access) == 1 && access[0] == corev1.ReadWriteMany
	}, 15*time.Second, 200*time.Millisecond, "sandbox PVC should be created as RWX")

	// NetworkPolicy: deny-by-default egress, no internet IPBlock rule (denied mode).
	var np networkingv1.NetworkPolicy
	require.Eventually(t, func() bool {
		return adminClient.Get(ctx, types.NamespacedName{Name: "walter-pool-worker-egress", Namespace: ns}, &np) == nil
	}, 15*time.Second, 200*time.Millisecond, "worker egress NetworkPolicy should be created")
	assert.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}, np.Spec.PolicyTypes)
	hasInternetBlock := false
	for _, e := range np.Spec.Egress {
		for _, peer := range e.To {
			if peer.IPBlock != nil && peer.IPBlock.CIDR == "0.0.0.0/0" {
				hasInternetBlock = true
			}
		}
	}
	assert.False(t, hasInternetBlock, "denied mode must not allow internet egress")
	// Every egress peer is restricted by a pod selector (and gateway rules by a
	// port), so an enabled bash tool cannot reach unrelated colocated pods on
	// arbitrary ports (REQ-010) — no namespace-wide "any pod, any port" rule.
	for _, e := range np.Spec.Egress {
		require.NotEmpty(t, e.To, "egress rule must have at least one peer")
		for _, peer := range e.To {
			if peer.IPBlock != nil {
				continue
			}
			require.NotNil(t, peer.PodSelector, "egress must be restricted by a pod selector (no namespace-wide rule)")
		}
	}

	// Pods + ConfigMaps created with the expected shape.
	require.Eventually(t, func() bool {
		var pods corev1.PodList
		if err := adminClient.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels{"platform.iterabase.com/agentpool": "walter-pool"}); err != nil {
			return false
		}
		return len(pods.Items) == 2
	}, 15*time.Second, 200*time.Millisecond, "two warm-worker pods should be created")

	// The pod carries the cert-manager CSI TLS volume with the SPIFFE URI SAN.
	var pod corev1.Pod
	require.NoError(t, adminClient.Get(ctx, types.NamespacedName{Name: "walter-pool-worker-0", Namespace: ns}, &pod))
	var csiVol *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == "harness-tls" {
			csiVol = &pod.Spec.Volumes[i]
		}
	}
	require.NotNil(t, csiVol, "pod must have the harness-tls CSI volume")
	require.NotNil(t, csiVol.CSI)
	assert.Equal(t, "csi.cert-manager.io", csiVol.CSI.Driver)
	assert.Contains(t, csiVol.CSI.VolumeAttributes["csi.cert-manager.io/uri-sans"], "spiffe://iterabase.local/pools/")
	assert.Contains(t, csiVol.CSI.VolumeAttributes["csi.cert-manager.io/uri-sans"], "/workers/walter-pool-worker-0")
	assert.Equal(t, "ClusterIssuer", csiVol.CSI.VolumeAttributes["csi.cert-manager.io/issuer-kind"])

	// The per-pod config ConfigMap renders workerId (pod name) + poolId.
	var cm corev1.ConfigMap
	require.NoError(t, adminClient.Get(ctx, types.NamespacedName{Name: "walter-pool-worker-0-config", Namespace: ns}, &cm))
	cfg := cm.Data["config.yaml"]
	assert.Contains(t, cfg, "workerId: walter-pool-worker-0")
	assert.Contains(t, cfg, "toolGateway:")
	assert.Contains(t, cfg, "inferenceGateway:")

	// Status observed generation advanced (Ready stays false: no kubelet).
	require.Eventually(t, func() bool {
		var got v1alpha1.AgentPool
		if err := adminClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.ObservedGeneration == got.Generation
	}, 15*time.Second, 200*time.Millisecond, "AgentPool status should reflect reconciliation")

	// Delete the CR (finalizer cleanup must be authorized under RBAC).
	require.NoError(t, adminClient.Delete(ctx, pool))
	require.Eventually(t, func() bool {
		var got v1alpha1.AgentPool
		return errors.IsNotFound(adminClient.Get(ctx, nn, &got))
	}, 15*time.Second, 200*time.Millisecond, "AgentPool should be deleted after finalizer cleanup")
}

// TestAgentPoolValidation asserts structural validation (validateSpec)
// rejects bad specs directly against a fake client — no manager/envtest, so it
// runs fast and in -short mode. Semantic tool-registry validation is the
// gateway's job (HOR-392/397); this covers the structural acceptance criteria
// (unknown/invalid grants, invalid slot/Secret references, resource constraints).
func TestAgentPoolValidation(t *testing.T) {
	ns := "default"
	ca := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "platform-ca", Namespace: ns}, StringData: map[string]string{"tls.crt": "c", "tls.key": "k"}}
	creds := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "graph-creds", Namespace: ns}, StringData: map[string]string{"token": "v"}}

	newReconciler := func(secrets ...client.Object) *AgentPoolReconciler {
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = v1alpha1.AddToScheme(scheme)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secrets...).Build()
		return &AgentPoolReconciler{Client: c, Scheme: scheme, APIReader: c}
	}
	ctx := context.Background()

	// Valid spec passes.
	good := validAgentPool("ok", ns)
	good.Spec.Replicas = 1
	assert.NoError(t, newReconciler(ca, creds).validateSpec(ctx, good))

	// Credential binding references a non-existent Secret -> rejected.
	badBinding := validAgentPool("b1", ns)
	badBinding.Spec.Replicas = 1
	badBinding.Spec.CredentialBindings = []v1alpha1.CredentialBinding{
		{ToolName: "graph.read", Slot: "x", Scheme: "bearer", Bearer: &v1alpha1.BearerCredential{ValueSecretRef: v1alpha1.SecretKeyRef{Name: "does-not-exist", Key: "k"}}},
	}
	err := newReconciler(ca).validateSpec(ctx, badBinding)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Duplicate gateway grants -> rejected.
	dup := validAgentPool("b2", ns)
	dup.Spec.Replicas = 1
	dup.Spec.CredentialBindings = nil
	dup.Spec.GatewayGrants = []v1alpha1.GatewayGrant{
		{Tool: "graph.read", MaxEffectClass: "read_only"},
		{Tool: "graph.read", MaxEffectClass: "idempotent_write"},
	}
	err = newReconciler(ca).validateSpec(ctx, dup)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicated")

	// Invalid max effect class -> rejected.
	noPerm := validAgentPool("b3", ns)
	noPerm.Spec.Replicas = 1
	noPerm.Spec.CredentialBindings = nil
	noPerm.Spec.GatewayGrants = []v1alpha1.GatewayGrant{{Tool: "t", MaxEffectClass: "bogus"}}
	err = newReconciler(ca).validateSpec(ctx, noPerm)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maxEffectClass")

	// Missing CA Secret -> rejected.
	noCA := validAgentPool("b4", ns)
	noCA.Spec.Replicas = 1
	noCA.Spec.CredentialBindings = nil
	err = newReconciler().validateSpec(ctx, noCA) // no secrets seeded
	require.Error(t, err)
	assert.Contains(t, err.Error(), "caSecretRef")

	// Non-RWX access mode -> rejected.
	badAccess := validAgentPool("b5", ns)
	badAccess.Spec.Replicas = 1
	badAccess.Spec.CredentialBindings = nil
	badAccess.Spec.Sandbox.AccessMode = corev1.ReadWriteOnce
	err = newReconciler(ca, creds).validateSpec(ctx, badAccess)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReadWriteMany")
}

// TestAgentPoolGatewayMaterialization exercises the Git->DB bridge (HOR-245,
// ARCH-018/migration 000011): creating an AgentPool materializes the pool +
// grants + credential bindings into toolgateway; updating the CR removes stale
// grants (soft-delete); deleting the CR soft-deletes the pool + its rows so the
// gateway fails closed. Requires KUBEBUILDER_ASSETS (envtest) + Postgres.
func TestAgentPoolGatewayMaterialization(t *testing.T) {
	pgPool := testutil.NewPostgresPool(t)
	store := gateway.NewStore(pgPool)
	adminClient, ctx := newAgentPoolTestEnv(t, store)
	ns := "default"

	require.NoError(t, adminClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "platform-ca", Namespace: ns}, StringData: map[string]string{"tls.crt": "c", "tls.key": "k"}}))
	require.NoError(t, adminClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "graph-creds", Namespace: ns}, StringData: map[string]string{"token": "v"}}))

	pool := validAgentPool("gw-pool", ns)
	pool.Spec.Replicas = 0 // focus on gateway materialization, not pod assembly
	require.NoError(t, adminClient.Create(ctx, pool))

	spiffe := "spiffe://iterabase.local/pools/" + string(pool.UID) + "/workers/gw-pool-worker-0"
	key := "default/gw-pool"

	// Pool + grant + binding materialized (live).
	var p gateway.Pool
	require.Eventually(t, func() bool {
		got, err := store.ResolvePoolBySpiffePrefix(ctx, spiffe)
		if err != nil || got.Key != key {
			return false
		}
		p = got
		if _, err := store.GetPoolGrant(ctx, p.ID, "graph.read"); err != nil {
			return false
		}
		binds, err := store.ResolveCredentialBindings(ctx, p.ID, "graph.read")
		return err == nil && len(binds) == 1 && binds[0].SlotName == "graphToken" && binds[0].Scheme == gateway.CredBearer
	}, 15*time.Second, 200*time.Millisecond, "pool/grant/binding should be materialized")

	// Wait until the first reconciliation settles, then re-fetch + update the CR
	// (retry on conflict: the reconciler may still be patching status).
	require.Eventually(t, func() bool {
		var got v1alpha1.AgentPool
		if err := adminClient.Get(ctx, types.NamespacedName{Name: "gw-pool", Namespace: ns}, &got); err != nil {
			return false
		}
		if got.Status.ObservedGeneration != got.Generation {
			return false
		}
		got.Spec.GatewayGrants = nil
		got.Spec.CredentialBindings = []v1alpha1.CredentialBinding{
			{ToolName: "graph.read", Slot: "renamed", Scheme: "bearer", Bearer: &v1alpha1.BearerCredential{ValueSecretRef: v1alpha1.SecretKeyRef{Name: "graph-creds", Key: "token"}}},
		}
		return adminClient.Update(ctx, &got) == nil
	}, 15*time.Second, 200*time.Millisecond, "should update the AgentPool to drop the grant + rename the binding")

	require.Eventually(t, func() bool {
		if _, err := store.GetPoolGrant(ctx, p.ID, "graph.read"); err != gateway.ErrNotFound {
			return false // stale grant must be soft-deleted (not live)
		}
		binds, err := store.ResolveCredentialBindings(ctx, p.ID, "graph.read")
		return err == nil && len(binds) == 1 && binds[0].SlotName == "renamed"
	}, 15*time.Second, 200*time.Millisecond, "stale grant/binding should be soft-deleted; new binding live")

	// The old binding slot is soft-deleted (not returned by the live read above).
	// Direct check: deleted_at is set on the stale graphToken binding row.
	var staleDeleted *time.Time
	require.NoError(t, pgPool.QueryRow(ctx, `SELECT deleted_at FROM toolgateway.credential_bindings WHERE pool_id = $1 AND slot_name = 'graphToken'`, p.ID).Scan(&staleDeleted))
	require.NotNil(t, staleDeleted, "stale binding must be soft-deleted")

	// Delete the CR -> pool + rows soft-deleted (gateway fails closed).
	require.NoError(t, adminClient.Delete(ctx, pool))
	require.Eventually(t, func() bool {
		_, err := store.ResolvePoolBySpiffePrefix(ctx, spiffe)
		return err == gateway.ErrNotFound
	}, 15*time.Second, 200*time.Millisecond, "pool should be soft-deleted on CR deletion")
}
