package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/catalog"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// newCatalogStore returns a Store backed by a fresh migrated Postgres.
func newCatalogStore(t *testing.T) *catalog.Store {
	t.Helper()
	return catalog.NewStore(testutil.NewPostgresPool(t))
}

// TestModelBackendReconcile exercises the ModelBackend reconciler UNDER RBAC:
// the reconciler runs as a ServiceAccount bound to the generated role.yaml.
// vLLM deploys a GPU workload + Service and materializes catalog.backends;
// external records a baseURL with no workload; SGLang is a recognized stub.
// Deleting a CR soft-deletes its catalog row. Requires Docker + KUBEBUILDER_ASSETS.
func TestModelBackendReconcile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run envtest (make setup-envtest)")
	}

	store := newCatalogStore(t)
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

	mgr, err := ctrl.NewManager(saCfg, ctrl.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, (&ModelBackendReconciler{
		Client: mgr.GetClient(),
		Scheme: scheme,
		Store:  store,
	}).SetupWithManager(mgr))

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(mgrCtx) }()

	t.Run("vLLM deploys a GPU workload and materializes catalog.backends", func(t *testing.T) {
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "vllm-qwen", Namespace: "default"},
			Spec: v1alpha1.ModelBackendSpec{
				Kind:  "vLLM",
				Model: "Qwen/Qwen3-27B",
				// image left empty to exercise the default.
			},
		}
		require.NoError(t, adminClient.Create(ctx, mb))
		nn := types.NamespacedName{Name: "vllm-qwen", Namespace: "default"}

		// Wait for deployed=true (Deployment + Service reconciled under RBAC).
		require.Eventually(t, func() bool {
			var got v1alpha1.ModelBackend
			if err := adminClient.Get(ctx, nn, &got); err != nil {
				return false
			}
			return got.Status.Deployed
		}, 15*time.Second, 200*time.Millisecond, "ModelBackend should report deployed")

		// The Deployment carries the forge GPU contract (HOR-240):
		// runtimeClassName nvidia + nvidia.com/gpu + GPU node selector + /health probe.
		var dep appsv1.Deployment
		require.NoError(t, adminClient.Get(ctx, nn, &dep))
		require.NotNil(t, dep.Spec.Template.Spec.RuntimeClassName)
		assert.Equal(t, "nvidia", *dep.Spec.Template.Spec.RuntimeClassName)
		assert.Equal(t, "true", dep.Spec.Template.Spec.NodeSelector["nvidia.com/gpu.present"])

		c := dep.Spec.Template.Spec.Containers[0]
		assert.Equal(t, defaultVLLMImage, c.Image, "empty spec.image should default")
		assert.Contains(t, c.Args, "--model")
		assert.Contains(t, c.Args, "Qwen/Qwen3-27B")
		gpu := corev1.ResourceName("nvidia.com/gpu")
		gpuLimit := c.Resources.Limits[gpu]
		gpuRequest := c.Resources.Requests[gpu]
		assert.Equal(t, "1", gpuLimit.String(), "GPU limit should default to 1")
		assert.Equal(t, "1", gpuRequest.String(), "GPU request should default to 1")
		require.NotNil(t, c.ReadinessProbe)
		require.NotNil(t, c.ReadinessProbe.HTTPGet)
		assert.Equal(t, "/health", c.ReadinessProbe.HTTPGet.Path)
		// startupProbe gives vLLM time to download + load the model before the
		// liveness probe can kill it (HOR-338).
		require.NotNil(t, c.StartupProbe, "vLLM pod must have a startupProbe")
		assert.Equal(t, int32(60), c.StartupProbe.FailureThreshold, "startupProbe should allow ~10m for model download + GPU load")

		// The Service exposes the serving port and selects the workload.
		var svc corev1.Service
		require.NoError(t, adminClient.Get(ctx, nn, &svc))
		require.Len(t, svc.Spec.Ports, 1)
		assert.Equal(t, int32(defaultServingPort), svc.Spec.Ports[0].Port)
		assert.Equal(t, "vllm-qwen", svc.Spec.Selector["platform.iterabase.com/modelbackend"])

		// The backend is materialized in catalog.backends. healthy stays false:
		// envtest has no kubelet, so the Deployment never becomes Available.
		// (Real serving is validated on the GPU VM + forge GPU E2E, HOR-324.)
		b, err := store.GetBackendByKey(ctx, "default/vllm-qwen")
		require.NoError(t, err)
		assert.Equal(t, "vLLM", b.Kind)
		assert.Equal(t, "Qwen/Qwen3-27B", b.Model)
		assert.Equal(t, "http://vllm-qwen.default.svc:8000", b.ServiceURL)
		assert.True(t, b.Deployed)
		assert.False(t, b.Healthy, "healthy must stay false without a real running pod")

		// Deleting the CR soft-deletes the catalog row (finalizer cleanup under RBAC).
		require.NoError(t, adminClient.Delete(ctx, mb))
		require.Eventually(t, func() bool {
			var got v1alpha1.ModelBackend
			return errors.IsNotFound(adminClient.Get(ctx, nn, &got))
		}, 15*time.Second, 200*time.Millisecond, "ModelBackend should be deleted after finalizer cleanup")
		_, err = store.GetBackendByKey(ctx, "default/vllm-qwen")
		assert.ErrorIs(t, err, catalog.ErrNotFound, "soft-deleted backend should not be active")
	})

	t.Run("external records a baseURL with no workload", func(t *testing.T) {
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "ext-anthropic", Namespace: "default"},
			Spec: v1alpha1.ModelBackendSpec{
				Kind: "external",
				External: &v1alpha1.ExternalBackendSpec{
					BaseURL: "https://api.anthropic.com",
					AuthRef: "anthropic-key",
				},
			},
		}
		require.NoError(t, adminClient.Create(ctx, mb))
		nn := types.NamespacedName{Name: "ext-anthropic", Namespace: "default"}

		require.Eventually(t, func() bool {
			var got v1alpha1.ModelBackend
			if err := adminClient.Get(ctx, nn, &got); err != nil {
				return false
			}
			return got.Status.Deployed && got.Status.Healthy
		}, 15*time.Second, 200*time.Millisecond, "external backend should be deployed+healthy (skeleton)")

		// No Deployment is created for an external backend.
		var dep appsv1.Deployment
		assert.True(t, errors.IsNotFound(adminClient.Get(ctx, nn, &dep)), "external must not deploy a workload")

		b, err := store.GetBackendByKey(ctx, "default/ext-anthropic")
		require.NoError(t, err)
		assert.Equal(t, "external", b.Kind)
		assert.Equal(t, "https://api.anthropic.com", b.ServiceURL)
		assert.True(t, b.Deployed)
		assert.True(t, b.Healthy, "external healthy assumed true; reachability deferred to HOR-307")
	})

	t.Run("SGLang is a recognized stub", func(t *testing.T) {
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "sglang-stub", Namespace: "default"},
			Spec:       v1alpha1.ModelBackendSpec{Kind: "SGLang", Model: "Qwen/Qwen3-27B"},
		}
		require.NoError(t, adminClient.Create(ctx, mb))
		nn := types.NamespacedName{Name: "sglang-stub", Namespace: "default"}

		require.Eventually(t, func() bool {
			var got v1alpha1.ModelBackend
			if err := adminClient.Get(ctx, nn, &got); err != nil {
				return false
			}
			return !got.Status.Deployed && got.Status.Message != ""
		}, 15*time.Second, 200*time.Millisecond, "SGLang should report a stub message")

		var got v1alpha1.ModelBackend
		require.NoError(t, adminClient.Get(ctx, nn, &got))
		assert.Contains(t, got.Status.Message, "HOR-323")

		b, err := store.GetBackendByKey(ctx, "default/sglang-stub")
		require.NoError(t, err)
		assert.False(t, b.Deployed)
		assert.False(t, b.Healthy)
	})

	// Sanity: the resource defaulting path is exercised when GPU is pre-set.
	t.Run("spec.resources GPU is preserved", func(t *testing.T) {
		two := resource.MustParse("2")
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "vllm-twogpu", Namespace: "default"},
			Spec: v1alpha1.ModelBackendSpec{
				Kind:  "vLLM",
				Model: "Qwen/Qwen3-27B",
				Resources: corev1.ResourceRequirements{
					Limits:   corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): two},
					Requests: corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): two},
				},
			},
		}
		require.NoError(t, adminClient.Create(ctx, mb))
		nn := types.NamespacedName{Name: "vllm-twogpu", Namespace: "default"}

		require.Eventually(t, func() bool {
			var got v1alpha1.ModelBackend
			return adminClient.Get(ctx, nn, &got) == nil && got.Status.Deployed
		}, 15*time.Second, 200*time.Millisecond, "should report deployed")

		var dep appsv1.Deployment
		require.NoError(t, adminClient.Get(ctx, nn, &dep))
		gpu := corev1.ResourceName("nvidia.com/gpu")
		twoGpu := dep.Spec.Template.Spec.Containers[0].Resources.Limits[gpu]
		assert.Equal(t, "2", twoGpu.String(),
			"a pre-set GPU request must not be overwritten by the default")
	})
}
