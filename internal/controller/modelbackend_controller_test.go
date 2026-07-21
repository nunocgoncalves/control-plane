package controller

import (
	"context"
	"fmt"
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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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

		// GPU-safe rollout (HOR-378): maxSurge=0 + maxUnavailable=1 so a
		// single-GPU node doesn't deadlock (new pod Pending while the old pod
		// holds the only nvidia.com/gpu); the grace period lets vLLM drain on SIGTERM.
		assert.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, dep.Spec.Strategy.Type)
		require.NotNil(t, dep.Spec.Strategy.RollingUpdate)
		assert.Equal(t, "0", dep.Spec.Strategy.RollingUpdate.MaxSurge.String(),
			"maxSurge must be 0 to avoid surging a pod the GPU can't satisfy")
		assert.Equal(t, "1", dep.Spec.Strategy.RollingUpdate.MaxUnavailable.String())
		require.NotNil(t, dep.Spec.Template.Spec.TerminationGracePeriodSeconds)
		assert.Equal(t, int64(defaultTerminationGracePeriodSeconds), *dep.Spec.Template.Spec.TerminationGracePeriodSeconds)

		// Sized /dev/shm (HOR-382): a memory-backed emptyDir at /dev/shm so
		// --mm-processor-cache-type shm doesn't exhaust the runtime default
		// ~64 MiB tmpfs (sem_open ENOSPC, CrashLoopBackOff before binding :8000).
		// Default sizeLimit is 2Gi.
		var dshmVol *corev1.Volume
		for i := range dep.Spec.Template.Spec.Volumes {
			if dep.Spec.Template.Spec.Volumes[i].Name == devShmVolumeName {
				dshmVol = &dep.Spec.Template.Spec.Volumes[i]
				break
			}
		}
		require.NotNil(t, dshmVol, "vLLM pod must mount a dshm volume")
		require.NotNil(t, dshmVol.EmptyDir, "dshm must be an emptyDir")
		assert.Equal(t, corev1.StorageMediumMemory, dshmVol.EmptyDir.Medium,
			"dshm must be memory-backed (tmpfs), not node disk")
		require.NotNil(t, dshmVol.EmptyDir.SizeLimit)
		assert.Equal(t, "2Gi", dshmVol.EmptyDir.SizeLimit.String(),
			"default /dev/shm sizeLimit must be 2Gi")
		var dshmMount *corev1.VolumeMount
		for i := range c.VolumeMounts {
			if c.VolumeMounts[i].Name == devShmVolumeName {
				dshmMount = &c.VolumeMounts[i]
				break
			}
		}
		require.NotNil(t, dshmMount, "vLLM container must mount dshm")
		assert.Equal(t, devShmMountPath, dshmMount.MountPath)

		// The Service exposes the serving port and selects the workload.
		var svc corev1.Service
		require.NoError(t, adminClient.Get(ctx, nn, &svc))
		require.Len(t, svc.Spec.Ports, 1)
		assert.Equal(t, int32(defaultServingPort), svc.Spec.Ports[0].Port)
		assert.Equal(t, "vllm-qwen", svc.Spec.Selector["platform.iterabase.com/modelbackend"])

		// The backend is materialized in catalog.backends. healthy stays false:
		// envtest has no kubelet, so no pod ever becomes Ready (deploymentHealthy
		// gates on ReadyReplicas, not the Available condition). Real serving is
		// validated on the GPU VM + forge GPU E2E (HOR-324).
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

	// extraArgs (HOR-370) are appended after the controller-managed
	// --model/--port/--host; --port/--host overrides are rejected (Service +
	// probe contract).
	t.Run("vLLM appends spec.extraArgs after the controller-managed args", func(t *testing.T) {
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "vllm-extraargs", Namespace: "default"},
			Spec: v1alpha1.ModelBackendSpec{
				Kind:  "vLLM",
				Model: "Qwen/Qwen3-27B",
				ExtraArgs: []string{
					"--quantization", "modelopt",
					"--max-model-len", "262144",
				},
			},
		}
		require.NoError(t, adminClient.Create(ctx, mb))
		nn := types.NamespacedName{Name: "vllm-extraargs", Namespace: "default"}

		require.Eventually(t, func() bool {
			var got v1alpha1.ModelBackend
			return adminClient.Get(ctx, nn, &got) == nil && got.Status.Deployed
		}, 15*time.Second, 200*time.Millisecond, "ModelBackend with extraArgs should report deployed")

		var dep appsv1.Deployment
		require.NoError(t, adminClient.Get(ctx, nn, &dep))
		c := dep.Spec.Template.Spec.Containers[0]
		// extraArgs are appended after the controller-managed --model/--port/--host.
		assert.Equal(t, []string{
			"--model", "Qwen/Qwen3-27B",
			"--port", fmt.Sprintf("%d", defaultServingPort),
			"--host", "0.0.0.0",
			"--quantization", "modelopt",
			"--max-model-len", "262144",
		}, c.Args, "extraArgs must be appended after the controller-managed defaults")
	})

	t.Run("vLLM rejects --port/--host overrides in spec.extraArgs", func(t *testing.T) {
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "vllm-badargs", Namespace: "default"},
			Spec: v1alpha1.ModelBackendSpec{
				Kind:  "vLLM",
				Model: "Qwen/Qwen3-27B",
				// --port is controller-managed (backs the Service + probes); rejected.
				ExtraArgs: []string{"--port", "9000"},
			},
		}
		require.NoError(t, adminClient.Create(ctx, mb))
		nn := types.NamespacedName{Name: "vllm-badargs", Namespace: "default"}

		require.Eventually(t, func() bool {
			var got v1alpha1.ModelBackend
			if err := adminClient.Get(ctx, nn, &got); err != nil {
				return false
			}
			return !got.Status.Deployed && got.Status.Message != ""
		}, 15*time.Second, 200*time.Millisecond, "ModelBackend with --port in extraArgs should be rejected")

		var got v1alpha1.ModelBackend
		require.NoError(t, adminClient.Get(ctx, nn, &got))
		assert.False(t, got.Status.Deployed, "rejected extraArgs must not deploy a workload")
		assert.Contains(t, got.Status.Message, "--port")
		assert.Contains(t, got.Status.Message, "extraArgs")

		// No Deployment is created for a rejected backend.
		var dep appsv1.Deployment
		assert.True(t, errors.IsNotFound(adminClient.Get(ctx, nn, &dep)),
			"rejected extraArgs must not create a Deployment")
	})
}

// TestValidateExtraArgs covers the controller-managed --port/--host rejection
// (both space- and =-separated forms) without needing envtest.
func TestValidateExtraArgs(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "empty", args: nil},
		{name: "valid serving flags", args: []string{"--quantization", "modelopt", "--max-model-len", "262144"}},
		{name: "valid =-form flag", args: []string{"--quantization=modelopt"}},
		{name: "rejects --port space form", args: []string{"--port", "9000"}, wantErr: "--port"},
		{name: "rejects --host space form", args: []string{"--host", "0.0.0.0"}, wantErr: "--host"},
		{name: "rejects --port= form", args: []string{"--port=9000"}, wantErr: "--port"},
		{name: "rejects --host= form", args: []string{"--host=0.0.0.0"}, wantErr: "--host"},
		{name: "rejects after valid flags", args: []string{"--max-model-len", "262144", "--port", "9000"}, wantErr: "--port"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateExtraArgs(tc.args)
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestDeploymentHealthy guards the HOR-378 regression: with maxUnavailable=1 on
// a 1-replica Deployment, the Deployment's Available condition is True even with
// zero ready pods (minAvailable = replicas - maxUnavailable = 0). healthy must
// gate on ReadyReplicas (a pod passed /health), not the Available condition.
func TestDeploymentHealthy(t *testing.T) {
	mb := &v1alpha1.ModelBackend{ObjectMeta: metav1.ObjectMeta{Name: "mb", Namespace: "ns"}}

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	cases := []struct {
		name string
		dep  *appsv1.Deployment
		want bool
	}{
		{
			name: "Available=True but 0 ready (HOR-378 maxUnavailable=1, pod pulling/FailedCreate) -> false",
			dep: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "mb", Namespace: "ns"},
				Status: appsv1.DeploymentStatus{ReadyReplicas: 0, AvailableReplicas: 0, Conditions: []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable"},
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetCreated"},
					{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate"},
				}}},
			want: false,
		},
		{
			name: "1 ready pod -> true",
			dep: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "mb", Namespace: "ns"},
				Status: appsv1.DeploymentStatus{ReadyReplicas: 1, AvailableReplicas: 1}},
			want: true,
		},
		{
			name: "Available=False, 0 ready -> false",
			dep: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "mb", Namespace: "ns"},
				Status: appsv1.DeploymentStatus{ReadyReplicas: 0, Conditions: []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse, Reason: "MinimumReplicasUnavailable"},
				}}},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.dep).Build()
			r := &ModelBackendReconciler{Client: cl, Scheme: scheme}
			assert.Equal(t, tc.want, r.deploymentHealthy(context.Background(), mb))
		})
	}

	t.Run("deployment missing -> false", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).Build() // no Deployment
		r := &ModelBackendReconciler{Client: cl, Scheme: scheme}
		assert.False(t, r.deploymentHealthy(context.Background(), mb))
	})
}

// TestBuildDeploymentSpecDevShm guards HOR-382: the vLLM pod must mount a
// memory-backed /dev/shm so --mm-processor-cache-type shm doesn't exhaust the
// runtime default ~64 MiB tmpfs (sem_open ENOSPC, CrashLoopBackOff). The default
// sizeLimit is 2Gi; spec.devShmSize overrides it. Pure (no envtest/Postgres).
func TestBuildDeploymentSpecDevShm(t *testing.T) {
	port := int32(defaultServingPort)
	quantityPtr := func(s string) *resource.Quantity {
		q := resource.MustParse(s)
		return &q
	}
	findVol := func(spec appsv1.DeploymentSpec, name string) *corev1.Volume {
		for i := range spec.Template.Spec.Volumes {
			if spec.Template.Spec.Volumes[i].Name == name {
				return &spec.Template.Spec.Volumes[i]
			}
		}
		return nil
	}
	findMount := func(c corev1.Container, name string) *corev1.VolumeMount {
		for i := range c.VolumeMounts {
			if c.VolumeMounts[i].Name == name {
				return &c.VolumeMounts[i]
			}
		}
		return nil
	}

	t.Run("defaults to a 2Gi memory-backed /dev/shm", func(t *testing.T) {
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "vllm-shm", Namespace: "default"},
			Spec:       v1alpha1.ModelBackendSpec{Kind: "vLLM", Model: "Qwen/Qwen3-27B"},
		}
		spec := buildDeploymentSpec(mb, port)

		// The hf-cache hostPath volume must be preserved alongside the new dshm.
		assert.NotNil(t, findVol(spec, "hf-cache"), "hf-cache volume must be preserved")

		vol := findVol(spec, devShmVolumeName)
		require.NotNil(t, vol, "vLLM pod must mount a dshm volume")
		require.NotNil(t, vol.EmptyDir, "dshm must be an emptyDir")
		assert.Equal(t, corev1.StorageMediumMemory, vol.EmptyDir.Medium,
			"dshm must be memory-backed (tmpfs), not node disk")
		require.NotNil(t, vol.EmptyDir.SizeLimit)
		assert.Equal(t, "2Gi", vol.EmptyDir.SizeLimit.String(),
			"default /dev/shm sizeLimit must be 2Gi")

		mount := findMount(spec.Template.Spec.Containers[0], devShmVolumeName)
		require.NotNil(t, mount, "server container must mount dshm")
		assert.Equal(t, devShmMountPath, mount.MountPath)
	})

	t.Run("spec.devShmSize overrides the 2Gi default", func(t *testing.T) {
		mb := &v1alpha1.ModelBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "vllm-shm-override", Namespace: "default"},
			Spec: v1alpha1.ModelBackendSpec{
				Kind:       "vLLM",
				Model:      "Qwen/Qwen3-27B",
				DevShmSize: quantityPtr("8Gi"),
			},
		}
		spec := buildDeploymentSpec(mb, port)

		vol := findVol(spec, devShmVolumeName)
		require.NotNil(t, vol)
		require.NotNil(t, vol.EmptyDir.SizeLimit)
		assert.Equal(t, "8Gi", vol.EmptyDir.SizeLimit.String(),
			"spec.devShmSize must override the 2Gi default")
	})
}
