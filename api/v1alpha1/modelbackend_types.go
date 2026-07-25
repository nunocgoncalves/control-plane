package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ModelBackendSpec defines the desired state of a ModelBackend: a compute
// backend that serves a model. For internal kinds (vLLM/SGLang) the reconciler
// deploys a GPU workload; for external it records a base URL. The ModelBackend
// owns the serving lifecycle only — the Model CRD (HOR-268) is the catalog
// offering that references it. The reconciler materializes this into the
// Postgres catalog.backends table (Git -> DB bridge).
// +kubebuilder:object:generate=true
type ModelBackendSpec struct {
	// kind is the backend kind. vLLM and SGLang deploy an internal GPU
	// workload; external records a base URL (reachability validation is
	// deferred to HOR-307).
	// +kubebuilder:validation:Enum=vLLM;SGLang;external
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// model is the HuggingFace model id the backend loads (vLLM/SGLang
	// `--model`). Required for vLLM/SGLang; ignored for external.
	// +optional
	Model string `json:"model,omitempty"`

	// image is the serving container image. If empty the reconciler applies a
	// default per kind (e.g. vllm/vllm-openai for vLLM).
	// +optional
	Image string `json:"image,omitempty"`

	// extraArgs are additional vLLM serving flags appended after the
	// controller-managed --model/--port/--host defaults. Use this to pass flags
	// the controller does not model, e.g. --quantization modelopt,
	// --max-model-len 262144, or --tool-call-parser qwen3_coder
	// --enable-auto-tool-choice. The controller-managed --port and --host may
	// not be overridden here: they back the Service + probe contract, so the
	// reconciler rejects any extraArgs setting either. Ignored for external.
	// +optional
	ExtraArgs []string `json:"extraArgs,omitempty"`

	// replicas is the desired pod count. v1 runs a single replica; multi-replica
	// (Tensor/Pipeline Parallel) is deferred to deepen.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// resources are the workload's resource requirements. If the GPU request is
	// absent the reconciler defaults nvidia.com/gpu to "1".
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// nodeSelector constrains the pod to GPU nodes. If empty the reconciler
	// defaults to {nvidia.com/gpu.present: "true"} (applied by the GPU
	// Operator's GFD).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// tolerations are applied to the pod. forge applies no default GPU taint,
	// so this is typically empty.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// devShmSize is the size limit of the memory-backed /dev/shm tmpfs the
	// reconciler mounts for vLLM (and, once implemented, SGLang) workloads.
	// Multimodal models that opt into --mm-processor-cache-type shm place the
	// processor cache in /dev/shm; without a sized mount the container inherits
	// the runtime default ~64 MiB tmpfs and crashes (sem_open ENOSPC) once the
	// cache exceeds it (HOR-382). Defaults to 2Gi when unset; the sizeLimit
	// counts against the pod's memory. Ignored for external.
	// +optional
	DevShmSize *resource.Quantity `json:"devShmSize,omitempty"`

	// healthProbe overrides the readiness/liveness probe target. Defaults to
	// GET /health on the serving port.
	// +optional
	HealthProbe *HealthProbeSpec `json:"healthProbe,omitempty"`

	// command overrides the serving container ENTRYPOINT. When set together
	// with args (or alone), the controller uses it verbatim. Use this for custom
	// vLLM builds whose entrypoint is not `vllm serve` (e.g. a wrapper script
	// or `python -m vllm.entrypoints.cli.main`). Ignored for external.
	// +optional
	Command []string `json:"command,omitempty"`

	// args overrides the serving container CMD. When set, the controller uses
	// it verbatim and SKIPS its controller-managed `--model/--port/--host`
	// assembly and the extraArgs validation — use this for custom vLLM builds
	// whose arg shape differs from `--model <id> --port --host <flags>` (e.g.
	// the positional `vllm ... serve <model>` shape, or a build that takes its
	// flags via a different CLI). When unset, the controller assembles
	// `--model <spec.model> --port <port> --host 0.0.0.0 <extraArgs>` as today.
	// The Service + probes always target servingPort (healthProbe.port, default
	// 8000); when args is overridden the deployer MUST ensure the server binds
	// that port — the readiness probe fails loud on a mismatch. Ignored for
	// external.
	// +optional
	Args []string `json:"args,omitempty"`

	// env are extra environment variables for the serving container, appended
	// after the controller-managed HF_HOME. The controller injects
	// HF_HOME=/data/hf-cache only if env does not already set it (user wins).
	// EnvVar.valueFrom is supported (e.g. an HF_TOKEN from a Secret for gated
	// models). Use this for build-tuning vars of custom vLLM builds
	// (e.g. CUTE_DSL_ARCH, VLLM_USE_AOT_COMPILE). Ignored for external.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// volumes are extra pod volumes, appended after the controller-managed
	// hf-cache (hostPath /data/hf-cache) and dshm (memory emptyDir /dev/shm)
	// volumes. The reserved names `hf-cache` and `dshm` may not be reused —
	// the reconciler rejects a CR that declares either. Use this for custom
	// vLLM builds that need file-artifact overlays (e.g. a ConfigMap of patch
	// .py files subPath-mounted over venv paths) or a PV. Ignored for external.
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`

	// volumeMounts are extra container volume mounts, appended after the
	// controller-managed hf-cache and dshm mounts. Reserved mount names
	// `hf-cache` and `dshm` may not be reused. Pair with volumes to overlay
	// file artifacts onto the serving image. Ignored for external.
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`

	// hostIPC sets pod-level hostIPC. Default false. vLLM tensor-parallel on a
	// single multi-GPU pod normally only needs the sized /dev/shm tmpfs
	// (see devShmSize); flip this on only if a custom build's NCCL/CUDA IPC
	// proves to need the host IPC namespace. Ignored for external.
	// +optional
	HostIPC bool `json:"hostIPC,omitempty"`

	// securityContext is the container-level SecurityContext applied to the
	// serving container (corev1.SecurityContext passthrough). Use this for
	// capabilities custom vLLM builds need — notably `capabilities.add:
	// [IPC_LOCK]` for NCCL tensor-parallel, where CAP_IPC_LOCK bypasses
	// RLIMIT_MEMLOCK (the K8s equivalent of docker's `--ulimit memlock=-1`; a
	// bash `ulimit -l unlimited` wrapper is a no-op without CAP_SYS_RESOURCE
	// since the hard limit is capped). Also covers runAsUser,
	// readOnlyRootFilesystem, etc. for other custom deployments. Ignored for
	// external.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// external configures an external (non-deployed) backend. Required when
	// kind is external.
	// +optional
	External *ExternalBackendSpec `json:"external,omitempty"`
}

// HealthProbeSpec configures the backend health probe.
// +kubebuilder:object:generate=true
type HealthProbeSpec struct {
	// path is the HTTP path probed (default /health).
	// +optional
	Path string `json:"path,omitempty"`

	// port is the probed port (default 8000). It is the sole source of truth for
	// the serving port: the Service, readiness/liveness probes, and the
	// controller-managed --port (when args is not overridden) all target it.
	// +optional
	Port int32 `json:"port,omitempty"`

	// startupTimeoutSeconds is the total time the startupProbe grants vLLM to
	// download weights + load them into GPU before liveness can kill the pod.
	// The controller renders periodSeconds=10, failureThreshold=ceil(this/10).
	// Defaults to 600 (10 minutes). Raise for large/long-context models whose
	// warmup is slow (e.g. a 1M-context preset can take ~19 min).
	// +optional
	StartupTimeoutSeconds int32 `json:"startupTimeoutSeconds,omitempty"`
}

// ExternalBackendSpec configures an external provider backend.
// +kubebuilder:object:generate=true
type ExternalBackendSpec struct {
	// baseURL is the OpenAI-compatible endpoint of the external provider.
	// +kubebuilder:validation:Required
	BaseURL string `json:"baseURL"`

	// authRef is the name of a Kubernetes Secret holding the provider
	// credential (key API key/token).
	// +optional
	AuthRef string `json:"authRef,omitempty"`
}

// ModelBackendStatus is the observed state reported by the reconciler.
// +kubebuilder:object:generate=true
type ModelBackendStatus struct {
	// deployed is true once the workload + Service are reconciled (internal) or
	// the external entry is recorded.
	// +optional
	Deployed bool `json:"deployed,omitempty"`

	// healthy is true once the workload is reporting ready (internal). For
	// external, healthy is assumed true in the skeleton (reachability deferred
	// to HOR-307).
	// +optional
	Healthy bool `json:"healthy,omitempty"`

	// serviceURL is the in-cluster address the gateway routes to
	// (<name>.<ns>.svc:<port> for internal; baseURL for external).
	// +optional
	ServiceURL string `json:"serviceURL,omitempty"`

	// lastReconciled is the time of the last successful reconciliation.
	// +optional
	LastReconciled *metav1.Time `json:"lastReconciled,omitempty"`

	// observedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// message surfaces the last reconciliation error or notice. Empty on success.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=modelbackends,scope=Namespaced,shortName=mb
// +kubebuilder:singular=modelbackend
//
// ModelBackend is a compute/provider backend that serves a model. The
// control-plane operator deploys internal vLLM/SGLang workloads or records an
// external provider endpoint, and materializes the backend into the catalog
// (catalog.backends) for the Model CRD (HOR-268) to reference.
type ModelBackend struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelBackendSpec   `json:"spec,omitempty"`
	Status ModelBackendStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// ModelBackendList is a list of ModelBackend.
type ModelBackendList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ModelBackend `json:"items"`
}
