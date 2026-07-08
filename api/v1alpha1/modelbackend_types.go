package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
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

	// healthProbe overrides the readiness/liveness probe target. Defaults to
	// GET /health on the serving port.
	// +optional
	HealthProbe *HealthProbeSpec `json:"healthProbe,omitempty"`

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

	// port is the probed port (default 8000).
	// +optional
	Port int32 `json:"port,omitempty"`
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
