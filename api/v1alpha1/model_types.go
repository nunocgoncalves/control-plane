package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ModelSpec defines the desired state of a Model: a catalog offering the
// inference-gateway consumes. It references a ModelBackend (HOR-306) that serves
// it and carries per-alias request config (translating the gateway's
// registry.Model). Multiple Models may reference one ModelBackend (aliases). The
// reconciler materializes this into catalog.models (Git -> DB bridge).
// +kubebuilder:object:generate=true
type ModelSpec struct {
	// modelID is the client-facing alias clients put in the OpenAI `model` field
	// (e.g. "qwen3-27b-thinking"). Unique per namespace.
	// +kubebuilder:validation:Required
	ModelID string `json:"modelID"`

	// displayName is a human-readable name.
	// +optional
	DisplayName string `json:"displayName,omitempty"`

	// contextLength is the model's maximum context window (tokens).
	// +optional
	ContextLength int `json:"contextLength,omitempty"`

	// capabilities advertise what the model supports (e.g. ["chat","tools"]).
	// +optional
	Capabilities []string `json:"capabilities,omitempty"`

	// backendRef is the name of the ModelBackend (same namespace) that serves
	// this model.
	// +kubebuilder:validation:Required
	BackendRef string `json:"backendRef"`

	// defaultParams are default sampling parameters injected when the client
	// omits them. Pointer fields keep "not set" distinct from zero.
	// +optional
	DefaultParams ModelDefaultParams `json:"defaultParams,omitempty"`

	// reasoningConfig controls reasoning/thinking behaviour (forwarded to vLLM
	// as chat_template_kwargs.enable_thinking).
	// +optional
	ReasoningConfig ModelReasoningConfig `json:"reasoningConfig,omitempty"`

	// transforms define request/response transformation rules applied per alias.
	// +optional
	Transforms ModelTransforms `json:"transforms,omitempty"`

	// rateLimits are per-alias rate-limit overrides.
	// +optional
	RateLimits ModelRateLimits `json:"rateLimits,omitempty"`
}

// ModelDefaultParams are default sampling parameters (pointer fields keep "not
// set" distinct from zero). Mirrors the gateway's registry.DefaultParams.
// +kubebuilder:object:generate=true
type ModelDefaultParams struct {
	// +optional
	Temperature *float64 `json:"temperature,omitempty"`
	// +optional
	TopP *float64 `json:"top_p,omitempty"`
	// +optional
	MaxTokens *int `json:"max_tokens,omitempty"`
	// +optional
	FrequencyPenalty *float64 `json:"frequency_penalty,omitempty"`
	// +optional
	PresencePenalty *float64 `json:"presence_penalty,omitempty"`
	// +optional
	RepetitionPenalty *float64 `json:"repetition_penalty,omitempty"`
	// +optional
	TopK *int `json:"top_k,omitempty"`
	// +optional
	MinP *float64 `json:"min_p,omitempty"`
	// +optional
	Stop []string `json:"stop,omitempty"`
}

// ModelReasoningConfig controls reasoning/thinking behaviour.
// EnableThinking == nil → passthrough; true → force on; false → force off.
// Mirrors the gateway's registry.ReasoningConfig.
// +kubebuilder:object:generate=true
type ModelReasoningConfig struct {
	// +optional
	EnableThinking *bool `json:"enable_thinking,omitempty"`
}

// ModelTransforms define request/response transformation rules per alias.
// Mirrors the gateway's registry.Transforms.
// +kubebuilder:object:generate=true
type ModelTransforms struct {
	// +optional
	SystemPromptPrefix string `json:"system_prompt_prefix,omitempty"`
	// RewriteModelName rewrites the request `model` field from the alias to the
	// backend's served id (the gateway's existing transform).
	// +optional
	RewriteModelName bool `json:"rewrite_model_name,omitempty"`
}

// ModelRateLimits are per-alias rate-limit overrides.
// Mirrors the gateway's registry.ModelRateLimits.
// +kubebuilder:object:generate=true
type ModelRateLimits struct {
	// +optional
	RPM *int `json:"rpm,omitempty"`
	// +optional
	TPM *int `json:"tpm,omitempty"`
}

// ModelStatus is the observed state reported by the reconciler.
// +kubebuilder:object:generate=true
type ModelStatus struct {
	// available is true once the referenced ModelBackend is deployed+healthy.
	// +optional
	Available bool `json:"available,omitempty"`

	// healthy mirrors the referenced ModelBackend's health.
	// +optional
	Healthy bool `json:"healthy,omitempty"`

	// lastChecked is the time health was last evaluated.
	// +optional
	LastChecked *metav1.Time `json:"lastChecked,omitempty"`

	// observedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// message surfaces the last reconciliation error or notice. Empty on success.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=models,scope=Namespaced,shortName=mdl
// +kubebuilder:singular=model
//
// Model is a catalog offering the inference-gateway consumes. It references a
// ModelBackend (HOR-306) and carries per-alias request config. The control-plane
// operator materializes it into catalog.models; the effective_catalog view joins
// Model -> ModelBackend for the gateway (HOR-247) to read directly.
type Model struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelSpec   `json:"spec,omitempty"`
	Status ModelStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// ModelList is a list of Model.
type ModelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Model `json:"items"`
}
