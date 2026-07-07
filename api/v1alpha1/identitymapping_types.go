package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IdentityMappingSpec defines the desired state of an IdentityMapping: an
// identity and the external provider bindings that resolve to it. The
// reconciler materializes this into the Postgres identity store; permission
// fields belong to PermissionPolicy (HOR-243), not here.
// +kubebuilder:object:generate=true
type IdentityMappingSpec struct {
	// identity describes the platform identity this mapping creates.
	// +kubebuilder:validation:Required
	Identity IdentitySpec `json:"identity"`

	// bindings are the external provider IDs that resolve to this identity.
	// +optional
	Bindings []Binding `json:"bindings,omitempty"`
}

// IdentitySpec describes the identity created by an IdentityMapping.
// +kubebuilder:object:generate=true
type IdentitySpec struct {
	// kind is the identity kind.
	// +kubebuilder:validation:Enum=user;group
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// displayName is a human-readable name.
	// +optional
	DisplayName string `json:"displayName,omitempty"`
}

// Binding pairs an identity to an external provider ID.
// +kubebuilder:object:generate=true
type Binding struct {
	// provider is the external provider.
	// +kubebuilder:validation:Enum=slack;teams
	// +kubebuilder:validation:Required
	Provider string `json:"provider"`

	// type is the binding type.
	// +kubebuilder:validation:Enum=user;group
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// externalID is the provider-specific identifier.
	// +kubebuilder:validation:Required
	ExternalID string `json:"externalID"`
}

// IdentityMappingStatus is the observed state reported by the reconciler.
// +kubebuilder:object:generate=true
type IdentityMappingStatus struct {
	// ready is true once the identity + bindings are materialized in Postgres.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// identityID is the UUID of the materialized identity row.
	// +optional
	IdentityID string `json:"identityID,omitempty"`

	// observedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// message surfaces the last reconciliation error (e.g. a binding claimed by
	// another identity). Empty on success.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=identitymappings,scope=Namespaced,shortName=im
// +kubebuilder:singular=identitymapping
//
// IdentityMapping maps an identity to its external provider bindings. The
// control-plane operator materializes it into the Postgres identity store.
type IdentityMapping struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IdentityMappingSpec   `json:"spec,omitempty"`
	Status IdentityMappingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// IdentityMappingList is a list of IdentityMapping.
type IdentityMappingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []IdentityMapping `json:"items"`
}
