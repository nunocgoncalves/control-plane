package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PermissionPolicySpec defines the desired state of a PermissionPolicy: the
// subject it applies to. The control-plane operator materializes it into the
// Postgres permissions store. In v1 (broad-default mode) policy contents are
// stored but not enforced — every linked (active) identity gets all
// capabilities via the permissions.effective_capabilities view; fine-grained
// scopes (PermissionPolicy.scopes) land in deepen-phase.
// +kubebuilder:object:generate=true
type PermissionPolicySpec struct {
	// subject is the identity or group this policy targets (by kind + key).
	// +kubebuilder:validation:Required
	Subject SubjectSpec `json:"subject"`
}

// SubjectSpec identifies the subject a PermissionPolicy applies to. key
// references an identity key (e.g. an IdentityMapping "<ns>/<name>") or a group.
// +kubebuilder:object:generate=true
type SubjectSpec struct {
	// kind is the subject's identity kind.
	// +kubebuilder:validation:Enum=user;group;service_account;workflow
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// key is the subject's identity key (e.g. "default/alice").
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// PermissionPolicyStatus is the observed state reported by the reconciler.
// +kubebuilder:object:generate=true
type PermissionPolicyStatus struct {
	// ready is true once the policy is materialized in Postgres.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// policyID is the UUID of the materialized policy row.
	// +optional
	PolicyID string `json:"policyID,omitempty"`

	// observedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// message surfaces the last reconciliation error. Empty on success.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=permissionpolicies,scope=Namespaced,shortName=pp
// +kubebuilder:singular=permissionpolicy
//
// PermissionPolicy declares a permission policy for a subject. The control-plane
// operator materializes it into the Postgres permissions store.
type PermissionPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PermissionPolicySpec   `json:"spec,omitempty"`
	Status PermissionPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// PermissionPolicyList is a list of PermissionPolicy.
type PermissionPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []PermissionPolicy `json:"items"`
}
