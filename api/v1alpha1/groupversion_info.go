// Package v1alpha1 contains the control-plane Custom Resource Definitions for
// platform.iterabase.com/v1alpha1.
//
// +kubebuilder:object:generate=true
// +groupName=platform.iterabase.com
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group/version for control-plane CRDs.
	GroupVersion = schema.GroupVersion{Group: "platform.iterabase.com", Version: "v1alpha1"}

	// SchemeBuilder registers types with the runtime scheme. Uses
	// runtime.SchemeBuilder (not the deprecated controller-runtime
	// scheme.Builder) so the api package depends only on apimachinery.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds all types in this group/version to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers the IdentityMapping types with the scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &IdentityMapping{}, &IdentityMappingList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
