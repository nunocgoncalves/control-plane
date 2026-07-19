package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EgressRouteSpec defines the desired state of an EgressRoute: one upstream
// the per-sandbox egress proxy (HOR-244) may forward to, plus how it injects
// the real credential on the wire (credentials-as-scope). The reconciler
// materializes this into the Postgres egress store; the AgentSandbox operator
// (HOR-245) resolves the effective routes for a sandbox's scope identity via
// internal/egress.Resolve and bakes them into the proxy. The credential value
// never lives here or in Postgres -- only the K8s Secret reference; the value
// stays in a Secret mounted into the proxy.
// +kubebuilder:object:generate=true
type EgressRouteSpec struct {
	// upstream is the real target the proxy forwards to. The proxy terminates
	// the inbound leg (TLS, localhost) and originates a fresh outbound TLS
	// connection here.
	// +kubebuilder:validation:Required
	Upstream UpstreamSpec `json:"upstream"`

	// auth is how the proxy injects the real credential. The proxy strips any
	// inbound Authorization/api-key (the harness/tool sends a placeholder or
	// none) and injects this. scheme selects which fields apply.
	// +kubebuilder:validation:Required
	Auth AuthSpec `json:"auth"`

	// subject scopes the route to an identity/group. Omitted (or "*") =
	// broad-default (every active identity) in v1. A specific subject is
	// stored but NOT enforced in v1 (the effective_routes view ignores it);
	// group-scoped narrowing is additive later (SSO/HOR-314).
	// +optional
	Subject *SubjectSpec `json:"subject,omitempty"`
}

// UpstreamSpec names the real target the proxy forwards to.
// +kubebuilder:object:generate=true
type UpstreamSpec struct {
	// baseURL is the absolute upstream origin, e.g. https://graph.microsoft.com.
	// The proxy forwards /upstreams/<route-id>/<rest> -> <baseURL>/<rest>.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^https?://`
	BaseURL string `json:"baseURL"`
}

// AuthSpec configures credential injection for a route. scheme selects the
// fields; the reconciler stores the whole block as JSONB in egress.credentials.
// +kubebuilder:object:generate=true
type AuthSpec struct {
	// scheme is the credential-injection scheme.
	// +kubebuilder:validation:Enum=bearer;oauthClientCredentials
	// +kubebuilder:validation:Required
	Scheme string `json:"scheme"`

	// secretRef names the K8s Secret key holding a static bearer token.
	// Required when scheme is bearer.
	// +optional
	SecretRef *SecretRef `json:"secretRef,omitempty"`

	// tokenURL is the OAuth2 token endpoint. Required when scheme is
	// oauthClientCredentials.
	// +optional
	TokenURL string `json:"tokenURL,omitempty"`

	// clientID is the OAuth2 client id. Required when scheme is
	// oauthClientCredentials.
	// +optional
	ClientID string `json:"clientID,omitempty"`

	// clientSecretRef names the K8s Secret key holding the OAuth2 client
	// secret. Required when scheme is oauthClientCredentials.
	// +optional
	ClientSecretRef *SecretRef `json:"clientSecretRef,omitempty"`

	// scope is the OAuth2 scope requested. Optional.
	// +optional
	Scope string `json:"scope,omitempty"`
}

// SecretRef names a key within a K8s Secret. The proxy reads the value from a
// mounted volume at /secrets/<name>/<key> (convention; the AgentSandbox
// operator mounts each referenced Secret to /secrets/<name>/).
// +kubebuilder:object:generate=true
type SecretRef struct {
	// name is the Kubernetes Secret name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// key is the key within the Secret.
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// SubjectSpec (defined in permissionpolicy_types.go) is reused for EgressRoute
// subjects — same shape (kind + key), stored but not enforced in v1.

// EgressRouteStatus is the observed state reported by the reconciler.
// +kubebuilder:object:generate=true
type EgressRouteStatus struct {
	// ready is true once the route is materialized in Postgres.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// observedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// message surfaces the last reconciliation error. Empty on success.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=egressroutes,scope=Namespaced,shortName=er
// +kubebuilder:singular=egressroute
//
// EgressRoute declares one upstream the per-sandbox egress proxy may forward
// to, plus how it injects the real credential. The control-plane operator
// materializes it into the Postgres egress store; the AgentSandbox operator
// (HOR-245) resolves the effective routes per sandbox scope identity.
type EgressRoute struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EgressRouteSpec   `json:"spec,omitempty"`
	Status EgressRouteStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// EgressRouteList is a list of EgressRoute.
type EgressRouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []EgressRoute `json:"items"`
}
