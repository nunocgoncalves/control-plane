package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentPoolSpec defines the desired state of an AgentPool: a bounded set of
// isolated warm-worker pods that execute Platform-v1 turns with the approved
// supervisor/child model, fixed local-workspace boundary, and maximum gateway
// permissions — without proxy sidecars or customer credentials in the sandbox
// (ARCH-003/009/010/016/018). The operator provisions the RWX sandbox PVC,
// per-pod SPIFFE certs (via the cert-manager CSI driver), the rendered harness
// boot config, and a deny-by-default NetworkPolicy. HOR-249 owns dispatch and
// the warm-pool scaling/credit protocol; HOR-245 only maintains a static
// `replicas` warm set.
// +kubebuilder:object:generate=true
type AgentPoolSpec struct {
	// replicas is the bounded warm-worker pod count the operator maintains. v1
	// reconciles a static set of named pods (<pool>-worker-<i>); dynamic
	// scale-out/credit-based dispatch is HOR-249.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	Replicas int32 `json:"replicas"`

	// workerImage is the harness container image (HOR-351/381): the trusted
	// supervisor + the per-turn pi child. Never baked with customer data or
	// credentials.
	// +kubebuilder:validation:Required
	WorkerImage string `json:"workerImage"`

	// terminationGracePeriodSeconds is the pod termination grace, sized to let
	// the supervisor abort/kill the child and flush the audit WAL (HOR-381).
	// Defaults to 30.
	// +kubebuilder:validation:Minimum=1
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// resources are the worker pod's resource requirements (supervisor + child
	// share the pod). Applied to the supervisor container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// podSecurity is the target Pod Security Standard level the rendered worker
	// pod satisfies. Must be `baseline`: the per-turn child UID/GID launcher
	// (HOR-381 setpriv) requires CAP_SETUID/CAP_SETGID on the supervisor, which
	// PSS `restricted` forbids. The operator validates the rendered pod meets
	// this level.
	// +kubebuilder:validation:Enum=baseline
	// +kubebuilder:validation:Required
	PodSecurity string `json:"podSecurity"`

	// identity configures the worker SPIFFE/mTLS identity. Per-pod leaf certs
	// are issued by the cert-manager CSI driver (annotated on each pod) with
	// URI SAN spiffe://<trustDomain>/pools/<pool-uid>/workers/<pod-name>; the
	// operator never holds the CA key (cert-manager owns the CA). Servers
	// (gateway/inference/future Work server) trust caSecretRef's cert as
	// ClientCAs. The child UID cannot read the key (fsGroup + dropped groups).
	// +kubebuilder:validation:Required
	Identity PoolIdentitySpec `json:"identity"`

	// sandbox configures the shared RWX PVC that backs per-session sandboxes.
	// Each session gets a 0700 subdir owned by its stable session UID/GID
	// (provisioning/ownership enforced by the supervisor, HOR-381); the pool
	// PVC request is the per-pool storage quota.
	// +kubebuilder:validation:Required
	Sandbox SandboxSpec `json:"sandbox"`

	// gateways are the supervisor's platform endpoints (ARCH-010 — separate
	// domain gateways, one worker SPIFFE cert). The disposable child receives
	// none of these; it only talks the supervisor over fd 4/fd 5.
	// +kubebuilder:validation:Required
	Gateways PoolGatewaysSpec `json:"gateways"`

	// networkPolicy configures the worker pod NetworkPolicy (ARCH-003). Egress
	// is deny-by-default; `denied` (default) allows only kube-dns + the three
	// gateways + kube API; `internet` additionally allows outbound to
	// non-cluster Internet (per-pool operator opt-in for e.g. coding agents
	// fetching packages). Customer-system credentialed access still routes
	// through the gateway regardless — the sandbox never holds customer creds.
	// +optional
	NetworkPolicy NetworkPolicySpec `json:"networkPolicy,omitempty"`

	// workspaceTools is the deny-by-default local-workspace tool switch
	// (ARCH-016). false (default) exposes no local tools (normal operational
	// workflow mode); true exposes exactly pi's built-in read/write/edit/bash as
	// one capability set. No wildcard, custom package, partial selection, or
	// per-turn widening. These tools retain session UID/GID isolation and no
	// arbitrary customer-system access.
	// +optional
	WorkspaceTools bool `json:"workspaceTools,omitempty"`

	// gatewayGrants are the maximum gateway capability permissions available
	// to work dispatched to this pool (ARCH-018). A turn/workflow cannot widen
	// these. The gateway intersects deployed registry availability, these pool
	// grants, workflow-requested capabilities, and durable customer/identity
	// policy at discovery (HOR-392). Semantic validation that a granted tool is
	// registered is the gateway's responsibility (HOR-392/397); the operator
	// validates structure only (required fields, no duplicate tools).
	// +optional
	GatewayGrants []GatewayGrant `json:"gatewayGrants,omitempty"`

	// credentialBindings bind logical credential slots (declared by runner tool
	// manifests, ARCH-008/018) to K8s Secret references. The agent supplies
	// business arguments only and cannot select a secret; the gateway resolves
	// the binding at invocation. Secret VALUES never enter the CRD or Postgres —
	// only the reference. The operator validates the referenced Secret exists.
	// +optional
	CredentialBindings []CredentialBinding `json:"credentialBindings,omitempty"`

	// resourceConstraints are non-secret resource scopes/constraints bound at
	// the pool level (ARCH-018), e.g. a tenant/site/drive the pool is scoped to.
	// +optional
	ResourceConstraints []ResourceConstraint `json:"resourceConstraints,omitempty"`

	// piDirs are the read-only overlay paths mounted into the worker (skills +
	// the built-in model/gateway bridges). Arbitrary sandbox-local tool
	// extensions are removed (ARCH-016); authored skills remain. Defaults to
	// ["/pi/product", "/pi/client"].
	// +optional
	PiDirs []string `json:"piDirs,omitempty"`

	// walDir is the supervisor's WAL spool directory (an emptyDir). Defaults to
	// "/var/harness/wal".
	// +optional
	WalDir string `json:"walDir,omitempty"`

	// probe is the plain-HTTP kubelet probe port (/healthz + /readyz).
	// +optional
	Probe PoolProbeSpec `json:"probe,omitempty"`

	// poolScopeIdentityId is an optional pool-level scope identity for
	// defense-in-depth: the supervisor validates AssignTurn's scope_identity_id
	// against it. Maps to the harness boot config's poolScopeIdentityId.
	// +optional
	PoolScopeIdentityId string `json:"poolScopeIdentityId,omitempty"`
}

// PoolIdentitySpec configures the worker SPIFFE/mTLS identity.
// +kubebuilder:object:generate=true
type PoolIdentitySpec struct {
	// trustDomain is the SPIFFE trust domain. Defaults to "iterabase.local".
	// +optional
	TrustDomain string `json:"trustDomain,omitempty"`

	// caSecretRef names the K8s Secret holding the platform CA (cert+key),
	// issued by cert-manager. The cert-manager CSI driver uses a ClusterIssuer
	// backed by this CA to mint per-pod leaf certs; the operator never reads
	// the CA key. Servers trust this CA's cert as ClientCAs.
	// +kubebuilder:validation:Required
	CASecretRef LocalKeyRef `json:"caSecretRef"`

	// certMountPath is where the CSI driver materialises tls.crt/tls.key/ca.crt
	// in the supervisor. Defaults to "/etc/harness/tls". Mounted readOnly; the
	// key is child-inaccessible via fsGroup + dropped supplementary groups.
	// +optional
	CertMountPath string `json:"certMountPath,omitempty"`
}

// SandboxSpec configures the shared RWX sandbox PVC.
// +kubebuilder:object:generate=true
type SandboxSpec struct {
	// storageClassName is the RWX-capable StorageClass. The operator validates
	// it exists and supports ReadWriteMany.
	// +kubebuilder:validation:Required
	StorageClassName string `json:"storageClassName"`

	// accessMode must be ReadWriteMany (the warm pool shares one PVC; per-turn
	// children of different sessions run concurrently across pods).
	// +kubebuilder:validation:Enum=ReadWriteMany
	// +kubebuilder:validation:Required
	AccessMode corev1.PersistentVolumeAccessMode `json:"accessMode"`

	// size is the PVC request (the per-pool storage quota).
	// +kubebuilder:validation:Required
	Size resource.Quantity `json:"size"`

	// mountPath is where the PVC is mounted in the worker. Defaults to
	// "/data/sandboxes". Per-session subdirs are created beneath this by the
	// supervisor (HOR-381).
	// +optional
	MountPath string `json:"mountPath,omitempty"`
}

// PoolGatewaysSpec names the supervisor's platform endpoints.
// +kubebuilder:object:generate=true
type PoolGatewaysSpec struct {
	// controlPlane is the Work bidi-stream gRPC server (HOR-249).
	// +kubebuilder:validation:Required
	ControlPlane GatewayEndpoint `json:"controlPlane"`

	// toolGateway is the GatewayService gRPC endpoint (HOR-392).
	// +kubebuilder:validation:Required
	ToolGateway GatewayEndpoint `json:"toolGateway"`

	// inferenceGateway is the /v1/chat/completions workload mTLS listener
	// (HOR-398).
	// +kubebuilder:validation:Required
	InferenceGateway GatewayEndpoint `json:"inferenceGateway"`
}

// GatewayEndpoint is an mTLS gateway URL + expected server cert SAN.
// +kubebuilder:object:generate=true
type GatewayEndpoint struct {
	// url is the absolute endpoint, e.g. https://control-plane:8443.
	// +kubebuilder:validation:Pattern=`^https://`
	// +kubebuilder:validation:Required
	URL string `json:"url"`

	// serverName is the expected server certificate SAN.
	// +kubebuilder:validation:Required
	ServerName string `json:"serverName"`
}

// NetworkPolicySpec configures worker pod egress.
// +kubebuilder:object:generate=true
type NetworkPolicySpec struct {
	// egress selects the egress mode. `denied` (default) allows only kube-dns +
	// the three gateways + kube API; `internet` additionally allows outbound to
	// non-cluster Internet (per-pool operator opt-in). Customer-system
	// credentialed access still routes through the gateway regardless.
	// +kubebuilder:validation:Enum=denied;internet
	// +optional
	Egress string `json:"egress,omitempty"`
}

// GatewayGrant is one maximum gateway capability permission (ARCH-018).
// +kubebuilder:object:generate=true
type GatewayGrant struct {
	// tool is the logical gateway tool/capability name (e.g. "graph.read").
	// +kubebuilder:validation:Required
	Tool string `json:"tool"`

	// permissions are the maximum permissions granted for this tool. A turn
	// cannot widen them. Must be non-empty; the gateway intersects with
	// workflow/customer policy at discovery.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:Required
	Permissions []string `json:"permissions"`
}

// CredentialBinding binds a logical credential slot to a K8s Secret key.
// +kubebuilder:object:generate=true
type CredentialBinding struct {
	// slot is the logical credential slot name declared by a runner tool
	// manifest (ARCH-008). The gateway resolves it at invocation.
	// +kubebuilder:validation:Required
	Slot string `json:"slot"`

	// secretRef names the K8s Secret + key holding the credential VALUE. The
	// value never enters the CRD or Postgres. The operator validates the Secret
	// exists.
	// +kubebuilder:validation:Required
	SecretRef SecretKeyRef `json:"secretRef"`
}

// SecretKeyRef names a key within a K8s Secret in the pool's namespace.
// +kubebuilder:object:generate=true
type SecretKeyRef struct {
	// name is the Kubernetes Secret name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// key is the key within the Secret.
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// LocalKeyRef names a K8s Secret in the pool's namespace (no key).
// +kubebuilder:object:generate=true
type LocalKeyRef struct {
	// name is the Kubernetes Secret name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// ResourceConstraint is a non-secret pool-level resource scope (ARCH-018).
// +kubebuilder:object:generate=true
type ResourceConstraint struct {
	// resource is the logical resource name (e.g. "site", "tenant").
	// +kubebuilder:validation:Required
	Resource string `json:"resource"`

	// value is the non-secret resource value (e.g. "walter").
	// +kubebuilder:validation:Required
	Value string `json:"value"`
}

// PoolProbeSpec is the worker kubelet probe port.
// +kubebuilder:object:generate=true
type PoolProbeSpec struct {
	// port is the plain-HTTP probe port (/healthz + /readyz). Defaults to 8081.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Port int32 `json:"port,omitempty"`
}

// AgentPoolStatus is the observed state reported by the reconciler.
// +kubebuilder:object:generate=true
type AgentPoolStatus struct {
	// ready is true once the warm-worker pods + PVC + NetworkPolicy are
	// reconciled and at least one pod is Ready (envtest has no kubelet, so this
	// stays false there — real readiness is validated on-cluster).
	// +optional
	Ready bool `json:"ready,omitempty"`

	// readyReplicas is the number of warm-worker pods reporting Ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// observedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// message surfaces the last reconciliation error or notice. Empty on success.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=agentpools,scope=Namespaced,shortName=ap
// +kubebuilder:singular=agentpool
//
// AgentPool is the deployable workflow security/integration boundary (ARCH-018):
// it provisions isolated warm-worker pods and declares the maximum gateway
// capability grants + logical credential-slot bindings for work dispatched to
// the pool. No separate Tool/EgressRoute/IntegrationBinding CRD exists in v1.
type AgentPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentPoolSpec   `json:"spec,omitempty"`
	Status AgentPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// AgentPoolList is a list of AgentPool.
type AgentPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []AgentPool `json:"items"`
}

// DefaultEgressMode is the deny-by-default NetworkPolicy egress mode.
const DefaultEgressMode = "denied"

// DefaultTrustDomain is the SPIFFE trust domain (mirrors internal/spiffe).
const DefaultTrustDomain = "iterabase.local"
