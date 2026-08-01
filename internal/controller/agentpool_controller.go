package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	yaml "sigs.k8s.io/yaml"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
)

const (
	agentPoolFinalizer = "platform.iterabase.com/agentpool-finalizer"

	// defaultTerminationGrace gives the supervisor time to abort/kill the child
	// and fsync the audit WAL before the kubelet force-kills the pod (HOR-381).
	defaultAgentPoolTerminationGrace int64 = 30
	// defaultProbePort mirrors the harness probe default (harness/src/config.ts).
	defaultAgentPoolProbePort int32 = 8081
	// supervisorUID is the supervisor's runAsUser. Root (0) is required so the
	// supervisor can read the cert-manager CSI driver's root-owned 0600 tls.key
	// and launch the per-turn child as the session UID via setpriv. The child
	// (session UID, supplementary groups cleared, no_new_privs) cannot read the
	// root-owned key. PSS baseline permits runAsUser=0 + CAP_SETUID/SETGID.
	supervisorUID int64 = 0
)

// AgentPoolReconciler maintains a bounded set of isolated warm-worker pods +
// the shared RWX sandbox PVC + a deny-by-default NetworkPolicy for each
// AgentPool CR (HOR-245). Per-pod SPIFFE certs are issued by the cert-manager
// CSI driver (annotated on each pod); the operator never holds the CA key.
type AgentPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles AgentPool create/update/delete events.
//
// nolint:gocyclo
func (r *AgentPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pool v1alpha1.AgentPool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: finalizer cleanup. Owned pods/configmaps/pvc/networkpolicy
	// are garbage-collected by the owner ref, but the finalizer guarantees we
	// don't leave dangling named children if GC is delayed.
	if !pool.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&pool, agentPoolFinalizer) {
			controllerutil.RemoveFinalizer(&pool, agentPoolFinalizer)
			if err := r.Update(ctx, &pool); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("finalized AgentPool")
		}
		return ctrl.Result{}, nil
	}

	// Validate (structural) before doing anything. A validation error is
	// surfaced in status and does not requeue — the user must fix the CR.
	if err := r.validateSpec(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("validation: %v", err))
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&pool, agentPoolFinalizer) {
		controllerutil.AddFinalizer(&pool, agentPoolFinalizer)
		if err := r.Update(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.ensurePVC(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("ensure PVC: %v", err))
		return ctrl.Result{}, err
	}
	if err := r.ensureNetworkPolicy(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("ensure NetworkPolicy: %v", err))
		return ctrl.Result{}, err
	}
	if err := r.reconcileWorkers(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("ensure workers: %v", err))
		return ctrl.Result{}, err
	}

	readyReplicas := r.countReadyWorkers(ctx, &pool)
	ready := readyReplicas > 0 || pool.Spec.Replicas == 0
	msg := ""
	if pool.Spec.Replicas == 0 {
		msg = "scaled to zero"
	} else if !ready {
		msg = "waiting for worker pods to become Ready"
	}
	if err := r.patchStatus(ctx, &pool, ready, readyReplicas, msg); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("reconciled AgentPool", "replicas", pool.Spec.Replicas, "ready", readyReplicas)
	// Requeue to refresh readyReplicas as pods come and go (envtest has no
	// kubelet, so Ready stays false there; real readiness is on-cluster).
	return ctrl.Result{RequeueAfter: healthRequeueSeconds}, nil
}

// validateSpec validates structural correctness (ARCH-018): required fields,
// RWX access mode, egress mode, no duplicate gateway grants, and existence of
// referenced Secrets (credential bindings + CA). Semantic validation that a
// granted tool is registered is the gateway's responsibility (HOR-392/397).
//
// nolint:gocyclo
func (r *AgentPoolReconciler) validateSpec(ctx context.Context, pool *v1alpha1.AgentPool) error {
	if pool.Spec.Replicas < 0 {
		return fmt.Errorf("spec.replicas must be >= 0")
	}
	if pool.Spec.WorkerImage == "" {
		return fmt.Errorf("spec.workerImage is required")
	}
	if pool.Spec.PodSecurity != "baseline" {
		return fmt.Errorf("spec.podSecurity must be \"baseline\" (per-turn UID launch requires CAP_SETUID/SETGID, which PSS restricted forbids)")
	}
	if pool.Spec.Sandbox.StorageClassName == "" {
		return fmt.Errorf("spec.sandbox.storageClassName is required")
	}
	if pool.Spec.Sandbox.AccessMode != corev1.ReadWriteMany {
		return fmt.Errorf("spec.sandbox.accessMode must be ReadWriteMany")
	}
	if pool.Spec.Sandbox.Size.IsZero() {
		return fmt.Errorf("spec.sandbox.size is required and must be positive")
	}
	if pool.Spec.Identity.CASecretRef.Name == "" {
		return fmt.Errorf("spec.identity.caSecretRef.name is required")
	}
	// CA Secret existence.
	if err := r.secretExists(ctx, pool.Namespace, pool.Spec.Identity.CASecretRef.Name); err != nil {
		return fmt.Errorf("caSecretRef: %w", err)
	}
	// Gateways.
	for name, ep := range map[string]v1alpha1.GatewayEndpoint{
		"controlPlane":     pool.Spec.Gateways.ControlPlane,
		"toolGateway":      pool.Spec.Gateways.ToolGateway,
		"inferenceGateway": pool.Spec.Gateways.InferenceGateway,
	} {
		if ep.URL == "" {
			return fmt.Errorf("spec.gateways.%s.url is required", name)
		}
		if !strings.HasPrefix(ep.URL, "https://") {
			return fmt.Errorf("spec.gateways.%s.url must be https://", name)
		}
		if ep.ServerName == "" {
			return fmt.Errorf("spec.gateways.%s.serverName is required", name)
		}
	}
	// Egress mode (CRD enum already constrains, but default-then-check).
	switch pool.Spec.NetworkPolicy.Egress {
	case "", "denied", "internet":
	default:
		return fmt.Errorf("spec.networkPolicy.egress must be \"denied\" or \"internet\"")
	}
	// Gateway grants: no duplicate tools, non-empty permissions.
	seen := make(map[string]bool)
	for i, g := range pool.Spec.GatewayGrants {
		if g.Tool == "" {
			return fmt.Errorf("spec.gatewayGrants[%d].tool is required", i)
		}
		if seen[g.Tool] {
			return fmt.Errorf("spec.gatewayGrants[%d].tool %q is duplicated", i, g.Tool)
		}
		seen[g.Tool] = true
		if len(g.Permissions) == 0 {
			return fmt.Errorf("spec.gatewayGrants[%d].permissions must be non-empty", i)
		}
		for _, p := range g.Permissions {
			if strings.TrimSpace(p) == "" {
				return fmt.Errorf("spec.gatewayGrants[%d].permissions contains an empty value", i)
			}
		}
	}
	// Credential bindings: required fields + Secret existence.
	for i, b := range pool.Spec.CredentialBindings {
		if b.Slot == "" {
			return fmt.Errorf("spec.credentialBindings[%d].slot is required", i)
		}
		if b.SecretRef.Name == "" || b.SecretRef.Key == "" {
			return fmt.Errorf("spec.credentialBindings[%d].secretRef name+key are required", i)
		}
		if err := r.secretExists(ctx, pool.Namespace, b.SecretRef.Name); err != nil {
			return fmt.Errorf("credentialBindings[%d].secretRef: %w", i, err)
		}
	}
	// Resource constraints: required fields.
	for i, c := range pool.Spec.ResourceConstraints {
		if c.Resource == "" {
			return fmt.Errorf("spec.resourceConstraints[%d].resource is required", i)
		}
		if c.Value == "" {
			return fmt.Errorf("spec.resourceConstraints[%d].value is required", i)
		}
	}
	return nil
}

// secretExists returns nil if the Secret is present, or a descriptive error.
func (r *AgentPoolReconciler) secretExists(ctx context.Context, ns, name string) error {
	var s corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &s); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("secret %s/%s not found", ns, name)
		}
		return err
	}
	return nil
}

// ensurePVC creates/updates the shared RWX sandbox PVC.
func (r *AgentPoolReconciler) ensurePVC(ctx context.Context, pool *v1alpha1.AgentPool) error {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: sandboxPVCName(pool), Namespace: pool.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		if err := controllerutil.SetControllerReference(pool, pvc, r.Scheme); err != nil {
			return err
		}
		sc := pool.Spec.Sandbox.StorageClassName
		access := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		pvc.Spec.AccessModes = access
		pvc.Spec.StorageClassName = &sc
		pvc.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: pool.Spec.Sandbox.Size}
		return nil
	})
	return err
}

// ensureNetworkPolicy creates/updates the deny-by-default worker egress policy.
func (r *AgentPoolReconciler) ensureNetworkPolicy(ctx context.Context, pool *v1alpha1.AgentPool) error {
	np := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: networkPolicyName(pool), Namespace: pool.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		if err := controllerutil.SetControllerReference(pool, np, r.Scheme); err != nil {
			return err
		}
		np.Spec = buildNetworkPolicySpec(pool)
		return nil
	})
	return err
}

// reconcileWorkers ensures exactly `replicas` named worker pods (+ their
// per-pod config ConfigMaps) and deletes surplus ones.
func (r *AgentPoolReconciler) reconcileWorkers(ctx context.Context, pool *v1alpha1.AgentPool) error {
	// Ensure desired workers.
	for i := int32(0); i < pool.Spec.Replicas; i++ {
		name := workerName(pool, i)
		if err := r.ensureWorkerConfig(ctx, pool, name); err != nil {
			return fmt.Errorf("worker %s config: %w", name, err)
		}
		if err := r.ensureWorkerPod(ctx, pool, name); err != nil {
			return fmt.Errorf("worker %s pod: %w", name, err)
		}
	}
	// Delete surplus workers (replicas decreased).
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(pool.Namespace), client.MatchingLabels(poolLabels(pool))); err != nil {
		return err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if !isDesiredWorker(pool, p.Name) {
			if err := r.Delete(ctx, p); err != nil && !errors.IsNotFound(err) {
				return err
			}
			// Best-effort config cleanup; owned ConfigMaps are GC'd via owner
			// ref, but explicit delete avoids relying on GC timing.
			_ = r.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: p.Name + "-config", Namespace: pool.Namespace}})
		}
	}
	return nil
}

func (r *AgentPoolReconciler) ensureWorkerConfig(ctx context.Context, pool *v1alpha1.AgentPool, name string) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-config", Namespace: pool.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if err := controllerutil.SetControllerReference(pool, cm, r.Scheme); err != nil {
			return err
		}
		cm.Labels = poolLabels(pool)
		cm.Data = map[string]string{"config.yaml": renderHarnessConfig(pool, name)}
		return nil
	})
	return err
}

func (r *AgentPoolReconciler) ensureWorkerPod(ctx context.Context, pool *v1alpha1.AgentPool, name string) error {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pool.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pod, func() error {
		if err := controllerutil.SetControllerReference(pool, pod, r.Scheme); err != nil {
			return err
		}
		pod.Labels = poolLabels(pool)
		pod.Labels["platform.iterabase.com/worker"] = name
		pod.Spec = buildWorkerPodSpec(pool, name)
		return nil
	})
	return err
}

// countReadyWorkers counts worker pods reporting Ready.
func (r *AgentPoolReconciler) countReadyWorkers(ctx context.Context, pool *v1alpha1.AgentPool) int32 {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(pool.Namespace), client.MatchingLabels(poolLabels(pool))); err != nil {
		return 0
	}
	var n int32
	for i := range pods.Items {
		if podIsReady(&pods.Items[i]) {
			n++
		}
	}
	return n
}

// buildWorkerPodSpec renders the warm-worker pod: supervisor container with
// the cert-manager CSI TLS volume, the shared RWX sandbox PVC, the WAL
// emptyDir, the per-pod config ConfigMap, and read-only piDirs (placeholder
// mounts; overlay content is HOR-393). The supervisor runs as root (UID 0) to
// read the CSI driver's root-owned 0600 key and launch the per-turn child as
// the session UID via setpriv (PSS baseline: CAP_SETUID/SETGID allowed).
func buildWorkerPodSpec(pool *v1alpha1.AgentPool, name string) corev1.PodSpec {
	probePort := pool.Spec.Probe.Port
	if probePort == 0 {
		probePort = defaultAgentPoolProbePort
	}
	termGrace := defaultAgentPoolTerminationGrace
	if pool.Spec.TerminationGracePeriodSeconds != nil && *pool.Spec.TerminationGracePeriodSeconds > 0 {
		termGrace = *pool.Spec.TerminationGracePeriodSeconds
	}
	certMount := pool.Spec.Identity.CertMountPath
	if certMount == "" {
		certMount = "/etc/harness/tls"
	}
	sandboxMount := pool.Spec.Sandbox.MountPath
	if sandboxMount == "" {
		sandboxMount = "/data/sandboxes"
	}
	walDir := pool.Spec.WalDir
	if walDir == "" {
		walDir = "/var/harness/wal"
	}
	piDirs := pool.Spec.PiDirs
	if len(piDirs) == 0 {
		piDirs = []string{"/pi/product", "/pi/client"}
	}
	trustDomain := pool.Spec.Identity.TrustDomain
	if trustDomain == "" {
		trustDomain = v1alpha1.DefaultTrustDomain
	}

	volumes := []corev1.Volume{
		{
			Name: "harness-tls",
			VolumeSource: corev1.VolumeSource{
				CSI: &corev1.CSIVolumeSource{
					Driver:   "csi.cert-manager.io",
					ReadOnly: boolPtr(true),
					VolumeAttributes: map[string]string{
						"csi.cert-manager.io/issuer-name": "platform-spiffe-ca",
						"csi.cert-manager.io/issuer-kind": "ClusterIssuer",
						// URI SAN binds pool + worker slot identity (ARCH-010).
						// worker_id = pod name (stable slot); HOR-249's
						// connection-generation fence handles instance freshness.
						"csi.cert-manager.io/uri-sans": fmt.Sprintf("spiffe://%s/pools/%s/workers/%s", trustDomain, string(pool.UID), name),
						"csi.cert-manager.io/duration": "24h",
					},
				},
			},
		},
		{Name: "sandbox", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: sandboxPVCName(pool)}}},
		{Name: "wal", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{
			Name:         "config",
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: name + "-config"}}},
		},
	}
	mounts := []corev1.VolumeMount{
		{Name: "harness-tls", MountPath: certMount, ReadOnly: true},
		{Name: "sandbox", MountPath: sandboxMount},
		{Name: "wal", MountPath: walDir},
		{Name: "config", MountPath: "/etc/harness"},
	}
	for i, d := range piDirs {
		vn := fmt.Sprintf("pi-%d", i)
		volumes = append(volumes, corev1.Volume{Name: vn, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		mounts = append(mounts, corev1.VolumeMount{Name: vn, MountPath: d, ReadOnly: true})
	}

	setuid := corev1.Capability("SETUID")
	setgid := corev1.Capability("SETGID")
	resources := pool.Spec.Resources
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}

	return corev1.PodSpec{
		TerminationGracePeriodSeconds: &termGrace,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsUser:      int64Ptr(supervisorUID),
			RunAsGroup:     int64Ptr(supervisorUID),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Volumes: volumes,
		Containers: []corev1.Container{
			{
				Name:           "supervisor",
				Image:          pool.Spec.WorkerImage,
				Env:            []corev1.EnvVar{{Name: "HARNESS_CONFIG", Value: "/etc/harness/config.yaml"}},
				Ports:          []corev1.ContainerPort{{ContainerPort: probePort, Name: "probe"}},
				VolumeMounts:   mounts,
				Resources:      resources,
				ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromInt32(probePort)}}},
				LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(probePort)}}},
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:                int64Ptr(supervisorUID),
					RunAsGroup:               int64Ptr(supervisorUID),
					AllowPrivilegeEscalation: boolPtr(false),
					Capabilities: &corev1.Capabilities{
						Add: []corev1.Capability{setuid, setgid}, // PSS baseline allows the default cap set (incl. SETUID/SETGID).
					},
				},
			},
		},
	}
}

// buildNetworkPolicySpec renders a deny-by-default egress policy selecting the
// pool's worker pods. `denied` allows only kube-dns + the pool namespace (the
// three gateways run there); `internet` additionally allows outbound to
// non-cluster Internet (RFC1918/loopback/link-local excluded, so credentialed
// customer systems on private IPs remain reachable only via the gateway).
func buildNetworkPolicySpec(pool *v1alpha1.AgentPool) networkingv1.NetworkPolicySpec {
	dnsPort := intstr.FromInt(53)
	udp := corev1.ProtocolUDP
	tcp := corev1.ProtocolTCP
	egress := []networkingv1.NetworkPolicyEgressRule{
		// kube-dns (Service resolution for the gateway hostnames).
		{
			To: []networkingv1.NetworkPolicyPeer{{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"}},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{Port: &dnsPort, Protocol: &udp}, {Port: &dnsPort, Protocol: &tcp}},
		},
		// The three gateways run in the pool's namespace (control-plane, tool
		// gateway, inference gateway). The harness is a gRPC client to them;
		// it is not a Kubernetes API client, so no kube-API egress is needed.
		{
			To: []networkingv1.NetworkPolicyPeer{{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": pool.Namespace}},
			}},
		},
	}
	if pool.Spec.NetworkPolicy.Egress == "internet" {
		egress = append(egress, networkingv1.NetworkPolicyEgressRule{
			To: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{
					CIDR: "0.0.0.0/0",
					Except: []string{
						"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", // RFC1918 (cluster + customer private nets)
						"169.254.0.0/16", // link-local
						"127.0.0.0/8",    // loopback
					},
				},
			}},
		})
	}
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: poolLabels(pool)},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
		Egress:      egress,
	}
}

// renderHarnessConfig renders the infra-only harness boot config.yaml for one
// worker pod (workerId = pod name; poolId = pool UID). The harness applies its
// own defaults for transport/reconnect/child/outbox/modelRetry/tokenDelta, so
// only the infra fields are rendered (harness/src/config.ts).
func renderHarnessConfig(pool *v1alpha1.AgentPool, workerName string) string {
	probePort := pool.Spec.Probe.Port
	if probePort == 0 {
		probePort = defaultAgentPoolProbePort
	}
	sandboxMount := pool.Spec.Sandbox.MountPath
	if sandboxMount == "" {
		sandboxMount = "/data/sandboxes"
	}
	walDir := pool.Spec.WalDir
	if walDir == "" {
		walDir = "/var/harness/wal"
	}
	certMount := pool.Spec.Identity.CertMountPath
	if certMount == "" {
		certMount = "/etc/harness/tls"
	}
	piDirs := pool.Spec.PiDirs
	if len(piDirs) == 0 {
		piDirs = []string{"/pi/product", "/pi/client"}
	}
	cfg := map[string]any{
		"controlPlane":     map[string]any{"url": pool.Spec.Gateways.ControlPlane.URL, "serverName": pool.Spec.Gateways.ControlPlane.ServerName},
		"worker":           map[string]any{"workerId": workerName, "poolId": string(pool.UID)},
		"tls":              map[string]any{"cert": certMount + "/tls.crt", "key": certMount + "/tls.key", "ca": certMount + "/ca.crt"},
		"sandboxRoot":      sandboxMount,
		"piDirs":           piDirs,
		"toolGateway":      map[string]any{"url": pool.Spec.Gateways.ToolGateway.URL, "serverName": pool.Spec.Gateways.ToolGateway.ServerName},
		"inferenceGateway": map[string]any{"url": pool.Spec.Gateways.InferenceGateway.URL, "serverName": pool.Spec.Gateways.InferenceGateway.ServerName},
		"walDir":           walDir,
		"probe":            map[string]any{"port": probePort},
	}
	if pool.Spec.WorkspaceTools {
		cfg["poolWorkspaceTools"] = true
	}
	if pool.Spec.PoolScopeIdentityId != "" {
		cfg["poolScopeIdentityId"] = pool.Spec.PoolScopeIdentityId
	}
	b, err := yaml.Marshal(cfg)
	if err != nil {
		// map[string]any marshals; this is unreachable in practice.
		return fmt.Sprintf("# render error: %v\n", err)
	}
	return string(b)
}

// poolLabels are the labels shared by the pool's worker pods (and used as the
// NetworkPolicy + list selector).
func poolLabels(pool *v1alpha1.AgentPool) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by":     "control-plane",
		"platform.iterabase.com/agentpool": pool.Name,
	}
}

func sandboxPVCName(pool *v1alpha1.AgentPool) string    { return pool.Name + "-sandbox" }
func networkPolicyName(pool *v1alpha1.AgentPool) string { return pool.Name + "-worker-egress" }
func workerName(pool *v1alpha1.AgentPool, i int32) string {
	return fmt.Sprintf("%s-worker-%d", pool.Name, i)
}

// isDesiredWorker reports whether name is one of the current desired workers.
func isDesiredWorker(pool *v1alpha1.AgentPool, name string) bool {
	for i := int32(0); i < pool.Spec.Replicas; i++ {
		if workerName(pool, i) == name {
			return true
		}
	}
	return false
}

// podIsReady reports whether a pod has a Ready condition True.
func podIsReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *AgentPoolReconciler) patchStatus(ctx context.Context, pool *v1alpha1.AgentPool, ready bool, readyReplicas int32, message string) error {
	base := pool.DeepCopy()
	pool.Status.Ready = ready
	pool.Status.ReadyReplicas = readyReplicas
	pool.Status.ObservedGeneration = pool.Generation
	pool.Status.Message = message
	return r.Status().Patch(ctx, pool, client.MergeFrom(base))
}

// SetupWithManager registers the reconciler and watches owned
// pods/configmaps/PVCs/networkpolicies for status propagation.
func (r *AgentPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.AgentPool{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Complete(r)
}

// boolPtr/int64Ptr helpers.
func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }
