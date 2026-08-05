package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/url"
	"strconv"
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
	"github.com/nunocgoncalves/control-plane/internal/gateway"
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

	// workerTemplateHashAnnotation records a hash of the desired worker pod
	// template. PodSpec is largely immutable; on hash mismatch (pool spec
	// changed) the reconciler deletes and recreates the pod rather than
	// attempting a forbidden spec mutation that also drops scheduler-owned
	// state (e.g. nodeName).
	workerTemplateHashAnnotation = "platform.iterabase.com/pod-template-hash"
)

// AgentPoolReconciler maintains a bounded set of isolated warm-worker pods +
// the shared RWX sandbox PVC + a deny-by-default NetworkPolicy for each
// AgentPool CR (HOR-245). Per-pod SPIFFE certs are issued by the cert-manager
// CSI driver (annotated on each pod); the operator never holds the CA key.
type AgentPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader is an uncached reader used only for Secret existence checks,
	// so raw credential/CA values are never retained in (nor a Secret informer
	// started by) this reconciler (ARCH-008/010). It must be set by the caller.
	APIReader client.Reader
	// Store materializes AgentPool CRs into the toolgateway Postgres schema
	// (the Git->DB bridge for pools/pool_grants/credential_bindings,
	// migration 000011/ARCH-018). It is a PoolMaterializer so the reconciler
	// depends only on the materialization contract (and tests can inject a
	// flaky fake). Optional: when nil, gateway materialization is skipped
	// (e.g. envtest without Postgres).
	Store PoolMaterializer
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

// PoolMaterializer is the contract by which the AgentPool reconciler
// materializes CRs into the toolgateway schema (the Git->DB bridge,
// ARCH-018/migration 000011) and revokes them on CR deletion. MaterializePool
// MUST be atomic (all-or-nothing across pool + grants + bindings) so the
// gateway never observes mixed old/new authorization, and the reconciler
// records success (ObservedGeneration) only after it returns nil — on error it
// retries the whole generation. *gateway.Store implements it; tests may inject
// a fake.
type PoolMaterializer interface {
	MaterializePool(ctx context.Context, key, name, spiffePrefix string, grants []gateway.PoolGrantInput, bindings []gateway.CredentialBindingInput) error
	SoftDeletePoolByKey(ctx context.Context, key string) error
}

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
	// don't leave dangling named children if GC is delayed, and that the
	// toolgateway rows are revoked (soft-deleted) before the CR is gone.
	if !pool.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&pool, agentPoolFinalizer) {
			if r.Store != nil {
				key := agentPoolKey(&pool)
				if err := r.Store.SoftDeletePoolByKey(ctx, key); err != nil {
					logger.Error(err, "failed to soft-delete gateway pool on CR deletion", "key", key)
					return ctrl.Result{}, err
				}
			}
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
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("validation: %v", err), true)
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&pool, agentPoolFinalizer) {
		controllerutil.AddFinalizer(&pool, agentPoolFinalizer)
		if err := r.Update(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Materialize gateway authorization (the Git->DB bridge) BEFORE pod/storage
	// assembly. REQ-010 requires denied capabilities to fail without broader
	// fallback, and ARCH-018 makes AgentPool the deployable gateway-permission
	// boundary, so revocation MUST converge independently of unrelated resource
	// assembly — otherwise an unreconcilable PVC/NetworkPolicy change (e.g. an
	// immutable storage class) would block this line forever and a revoked
	// grant/binding would stay live in the gateway indefinitely. Do NOT advance
	// ObservedGeneration on error: the materializeGateway generation gate would
	// then skip the retry, leaving the bridge unconverged (mixed/missing
	// authorization). Retry the whole generation until MaterializePool commits
	// atomically (ARCH-018/REQ-010).
	if err := r.materializeGateway(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("materialize gateway: %v", err), false)
		return ctrl.Result{}, err
	}
	if err := r.ensurePVC(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("ensure PVC: %v", err), false)
		return ctrl.Result{}, err
	}
	if err := r.ensureNetworkPolicy(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("ensure NetworkPolicy: %v", err), false)
		return ctrl.Result{}, err
	}
	if err := r.reconcileWorkers(ctx, &pool); err != nil {
		_ = r.patchStatus(ctx, &pool, false, 0, fmt.Sprintf("ensure workers: %v", err), false)
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
	if err := r.patchStatus(ctx, &pool, ready, readyReplicas, msg, true); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("reconciled AgentPool", "replicas", pool.Spec.Replicas, "ready", readyReplicas)
	// Requeue to refresh readyReplicas as pods come and go (envtest has no
	// kubelet, so Ready stays false there; real readiness is on-cluster).
	return ctrl.Result{RequeueAfter: healthRequeueInterval}, nil
}

// validateSpec validates structural correctness (ARCH-018): required fields,
// sandbox access mode (RWX multi-worker / RWO single-worker with replicas==1),
// egress mode, no duplicate gateway grants, and existence of referenced
// Secrets (credential bindings + CA). Semantic validation that a granted tool
// is registered is the gateway's responsibility (HOR-392/397).
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
	switch pool.Spec.Sandbox.AccessMode {
	case corev1.ReadWriteMany:
		// RWX is required for interchangeable/concurrent workers and supports
		// any replica count (HOR-427 preserves existing multi-worker behavior).
	case corev1.ReadWriteOnce:
		// RWO is a single-worker deployment mode only: a bound RWO PVC can be
		// mounted by exactly one node/pod, so more than one replica would break
		// at schedule time. Reject both creating a multi-replica RWO pool and
		// changing a live multi-replica pool to RWO before any workload is
		// rolled out (HOR-427).
		if pool.Spec.Replicas != 1 {
			return fmt.Errorf("spec.sandbox.accessMode ReadWriteOnce requires spec.replicas == 1 (single-worker RWO mode); set replicas to 1 or use ReadWriteMany")
		}
	default:
		return fmt.Errorf("spec.sandbox.accessMode must be ReadWriteMany or ReadWriteOnce")
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
		if err := validateGatewayPodSelector(ep.Selector); err != nil {
			return fmt.Errorf("spec.gateways.%s.selector: %w", name, err)
		}
	}
	// Egress mode (CRD enum already constrains, but default-then-check).
	switch pool.Spec.NetworkPolicy.Egress {
	case "", "denied", "internet":
	default:
		return fmt.Errorf("spec.networkPolicy.egress must be \"denied\" or \"internet\"")
	}
	// Gateway grants: no duplicate tools, valid max effect class, no empty
	// allowed actions (mirrors toolgateway.pool_grants).
	seen := make(map[string]bool)
	for i, g := range pool.Spec.GatewayGrants {
		if g.Tool == "" {
			return fmt.Errorf("spec.gatewayGrants[%d].tool is required", i)
		}
		if seen[g.Tool] {
			return fmt.Errorf("spec.gatewayGrants[%d].tool %q is duplicated", i, g.Tool)
		}
		seen[g.Tool] = true
		switch g.MaxEffectClass {
		case "read_only", "idempotent_write", "non_idempotent_write":
		default:
			return fmt.Errorf("spec.gatewayGrants[%d].maxEffectClass must be read_only, idempotent_write, or non_idempotent_write", i)
		}
		for _, a := range g.AllowedActions {
			if strings.TrimSpace(a) == "" {
				return fmt.Errorf("spec.gatewayGrants[%d].allowedActions contains an empty value", i)
			}
		}
	}
	// Credential bindings: required fields, scheme-specific Secret refs +
	// existence, resource constraints (mirrors toolgateway.credential_bindings).
	// Duplicate (toolName, slot) bindings and duplicate resource keys are
	// rejected because MaterializePool upserts bindings in array order and
	// toGatewayBindingInputs collapses resource keys into a map, so the last
	// declaration would silently win instead of failing closed (ARCH-018/
	// HOR-245).
	seenBindings := make(map[string]bool)
	for i, b := range pool.Spec.CredentialBindings {
		if b.ToolName == "" {
			return fmt.Errorf("spec.credentialBindings[%d].toolName is required", i)
		}
		if b.Slot == "" {
			return fmt.Errorf("spec.credentialBindings[%d].slot is required", i)
		}
		bindingKey := b.ToolName + "/" + b.Slot
		if seenBindings[bindingKey] {
			return fmt.Errorf("spec.credentialBindings[%d].toolName+slot %q is duplicated", i, bindingKey)
		}
		seenBindings[bindingKey] = true
		var secretName string
		switch b.Scheme {
		case "bearer":
			if b.Bearer == nil {
				return fmt.Errorf("spec.credentialBindings[%d].bearer is required when scheme=bearer", i)
			}
			if b.Bearer.ValueSecretRef.Name == "" || b.Bearer.ValueSecretRef.Key == "" {
				return fmt.Errorf("spec.credentialBindings[%d].bearer.valueSecretRef name+key are required", i)
			}
			secretName = b.Bearer.ValueSecretRef.Name
		case "oauth_client_credentials":
			if b.OAuth == nil {
				return fmt.Errorf("spec.credentialBindings[%d].oauth is required when scheme=oauth_client_credentials", i)
			}
			if b.OAuth.ClientID == "" {
				return fmt.Errorf("spec.credentialBindings[%d].oauth.clientId is required", i)
			}
			if b.OAuth.ClientSecretRef.Name == "" || b.OAuth.ClientSecretRef.Key == "" {
				return fmt.Errorf("spec.credentialBindings[%d].oauth.clientSecretRef name+key are required", i)
			}
			if b.OAuth.TokenURL == "" {
				return fmt.Errorf("spec.credentialBindings[%d].oauth.tokenUrl is required", i)
			}
			secretName = b.OAuth.ClientSecretRef.Name
		default:
			return fmt.Errorf("spec.credentialBindings[%d].scheme must be bearer or oauth_client_credentials", i)
		}
		if err := r.secretExists(ctx, pool.Namespace, secretName); err != nil {
			return fmt.Errorf("credentialBindings[%d]: %w", i, err)
		}
		seenResources := make(map[string]bool)
		for j, c := range b.ResourceConstraints {
			if c.Resource == "" {
				return fmt.Errorf("spec.credentialBindings[%d].resourceConstraints[%d].resource is required", i, j)
			}
			if c.Value == "" {
				return fmt.Errorf("spec.credentialBindings[%d].resourceConstraints[%d].value is required", i, j)
			}
			if seenResources[c.Resource] {
				return fmt.Errorf("spec.credentialBindings[%d].resourceConstraints[%d].resource %q is duplicated", i, j, c.Resource)
			}
			seenResources[c.Resource] = true
		}
	}
	return nil
}

// validateGatewayPodSelector requires a non-empty pod selector (at least one
// matchLabel or matchExpression) so NetworkPolicy egress is actually
// restricted to the intended gateway pods rather than the whole namespace.
func validateGatewayPodSelector(sel v1alpha1.GatewayPodSelector) error {
	if len(sel.PodSelector.MatchLabels) == 0 && len(sel.PodSelector.MatchExpressions) == 0 {
		return fmt.Errorf("podSelector must be non-empty (at least one matchLabel or matchExpression)")
	}
	return nil
}

// materializeGateway persists the AgentPool + its grants/bindings into the
// toolgateway Postgres schema (the Git->DB bridge, migration 000011/ARCH-018).
// It runs once per pool generation so pod-event reconciles do not churn the
// DB. On CR deletion the pool + grants/bindings are soft-deleted (see
// Reconcile's deletion path). Skipped when Store is nil.
func (r *AgentPoolReconciler) materializeGateway(ctx context.Context, pool *v1alpha1.AgentPool) error {
	if r.Store == nil {
		return nil
	}
	if pool.Status.ObservedGeneration != 0 && pool.Generation == pool.Status.ObservedGeneration {
		return nil // already materialized for this generation
	}
	key := agentPoolKey(pool)
	trustDomain := pool.Spec.Identity.TrustDomain
	if trustDomain == "" {
		trustDomain = v1alpha1.DefaultTrustDomain
	}
	spiffePrefix := fmt.Sprintf("spiffe://%s/pools/%s/", trustDomain, pool.UID)
	grants := toGatewayGrantInputs(pool)
	bindings, err := toGatewayBindingInputs(pool)
	if err != nil {
		return fmt.Errorf("marshal gateway bindings: %w", err)
	}
	// Atomic: pool + grants + bindings commit in one transaction, so the
	// gateway never sees mixed old/new authorization for a generation. On
	// error the caller MUST NOT advance ObservedGeneration (see Reconcile) so
	// the whole generation is retried until it converges (ARCH-018/REQ-010).
	if err := r.Store.MaterializePool(ctx, key, pool.Name, spiffePrefix, grants, bindings); err != nil {
		return fmt.Errorf("materialize gateway: %w", err)
	}
	return nil
}

// agentPoolKey is the stable toolgateway.pools natural key for a CR
// ("<namespace>/<name>"), matching ResolvePoolBySpiffePrefix's key convention.
func agentPoolKey(pool *v1alpha1.AgentPool) string {
	return fmt.Sprintf("%s/%s", pool.Namespace, pool.Name)
}

// toGatewayGrantInputs maps CRD grants to store grant inputs.
func toGatewayGrantInputs(pool *v1alpha1.AgentPool) []gateway.PoolGrantInput {
	out := make([]gateway.PoolGrantInput, 0, len(pool.Spec.GatewayGrants))
	for _, g := range pool.Spec.GatewayGrants {
		out = append(out, gateway.PoolGrantInput{
			ToolName:       g.Tool,
			MaxEffect:      gateway.EffectClass(g.MaxEffectClass),
			AllowedActions: g.AllowedActions,
		})
	}
	return out
}

// toGatewayBindingInputs maps CRD credential bindings to store binding inputs,
// marshaling the scheme-dependent secret_ref + non-secret resource constraints
// to the JSONB shapes stored in toolgateway.credential_bindings.
func toGatewayBindingInputs(pool *v1alpha1.AgentPool) ([]gateway.CredentialBindingInput, error) {
	out := make([]gateway.CredentialBindingInput, 0, len(pool.Spec.CredentialBindings))
	for _, b := range pool.Spec.CredentialBindings {
		var secretSpec map[string]any
		switch b.Scheme {
		case "bearer":
			secretSpec = map[string]any{"value_ref": map[string]any{"name": b.Bearer.ValueSecretRef.Name, "key": b.Bearer.ValueSecretRef.Key}}
		case "oauth_client_credentials":
			secretSpec = map[string]any{
				"client_id":         b.OAuth.ClientID,
				"client_secret_ref": map[string]any{"name": b.OAuth.ClientSecretRef.Name, "key": b.OAuth.ClientSecretRef.Key},
				"token_url":         b.OAuth.TokenURL,
				"scope":             b.OAuth.Scopes,
			}
		}
		specJSON, err := json.Marshal(secretSpec)
		if err != nil {
			return nil, fmt.Errorf("marshal secret spec for %s/%s: %w", b.ToolName, b.Slot, err)
		}
		rc := make(map[string]string, len(b.ResourceConstraints))
		for _, c := range b.ResourceConstraints {
			rc[c.Resource] = c.Value
		}
		rcJSON, err := json.Marshal(rc)
		if err != nil {
			return nil, fmt.Errorf("marshal resource constraints for %s/%s: %w", b.ToolName, b.Slot, err)
		}
		out = append(out, gateway.CredentialBindingInput{
			ToolName:            b.ToolName,
			SlotName:            b.Slot,
			Scheme:              gateway.CredentialScheme(b.Scheme),
			SecretSpec:          specJSON,
			ResourceConstraints: rcJSON,
		})
	}
	return out, nil
}

// secretExists returns nil if the Secret is present, or a descriptive error.
// It uses the uncached APIReader with a metadata-only object, so raw Secret
// values (customer credentials, the platform-ca key) are never retained in the
// operator process nor cached in a Secret informer (ARCH-008/010).
func (r *AgentPoolReconciler) secretExists(ctx context.Context, ns, name string) error {
	var meta metav1.PartialObjectMetadata
	meta.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &meta); err != nil {
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
		access := []corev1.PersistentVolumeAccessMode{pool.Spec.Sandbox.AccessMode}
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
	desiredHash := workerPodTemplateHash(pool, name)
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, &pod)
	if errors.IsNotFound(err) {
		return r.createWorkerPod(ctx, pool, name, desiredHash)
	}
	if err != nil {
		return err
	}
	// A pod already terminating (e.g. a rollout in progress) is left to finish;
	// it is recreated once fully gone. The owned-pod watch requeues us.
	if !pod.DeletionTimestamp.IsZero() {
		return nil
	}
	// PodSpec is largely immutable. Reconcile template changes by deleting and
	// recreating the pod rather than overwriting Spec in place, which would both
	// attempt forbidden immutable-field mutations (e.g. nodeName) and silently
	// drop scheduler/API-defaulted state. Recreation happens on the next
	// reconcile once the old pod is gone.
	if got := pod.Annotations[workerTemplateHashAnnotation]; got != desiredHash {
		if err := r.Delete(ctx, &pod); err != nil && !errors.IsNotFound(err) {
			return err
		}
		return nil
	}
	return nil
}

// createWorkerPod builds and creates a worker pod tagged with its template
// hash. The hash lets later reconciles detect spec changes and recreate the
// (largely immutable) pod instead of mutating it.
func (r *AgentPoolReconciler) createWorkerPod(ctx context.Context, pool *v1alpha1.AgentPool, name, hash string) error {
	pod := buildWorkerPod(pool, name, hash)
	if err := controllerutil.SetControllerReference(pool, pod, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, pod)
}

// buildWorkerPod assembles the desired worker Pod object (labels, template-hash
// annotation, spec). The controller reference is set by the caller.
func buildWorkerPod(pool *v1alpha1.AgentPool, name, hash string) *corev1.Pod {
	labels := poolLabels(pool)
	labels["platform.iterabase.com/worker"] = name
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   pool.Namespace,
			Labels:      labels,
			Annotations: map[string]string{workerTemplateHashAnnotation: hash},
		},
		Spec: buildWorkerPodSpec(pool, name),
	}
}

// workerPodTemplateHash is a stable hash of the desired worker pod template +
// rendered boot config, used to detect changes that require pod recreation. It
// is computed from the marshaled desired PodSpec and the rendered harness
// config.yaml, so config-only changes (a workspaceTools toggle, a gateway
// endpoint/serverName change, poolScopeIdentityId) also roll out via
// delete+recreate — the harness loads config at process startup, so without the
// config digest an updated ConfigMap would leave the old worker running with a
// revoked workspaceTools maximum.
func workerPodTemplateHash(pool *v1alpha1.AgentPool, name string) string {
	spec := buildWorkerPodSpec(pool, name)
	specBytes, err := json.Marshal(spec)
	if err != nil {
		// PodSpec always marshals; fall back to a constant so creation proceeds.
		return "unhashable"
	}
	cfgBytes := []byte(renderHarnessConfig(pool, name))
	h := fnv.New64a()
	_, _ = h.Write(specBytes)
	_, _ = h.Write([]byte{0}) // separator so spec/config boundaries can't collide
	_, _ = h.Write(cfgBytes)
	return strconv.FormatUint(h.Sum64(), 36)
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
		// init-wal provisions the supervisor-only WAL directory before the
		// supervisor starts: the WAL emptyDir defaults to a root-owned 0777
		// mount, which a session-UID child (with cleared supplementary groups)
		// could traverse to read other turns' audit records. chmod 0700 makes
		// it supervisor-only (HOR-381 isolation contract).
		InitContainers: []corev1.Container{walInitContainer(pool, walDir)},
		Volumes:        volumes,
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

// buildNetworkPolicySpec renders a deny-by-default egress policy selecting
// the pool's worker pods. Egress is restricted to: the DNS pods (port 53) and
// each gateway's pods (selected by label) on that gateway's URL port. `denied`
// (default) allows only those; `internet` additionally allows outbound to
// non-cluster Internet (RFC1918/loopback/link-local excluded, so credentialed
// customer systems on private IPs remain reachable only via the gateway). An
// enabled `bash` tool therefore cannot reach unrelated colocated databases,
// caches, or adapters (REQ-010/ARCH-003).
func buildNetworkPolicySpec(pool *v1alpha1.AgentPool) networkingv1.NetworkPolicySpec {
	dnsPort := intstr.FromInt(53)
	udp := corev1.ProtocolUDP
	tcp := corev1.ProtocolTCP

	egress := []networkingv1.NetworkPolicyEgressRule{
		// DNS pods only (Service resolution for the gateway hostnames).
		{
			To:    []networkingv1.NetworkPolicyPeer{dnsPolicyPeer(pool)},
			Ports: []networkingv1.NetworkPolicyPort{{Port: &dnsPort, Protocol: &udp}, {Port: &dnsPort, Protocol: &tcp}},
		},
	}
	// Each gateway: its pods only, on its port.
	for _, ep := range []v1alpha1.GatewayEndpoint{
		pool.Spec.Gateways.ControlPlane,
		pool.Spec.Gateways.ToolGateway,
		pool.Spec.Gateways.InferenceGateway,
	} {
		egress = append(egress, gatewayEgressRule(pool, ep))
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

// gatewayEgressRule builds an egress rule restricted to one gateway's pods on
// its URL port.
func gatewayEgressRule(pool *v1alpha1.AgentPool, ep v1alpha1.GatewayEndpoint) networkingv1.NetworkPolicyEgressRule {
	port := gatewayPort(ep.URL)
	tcp := corev1.ProtocolTCP
	return networkingv1.NetworkPolicyEgressRule{
		To:    []networkingv1.NetworkPolicyPeer{gatewayPolicyPeer(pool, ep.Selector)},
		Ports: []networkingv1.NetworkPolicyPort{{Port: &port, Protocol: &tcp}},
	}
}

// gatewayPolicyPeer builds a NetworkPolicy peer selecting the gateway's pods in
// the selected namespace (defaulting to the pool's namespace).
func gatewayPolicyPeer(pool *v1alpha1.AgentPool, sel v1alpha1.GatewayPodSelector) networkingv1.NetworkPolicyPeer {
	nsSel := sel.NamespaceSelector
	if nsSel == nil {
		nsSel = &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": pool.Namespace}}
	}
	podSel := sel.PodSelector
	return networkingv1.NetworkPolicyPeer{
		NamespaceSelector: nsSel,
		PodSelector:       &podSel,
	}
}

// dnsPolicyPeer builds the DNS egress peer, defaulting to kube-system +
// k8s-app=kube-dns when no selector is configured.
func dnsPolicyPeer(pool *v1alpha1.AgentPool) networkingv1.NetworkPolicyPeer {
	if pool.Spec.NetworkPolicy.DnsSelector != nil {
		return gatewayPolicyPeer(pool, *pool.Spec.NetworkPolicy.DnsSelector)
	}
	return networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"}},
		PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"k8s-app": "kube-dns"}},
	}
}

// gatewayPort extracts the port from a gateway URL (defaults to 443 for https).
func gatewayPort(rawURL string) intstr.IntOrString {
	if u, err := url.Parse(rawURL); err == nil && u.Port() != "" {
		if p, err := strconv.Atoi(u.Port()); err == nil {
			return intstr.FromInt(p)
		}
	}
	return intstr.FromInt(443)
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

func (r *AgentPoolReconciler) patchStatus(ctx context.Context, pool *v1alpha1.AgentPool, ready bool, readyReplicas int32, message string, recordObserved bool) error {
	base := pool.DeepCopy()
	pool.Status.Ready = ready
	pool.Status.ReadyReplicas = readyReplicas
	// ObservedGeneration is advanced only on definitive outcomes: a successful
	// reconcile or a structural validation rejection (no retry until the spec
	// changes). Transient errors (PVC/NetworkPolicy/gateway/workers) MUST NOT
	// advance it, or the materializeGateway generation gate would skip the
	// retry and leave the Git->DB bridge unconverged (ARCH-018/REQ-010).
	if recordObserved {
		pool.Status.ObservedGeneration = pool.Generation
	}
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

// walInitContainer renders the init container that provisions the
// supervisor-only 0700 WAL directory (HOR-381). It runs as root (the
// supervisor UID) and only chmods the WAL emptyDir mount before the supervisor
// starts; the session-UID child then cannot traverse the directory to read
// other turns' WAL records.
func walInitContainer(pool *v1alpha1.AgentPool, walDir string) corev1.Container {
	return corev1.Container{
		Name:    "init-wal",
		Image:   pool.Spec.WorkerImage,
		Command: []string{"/bin/sh", "-c", fmt.Sprintf("mkdir -p %q && chmod 0700 %q", walDir, walDir)},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "wal", MountPath: walDir},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                int64Ptr(supervisorUID),
			RunAsGroup:               int64Ptr(supervisorUID),
			AllowPrivilegeEscalation: boolPtr(false),
		},
	}
}

// boolPtr/int64Ptr helpers.
func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }
