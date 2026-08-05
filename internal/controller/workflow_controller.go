package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/gateway"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

const workflowFinalizer = "platform.iterabase.com/workflow-finalizer"

// poolBindingRequeueInterval is the transient requeue cadence used while the
// referenced AgentPool has not yet been materialized into the gateway pool
// registry (the AgentPool reconciler writes it). The workflow is valid but not
// yet ready until the pool binding is written.
const poolBindingRequeueInterval = 10 * time.Second

// PoolBindingStore is the gateway-store contract the Workflow reconciler uses
// to resolve the referenced AgentPool's pool_id and bind the workflow's
// permitted tools (ARCH-018). *gateway.Store implements it; tests may inject
// a fake.
type PoolBindingStore interface {
	GetPoolByKey(ctx context.Context, key string) (gateway.Pool, error)
	UpsertWorkflowPoolBinding(ctx context.Context, workflowDefinitionKey, poolID string, permitted []gateway.Capability) error
	SoftDeleteWorkflowPoolBindingByKey(ctx context.Context, workflowDefinitionKey string) error
}

// IdentityMaterializer is the identity-store contract the Workflow reconciler
// uses to create/revive the kind=workflow scope identity runs execute under
// (HOR-242). *identity.Store implements it; tests may inject a fake.
type IdentityMaterializer interface {
	UpsertIdentity(ctx context.Context, key, kind, source, displayName string) (identity.Identity, error)
	SoftDeleteIdentityByKey(ctx context.Context, key string) error
}

// SourceAdapterRegistry reports the source adapters actually installed in this
// control-plane deployment. Recognizing a source enum is not enough to mark a
// workflow Ready: its ingress adapter must be present (HOR-252 acceptance).
type SourceAdapterRegistry interface {
	IsInstalled(sourceType string) bool
}

// StaticSourceAdapterRegistry is the compile/deployment-time adapter set used by
// the manager. Downstream ingress slices add their source only when the adapter
// implementation is wired into the running control plane.
type StaticSourceAdapterRegistry map[string]struct{}

// IsInstalled reports whether sourceType has an installed implementation.
func (s StaticSourceAdapterRegistry) IsInstalled(sourceType string) bool {
	_, ok := s[sourceType]
	return ok
}

// WorkflowReconciler materializes Workflow CRs into the Postgres workflow store
// (Git -> DB bridge, HOR-252): it validates the definition before execution,
// registers an immutable versioned definition + non-secret trigger bindings,
// creates the kind=workflow scope identity, and binds the workflow's permitted
// gateway tools to the referenced AgentPool (ARCH-018). On CR deletion it
// soft-deletes the definition, bindings, pool binding, and scope identity.
type WorkflowReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	Store          *workflow.Store
	Pools          PoolBindingStore     // *gateway.Store
	Identities     IdentityMaterializer // *identity.Store
	SourceAdapters SourceAdapterRegistry
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=workflows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=workflows/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=workflows/finalizers,verbs=update
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=models,verbs=get;list;watch

// Reconcile handles Workflow create/update/delete events.
//
// nolint:gocyclo
func (r *WorkflowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var wf v1alpha1.Workflow
	if err := r.Get(ctx, req.NamespacedName, &wf); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: finalizer cleanup. Revokes the definition + trigger
	// bindings (workflow store), the workflow_pool_binding for EVERY owned
	// version (gateway store), and the scope identity (identity store). Rows are
	// retained for history/audit. Cleanup fails closed (REQ-010): if any store
	// revocation errors, the finalizer is NOT removed and the reconcile requeues
	// so no usable gateway authorization outlives the Workflow CR.
	if !wf.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&wf, workflowFinalizer) {
			if err := r.cleanupWorkflow(ctx, &wf); err != nil {
				logger.Error(err, "workflow cleanup failed; keeping finalizer and requeuing")
				return ctrl.Result{Requeue: true}, nil
			}
			controllerutil.RemoveFinalizer(&wf, workflowFinalizer)
			if err := r.Update(ctx, &wf); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("finalized Workflow")
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&wf, workflowFinalizer) {
		controllerutil.AddFinalizer(&wf, workflowFinalizer)
		if err := r.Update(ctx, &wf); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Validate before execution (acceptance: unknown source/node/capability/
	// route/binding fails before execution; a workflow cannot request
	// capabilities beyond its AgentPool policy). A validation error is surfaced
	// in status as invalid and does not requeue — the user must fix the CR.
	if err := r.validateSpec(ctx, &wf); err != nil {
		_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationInvalid, fmt.Sprintf("validation: %v", err), true)
		return ctrl.Result{}, nil
	}

	// Materialize the scope identity (kind=workflow) runs execute under.
	scopeKey := workflowScopeIdentityKey(&wf)
	ident, err := r.Identities.UpsertIdentity(ctx, scopeKey, "workflow", identity.SourceLocal, wf.Spec.Presentation.WorkflowTitle)
	if err != nil {
		_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationValid, fmt.Sprintf("upsert scope identity: %v", err), false)
		return ctrl.Result{}, err
	}

	// Register the immutable versioned definition. A content change under an
	// already-registered (key, version) is rejected (ARCH-007 immutability) and
	// surfaced as invalid — the operator must publish a new version.
	def, digest, err := r.registerDefinition(ctx, &wf, ident.ID)
	if err != nil {
		if errors.Is(err, workflow.ErrImmutableVersion) || errors.Is(err, workflow.ErrDefinitionOwnership) {
			_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationInvalid, fmt.Sprintf("validation: %v", err), true)
			return ctrl.Result{}, nil
		}
		_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationValid, fmt.Sprintf("register definition: %v", err), false)
		return ctrl.Result{}, err
	}

	// Replace the non-secret trigger bindings.
	if err := r.Store.ReplaceTriggerBindings(ctx, def.ID, wf.Spec.Source.Type, toTriggerBindingInputs(wf.Spec.Source.Type, wf.Spec.Source.TriggerBindings)); err != nil {
		_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationValid, fmt.Sprintf("replace trigger bindings: %v", err), false)
		return ctrl.Result{}, err
	}

	// Bind the workflow's permitted tools to the referenced AgentPool. The pool
	// must be materialized in the gateway registry (the AgentPool reconciler
	// writes it); if it is not yet present this is transient — requeue without
	// advancing observedGeneration so the binding converges.
	poolKey := agentPoolKeyFromRef(&wf)
	pool, err := r.Pools.GetPoolByKey(ctx, poolKey)
	if err != nil {
		if errors.Is(err, gateway.ErrNotFound) {
			_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationValid,
				fmt.Sprintf("waiting for AgentPool %q to be materialized in the gateway", wf.Spec.PoolRef), false)
			return ctrl.Result{RequeueAfter: poolBindingRequeueInterval}, nil
		}
		_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationValid, fmt.Sprintf("resolve pool: %v", err), false)
		return ctrl.Result{}, err
	}
	permitted := permittedCapabilities(wf.Spec.RequestedCapabilities)
	if err := r.Pools.UpsertWorkflowPoolBinding(ctx, workflow.DefinitionKey(def.Key, def.Version), pool.ID, permitted); err != nil {
		_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationValid, fmt.Sprintf("bind pool: %v", err), false)
		return ctrl.Result{}, err
	}

	if err := r.patchStatus(ctx, &wf, true, v1alpha1.ValidationValid, "", true); err != nil {
		return ctrl.Result{}, err
	}
	// Stash the immutable identity + IDs on status for inspection.
	base := wf.DeepCopy()
	wf.Status.VersionDigest = digest
	wf.Status.DefinitionID = def.ID
	wf.Status.ScopeIdentityID = ident.ID
	if err := r.Status().Patch(ctx, &wf, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("materialized workflow", "key", def.Key, "version", def.Version, "digest", digest, "pool", poolKey)
	return ctrl.Result{}, nil
}

// cleanupWorkflow revokes the workflow's materialized state on CR deletion.
// It fails closed (REQ-010): any store error is returned so the caller keeps
// the finalizer and requeues, never leaving usable gateway authorization
// after the Workflow CR is gone. Revocation order is authorization-first: the
// workflow_pool_binding for EVERY definition owned by this CR is revoked before
// the definitions and scope identity, so a transient failure cannot retain a
// usable binding for an older version or a previous spec.key. All steps are
// idempotent (soft-delete is a no-op on already-deleted rows), so retries
// converge.
func (r *WorkflowReconciler) cleanupWorkflow(ctx context.Context, wf *v1alpha1.Workflow) error {
	scopeKey := workflowScopeIdentityKey(wf)
	// scopeKey is derived from immutable object metadata, while spec.key may
	// change. Every definition persists the corresponding scope_identity_id, so
	// owner-based enumeration finds all keys and versions created by this CR.
	defs, err := r.Store.ListDefinitionsByOwner(ctx, scopeKey)
	if err != nil {
		return fmt.Errorf("list workflow definitions for cleanup: %w", err)
	}
	for _, d := range defs {
		defKey := workflow.DefinitionKey(d.Key, d.Version)
		if err := r.Pools.SoftDeleteWorkflowPoolBindingByKey(ctx, defKey); err != nil {
			return fmt.Errorf("soft-delete workflow pool binding %s: %w", defKey, err)
		}
	}
	// Revoke every owned definition + trigger binding, then its scope identity.
	// Rows are retained for history/audit.
	if err := r.Store.SoftDeleteDefinitionsByOwner(ctx, scopeKey); err != nil {
		return fmt.Errorf("soft-delete workflow definitions: %w", err)
	}
	if err := r.Identities.SoftDeleteIdentityByKey(ctx, scopeKey); err != nil {
		return fmt.Errorf("soft-delete workflow scope identity %q: %w", scopeKey, err)
	}
	return nil
}

// validateSpec validates the Workflow spec before execution (acceptance:
// unknown source/node/capability/route/binding fails before
// execution; a workflow cannot request capabilities beyond its AgentPool
// policy). It reads the referenced AgentPool CR for the maximum gateway grants.
//
// nolint:gocyclo
func (r *WorkflowReconciler) validateSpec(ctx context.Context, wf *v1alpha1.Workflow) error {
	if wf.Spec.Key == "" {
		return fmt.Errorf("spec.key is required")
	}
	if wf.Spec.Version == "" {
		return fmt.Errorf("spec.version is required")
	}
	// key/version must not contain ":" — the definition_key wire format is
	// "<key>:<version>", so ":" would make the concatenated key ambiguous and
	// let one workflow overwrite another's pool binding (REQ-010 scope
	// isolation). The CRD pattern enforces this for API-server writes; this is
	// defense-in-depth for direct/unvalidated writes.
	if strings.Contains(wf.Spec.Key, ":") {
		return fmt.Errorf("spec.key must not contain \":\" (definition_key wire format is \"<key>:<version>\")")
	}
	if strings.Contains(wf.Spec.Version, ":") {
		return fmt.Errorf("spec.version must not contain \":\" (definition_key wire format is \"<key>:<version>\")")
	}
	if wf.Spec.PoolRef == "" {
		return fmt.Errorf("spec.poolRef is required")
	}
	// Source type + installed adapter availability. A recognized enum is only a
	// schema value; it must not become Ready until this deployment has the
	// corresponding ingress implementation installed (HOR-252 acceptance).
	switch wf.Spec.Source.Type {
	case v1alpha1.SourceGraphEmail, v1alpha1.SourceOperatorArtifact:
	default:
		return fmt.Errorf("spec.source.type must be %q or %q", v1alpha1.SourceGraphEmail, v1alpha1.SourceOperatorArtifact)
	}
	if r.SourceAdapters == nil || !r.SourceAdapters.IsInstalled(wf.Spec.Source.Type) {
		return fmt.Errorf("spec.source.type %q has no installed source adapter", wf.Spec.Source.Type)
	}
	// Trigger bindings use a source-specific typed payload and contain no opaque
	// config field. This makes raw credential persistence structurally
	// impossible; credentials remain in AgentPool bindings/K8s Secrets
	// (ARCH-008).
	seenBindings := make(map[string]bool)
	for i, b := range wf.Spec.Source.TriggerBindings {
		if b.Name == "" {
			return fmt.Errorf("spec.source.triggerBindings[%d].name is required", i)
		}
		if seenBindings[b.Name] {
			return fmt.Errorf("spec.source.triggerBindings[%d].name %q is duplicated", i, b.Name)
		}
		seenBindings[b.Name] = true
		if _, err := triggerBindingKey(wf.Spec.Source.Type, b); err != nil {
			return fmt.Errorf("spec.source.triggerBindings[%d]: %w", i, err)
		}
	}
	// Skills: every declared skill has an exact immutable version/digest and a
	// unique logical identity, so attempt snapshotting cannot silently follow a
	// mutable overlay skill (REQ-003/REQ-011).
	seenSkills := make(map[string]bool)
	for i, skill := range wf.Spec.Skills {
		if skill.Name == "" {
			return fmt.Errorf("spec.skills[%d].name is required", i)
		}
		if seenSkills[skill.Name] {
			return fmt.Errorf("spec.skills[%d].name %q is duplicated", i, skill.Name)
		}
		seenSkills[skill.Name] = true
		if skill.Version == "" {
			return fmt.Errorf("spec.skills[%d].version is required", i)
		}
		if skill.Digest == "" {
			return fmt.Errorf("spec.skills[%d].digest is required", i)
		}
	}
	// Requested capabilities: valid effect class, no duplicate tools. Graph
	// nodes narrow this workflow-level ceiling by logical tool name.
	seenCaps := make(map[string]bool)
	for i, c := range wf.Spec.RequestedCapabilities {
		if c.Tool == "" {
			return fmt.Errorf("spec.requestedCapabilities[%d].tool is required", i)
		}
		if c.Tool == "complete_step" {
			return fmt.Errorf("spec.requestedCapabilities[%d].tool complete_step is reserved by the platform", i)
		}
		if seenCaps[c.Tool] {
			return fmt.Errorf("spec.requestedCapabilities[%d].tool %q is duplicated", i, c.Tool)
		}
		seenCaps[c.Tool] = true
		switch c.MaxEffectClass {
		case string(gateway.EffectReadOnly), string(gateway.EffectIdempotentWrite), string(gateway.EffectNonIdempotentWrite):
		default:
			return fmt.Errorf("spec.requestedCapabilities[%d].maxEffectClass must be read_only, idempotent_write, or non_idempotent_write", i)
		}
		workflowActionAllowed := len(c.Actions) == 0
		for _, a := range c.Actions {
			if a == "" {
				return fmt.Errorf("spec.requestedCapabilities[%d].actions contains an empty value", i)
			}
			if a == c.Tool || a == "*" {
				workflowActionAllowed = true
			}
		}
		if !workflowActionAllowed {
			return fmt.Errorf("spec.requestedCapabilities[%d].actions must include %q or \"*\" for the v1 undecomposed tool action", i, c.Tool)
		}
	}
	// Validate the complete graph after the workflow-level skill/capability sets
	// are known. This covers outcome routing, cycles, terminal reachability,
	// node narrowing, schemas, and agent/human node shape (ARCH-019/020).
	if err := workflow.ValidateGraph(buildCanonicalSpec(wf)); err != nil {
		return fmt.Errorf("spec.%w", err)
	}
	// Every referenced Model must exist in the Workflow namespace. Availability
	// may change after registration; attempt creation resolves and snapshots the
	// exact currently-available catalog entry again.
	modelRefs := map[string]struct{}{}
	if wf.Spec.DefaultModelRef != "" {
		modelRefs[wf.Spec.DefaultModelRef] = struct{}{}
	}
	for i, node := range wf.Spec.Graph.Nodes {
		if node.Label.EN == "" || node.Label.PT == "" {
			return fmt.Errorf("spec.graph.nodes[%d].label requires en and pt text", i)
		}
		if node.ModelRef != "" {
			modelRefs[node.ModelRef] = struct{}{}
		}
	}
	for ref := range modelRefs {
		var model v1alpha1.Model
		if err := r.Get(ctx, types.NamespacedName{Name: ref, Namespace: wf.Namespace}, &model); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("spec modelRef %q does not reference a Model in namespace %q", ref, wf.Namespace)
			}
			return fmt.Errorf("read Model %q: %w", ref, err)
		}
	}
	// Presentation (REQ-021): customer-facing labels + persona; no separate
	// Persona CRD.
	if wf.Spec.Presentation.WorkflowTitle == "" {
		return fmt.Errorf("spec.presentation.workflowTitle is required")
	}
	if wf.Spec.Presentation.PersonaName == "" {
		return fmt.Errorf("spec.presentation.personaName is required")
	}
	switch wf.Spec.Presentation.Locale {
	case "", "en", "pt":
	default:
		return fmt.Errorf("spec.presentation.locale must be en or pt")
	}
	// Capability validation against the AgentPool's maximum grants (acceptance:
	// "a workflow cannot request capabilities beyond its AgentPool/customer
	// policy"). The AgentPool CR is the operator-declared policy boundary
	// (ARCH-018); its gatewayGrants are the ceiling a workflow cannot widen.
	if err := r.validateCapabilitiesAgainstPool(ctx, wf); err != nil {
		return err
	}
	return nil
}

// validateCapabilitiesAgainstPool reads the referenced AgentPool CR and rejects
// any requested capability whose tool is not granted or whose effect class
// exceeds the pool grant's maximum (ARCH-016/018; REQ-010/SCN-009). Requested
// actions must be a subset of the pool grant's allowedActions when the grant
// narrows by action.
func (r *WorkflowReconciler) validateCapabilitiesAgainstPool(ctx context.Context, wf *v1alpha1.Workflow) error {
	var pool v1alpha1.AgentPool
	if err := r.Get(ctx, types.NamespacedName{Name: wf.Spec.PoolRef, Namespace: wf.Namespace}, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("spec.poolRef: AgentPool %q not found in namespace %q", wf.Spec.PoolRef, wf.Namespace)
		}
		return fmt.Errorf("read AgentPool: %w", err)
	}
	// Index pool grants by tool name.
	grants := make(map[string]v1alpha1.GatewayGrant, len(pool.Spec.GatewayGrants))
	for _, g := range pool.Spec.GatewayGrants {
		grants[g.Tool] = g
	}
	for _, node := range wf.Spec.Graph.Nodes {
		if node.WorkspaceTools && !pool.Spec.WorkspaceTools {
			return fmt.Errorf("graph node %q requests workspaceTools but AgentPool %q disables them", node.Key, wf.Spec.PoolRef)
		}
	}
	for _, c := range wf.Spec.RequestedCapabilities {
		g, ok := grants[c.Tool]
		if !ok {
			return fmt.Errorf("requested capability %q is not granted by AgentPool %q (deny-by-default, ARCH-016)", c.Tool, wf.Spec.PoolRef)
		}
		if effectRank(c.MaxEffectClass) > effectRank(g.MaxEffectClass) {
			return fmt.Errorf("requested capability %q effect %q exceeds AgentPool grant %q (max %q)", c.Tool, c.MaxEffectClass, wf.Spec.PoolRef, g.MaxEffectClass)
		}
		// Action narrowing: when the pool grant narrows by action, every
		// requested action must be within it. An empty workflow action set
		// inherits the pool's full (narrowed) set.
		if len(g.AllowedActions) > 0 && len(c.Actions) > 0 {
			allowed := make(map[string]bool, len(g.AllowedActions))
			for _, a := range g.AllowedActions {
				allowed[a] = true
			}
			for _, a := range c.Actions {
				if !allowed[a] {
					return fmt.Errorf("requested capability %q action %q is not allowed by AgentPool grant (allowed: %v)", c.Tool, a, g.AllowedActions)
				}
			}
		}
	}
	return nil
}

// effectRank maps an effect class to its severity rank for grant-ceiling
// comparison (mirrors gateway.effectRank): read_only (1) < idempotent_write
// (2) < non_idempotent_write (3).
func effectRank(c string) int {
	switch c {
	case string(gateway.EffectReadOnly):
		return 1
	case string(gateway.EffectIdempotentWrite):
		return 2
	case string(gateway.EffectNonIdempotentWrite):
		return 3
	}
	return 0
}

// registerDefinition builds the canonical spec, computes the immutable content
// digest, and registers the immutable versioned definition. Returns the
// definition + digest. An ErrImmutableVersion is surfaced by the caller as a
// validation failure.
func (r *WorkflowReconciler) registerDefinition(ctx context.Context, wf *v1alpha1.Workflow, scopeIdentityID string) (workflow.Definition, string, error) {
	spec := buildCanonicalSpec(wf)
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return workflow.Definition{}, "", fmt.Errorf("marshal canonical spec: %w", err)
	}
	sum := sha256.Sum256(specJSON)
	digest := hex.EncodeToString(sum[:])
	presentationJSON, err := json.Marshal(spec.Presentation)
	if err != nil {
		return workflow.Definition{}, "", fmt.Errorf("marshal presentation: %w", err)
	}
	def, err := r.Store.RegisterDefinition(ctx, workflow.Definition{
		Key:              wf.Spec.Key,
		Version:          wf.Spec.Version,
		Digest:           digest,
		SpecJSON:         specJSON,
		ValidationStatus: workflow.ValidationValid,
		ScopeIdentityID:  scopeIdentityID,
		SourceType:       wf.Spec.Source.Type,
		PoolKey:          agentPoolKeyFromRef(wf),
		Presentation:     presentationJSON,
	})
	if err != nil {
		return workflow.Definition{}, "", err
	}
	return def, digest, nil
}

// buildCanonicalSpec maps the CR spec to the deterministic canonical shape that
// is hashed into the immutable version digest (ARCH-007). The version string
// itself is excluded — it is the version identity component, not content.
func buildCanonicalSpec(wf *v1alpha1.Workflow) workflow.CanonicalSpec {
	maxTransitions := wf.Spec.Graph.MaxTransitions
	if maxTransitions == 0 {
		maxTransitions = 100
	}
	spec := workflow.CanonicalSpec{
		Key:              wf.Spec.Key,
		PoolRef:          wf.Spec.PoolRef,
		DefaultModelRef:  wf.Spec.DefaultModelRef,
		ValueModelRef:    wf.Spec.ValueModelRef,
		ScopeIdentityKey: workflowScopeIdentityKey(wf),
		Source: workflow.CanonicalSource{
			Type: wf.Spec.Source.Type,
		},
		Graph: workflow.CanonicalGraph{
			EntryNode:      wf.Spec.Graph.EntryNode,
			MaxTransitions: maxTransitions,
		},
		Presentation: workflow.CanonicalPresentation{
			WorkflowTitle: wf.Spec.Presentation.WorkflowTitle,
			PersonaName:   wf.Spec.Presentation.PersonaName,
			PersonaAvatar: wf.Spec.Presentation.PersonaAvatar,
			Locale:        wf.Spec.Presentation.Locale,
		},
	}
	for _, b := range wf.Spec.Source.TriggerBindings {
		bindingKey, _ := triggerBindingKey(wf.Spec.Source.Type, b) // validated before canonicalization
		spec.Source.TriggerBindings = append(spec.Source.TriggerBindings, workflow.CanonicalTrigger{
			Name:       b.Name,
			BindingKey: bindingKey,
		})
	}
	for _, skill := range wf.Spec.Skills {
		spec.Skills = append(spec.Skills, workflow.CanonicalSkill{
			Name: skill.Name, Version: skill.Version, Digest: skill.Digest,
		})
	}
	for _, n := range wf.Spec.Graph.Nodes {
		cn := workflow.CanonicalNode{
			Key: n.Key, Label: workflow.CanonicalLocalizedText{EN: n.Label.EN, PT: n.Label.PT},
			Kind: n.Kind, Prompt: n.Prompt, ModelRef: n.ModelRef,
			Skills: n.Skills, Capabilities: n.Capabilities,
			WorkspaceTools: n.WorkspaceTools, Outcomes: n.Outcomes,
			OutputSchema: rawJSON(n.OutputSchema),
		}
		if n.Timeout != nil {
			cn.Timeout = n.Timeout.Duration.String()
		}
		if n.ResultPresentation != nil {
			cn.ResultPresentation = canonicalResultPresentation(n.ResultPresentation)
		}
		if n.HumanGate != nil {
			cn.HumanGate = &workflow.CanonicalHumanGate{
				Type:           n.HumanGate.Type,
				Title:          workflow.CanonicalLocalizedText{EN: n.HumanGate.Title.EN, PT: n.HumanGate.Title.PT},
				Description:    workflow.CanonicalLocalizedText{EN: n.HumanGate.Description.EN, PT: n.HumanGate.Description.PT},
				ResponseSchema: rawJSON(n.HumanGate.ResponseSchema),
			}
			for _, label := range n.HumanGate.Presentation.Outcomes {
				cn.HumanGate.Presentation.Outcomes = append(cn.HumanGate.Presentation.Outcomes,
					workflow.CanonicalLocalizedText{EN: label.EN, PT: label.PT})
			}
			for _, field := range n.HumanGate.Presentation.Fields {
				canonicalField := workflow.CanonicalHumanGateFieldPresentation{
					Key: field.Key, Label: workflow.CanonicalLocalizedText{EN: field.Label.EN, PT: field.Label.PT},
				}
				for _, label := range field.Options {
					canonicalField.Options = append(canonicalField.Options,
						workflow.CanonicalLocalizedText{EN: label.EN, PT: label.PT})
				}
				cn.HumanGate.Presentation.Fields = append(cn.HumanGate.Presentation.Fields, canonicalField)
			}
		}
		spec.Graph.Nodes = append(spec.Graph.Nodes, cn)
	}
	for _, e := range wf.Spec.Graph.Edges {
		spec.Graph.Edges = append(spec.Graph.Edges, workflow.CanonicalEdge{From: e.From, Outcome: e.Outcome, To: e.To})
	}
	for _, t := range wf.Spec.Graph.TerminalOutcomes {
		spec.Graph.TerminalOutcomes = append(spec.Graph.TerminalOutcomes, workflow.CanonicalTerminalOutcome{Node: t.Node, Outcome: t.Outcome})
	}
	for _, c := range wf.Spec.RequestedCapabilities {
		spec.RequestedCapabilities = append(spec.RequestedCapabilities, workflow.CanonicalCapability{
			Tool:           c.Tool,
			MaxEffectClass: c.MaxEffectClass,
			Actions:        c.Actions,
		})
	}
	return spec
}

func canonicalResultPresentation(in *v1alpha1.ResultPresentation) *workflow.CanonicalResultPresentation {
	out := &workflow.CanonicalResultPresentation{}
	for _, outcome := range in.Outcomes {
		out.Outcomes = append(out.Outcomes, workflow.CanonicalResultOutcomePresentation{
			Outcome: outcome.Outcome,
			Summary: workflow.CanonicalLocalizedText{EN: outcome.Summary.EN, PT: outcome.Summary.PT},
		})
	}
	out.Fields = canonicalResultFields(in.Fields)
	return out
}

func canonicalResultFields(in []v1alpha1.ResultFieldPresentation) []workflow.CanonicalResultFieldPresentation {
	out := make([]workflow.CanonicalResultFieldPresentation, 0, len(in))
	for _, field := range in {
		canonical := workflow.CanonicalResultFieldPresentation{
			Path:  append([]string(nil), field.Path...),
			Label: workflow.CanonicalLocalizedText{EN: field.Label.EN, PT: field.Label.PT},
		}
		for _, option := range field.Options {
			canonical.Options = append(canonical.Options, workflow.CanonicalResultValuePresentation{
				Value: append(json.RawMessage(nil), option.Value.Raw...),
				Label: workflow.CanonicalLocalizedText{EN: option.Label.EN, PT: option.Label.PT},
			})
		}
		out = append(out, canonical)
	}
	return out
}

// triggerBindingKey validates a source-specific non-secret trigger payload and
// returns the normalized routing identifier persisted in Postgres. There is no
// opaque payload or credential-shaped field (ARCH-008).
func triggerBindingKey(sourceType string, b v1alpha1.TriggerBinding) (string, error) {
	switch sourceType {
	case v1alpha1.SourceGraphEmail:
		if b.GraphEmail == nil || b.OperatorArtifact != nil {
			return "", fmt.Errorf("graphEmail must be set exclusively for source type %q", sourceType)
		}
		parsed, err := mail.ParseAddress(b.GraphEmail.MailboxAddress)
		if err != nil || parsed.Address != b.GraphEmail.MailboxAddress {
			return "", fmt.Errorf("graphEmail.mailboxAddress must be a plain valid email address")
		}
		return parsed.Address, nil
	case v1alpha1.SourceOperatorArtifact:
		if b.OperatorArtifact == nil || b.GraphEmail != nil {
			return "", fmt.Errorf("operatorArtifact must be set exclusively for source type %q", sourceType)
		}
		if errs := utilvalidation.IsDNS1123Subdomain(b.OperatorArtifact.SourceID); len(errs) > 0 {
			return "", fmt.Errorf("operatorArtifact.sourceID must be a DNS-1123 subdomain: %s", strings.Join(errs, "; "))
		}
		return b.OperatorArtifact.SourceID, nil
	default:
		return "", fmt.Errorf("unknown source type %q", sourceType)
	}
}

// rawJSON returns the raw bytes of an apiextensionsv1.JSON, or nil.
func rawJSON(j *apiextensionsv1.JSON) json.RawMessage {
	if j == nil {
		return nil
	}
	return json.RawMessage(j.Raw)
}

// toTriggerBindingInputs maps validated source-specific CRD bindings to the
// exact non-secret store shape.
func toTriggerBindingInputs(sourceType string, in []v1alpha1.TriggerBinding) []workflow.TriggerBindingInput {
	out := make([]workflow.TriggerBindingInput, 0, len(in))
	for _, b := range in {
		bindingKey, _ := triggerBindingKey(sourceType, b) // validated before materialization
		out = append(out, workflow.TriggerBindingInput{Name: b.Name, BindingKey: bindingKey})
	}
	return out
}

// permittedCapabilities maps the workflow's requested capabilities to the
// gateway capability narrowing persisted in workflow_pool_bindings.permitted_tools
// (tool + maxEffectClass + actions). The gateway enforces both effect and action
// ceilings at discovery/authorization so the workflow is not widened back to
// the pool ceiling (ARCH-016; REQ-001/REQ-010).
func permittedCapabilities(caps []v1alpha1.RequestedCapability) []gateway.Capability {
	out := make([]gateway.Capability, 0, len(caps))
	for _, c := range caps {
		out = append(out, gateway.Capability{Tool: c.Tool, MaxEffectClass: c.MaxEffectClass, Actions: c.Actions})
	}
	return out
}

// workflowScopeIdentityKey is the natural key of the kind=workflow scope
// identity runs execute under. It is a collision-free, workflow-owned key:
// the "workflow:" prefix reserves a keyspace distinct from IdentityMapping's
// "<ns>/<name>" format, so a Workflow CR cannot overwrite or soft-delete an
// unrelated identity (REQ-010 durable workflow-identity boundary). The CR key
// "<ns>/<name>" is stable across version updates of the same Workflow, so all
// versions of one workflow share one execution identity.
func workflowScopeIdentityKey(wf *v1alpha1.Workflow) string {
	return fmt.Sprintf("workflow:%s/%s", wf.Namespace, wf.Name)
}

// agentPoolKeyFromRef is the toolgateway.pools natural key for the referenced
// AgentPool ("<namespace>/<poolRef>").
func agentPoolKeyFromRef(wf *v1alpha1.Workflow) string {
	return fmt.Sprintf("%s/%s", wf.Namespace, wf.Spec.PoolRef)
}

func (r *WorkflowReconciler) patchStatus(ctx context.Context, wf *v1alpha1.Workflow, ready bool, validationStatus, message string, recordObserved bool) error {
	base := wf.DeepCopy()
	wf.Status.Ready = ready
	wf.Status.ValidationStatus = validationStatus
	wf.Status.ValidationMessage = ""
	if validationStatus == v1alpha1.ValidationInvalid {
		wf.Status.ValidationMessage = message
	}
	// ObservedGeneration is advanced only on definitive outcomes: a successful
	// reconcile or a structural validation rejection (no retry until the spec
	// changes). Transient errors (pool not yet materialized, store errors) MUST
	// NOT advance it, so the generation gate retries until converged.
	if recordObserved {
		wf.Status.ObservedGeneration = wf.Generation
	}
	wf.Status.Message = message
	return r.Status().Patch(ctx, wf, client.MergeFrom(base))
}

// SetupWithManager registers the reconciler and watches the referenced
// AgentPool so a workflow is re-reconciled when its pool's grants change or the
// pool is materialized (so the workflow_pool_binding converges).
func (r *WorkflowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Workflow{}).
		Watches(&v1alpha1.AgentPool{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj client.Object) []reconcile.Request {
				pool := obj.(*v1alpha1.AgentPool)
				var wfs v1alpha1.WorkflowList
				if err := mgr.GetClient().List(ctx, &wfs,
					client.InNamespace(pool.Namespace)); err != nil {
					return nil
				}
				var reqs []reconcile.Request
				for i := range wfs.Items {
					if wfs.Items[i].Spec.PoolRef == pool.Name {
						reqs = append(reqs, reconcile.Request{
							NamespacedName: types.NamespacedName{
								Namespace: wfs.Items[i].Namespace,
								Name:      wfs.Items[i].Name,
							},
						})
					}
				}
				return reqs
			},
		)).
		Complete(r)
}
