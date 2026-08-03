package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// WorkflowReconciler materializes Workflow CRs into the Postgres workflow store
// (Git -> DB bridge, HOR-252): it validates the definition before execution,
// registers an immutable versioned definition + non-secret trigger bindings,
// creates the kind=workflow scope identity, and binds the workflow's permitted
// gateway tools to the referenced AgentPool (ARCH-018). On CR deletion it
// soft-deletes the definition, bindings, pool binding, and scope identity.
type WorkflowReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Store      *workflow.Store
	Pools      PoolBindingStore     // *gateway.Store
	Identities IdentityMaterializer // *identity.Store
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=workflows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=workflows/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=workflows/finalizers,verbs=update
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=agentpools,verbs=get;list;watch

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

	// Validate before execution (acceptance: unknown source/step/capability/
	// completion rule/binding fails before execution; a workflow cannot request
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
		if errors.Is(err, workflow.ErrImmutableVersion) {
			_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationInvalid, fmt.Sprintf("validation: %v", err), true)
			return ctrl.Result{}, nil
		}
		_ = r.patchStatus(ctx, &wf, false, v1alpha1.ValidationValid, fmt.Sprintf("register definition: %v", err), false)
		return ctrl.Result{}, err
	}

	// Replace the non-secret trigger bindings.
	if err := r.Store.ReplaceTriggerBindings(ctx, def.ID, wf.Spec.Source.Type, toTriggerBindingInputs(wf.Spec.Source.TriggerBindings)); err != nil {
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
// workflow_pool_binding for EVERY published version is revoked before the
// definitions and scope identity, so a transient failure cannot retain a
// usable binding for an older version. All steps are idempotent (soft-delete
// is a no-op on already-deleted rows), so retries converge.
func (r *WorkflowReconciler) cleanupWorkflow(ctx context.Context, wf *v1alpha1.Workflow) error {
	key := wf.Spec.Key
	// Revoke the workflow_pool_binding for every owned version. Listing the
	// active definitions for the key enumerates every published version's
	// definition_key ("<key>:<version>").
	defs, err := r.Store.ListDefinitionsByKey(ctx, key)
	if err != nil {
		return fmt.Errorf("list workflow definitions for cleanup: %w", err)
	}
	for _, d := range defs {
		defKey := workflow.DefinitionKey(d.Key, d.Version)
		if err := r.Pools.SoftDeleteWorkflowPoolBindingByKey(ctx, defKey); err != nil {
			return fmt.Errorf("soft-delete workflow pool binding %s: %w", defKey, err)
		}
	}
	// Revoke the definitions + trigger bindings (all versions) and the scope
	// identity. Rows are retained for history/audit.
	if err := r.Store.SoftDeleteDefinitionByKey(ctx, key); err != nil {
		return fmt.Errorf("soft-delete workflow definitions: %w", err)
	}
	scopeKey := workflowScopeIdentityKey(wf)
	if err := r.Identities.SoftDeleteIdentityByKey(ctx, scopeKey); err != nil {
		return fmt.Errorf("soft-delete workflow scope identity %q: %w", scopeKey, err)
	}
	return nil
}

// validateSpec validates the Workflow spec before execution (acceptance:
// unknown source/step/capability/completion rule/binding fails before
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
	// Source type.
	switch wf.Spec.Source.Type {
	case v1alpha1.SourceGraphEmail, v1alpha1.SourceOperatorArtifact:
	default:
		return fmt.Errorf("spec.source.type must be %q or %q", v1alpha1.SourceGraphEmail, v1alpha1.SourceOperatorArtifact)
	}
	// Trigger bindings: unique names, non-empty bindingKey, and a non-secret
	// config (ARCH-008: raw credential values never enter Postgres). config is
	// validated as a flat JSON object whose keys do not match a secret denylist;
	// an opaque payload is not accepted because it cannot guarantee secrets are
	// excluded.
	seenBindings := make(map[string]bool)
	for i, b := range wf.Spec.Source.TriggerBindings {
		if b.Name == "" {
			return fmt.Errorf("spec.source.triggerBindings[%d].name is required", i)
		}
		if seenBindings[b.Name] {
			return fmt.Errorf("spec.source.triggerBindings[%d].name %q is duplicated", i, b.Name)
		}
		seenBindings[b.Name] = true
		if b.BindingKey == "" {
			return fmt.Errorf("spec.source.triggerBindings[%d].bindingKey is required", i)
		}
		if b.Config != nil {
			if err := validateNonSecretConfig(fmt.Sprintf("spec.source.triggerBindings[%d].config", i), b.Config.Raw); err != nil {
				return err
			}
		}
	}
	// Steps: at least one, unique names, valid kinds; index approval_gate steps
	// for completion/blocker reference checks. A tool_call step must declare its
	// tool and that tool must be a requested capability, so a workflow cannot
	// register a tool_call for an unauthorized/unknown tool (REQ-010; acceptance:
	// unknown tool fails before execution).
	requestedTools := make(map[string]bool, len(wf.Spec.RequestedCapabilities))
	for _, c := range wf.Spec.RequestedCapabilities {
		requestedTools[c.Tool] = true
	}
	if len(wf.Spec.Steps) == 0 {
		return fmt.Errorf("spec.steps must have at least one step")
	}
	seenSteps := make(map[string]string) // name -> kind
	for i, s := range wf.Spec.Steps {
		if s.Name == "" {
			return fmt.Errorf("spec.steps[%d].name is required", i)
		}
		if _, ok := seenSteps[s.Name]; ok {
			return fmt.Errorf("spec.steps[%d].name %q is duplicated", i, s.Name)
		}
		switch s.Kind {
		case v1alpha1.WorkflowStepAgentTask, v1alpha1.WorkflowStepToolCall, v1alpha1.WorkflowStepApprovalGate:
		default:
			return fmt.Errorf("spec.steps[%d].kind %q is unknown (must be agent_task, tool_call, or approval_gate)", i, s.Kind)
		}
		if s.Kind == v1alpha1.WorkflowStepToolCall {
			if s.Tool == "" {
				return fmt.Errorf("spec.steps[%d].tool is required for kind=tool_call", i)
			}
			if !requestedTools[s.Tool] {
				return fmt.Errorf("spec.steps[%d].tool %q is not a requested capability (tool_call must reference a requested gateway capability, REQ-010)", i, s.Tool)
			}
		}
		seenSteps[s.Name] = s.Kind
	}
	// Requested capabilities: valid effect class, no duplicate tools.
	seenCaps := make(map[string]bool)
	for i, c := range wf.Spec.RequestedCapabilities {
		if c.Tool == "" {
			return fmt.Errorf("spec.requestedCapabilities[%d].tool is required", i)
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
		for _, a := range c.Actions {
			if a == "" {
				return fmt.Errorf("spec.requestedCapabilities[%d].actions contains an empty value", i)
			}
		}
	}
	// Completion rule.
	switch wf.Spec.CompletionRule.Type {
	case v1alpha1.CompletionAllSteps:
	case v1alpha1.CompletionStepSucceeded:
		if wf.Spec.CompletionRule.Ref == "" {
			return fmt.Errorf("spec.completionRule.ref is required when type=step_succeeded")
		}
		if _, ok := seenSteps[wf.Spec.CompletionRule.Ref]; !ok {
			return fmt.Errorf("spec.completionRule.ref %q does not reference a known step", wf.Spec.CompletionRule.Ref)
		}
	default:
		return fmt.Errorf("spec.completionRule.type must be all_steps or step_succeeded")
	}
	// Blocker: step must reference an approval_gate step; behavior valid.
	if wf.Spec.Blocker != nil {
		kind, ok := seenSteps[wf.Spec.Blocker.Step]
		if !ok {
			return fmt.Errorf("spec.blocker.step %q does not reference a known step", wf.Spec.Blocker.Step)
		}
		if kind != v1alpha1.WorkflowStepApprovalGate {
			return fmt.Errorf("spec.blocker.step %q must be an approval_gate step", wf.Spec.Blocker.Step)
		}
		switch wf.Spec.Blocker.Behavior {
		case v1alpha1.BlockerInformation, v1alpha1.BlockerDecision, v1alpha1.BlockerApproval, v1alpha1.BlockerArtifact:
		default:
			return fmt.Errorf("spec.blocker.behavior must be information, decision, approval, or artifact")
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
	spec := workflow.CanonicalSpec{
		Key:           wf.Spec.Key,
		PoolRef:       wf.Spec.PoolRef,
		ValueModelRef: wf.Spec.ValueModelRef,
		Source: workflow.CanonicalSource{
			Type: wf.Spec.Source.Type,
		},
		CompletionRule: workflow.CanonicalCompletion{
			Type: wf.Spec.CompletionRule.Type,
			Ref:  wf.Spec.CompletionRule.Ref,
		},
		Presentation: workflow.CanonicalPresentation{
			WorkflowTitle: wf.Spec.Presentation.WorkflowTitle,
			PersonaName:   wf.Spec.Presentation.PersonaName,
			PersonaAvatar: wf.Spec.Presentation.PersonaAvatar,
			Locale:        wf.Spec.Presentation.Locale,
		},
	}
	for _, b := range wf.Spec.Source.TriggerBindings {
		spec.Source.TriggerBindings = append(spec.Source.TriggerBindings, workflow.CanonicalTrigger{
			Name:       b.Name,
			BindingKey: b.BindingKey,
			Config:     rawJSON(b.Config),
		})
	}
	for _, s := range wf.Spec.Steps {
		spec.Steps = append(spec.Steps, workflow.CanonicalStep{
			Name:   s.Name,
			Kind:   s.Kind,
			Config: rawJSON(s.Config),
		})
	}
	for _, c := range wf.Spec.RequestedCapabilities {
		spec.RequestedCapabilities = append(spec.RequestedCapabilities, workflow.CanonicalCapability{
			Tool:           c.Tool,
			MaxEffectClass: c.MaxEffectClass,
			Actions:        c.Actions,
		})
	}
	if wf.Spec.Blocker != nil {
		spec.Blocker = &workflow.CanonicalBlocker{
			Step:     wf.Spec.Blocker.Step,
			Behavior: wf.Spec.Blocker.Behavior,
		}
	}
	return spec
}

// secretKeyDenylist are case-insensitive substrings that indicate a config
// key names a credential/secret. Trigger-binding config carrying such a key is
// rejected so raw credential values never enter Postgres (ARCH-008). This is
// an enforceable boundary on the opaque config payload: config must be a flat
// JSON object of non-secret routing fields, never a secret value.
var secretKeyDenylist = []string{
	"secret", "token", "password", "passwd", "credential", "credential",
	"apikey", "api_key", "privatekey", "private_key", "accesskey", "access_key",
	"clientsecret", "client_secret",
}

// validateNonSecretConfig enforces the ARCH-008 boundary that raw credential
// values never enter Postgres on an opaque trigger-binding config payload. The
// payload must be a JSON object (not a scalar/array/secret blob) whose keys do
// not match the secret denylist. It does not interpret the fields (downstream
// ingress tickets HOR-356/393 define their own non-secret schemas); it only
// guarantees no secret-named value is persisted.
func validateNonSecretConfig(field string, raw []byte) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("%s must be a JSON object of non-secret routing fields: %w", field, err)
	}
	for k := range obj {
		lk := strings.ToLower(k)
		for _, bad := range secretKeyDenylist {
			if strings.Contains(lk, bad) {
				return fmt.Errorf("%s key %q looks like a secret; raw credential values must not be embedded (ARCH-008)", field, k)
			}
		}
	}
	return nil
}

// rawJSON returns the raw bytes of an apiextensionsv1.JSON, or nil.
func rawJSON(j *apiextensionsv1.JSON) json.RawMessage {
	if j == nil {
		return nil
	}
	return json.RawMessage(j.Raw)
}

// toTriggerBindingInputs maps CRD trigger bindings to store inputs.
func toTriggerBindingInputs(in []v1alpha1.TriggerBinding) []workflow.TriggerBindingInput {
	out := make([]workflow.TriggerBindingInput, 0, len(in))
	for _, b := range in {
		var cfg []byte
		if b.Config != nil {
			cfg = b.Config.Raw
		}
		out = append(out, workflow.TriggerBindingInput{
			Name:       b.Name,
			BindingKey: b.BindingKey,
			Config:     cfg,
		})
	}
	return out
}

// permittedCapabilities maps the workflow's requested capabilities to the
// gateway capability narrowing persisted in workflow_pool_bindings.permitted_tools
// (tool + maxEffectClass + actions). The gateway enforces the maxEffectClass
// ceiling at discovery/authorization so the workflow is not widened back to the
// pool ceiling (ARCH-016; REQ-001/REQ-010).
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
