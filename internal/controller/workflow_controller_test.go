package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/gateway"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

// jsonConfig wraps a raw JSON snippet into an apiextensionsv1.JSON pointer.
func jsonConfig(s string) *apiextensionsv1.JSON {
	if s == "" {
		return nil
	}
	return &apiextensionsv1.JSON{Raw: []byte(s)}
}

// poolWithGrants builds an AgentPool CR carrying the given gateway grants (the
// policy ceiling a workflow cannot widen). Only the fields the Workflow
// reconciler reads are populated.
func poolWithGrants(name, ns string, grants ...v1alpha1.GatewayGrant) *v1alpha1.AgentPool {
	return &v1alpha1.AgentPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.AgentPoolSpec{
			Replicas:       0,
			WorkerImage:    "harness:latest",
			PodSecurity:    "baseline",
			WorkspaceTools: false,
			Identity:       v1alpha1.PoolIdentitySpec{TrustDomain: "iterabase.local", CASecretRef: v1alpha1.LocalKeyRef{Name: "platform-ca"}},
			Sandbox:        v1alpha1.SandboxSpec{StorageClassName: "rwx", AccessMode: corev1.ReadWriteMany, Size: resource.MustParse("1Gi")},
			Gateways: v1alpha1.PoolGatewaysSpec{
				ControlPlane:     v1alpha1.GatewayEndpoint{URL: "https://cp:8443", ServerName: "cp", Selector: gwSelector("cp")},
				ToolGateway:      v1alpha1.GatewayEndpoint{URL: "https://gw:8443", ServerName: "gw", Selector: gwSelector("gw")},
				InferenceGateway: v1alpha1.GatewayEndpoint{URL: "https://ig:8443", ServerName: "ig", Selector: gwSelector("ig")},
			},
			GatewayGrants: grants,
		},
	}
}

// validWalterWorkflow builds a valid graph_email workflow referencing the pool.
func validWalterWorkflow(name, ns, poolName string) *v1alpha1.Workflow {
	return &v1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.WorkflowSpec{
			Key:     "walter/quotation",
			Version: "1",
			PoolRef: poolName,
			Source: v1alpha1.WorkflowSource{
				Type: v1alpha1.SourceGraphEmail,
				TriggerBindings: []v1alpha1.TriggerBinding{
					{Name: "inbox", BindingKey: "inbox@walter.example", Config: jsonConfig(`{"folder":"Inbox"}`)},
				},
			},
			Steps: []v1alpha1.WorkflowStep{
				{Name: "classify", Kind: v1alpha1.WorkflowStepAgentTask, Config: jsonConfig(`{"prompt":"classify"}`)},
				{Name: "write", Kind: v1alpha1.WorkflowStepToolCall},
				{Name: "review", Kind: v1alpha1.WorkflowStepApprovalGate},
			},
			RequestedCapabilities: []v1alpha1.RequestedCapability{
				{Tool: "graph.read", MaxEffectClass: "read_only", Actions: []string{"read", "list"}},
				{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
			},
			CompletionRule: v1alpha1.CompletionRule{Type: v1alpha1.CompletionAllSteps},
			Blocker:        &v1alpha1.BlockerSpec{Step: "review", Behavior: v1alpha1.BlockerDecision},
			Presentation:   v1alpha1.PresentationSpec{WorkflowTitle: "Quotation Processing", PersonaName: "Walter Ops", Locale: "en"},
		},
	}
}

// TestWorkflowValidation asserts structural + capability validation directly
// against a fake client — no manager/envtest, so it runs in -short mode. Covers
// the acceptance criteria: unknown source/step/capability/completion rule/
// binding fails, and a workflow cannot request capabilities beyond its pool.
func TestWorkflowValidation(t *testing.T) {
	ns := "default"
	pool := poolWithGrants("walter-pool", ns,
		v1alpha1.GatewayGrant{Tool: "graph.read", MaxEffectClass: "read_only", AllowedActions: []string{"read", "list"}},
		v1alpha1.GatewayGrant{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
	)

	newReconciler := func(objs ...client.Object) *WorkflowReconciler {
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = v1alpha1.AddToScheme(scheme)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		return &WorkflowReconciler{Client: c, Scheme: scheme}
	}
	ctx := context.Background()

	// Valid Walter workflow passes.
	assert.NoError(t, newReconciler(pool).validateSpec(ctx, validWalterWorkflow("w", ns, "walter-pool")))

	// Unknown source type -> rejected.
	badSource := validWalterWorkflow("w", ns, "walter-pool")
	badSource.Spec.Source.Type = "slack"
	require.Error(t, newReconciler(pool).validateSpec(ctx, badSource))

	// Unknown step kind -> rejected.
	badStep := validWalterWorkflow("w", ns, "walter-pool")
	badStep.Spec.Steps[0].Kind = "magic"
	require.Error(t, newReconciler(pool).validateSpec(ctx, badStep))

	// Duplicate step name -> rejected.
	dupStep := validWalterWorkflow("w", ns, "walter-pool")
	dupStep.Spec.Steps = append(dupStep.Spec.Steps, v1alpha1.WorkflowStep{Name: "classify", Kind: v1alpha1.WorkflowStepAgentTask})
	require.Error(t, newReconciler(pool).validateSpec(ctx, dupStep))

	// Unknown completion rule type -> rejected.
	badCompletion := validWalterWorkflow("w", ns, "walter-pool")
	badCompletion.Spec.CompletionRule = v1alpha1.CompletionRule{Type: "magic"}
	require.Error(t, newReconciler(pool).validateSpec(ctx, badCompletion))

	// step_succeeded with unknown ref -> rejected.
	badRef := validWalterWorkflow("w", ns, "walter-pool")
	badRef.Spec.CompletionRule = v1alpha1.CompletionRule{Type: v1alpha1.CompletionStepSucceeded, Ref: "nope"}
	require.Error(t, newReconciler(pool).validateSpec(ctx, badRef))

	// Blocker referencing a non-approval_gate step -> rejected.
	badBlocker := validWalterWorkflow("w", ns, "walter-pool")
	badBlocker.Spec.Blocker = &v1alpha1.BlockerSpec{Step: "classify", Behavior: v1alpha1.BlockerApproval}
	require.Error(t, newReconciler(pool).validateSpec(ctx, badBlocker))

	// Duplicate trigger binding name -> rejected.
	dupBinding := validWalterWorkflow("w", ns, "walter-pool")
	dupBinding.Spec.Source.TriggerBindings = append(dupBinding.Spec.Source.TriggerBindings,
		v1alpha1.TriggerBinding{Name: "inbox", BindingKey: "other@walter.example"})
	require.Error(t, newReconciler(pool).validateSpec(ctx, dupBinding))

	// Capability beyond pool: tool not granted -> rejected.
	ungranted := validWalterWorkflow("w", ns, "walter-pool")
	ungranted.Spec.RequestedCapabilities = append(ungranted.Spec.RequestedCapabilities,
		v1alpha1.RequestedCapability{Tool: "graph.mail.send", MaxEffectClass: "idempotent_write"})
	err := newReconciler(pool).validateSpec(ctx, ungranted)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not granted")

	// Capability beyond pool: effect class exceeds grant -> rejected.
	tooMuch := validWalterWorkflow("w", ns, "walter-pool")
	tooMuch.Spec.RequestedCapabilities = []v1alpha1.RequestedCapability{
		{Tool: "graph.read", MaxEffectClass: "non_idempotent_write"}, // pool grants read_only
	}
	err = newReconciler(pool).validateSpec(ctx, tooMuch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds AgentPool grant")

	// Capability action not in pool's allowedActions -> rejected.
	badAction := validWalterWorkflow("w", ns, "walter-pool")
	badAction.Spec.RequestedCapabilities = []v1alpha1.RequestedCapability{
		{Tool: "graph.read", MaxEffectClass: "read_only", Actions: []string{"send"}}, // pool allows read,list
	}
	err = newReconciler(pool).validateSpec(ctx, badAction)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed by AgentPool grant")

	// Referenced AgentPool not found -> rejected.
	err = newReconciler().validateSpec(ctx, validWalterWorkflow("w", ns, "missing-pool"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// operator_artifact (XBS) workflow is representable without customer rules.
	xbs := &v1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "xbs", Namespace: ns},
		Spec: v1alpha1.WorkflowSpec{
			Key: "xbs/shipment", Version: "1", PoolRef: "walter-pool",
			Source: v1alpha1.WorkflowSource{
				Type: v1alpha1.SourceOperatorArtifact,
				TriggerBindings: []v1alpha1.TriggerBinding{
					{Name: "exports", BindingKey: "xbs-exports", Config: jsonConfig(`{"artifactPath":"exports"}`)},
				},
			},
			Steps:          []v1alpha1.WorkflowStep{{Name: "map", Kind: v1alpha1.WorkflowStepAgentTask}},
			CompletionRule: v1alpha1.CompletionRule{Type: v1alpha1.CompletionAllSteps},
			Presentation:   v1alpha1.PresentationSpec{WorkflowTitle: "Shipment Map", PersonaName: "XBS Ops", Locale: "pt"},
		},
	}
	assert.NoError(t, newReconciler(pool).validateSpec(ctx, xbs), "XBS operator_artifact workflow should be representable")
}

// newWorkflowTestEnv stands up envtest (RBAC enforced) with the CRDs installed
// and the WorkflowReconciler running under the manager-role, backed by real
// Postgres stores (workflow/gateway/identity).
func newWorkflowTestEnv(t *testing.T) (client.Client, context.Context, *workflow.Store, *gateway.Store, *identity.Store) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run envtest (make setup-envtest)")
	}
	pgPool := testutil.NewPostgresPool(t)
	wfStore := workflow.NewStore(pgPool)
	gwStore := gateway.NewStore(pgPool)
	idStore := identity.NewStore(pgPool)
	ctx := context.Background()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	testEnv := &envtest.Environment{
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		},
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = testEnv.Stop() })

	adminClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	saCfg := rbacManagerConfig(t, ctx, cfg, scheme)

	mgr, err := ctrl.NewManager(saCfg, ctrl.Options{
		Scheme:     scheme,
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	require.NoError(t, err)
	require.NoError(t, (&WorkflowReconciler{
		Client:     mgr.GetClient(),
		Scheme:     scheme,
		Store:      wfStore,
		Pools:      gwStore,
		Identities: idStore,
	}).SetupWithManager(mgr))

	mgrCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(mgrCtx) }()
	return adminClient, ctx, wfStore, gwStore, idStore
}

// TestWorkflowReconcile exercises the full Git->DB bridge UNDER RBAC: a valid
// Walter graph_email workflow materializes an immutable definition + trigger
// bindings + a kind=workflow scope identity + a workflow_pool_binding; an
// invalid overlay (capability beyond pool) is rejected with inspectable
// validation status; deleting the CR soft-deletes the materialized state.
func TestWorkflowReconcile(t *testing.T) {
	adminClient, ctx, wfStore, gwStore, idStore := newWorkflowTestEnv(t)
	ns := "default"

	// Create an AgentPool CR (the policy ceiling) and pre-materialize its pool
	// row in toolgateway (simulating the AgentPool reconciler having run).
	pool := poolWithGrants("walter-pool", ns,
		v1alpha1.GatewayGrant{Tool: "graph.read", MaxEffectClass: "read_only", AllowedActions: []string{"read", "list"}},
		v1alpha1.GatewayGrant{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
	)
	require.NoError(t, adminClient.Create(ctx, pool))
	poolKey := "default/walter-pool"
	_, err := gwStore.UpsertPool(ctx, poolKey, "walter-pool", "spiffe://iterabase.local/pools/test/")
	require.NoError(t, err)

	wf := validWalterWorkflow("walter-quotation", ns, "walter-pool")
	require.NoError(t, adminClient.Create(ctx, wf))
	nn := types.NamespacedName{Name: "walter-quotation", Namespace: ns}

	// Wait for Ready + valid + inspectable immutable identity.
	require.Eventually(t, func() bool {
		var got v1alpha1.Workflow
		if err := adminClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.Ready && got.Status.ValidationStatus == v1alpha1.ValidationValid &&
			got.Status.VersionDigest != "" && got.Status.DefinitionID != "" && got.Status.ScopeIdentityID != ""
	}, 15*time.Second, 200*time.Millisecond, "Workflow should become Ready/valid with inspectable identity")

	// The immutable definition is registered (re-registering same digest is a no-op).
	var got v1alpha1.Workflow
	require.NoError(t, adminClient.Get(ctx, nn, &got))
	def, err := wfStore.GetDefinition(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, got.Status.VersionDigest, def.Digest)
	assert.Equal(t, workflow.ValidationValid, def.ValidationStatus)

	// Trigger bindings materialized (non-secret).
	bindings, err := wfStore.ListTriggerBindings(ctx, def.ID)
	require.NoError(t, err)
	require.Len(t, bindings, 1)
	assert.Equal(t, "inbox", bindings[0].Name)
	assert.Equal(t, "inbox@walter.example", bindings[0].BindingKey)

	// Scope identity (kind=workflow) materialized.
	ident, err := idStore.GetIdentityByID(ctx, got.Status.ScopeIdentityID)
	require.NoError(t, err)
	assert.Equal(t, "workflow", ident.Kind)

	// workflow_pool_binding materialized with the permitted tool set.
	binding, err := gwStore.GetWorkflowPoolBinding(ctx, workflow.DefinitionKey("walter/quotation", "1"))
	require.NoError(t, err)
	assert.Equal(t, []string{"graph.read", "graph.excel.write"}, binding.PermittedTools)

	// ResolveForAttempt returns the exact versioned definition + permitted tools.
	resolved, err := wfStore.ResolveForAttempt(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, def.ID, resolved.Definition.ID)
	assert.Equal(t, []string{"graph.read", "graph.excel.write"}, resolved.PermittedTools)

	// An invalid overlay (capability beyond pool) is rejected with inspectable
	// validation status and does not materialize a new definition version.
	bad := validWalterWorkflow("walter-bad", ns, "walter-pool")
	bad.Spec.Key = "walter/bad"
	bad.Spec.RequestedCapabilities = []v1alpha1.RequestedCapability{
		{Tool: "graph.read", MaxEffectClass: "non_idempotent_write"}, // exceeds read_only grant
	}
	require.NoError(t, adminClient.Create(ctx, bad))
	badNN := types.NamespacedName{Name: "walter-bad", Namespace: ns}
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		if err := adminClient.Get(ctx, badNN, &g); err != nil {
			return false
		}
		return g.Status.ValidationStatus == v1alpha1.ValidationInvalid && g.Status.ValidationMessage != ""
	}, 15*time.Second, 200*time.Millisecond, "invalid workflow should have inspectable invalid status")
	_, err = wfStore.GetDefinition(ctx, "walter/bad", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound, "invalid workflow must not materialize a definition")

	// A content change under the same version is rejected (ARCH-007 immutability).
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		if err := adminClient.Get(ctx, nn, &g); err != nil {
			return false
		}
		if g.Status.ObservedGeneration != g.Generation {
			return false
		}
		// Same version, different content (new step) -> immutability violation.
		g.Spec.Steps = append(g.Spec.Steps, v1alpha1.WorkflowStep{Name: "extra", Kind: v1alpha1.WorkflowStepAgentTask})
		return adminClient.Update(ctx, &g) == nil
	}, 15*time.Second, 200*time.Millisecond, "should update the workflow under the same version")

	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		if err := adminClient.Get(ctx, nn, &g); err != nil {
			return false
		}
		return g.Status.ValidationStatus == v1alpha1.ValidationInvalid &&
			assert.Contains(t, g.Status.ValidationMessage, "immutable")
	}, 15*time.Second, 200*time.Millisecond, "content change under same version should be rejected as immutable")

	// Delete the CR -> materialized state soft-deleted.
	require.NoError(t, adminClient.Delete(ctx, wf))
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		return errors.IsNotFound(adminClient.Get(ctx, nn, &g))
	}, 15*time.Second, 200*time.Millisecond, "Workflow should be deleted after finalizer cleanup")
	_, err = wfStore.GetDefinition(ctx, "walter/quotation", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound, "definition should be soft-deleted on CR deletion")
}
