package controller

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

func terminalResult(outcome, en, pt string) *v1alpha1.ResultPresentation {
	return &v1alpha1.ResultPresentation{Outcomes: []v1alpha1.ResultOutcomePresentation{{
		Outcome: outcome, Summary: v1alpha1.LocalizedText{EN: en, PT: pt},
	}}}
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
	processResult := terminalResult("completed", "Quotation processed", "Pedido de cotação processado")
	processResult.Fields = []v1alpha1.ResultFieldPresentation{{
		Path: []string{"classification"}, Label: v1alpha1.LocalizedText{EN: "Classification", PT: "Classificação"},
		Options: []v1alpha1.ResultValuePresentation{
			{Value: apiextensionsv1.JSON{Raw: []byte(`"engineering"`)}, Label: v1alpha1.LocalizedText{EN: "Engineering", PT: "Engenharia"}},
			{Value: apiextensionsv1.JSON{Raw: []byte(`"pricing"`)}, Label: v1alpha1.LocalizedText{EN: "Pricing", PT: "Preços"}},
		},
	}}
	return &v1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.WorkflowSpec{
			Key: "walter/quotation", Version: "1", PoolRef: poolName, DefaultModelRef: "model-one",
			Source: v1alpha1.WorkflowSource{
				Type: v1alpha1.SourceGraphEmail,
				TriggerBindings: []v1alpha1.TriggerBinding{
					{Name: "inbox", GraphEmail: &v1alpha1.GraphEmailTriggerBinding{MailboxAddress: "inbox@walter.example"}},
				},
			},
			Skills: []v1alpha1.SkillReference{{Name: "walter-quotation", Version: "1.0.0", Digest: "sha256:walter-skill-v1"}},
			RequestedCapabilities: []v1alpha1.RequestedCapability{
				{Tool: "graph.read", MaxEffectClass: "read_only", Actions: []string{"graph.read"}},
				{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
			},
			Graph: v1alpha1.WorkflowGraph{
				EntryNode: "process", MaxTransitions: 20,
				Nodes: []v1alpha1.WorkflowNode{
					{
						Key: "process", Label: v1alpha1.LocalizedText{EN: "Processing quotation", PT: "A processar cotação"},
						Kind: v1alpha1.WorkflowNodeAgentTask, Prompt: "Process the quotation", Skills: []string{"walter-quotation"},
						Capabilities: []string{"graph.read", "graph.excel.write"}, Outcomes: []string{"completed", "needs_review"},
						OutputSchema:       jsonConfig(`{"type":"object","additionalProperties":false,"properties":{"classification":{"type":"string","enum":["engineering","pricing"]}}}`),
						ResultPresentation: processResult,
					},
					{
						Key: "review", Label: v1alpha1.LocalizedText{EN: "Reviewing result", PT: "A rever resultado"},
						Kind: v1alpha1.WorkflowNodeHumanGate, Outcomes: []string{"approved", "changes_requested"},
						ResultPresentation: terminalResult("approved", "Review completed", "Revisão concluída"),
						HumanGate: &v1alpha1.HumanGateSpec{
							Type: v1alpha1.HumanGateDecision, Title: v1alpha1.LocalizedText{EN: "Review", PT: "Revisão"},
							Description: v1alpha1.LocalizedText{EN: "Review the result", PT: "Reveja o resultado"},
							Presentation: v1alpha1.HumanGatePresentation{Outcomes: []v1alpha1.LocalizedText{
								{EN: "Approve", PT: "Aprovar"}, {EN: "Request changes", PT: "Pedir alterações"},
							}},
						},
					},
				},
				Edges:            []v1alpha1.WorkflowEdge{{From: "process", Outcome: "needs_review", To: "review"}, {From: "review", Outcome: "changes_requested", To: "process"}},
				TerminalOutcomes: []v1alpha1.WorkflowTerminalOutcome{{Node: "process", Outcome: "completed"}, {Node: "review", Outcome: "approved"}},
			},
			Presentation: v1alpha1.PresentationSpec{WorkflowTitle: "Quotation Processing", PersonaName: "Walter Ops", Locale: "en"},
		},
	}
}

// TestWorkflowValidation asserts structural + capability validation directly
// against a fake client — no manager/envtest, so it runs in -short mode. Covers
// the acceptance criteria: unknown source/node/capability/route/binding fails,
// and a workflow cannot request capabilities beyond its pool.
func TestWorkflowValidation(t *testing.T) {
	ns := "default"
	pool := poolWithGrants("walter-pool", ns,
		v1alpha1.GatewayGrant{Tool: "graph.read", MaxEffectClass: "read_only", AllowedActions: []string{"graph.read"}},
		v1alpha1.GatewayGrant{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
	)

	newReconciler := func(objs ...client.Object) *WorkflowReconciler {
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = v1alpha1.AddToScheme(scheme)
		objs = append(objs, &v1alpha1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model-one", Namespace: ns}, Spec: v1alpha1.ModelSpec{ModelID: "model-one", BackendRef: "backend"}})
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		return &WorkflowReconciler{
			Client: c, Scheme: scheme,
			SourceAdapters: StaticSourceAdapterRegistry{
				v1alpha1.SourceGraphEmail: {}, v1alpha1.SourceOperatorArtifact: {},
			},
		}
	}
	ctx := context.Background()

	// Valid Walter workflow passes.
	assert.NoError(t, newReconciler(pool).validateSpec(ctx, validWalterWorkflow("w", ns, "walter-pool")))

	missingResultPresentation := validWalterWorkflow("w", ns, "walter-pool")
	missingResultPresentation.Spec.Graph.Nodes[0].ResultPresentation = nil
	err := newReconciler(pool).validateSpec(ctx, missingResultPresentation)
	assert.ErrorContains(t, err, "requires resultPresentation")

	missingResultField := validWalterWorkflow("w", ns, "walter-pool")
	missingResultField.Spec.Graph.Nodes[0].ResultPresentation.Fields = nil
	err = newReconciler(pool).validateSpec(ctx, missingResultField)
	assert.ErrorContains(t, err, "must present every declared schema property")

	missingResultOption := validWalterWorkflow("w", ns, "walter-pool")
	missingResultOption.Spec.Graph.Nodes[0].ResultPresentation.Fields[0].Options = nil
	err = newReconciler(pool).validateSpec(ctx, missingResultOption)
	assert.ErrorContains(t, err, "must localize every enum value")

	nestedResult := validWalterWorkflow("w", ns, "walter-pool")
	nestedResult.Spec.Graph.Nodes[0].OutputSchema = jsonConfig(`{"type":"object","additionalProperties":false,"properties":{"customer":{"type":"object","additionalProperties":false,"properties":{"name":{"type":"string"},"country":{"type":"string"}}}}}`)
	nestedResult.Spec.Graph.Nodes[0].ResultPresentation.Fields = []v1alpha1.ResultFieldPresentation{
		{Path: []string{"customer"}, Label: v1alpha1.LocalizedText{EN: "Customer", PT: "Cliente"}},
		{Path: []string{"customer", "name"}, Label: v1alpha1.LocalizedText{EN: "Name", PT: "Nome"}},
		{Path: []string{"customer", "country"}, Label: v1alpha1.LocalizedText{EN: "Country", PT: "País"}},
	}
	assert.NoError(t, newReconciler(pool).validateSpec(ctx, nestedResult))

	openNestedResult := nestedResult.DeepCopy()
	openNestedResult.Spec.Graph.Nodes[0].OutputSchema = jsonConfig(`{"type":"object","additionalProperties":false,"properties":{"customer":{"type":"object","properties":{"name":{"type":"string"},"country":{"type":"string"}}}}}`)
	err = newReconciler(pool).validateSpec(ctx, openNestedResult)
	assert.ErrorContains(t, err, "must set additionalProperties to false")

	missingBusinessLabel := validWalterWorkflow("w", ns, "walter-pool")
	missingBusinessLabel.Spec.Graph.Nodes[0].Label.PT = ""
	err = newReconciler(pool).validateSpec(ctx, missingBusinessLabel)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "label requires en and pt")

	missingOutcomeLabel := validWalterWorkflow("w", ns, "walter-pool")
	missingOutcomeLabel.Spec.Graph.Nodes[1].HumanGate.Presentation.Outcomes = nil
	err = newReconciler(pool).validateSpec(ctx, missingOutcomeLabel)
	assert.ErrorContains(t, err, "one localized label for each declared outcome")

	missingFieldLabel := validWalterWorkflow("w", ns, "walter-pool")
	missingFieldLabel.Spec.Graph.Nodes[1].HumanGate.ResponseSchema = jsonConfig(`{"type":"object","properties":{"route":{"type":"string","enum":["claims","operations"]}}}`)
	err = newReconciler(pool).validateSpec(ctx, missingFieldLabel)
	assert.ErrorContains(t, err, `property "route" requires localized presentation`)

	localizedResponse := validWalterWorkflow("w", ns, "walter-pool")
	localizedResponse.Spec.Graph.Nodes[1].HumanGate.ResponseSchema = jsonConfig(`{"type":"object","additionalProperties":false,"properties":{"route":{"type":"string","enum":["claims","operations"]}}}`)
	localizedResponse.Spec.Graph.Nodes[1].HumanGate.Presentation.Fields = []v1alpha1.HumanGateFieldPresentation{{
		Key: "route", Label: v1alpha1.LocalizedText{EN: "Route", PT: "Destino"},
		Options: []v1alpha1.LocalizedText{{EN: "Claims", PT: "Reclamações"}, {EN: "Operations", PT: "Operações"}},
	}}
	localizedResponse.Spec.Graph.Nodes[1].ResultPresentation.Fields = []v1alpha1.ResultFieldPresentation{{
		Path: []string{"route"}, Label: v1alpha1.LocalizedText{EN: "Route", PT: "Destino"},
		Options: []v1alpha1.ResultValuePresentation{
			{Value: apiextensionsv1.JSON{Raw: []byte(`"claims"`)}, Label: v1alpha1.LocalizedText{EN: "Claims", PT: "Reclamações"}},
			{Value: apiextensionsv1.JSON{Raw: []byte(`"operations"`)}, Label: v1alpha1.LocalizedText{EN: "Operations", PT: "Operações"}},
		},
	}}
	assert.NoError(t, newReconciler(pool).validateSpec(ctx, localizedResponse))

	rootScalarResponse := validWalterWorkflow("w", ns, "walter-pool")
	rootScalarResponse.Spec.Graph.Nodes[1].HumanGate.ResponseSchema = jsonConfig(`{"type":"string"}`)
	err = newReconciler(pool).validateSpec(ctx, rootScalarResponse)
	assert.ErrorContains(t, err, "must declare type object")

	composedResponse := validWalterWorkflow("w", ns, "walter-pool")
	composedResponse.Spec.Graph.Nodes[1].HumanGate.ResponseSchema = jsonConfig(`{"type":"object","allOf":[{"required":["amount"]}]}`)
	err = newReconciler(pool).validateSpec(ctx, composedResponse)
	assert.ErrorContains(t, err, `keyword "allOf" is not supported`)

	indirectRequiredResponse := validWalterWorkflow("w", ns, "walter-pool")
	indirectRequiredResponse.Spec.Graph.Nodes[1].HumanGate.ResponseSchema = jsonConfig(`{"type":"object","required":["amount"]}`)
	err = newReconciler(pool).validateSpec(ctx, indirectRequiredResponse)
	assert.ErrorContains(t, err, `required property "amount" must be declared directly`)

	indirectPropertyResponse := validWalterWorkflow("w", ns, "walter-pool")
	indirectPropertyResponse.Spec.Graph.Nodes[1].HumanGate.ResponseSchema = jsonConfig(`{"type":"object","properties":{"amount":{"$ref":"#/$defs/amount"}},"$defs":{"amount":{"type":"number"}}}`)
	err = newReconciler(pool).validateSpec(ctx, indirectPropertyResponse)
	assert.ErrorContains(t, err, `keyword "$defs" is not supported`)

	for name, property := range map[string]string{
		"string pattern": `{"type":"string","pattern":"^[A-Z]+$"}`,
		"string length":  `{"type":"string","minLength":3}`,
		"number range":   `{"type":"number","minimum":1}`,
		"nested object":  `{"type":"object","properties":{"code":{"type":"string"}}}`,
		"array items":    `{"type":"array","items":{"type":"string"}}`,
	} {
		t.Run("rejects unsupported response constraint "+name, func(t *testing.T) {
			constrained := validWalterWorkflow("w", ns, "walter-pool")
			constrained.Spec.Graph.Nodes[1].HumanGate.ResponseSchema = jsonConfig(
				`{"type":"object","properties":{"value":` + property + `}}`,
			)
			err := newReconciler(pool).validateSpec(ctx, constrained)
			assert.ErrorContains(t, err, "is not supported by the v1 Dashboard form")
		})
	}

	// Unknown source type -> rejected.
	badSource := validWalterWorkflow("w", ns, "walter-pool")
	badSource.Spec.Source.Type = "slack"
	require.Error(t, newReconciler(pool).validateSpec(ctx, badSource))

	// Recognized but not installed source adapter -> rejected before Ready.
	missingAdapter := newReconciler(pool)
	missingAdapter.SourceAdapters = StaticSourceAdapterRegistry{v1alpha1.SourceOperatorArtifact: {}}
	err = missingAdapter.validateSpec(ctx, validWalterWorkflow("w", ns, "walter-pool"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no installed source adapter")

	// Unknown node kind -> rejected.
	badNode := validWalterWorkflow("w", ns, "walter-pool")
	badNode.Spec.Graph.Nodes[0].Kind = "magic"
	require.Error(t, newReconciler(pool).validateSpec(ctx, badNode))

	// Duplicate node key -> rejected.
	dupNode := validWalterWorkflow("w", ns, "walter-pool")
	dupNode.Spec.Graph.Nodes = append(dupNode.Spec.Graph.Nodes, dupNode.Spec.Graph.Nodes[0])
	require.Error(t, newReconciler(pool).validateSpec(ctx, dupNode))

	// Every declared outcome requires exactly one edge or terminal route.
	uncovered := validWalterWorkflow("w", ns, "walter-pool")
	uncovered.Spec.Graph.TerminalOutcomes = uncovered.Spec.Graph.TerminalOutcomes[1:]
	require.Error(t, newReconciler(pool).validateSpec(ctx, uncovered))

	// Human gates require business-readable request metadata.
	badGate := validWalterWorkflow("w", ns, "walter-pool")
	badGate.Spec.Graph.Nodes[1].HumanGate = nil
	require.Error(t, newReconciler(pool).validateSpec(ctx, badGate))

	// Duplicate trigger binding name -> rejected.
	dupBinding := validWalterWorkflow("w", ns, "walter-pool")
	dupBinding.Spec.Source.TriggerBindings = append(dupBinding.Spec.Source.TriggerBindings,
		v1alpha1.TriggerBinding{Name: "inbox", GraphEmail: &v1alpha1.GraphEmailTriggerBinding{MailboxAddress: "other@walter.example"}})
	require.Error(t, newReconciler(pool).validateSpec(ctx, dupBinding))

	// Capability beyond pool: tool not granted -> rejected.
	ungranted := validWalterWorkflow("w", ns, "walter-pool")
	ungranted.Spec.RequestedCapabilities = append(ungranted.Spec.RequestedCapabilities,
		v1alpha1.RequestedCapability{Tool: "graph.mail.send", MaxEffectClass: "idempotent_write"})
	err = newReconciler(pool).validateSpec(ctx, ungranted)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not granted")

	// Capability beyond pool: effect class exceeds grant -> rejected.
	tooMuch := validWalterWorkflow("w", ns, "walter-pool")
	tooMuch.Spec.RequestedCapabilities = []v1alpha1.RequestedCapability{
		{Tool: "graph.read", MaxEffectClass: "non_idempotent_write"},    // pool grants read_only
		{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"}, // keep tool_call step authorized
	}
	err = newReconciler(pool).validateSpec(ctx, tooMuch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds AgentPool grant")

	// Capability action not in pool's allowedActions -> rejected.
	badAction := validWalterWorkflow("w", ns, "walter-pool")
	badAction.Spec.RequestedCapabilities = []v1alpha1.RequestedCapability{
		{Tool: "graph.read", MaxEffectClass: "read_only", Actions: []string{"*"}}, // pool allows only graph.read
		{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},           // keep tool_call step authorized
	}
	err = newReconciler(pool).validateSpec(ctx, badAction)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed by AgentPool grant")

	// V1 descriptors are undecomposed, so a non-empty workflow action set must
	// include the effective action (the tool name) or wildcard.
	badEffectiveAction := validWalterWorkflow("w", ns, "walter-pool")
	badEffectiveAction.Spec.RequestedCapabilities[0].Actions = []string{"read"}
	err = newReconciler(pool).validateSpec(ctx, badEffectiveAction)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "v1 undecomposed tool action")

	// Referenced AgentPool not found -> rejected.
	err = newReconciler().validateSpec(ctx, validWalterWorkflow("w", ns, "missing-pool"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Agent-node capability narrowing cannot name an undeclared workflow tool.
	unknownTool := validWalterWorkflow("w", ns, "walter-pool")
	unknownTool.Spec.Graph.Nodes[0].Capabilities = append(unknownTool.Spec.Graph.Nodes[0].Capabilities, "graph.mail.send")
	err = newReconciler(pool).validateSpec(ctx, unknownTool)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not requested by the workflow")

	// Trigger binding must use the source-specific typed payload. There is no
	// opaque config field through which a renamed/nested secret can be persisted.
	wrongBindingShape := validWalterWorkflow("w", ns, "walter-pool")
	wrongBindingShape.Spec.Source.TriggerBindings[0] = v1alpha1.TriggerBinding{
		Name: "inbox", OperatorArtifact: &v1alpha1.OperatorArtifactTriggerBinding{SourceID: "secret-shaped-route"},
	}
	err = newReconciler(pool).validateSpec(ctx, wrongBindingShape)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "graphEmail must be set exclusively")

	// Skill identity must be exact and immutable.
	missingSkillDigest := validWalterWorkflow("w", ns, "walter-pool")
	missingSkillDigest.Spec.Skills[0].Digest = ""
	err = newReconciler(pool).validateSpec(ctx, missingSkillDigest)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "digest is required")

	// key containing ":" -> rejected (definition_key wire format ambiguity, REQ-010).
	colonKey := validWalterWorkflow("w", ns, "walter-pool")
	colonKey.Spec.Key = "a:b"
	err = newReconciler(pool).validateSpec(ctx, colonKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not contain \":\"")

	// version containing ":" -> rejected.
	colonVer := validWalterWorkflow("w", ns, "walter-pool")
	colonVer.Spec.Version = "1:0"
	err = newReconciler(pool).validateSpec(ctx, colonVer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not contain \":\"")

	// operator_artifact (XBS) workflow is representable without customer rules.
	xbs := &v1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "xbs", Namespace: ns},
		Spec: v1alpha1.WorkflowSpec{
			Key: "xbs/shipment", Version: "1", PoolRef: "walter-pool", DefaultModelRef: "model-one",
			Source: v1alpha1.WorkflowSource{
				Type: v1alpha1.SourceOperatorArtifact,
				TriggerBindings: []v1alpha1.TriggerBinding{
					{Name: "exports", OperatorArtifact: &v1alpha1.OperatorArtifactTriggerBinding{SourceID: "xbs-exports"}},
				},
			},
			Graph: v1alpha1.WorkflowGraph{EntryNode: "map", MaxTransitions: 10,
				Nodes: []v1alpha1.WorkflowNode{{Key: "map", Label: v1alpha1.LocalizedText{EN: "Building shipment map", PT: "A criar mapa de envios"}, Kind: v1alpha1.WorkflowNodeAgentTask, Prompt: "Build the shipment map", Outcomes: []string{"completed"},
					ResultPresentation: terminalResult("completed", "Export processed", "Exportação processada")}},
				TerminalOutcomes: []v1alpha1.WorkflowTerminalOutcome{{Node: "map", Outcome: "completed"}}},
			Presentation: v1alpha1.PresentationSpec{WorkflowTitle: "Shipment Map", PersonaName: "XBS Ops", Locale: "pt"},
		},
	}
	assert.NoError(t, newReconciler(pool).validateSpec(ctx, xbs), "XBS operator_artifact workflow should be representable")
}

func TestWorkflowCanonicalSpecIncludesAllExecutionIdentityInputs(t *testing.T) {
	base := validWalterWorkflow("workflow-a", "default", "walter-pool")
	baseJSON, err := json.Marshal(buildCanonicalSpec(base))
	require.NoError(t, err)

	graphChanged := base.DeepCopy()
	graphChanged.Spec.Graph.Edges[1].To = "review"
	graphJSON, err := json.Marshal(buildCanonicalSpec(graphChanged))
	require.NoError(t, err)
	assert.NotEqual(t, string(baseJSON), string(graphJSON), "graph routing semantics must affect the immutable digest input")

	skillChanged := base.DeepCopy()
	skillChanged.Spec.Skills[0].Digest = "sha256:walter-skill-v2"
	skillJSON, err := json.Marshal(buildCanonicalSpec(skillChanged))
	require.NoError(t, err)
	assert.NotEqual(t, string(baseJSON), string(skillJSON), "skill identity must affect the immutable digest input")

	otherCR := base.DeepCopy()
	otherCR.Name = "workflow-b"
	otherJSON, err := json.Marshal(buildCanonicalSpec(otherCR))
	require.NoError(t, err)
	assert.NotEqual(t, string(baseJSON), string(otherJSON), "metadata-derived scope identity must affect immutable definition identity")
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
		SourceAdapters: StaticSourceAdapterRegistry{
			v1alpha1.SourceGraphEmail: {}, v1alpha1.SourceOperatorArtifact: {},
		},
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
// validation status; deleting the CR soft-deletes the materialized state; and
// recreating the same artifact revives that state and becomes Ready.
func TestWorkflowReconcile(t *testing.T) {
	adminClient, ctx, wfStore, gwStore, idStore := newWorkflowTestEnv(t)
	ns := "default"

	// Create an AgentPool CR (the policy ceiling) and pre-materialize its pool
	// row in toolgateway (simulating the AgentPool reconciler having run).
	pool := poolWithGrants("walter-pool", ns,
		v1alpha1.GatewayGrant{Tool: "graph.read", MaxEffectClass: "read_only", AllowedActions: []string{"graph.read"}},
		v1alpha1.GatewayGrant{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
	)
	require.NoError(t, adminClient.Create(ctx, pool))
	require.NoError(t, adminClient.Create(ctx, &v1alpha1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model-one", Namespace: ns}, Spec: v1alpha1.ModelSpec{ModelID: "model-one", BackendRef: "backend"}}))
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

	// workflow_pool_binding materialized with the permitted capability narrowing.
	binding, err := gwStore.GetWorkflowPoolBinding(ctx, workflow.DefinitionKey("walter/quotation", "1"))
	require.NoError(t, err)
	assert.Equal(t, []gateway.Capability{
		{Tool: "graph.read", MaxEffectClass: "read_only", Actions: []string{"graph.read"}},
		{Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"},
	}, binding.PermittedCapabilities)

	// ResolveForAttempt returns the exact versioned definition + permitted tools.
	resolved, err := wfStore.ResolveForAttempt(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, def.ID, resolved.Definition.ID)
	assert.Equal(t, []string{"graph.read", "graph.excel.write"}, resolved.PermittedTools)
	require.Len(t, resolved.Skills, 1)
	assert.Equal(t, "sha256:walter-skill-v1", resolved.Skills[0].Digest)
	assert.Equal(t, "process", resolved.Spec.Graph.EntryNode)
	assert.Equal(t, []string{"graph.read", "graph.excel.write"}, resolved.Spec.Graph.Nodes[0].Capabilities)
	assert.Equal(t, "workflow:default/walter-quotation", resolved.Spec.ScopeIdentityKey)

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
		// Same version, different graph content -> immutability violation.
		g.Spec.Graph.Nodes[0].Prompt = "changed prompt"
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

	// A separate CR cannot claim a different version of the same logical key;
	// unversioned resolution must never cross the durable workflow owner.
	duplicate := validWalterWorkflow("walter-duplicate", ns, "walter-pool")
	duplicate.Spec.Version = "2"
	require.NoError(t, adminClient.Create(ctx, duplicate))
	duplicateNN := types.NamespacedName{Name: duplicate.Name, Namespace: ns}
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		if err := adminClient.Get(ctx, duplicateNN, &g); err != nil {
			return false
		}
		return g.Status.ValidationStatus == v1alpha1.ValidationInvalid &&
			strings.Contains(g.Status.ValidationMessage, "owned by another workflow")
	}, 15*time.Second, 200*time.Millisecond, "logical definition key should retain one durable CR owner")
	require.NoError(t, adminClient.Delete(ctx, duplicate))

	// Move this CR to a new spec.key/version. The old definition remains valid
	// while the CR exists, but both keys retain the same persisted owner identity.
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		if err := adminClient.Get(ctx, nn, &g); err != nil {
			return false
		}
		if g.Status.ObservedGeneration != g.Generation {
			return false
		}
		g.Spec = validWalterWorkflow(g.Name, ns, "walter-pool").Spec
		g.Spec.Key = "walter/renamed"
		g.Spec.Version = "2"
		return adminClient.Update(ctx, &g) == nil
	}, 15*time.Second, 200*time.Millisecond, "should publish the renamed workflow key")
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		if err := adminClient.Get(ctx, nn, &g); err != nil {
			return false
		}
		return g.Status.Ready && g.Status.ValidationStatus == v1alpha1.ValidationValid &&
			g.Status.ObservedGeneration == g.Generation
	}, 15*time.Second, 200*time.Millisecond, "renamed workflow should become Ready")
	_, err = wfStore.GetDefinition(ctx, "walter/renamed", "2")
	require.NoError(t, err)

	// Delete the CR -> every definition and authorization binding owned by its
	// stable scope identity is revoked, including the previous spec.key.
	require.NoError(t, adminClient.Delete(ctx, wf))
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		return errors.IsNotFound(adminClient.Get(ctx, nn, &g))
	}, 15*time.Second, 200*time.Millisecond, "Workflow should be deleted after finalizer cleanup")
	_, err = wfStore.GetDefinition(ctx, "walter/quotation", "1")
	assert.ErrorIs(t, err, workflow.ErrNotFound, "original definition should be soft-deleted")
	_, err = wfStore.GetDefinition(ctx, "walter/renamed", "2")
	assert.ErrorIs(t, err, workflow.ErrNotFound, "renamed definition should be soft-deleted")
	_, err = gwStore.GetWorkflowPoolBinding(ctx, workflow.DefinitionKey("walter/quotation", "1"))
	assert.ErrorIs(t, err, gateway.ErrNotFound, "original authorization binding should be revoked")
	_, err = gwStore.GetWorkflowPoolBinding(ctx, workflow.DefinitionKey("walter/renamed", "2"))
	assert.ErrorIs(t, err, gateway.ErrNotFound, "renamed authorization binding should be revoked")

	// Recreate the original Git artifact under the same Workflow identity. The
	// durable definition, trigger binding, identity, and pool binding rows are
	// revived rather than blocked by their soft-deleted unique keys.
	recreated := validWalterWorkflow("walter-quotation", ns, "walter-pool")
	require.NoError(t, adminClient.Create(ctx, recreated))
	require.Eventually(t, func() bool {
		var g v1alpha1.Workflow
		if err := adminClient.Get(ctx, nn, &g); err != nil {
			return false
		}
		return g.Status.Ready && g.Status.ValidationStatus == v1alpha1.ValidationValid &&
			g.Status.ObservedGeneration == g.Generation
	}, 15*time.Second, 200*time.Millisecond, "recreated Workflow should become Ready")
	revived, err := wfStore.GetDefinition(ctx, "walter/quotation", "1")
	require.NoError(t, err)
	assert.Equal(t, def.ID, revived.ID, "recreation should preserve the immutable definition identity")
	revivedBindings, err := wfStore.ListTriggerBindings(ctx, revived.ID)
	require.NoError(t, err)
	require.Len(t, revivedBindings, 1)
	_, err = gwStore.GetWorkflowPoolBinding(ctx, workflow.DefinitionKey("walter/quotation", "1"))
	require.NoError(t, err, "recreation should revive the workflow authorization binding")
}
