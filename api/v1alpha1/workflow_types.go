package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkflowSpec defines the desired state of a Workflow: one versioned
// customer operational workflow deployed from product/client overlay artifacts
// (REQ-001). It carries its source adapter/trigger, deterministic steps and
// agent tasks, requested gateway capabilities, customer-facing workflow/persona
// labels, completion rule, blocker behavior, and value-model reference. The
// reconciler materializes it into the Postgres workflow schema (Git -> DB
// bridge) with an immutable version identity, validates it before execution,
// and binds it to the referenced AgentPool's maximum gateway grants
// (ARCH-018). Customer secret VALUES are never embedded here; authenticated
// source access resolves credentials through the AgentPool's credentialBindings
// via the gateway (ARCH-008).
// +kubebuilder:object:generate=true
type WorkflowSpec struct {
	// key is the stable natural key of the workflow (e.g. "walter/quotation").
	// It identifies the workflow across versions; a new version publishes a new
	// immutable definition row under the same key. It MUST NOT contain ":" — the
	// definition_key wire format is "<key>:<version>", so ":" would make the
	// concatenated key ambiguous (REQ-010 scope isolation).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[^:]+$`
	Key string `json:"key"`

	// version is the immutable version identity component (e.g. "1", "2"). A
	// content change MUST be published under a new version; the reconciler
	// rejects a content change under an already-registered (key, version)
	// (ARCH-007 immutability). It MUST NOT contain ":" — the definition_key wire
	// format is "<key>:<version>", so ":" would make the concatenated key
	// ambiguous (REQ-010 scope isolation).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[^:]+$`
	Version string `json:"version"`

	// poolRef names the AgentPool (in the Workflow's namespace) that is the
	// deployable security/integration boundary for this workflow (ARCH-018).
	// Requested capabilities are validated against this pool's gatewayGrants,
	// and the workflow's permitted tools are bound to this pool.
	// +kubebuilder:validation:Required
	PoolRef string `json:"poolRef"`

	// source defines the source adapter/trigger that starts a run of this
	// workflow (REQ-002). v1 supports Microsoft Graph email (Walter) and an
	// operator-supplied exported artifact (XBS) without inventing customer-
	// specific schema.
	// +kubebuilder:validation:Required
	Source WorkflowSource `json:"source"`

	// steps is the ordered deterministic plan (agent tasks / tool calls /
	// approval gates) the workflow executes. Mirrors runtime.run_steps kinds.
	// At least one step is required.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Steps []WorkflowStep `json:"steps"`

	// skills are the exact immutable skill artifacts this workflow may load.
	// Each reference carries a logical name plus version and content digest so
	// attempt creation can snapshot skill identity alongside the immutable
	// workflow/config and gateway-tool pins (REQ-003/REQ-011; HOR-252).
	// +optional
	Skills []SkillReference `json:"skills,omitempty"`

	// requestedCapabilities are the gateway tools the workflow requests
	// (REQ-010). Each is validated against the referenced AgentPool's
	// gatewayGrants: a workflow cannot request a tool, effect class, or action
	// beyond its pool. The complete narrowing is bound via
	// toolgateway.workflow_pool_bindings.
	// +optional
	RequestedCapabilities []RequestedCapability `json:"requestedCapabilities,omitempty"`

	// completionRule defines when a run is considered complete (REQ-006/030).
	// Completion is not correctness.
	// +kubebuilder:validation:Required
	CompletionRule CompletionRule `json:"completionRule"`

	// blocker defines the approval-gate behavior that produces a customer-
	// actionable Blocked state (REQ-020). Optional; absent means the workflow
	// has no human blocker step.
	// +optional
	Blocker *BlockerSpec `json:"blocker,omitempty"`

	// valueModelRef references the transparent value model for this workflow
	// (REQ-028). v1 stores the reference; the model definition/evaluation is a
	// downstream concern.
	// +optional
	ValueModelRef string `json:"valueModelRef,omitempty"`

	// presentation carries the customer-facing workflow/persona labels for the
	// single-workflow Dashboard (REQ-021). No separate Persona CRD exists in v1.
	// +kubebuilder:validation:Required
	Presentation PresentationSpec `json:"presentation"`
}

// WorkflowSource defines the source adapter and trigger bindings that start a
// workflow run (REQ-002).
// +kubebuilder:object:generate=true
type WorkflowSource struct {
	// type is the source adapter type. v1 supports graph_email (Microsoft Graph
	// email, Walter) and operator_artifact (operator-supplied exported artifact,
	// XBS). Public ingress validates/persists/acknowledges/starts in the control
	// plane; authenticated outbound reads/actions use the gateway (ARCH-012).
	// +kubebuilder:validation:Enum=graph_email;operator_artifact
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// triggerBindings register non-secret trigger routes for this source. A
	// binding carries only non-secret routing identifiers (mailbox address,
	// artifact source id); customer secret VALUES are never embedded —
	// authenticated source access resolves credentials through the AgentPool's
	// credentialBindings via the gateway (ARCH-008).
	// +optional
	TriggerBindings []TriggerBinding `json:"triggerBindings,omitempty"`
}

// TriggerBinding is one source-specific, non-secret trigger route registration.
// Exactly one source payload must be set and it must match WorkflowSource.type.
// There is deliberately no opaque config field: raw credential values cannot
// enter the Workflow CR or Postgres through trigger registration (ARCH-008).
// +kubebuilder:object:generate=true
type TriggerBinding struct {
	// name is the logical binding name, unique within the workflow.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// graphEmail identifies a Microsoft Graph mailbox route. It is required when
	// source.type=graph_email and forbidden for other source types.
	// +optional
	GraphEmail *GraphEmailTriggerBinding `json:"graphEmail,omitempty"`

	// operatorArtifact identifies a generic operator-supplied artifact source.
	// It is required when source.type=operator_artifact and forbidden for other
	// source types; it does not encode customer-specific artifact schema.
	// +optional
	OperatorArtifact *OperatorArtifactTriggerBinding `json:"operatorArtifact,omitempty"`
}

// GraphEmailTriggerBinding is the typed, non-secret Graph email route config.
// Credentials remain in AgentPool credentialBindings and K8s Secrets.
// +kubebuilder:object:generate=true
type GraphEmailTriggerBinding struct {
	// mailboxAddress is the mailbox whose validated notifications start work.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=3
	MailboxAddress string `json:"mailboxAddress"`
}

// OperatorArtifactTriggerBinding is the typed, non-secret exported-artifact
// route config. sourceID is a logical registration identity, not customer data.
// +kubebuilder:object:generate=true
type OperatorArtifactTriggerBinding struct {
	// sourceID identifies the configured artifact intake route.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	SourceID string `json:"sourceID"`
}

// WorkflowStep is one step of the workflow's deterministic plan. Kinds mirror
// runtime.run_steps (HOR-246).
// +kubebuilder:object:generate=true
type WorkflowStep struct {
	// name is the step name, unique within the workflow. Referenced by
	// completionRule.ref and blocker.step.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// kind is the step kind.
	// +kubebuilder:validation:Enum=agent_task;tool_call;approval_gate
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// config is opaque step configuration (prompt, arguments, branch condition,
	// approver scope). The runtime stores but does not interpret it at definition
	// time; HOR-249 evaluates it.
	//
	// +kubebuilder:pruning:NonPrefixed
	// +optional
	Config *apiextensionsv1.JSON `json:"config,omitempty"`

	// tool is the logical gateway tool/capability a tool_call step invokes. It
	// is required for kind=tool_call and MUST be declared in
	// requestedCapabilities, so a workflow cannot register a tool_call step for
	// an unauthorized or unknown tool (REQ-010; acceptance: unknown tool fails
	// before execution). Ignored for agent_task and approval_gate steps.
	// +optional
	Tool string `json:"tool,omitempty"`
}

// SkillReference identifies one immutable overlay-delivered skill. Name is the
// logical skill identity; version and digest together make updates explicit and
// attributable instead of silently changing an existing attempt (REQ-003/011).
// Skill-specific non-secret configuration belongs in the immutable workflow
// step config and is therefore included in the workflow definition digest.
// +kubebuilder:object:generate=true
type SkillReference struct {
	// name is the logical skill name/path in the product/client overlay.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// version is the operator-published skill version.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Version string `json:"version"`

	// digest is the immutable skill artifact content digest.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Digest string `json:"digest"`
}

// RequestedCapability is one gateway tool the workflow requests (REQ-010).
// Validated against the AgentPool's gatewayGrants: the tool must be granted and
// the requested effect class/action set must not exceed the pool's maximum.
// +kubebuilder:object:generate=true
type RequestedCapability struct {
	// tool is the logical gateway tool/capability name (e.g. "graph.read").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Tool string `json:"tool"`

	// maxEffectClass is the maximum effect class the workflow requests for this
	// tool. It must not exceed the pool grant's maxEffectClass.
	// +kubebuilder:validation:Enum=read_only;idempotent_write;non_idempotent_write
	// +kubebuilder:validation:Required
	MaxEffectClass string `json:"maxEffectClass"`

	// actions is an optional action narrowing. The gateway intersects it with
	// pool/customer policy at discovery and invocation. Under the approved v1
	// descriptor contract an undecomposed tool's effective action is its tool
	// name; an empty list applies only the effect-class ceiling.
	// +optional
	Actions []string `json:"actions,omitempty"`
}

// CompletionRule defines when a run is complete (REQ-006/030). Completion is
// not correctness.
// +kubebuilder:object:generate=true
type CompletionRule struct {
	// type is the completion rule type. all_steps = the run completes when all
	// non-skipped steps succeed; step_succeeded = the run completes when the
	// named step (ref) succeeds.
	// +kubebuilder:validation:Enum=all_steps;step_succeeded
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// ref is the step name referenced when type=step_succeeded. Required for
	// step_succeeded; ignored otherwise.
	// +optional
	Ref string `json:"ref,omitempty"`
}

// BlockerSpec defines the approval-gate behavior that produces a customer-
// actionable Blocked state (REQ-020).
// +kubebuilder:object:generate=true
type BlockerSpec struct {
	// step is the name of the approval_gate step that produces a blocker. It
	// must reference an existing step of kind=approval_gate.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Step string `json:"step"`

	// behavior is what the blocker asks the customer for.
	// +kubebuilder:validation:Enum=information;decision;approval;artifact
	// +kubebuilder:validation:Required
	Behavior string `json:"behavior"`
}

// PresentationSpec carries the customer-facing workflow/persona labels for the
// single-workflow Dashboard (REQ-021/022). No separate Persona CRD exists in v1.
// +kubebuilder:object:generate=true
type PresentationSpec struct {
	// workflowTitle is the customer-facing workflow title shown on the Dashboard.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	WorkflowTitle string `json:"workflowTitle"`

	// personaName is the customer-facing persona name. Worker/model/service
	// identities never replace the persona in normal UI (REQ-021).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	PersonaName string `json:"personaName"`

	// personaAvatar is the customer-facing persona avatar (a URL or reference).
	// Optional.
	// +optional
	PersonaAvatar string `json:"personaAvatar,omitempty"`

	// locale is the presentation locale (REQ-022).
	// +kubebuilder:validation:Enum=en;pt
	// +optional
	Locale string `json:"locale,omitempty"`
}

// WorkflowStatus is the observed state reported by the reconciler.
// +kubebuilder:object:generate=true
type WorkflowStatus struct {
	// ready is true once the workflow definition is materialized and valid.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// validationStatus is the inspectable validation state: valid | invalid.
	// +optional
	ValidationStatus string `json:"validationStatus,omitempty"`

	// validationMessage surfaces the validation error when invalid. Empty on
	// valid.
	// +optional
	ValidationMessage string `json:"validationMessage,omitempty"`

	// versionDigest is the immutable version identity (content digest) of the
	// materialized definition. Re-registering the same digest is idempotent; a
	// content change under the same version is rejected (ARCH-007).
	// +optional
	VersionDigest string `json:"versionDigest,omitempty"`

	// definitionID is the UUID of the materialized workflow definition row.
	// +optional
	DefinitionID string `json:"definitionID,omitempty"`

	// scopeIdentityID is the UUID of the kind=workflow identity runs of this
	// workflow execute under.
	// +optional
	ScopeIdentityID string `json:"scopeIdentityID,omitempty"`

	// observedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// message surfaces the last reconciliation error. Empty on success.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=workflows,scope=Namespaced,shortName=wf
// +kubebuilder:singular=workflow
//
// Workflow is an operator-defined, versioned customer operational workflow
// (REQ-001). The control-plane operator materializes it into the Postgres
// workflow store with an immutable version identity and validates it against
// the referenced AgentPool before execution.
type Workflow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkflowSpec   `json:"spec,omitempty"`
	Status WorkflowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// WorkflowList is a list of Workflow.
type WorkflowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Workflow `json:"items"`
}

// Source adapter types supported in v1 (REQ-002).
const (
	SourceGraphEmail       = "graph_email"
	SourceOperatorArtifact = "operator_artifact"
)

// Step kinds (mirror runtime.run_steps kinds, HOR-246).
const (
	WorkflowStepAgentTask    = "agent_task"
	WorkflowStepToolCall     = "tool_call"
	WorkflowStepApprovalGate = "approval_gate"
)

// Completion rule types.
const (
	CompletionAllSteps      = "all_steps"
	CompletionStepSucceeded = "step_succeeded"
)

// Blocker behaviors (REQ-020).
const (
	BlockerInformation = "information"
	BlockerDecision    = "decision"
	BlockerApproval    = "approval"
	BlockerArtifact    = "artifact"
)

// ValidationStatus values.
const (
	ValidationValid   = "valid"
	ValidationInvalid = "invalid"
)
