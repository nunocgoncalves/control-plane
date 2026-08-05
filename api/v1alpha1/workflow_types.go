package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkflowSpec defines one immutable, operator-authored customer workflow.
// Workflow execution is a single-active-node directed graph (ARCH-019): agent
// tasks use gateway tools as capabilities, while human gates pause the same
// attempt for a customer response. There is deliberately no top-level
// tool_call node.
// +kubebuilder:object:generate=true
type WorkflowSpec struct {
	// key is the stable logical workflow key. It excludes ':' because the
	// cross-schema definition identity is "<key>:<version>".
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[^:]+$`
	Key string `json:"key"`

	// version is the immutable version component. Content changes require a new
	// version and never mutate an existing definition.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[^:]+$`
	Version string `json:"version"`

	// poolRef names the AgentPool security/integration boundary.
	// +kubebuilder:validation:Required
	PoolRef string `json:"poolRef"`

	// defaultModelRef is the Model natural key used by agent nodes that do not
	// declare a narrower override. It is required when the graph has agent nodes.
	// +optional
	DefaultModelRef string `json:"defaultModelRef,omitempty"`

	// source defines the installed source adapter and non-secret trigger routes.
	// +kubebuilder:validation:Required
	Source WorkflowSource `json:"source"`

	// graph is the immutable executable workflow graph.
	// +kubebuilder:validation:Required
	Graph WorkflowGraph `json:"graph"`

	// skills are exact immutable overlay skill identities available to the
	// workflow. Agent nodes may expose a subset by logical name.
	// +optional
	Skills []SkillReference `json:"skills,omitempty"`

	// requestedCapabilities are the workflow-level maximum gateway capabilities.
	// Agent nodes may expose a subset by tool name and can never widen this set.
	// +optional
	RequestedCapabilities []RequestedCapability `json:"requestedCapabilities,omitempty"`

	// valueModelRef references an operator-configured transparent value model.
	// +optional
	ValueModelRef string `json:"valueModelRef,omitempty"`

	// presentation carries customer-facing workflow/persona labels.
	// +kubebuilder:validation:Required
	Presentation PresentationSpec `json:"presentation"`
}

// WorkflowGraph is a directed graph with one active node at a time. Every
// declared node outcome must have exactly one edge or terminal declaration.
// Cycles are legal; maxTransitions is the non-convergence safety bound.
// +kubebuilder:object:generate=true
type WorkflowGraph struct {
	// entryNode is the first node for every initial or revised attempt.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	EntryNode string `json:"entryNode"`

	// maxTransitions bounds edge traversals in one attempt.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=100
	MaxTransitions int32 `json:"maxTransitions,omitempty"`

	// nodes are immutable node definitions keyed within this graph.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Nodes []WorkflowNode `json:"nodes"`

	// edges deterministically map a reported outcome to the next node.
	// +optional
	Edges []WorkflowEdge `json:"edges,omitempty"`

	// terminalOutcomes complete the attempt as Done without claiming correctness.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	TerminalOutcomes []WorkflowTerminalOutcome `json:"terminalOutcomes"`
}

// WorkflowNode is either an agent task or a customer-actionable human gate.
// +kubebuilder:object:generate=true
type WorkflowNode struct {
	// key is unique within the graph and stable across repeated visits.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`

	// label is the localized business-stage name shown to customers. Both v1
	// locales are required by controller validation.
	// +kubebuilder:validation:Required
	Label LocalizedText `json:"label"`

	// kind selects agent execution or a durable customer blocker.
	// +kubebuilder:validation:Enum=agent_task;human_gate
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// prompt is required for agent_task and forbidden for human_gate.
	// +optional
	Prompt string `json:"prompt,omitempty"`

	// modelRef optionally overrides spec.defaultModelRef for this agent node.
	// +optional
	ModelRef string `json:"modelRef,omitempty"`

	// skills narrows spec.skills by logical name for this agent node.
	// +optional
	Skills []string `json:"skills,omitempty"`

	// capabilities narrows spec.requestedCapabilities by tool name for this
	// agent node. Empty means expose none, not inherit all.
	// +optional
	Capabilities []string `json:"capabilities,omitempty"`

	// workspaceTools requests the fixed read/write/edit/bash set. The referenced
	// AgentPool must enable it; a workflow/node can never widen the pool.
	// +optional
	WorkspaceTools bool `json:"workspaceTools,omitempty"`

	// timeout bounds one node execution. Zero uses the runtime default.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// outcomes are the only values complete_step (or a human response) may emit.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Outcomes []string `json:"outcomes"`

	// outputSchema validates complete_step.output for agent_task nodes. It is a
	// JSON Schema document and defaults to an unconstrained object when absent.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	OutputSchema *apiextensionsv1.JSON `json:"outputSchema,omitempty"`

	// humanGate is required for human_gate and forbidden for agent_task.
	// +optional
	HumanGate *HumanGateSpec `json:"humanGate,omitempty"`
}

// HumanGateSpec defines a business-readable blocker and its response contract.
// +kubebuilder:object:generate=true
type HumanGateSpec struct {
	// type is the customer action required.
	// +kubebuilder:validation:Enum=information;decision;approval;artifact
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// title and description are localized customer-facing copy.
	// +kubebuilder:validation:Required
	Title LocalizedText `json:"title"`
	// +kubebuilder:validation:Required
	Description LocalizedText `json:"description"`

	// responseSchema validates the response payload.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	ResponseSchema *apiextensionsv1.JSON `json:"responseSchema,omitempty"`

	// presentation supplies localized labels for every outcome and every
	// customer-visible response field/enum option.
	// +kubebuilder:validation:Required
	Presentation HumanGatePresentation `json:"presentation"`
}

// HumanGatePresentation keeps machine outcomes/schema keys out of customer UI.
// Labels follow the declaration order of WorkflowNode.outcomes and enum values.
// +kubebuilder:object:generate=true
type HumanGatePresentation struct {
	// +kubebuilder:validation:MinItems=1
	Outcomes []LocalizedText `json:"outcomes"`
	// +optional
	Fields []HumanGateFieldPresentation `json:"fields,omitempty"`
}

// HumanGateFieldPresentation localizes one top-level response-schema property.
// +kubebuilder:object:generate=true
type HumanGateFieldPresentation struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
	// +kubebuilder:validation:Required
	Label LocalizedText `json:"label"`
	// options follows the field enum order when the schema declares one.
	// +optional
	Options []LocalizedText `json:"options,omitempty"`
}

// LocalizedText carries approved English/Portuguese customer copy.
// +kubebuilder:object:generate=true
type LocalizedText struct {
	// +optional
	EN string `json:"en,omitempty"`
	// +optional
	PT string `json:"pt,omitempty"`
}

// WorkflowEdge maps exactly one node outcome to a target node.
// +kubebuilder:object:generate=true
type WorkflowEdge struct {
	// +kubebuilder:validation:Required
	From string `json:"from"`
	// +kubebuilder:validation:Required
	Outcome string `json:"outcome"`
	// +kubebuilder:validation:Required
	To string `json:"to"`
}

// WorkflowTerminalOutcome marks one node outcome as attempt completion.
// +kubebuilder:object:generate=true
type WorkflowTerminalOutcome struct {
	// +kubebuilder:validation:Required
	Node string `json:"node"`
	// +kubebuilder:validation:Required
	Outcome string `json:"outcome"`
}

// WorkflowSource defines the source adapter and trigger bindings.
// +kubebuilder:object:generate=true
type WorkflowSource struct {
	// +kubebuilder:validation:Enum=graph_email;operator_artifact
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// +optional
	TriggerBindings []TriggerBinding `json:"triggerBindings,omitempty"`
}

// TriggerBinding is one typed, non-secret source route.
// +kubebuilder:object:generate=true
type TriggerBinding struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +optional
	GraphEmail *GraphEmailTriggerBinding `json:"graphEmail,omitempty"`
	// +optional
	OperatorArtifact *OperatorArtifactTriggerBinding `json:"operatorArtifact,omitempty"`
}

// GraphEmailTriggerBinding is a non-secret Graph mailbox route.
// +kubebuilder:object:generate=true
type GraphEmailTriggerBinding struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=3
	MailboxAddress string `json:"mailboxAddress"`
}

// OperatorArtifactTriggerBinding is a non-secret exported-artifact route.
// +kubebuilder:object:generate=true
type OperatorArtifactTriggerBinding struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	SourceID string `json:"sourceID"`
}

// SkillReference identifies one immutable overlay skill.
// +kubebuilder:object:generate=true
type SkillReference struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Version string `json:"version"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Digest string `json:"digest"`
}

// RequestedCapability is one workflow-level gateway capability ceiling.
// +kubebuilder:object:generate=true
type RequestedCapability struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Tool string `json:"tool"`
	// +kubebuilder:validation:Enum=read_only;idempotent_write;non_idempotent_write
	// +kubebuilder:validation:Required
	MaxEffectClass string `json:"maxEffectClass"`
	// +optional
	Actions []string `json:"actions,omitempty"`
}

// PresentationSpec carries customer-facing workflow/persona labels.
// +kubebuilder:object:generate=true
type PresentationSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	WorkflowTitle string `json:"workflowTitle"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	PersonaName string `json:"personaName"`
	// +optional
	PersonaAvatar string `json:"personaAvatar,omitempty"`
	// +kubebuilder:validation:Enum=en;pt
	// +optional
	Locale string `json:"locale,omitempty"`
}

// WorkflowStatus is the reconciler-observed materialization state.
// +kubebuilder:object:generate=true
type WorkflowStatus struct {
	// +optional
	Ready bool `json:"ready,omitempty"`
	// +optional
	ValidationStatus string `json:"validationStatus,omitempty"`
	// +optional
	ValidationMessage string `json:"validationMessage,omitempty"`
	// +optional
	VersionDigest string `json:"versionDigest,omitempty"`
	// +optional
	DefinitionID string `json:"definitionID,omitempty"`
	// +optional
	ScopeIdentityID string `json:"scopeIdentityID,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=workflows,scope=Namespaced,shortName=wf
// +kubebuilder:singular=workflow
//
// Workflow is an immutable, versioned operator-authored workflow graph.
type Workflow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkflowSpec   `json:"spec,omitempty"`
	Status            WorkflowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
//
// WorkflowList is a list of Workflow.
type WorkflowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workflow `json:"items"`
}

const (
	SourceGraphEmail       = "graph_email"
	SourceOperatorArtifact = "operator_artifact"

	WorkflowNodeAgentTask = "agent_task"
	WorkflowNodeHumanGate = "human_gate"

	HumanGateInformation = "information"
	HumanGateDecision    = "decision"
	HumanGateApproval    = "approval"
	HumanGateArtifact    = "artifact"

	ValidationValid   = "valid"
	ValidationInvalid = "invalid"
)
