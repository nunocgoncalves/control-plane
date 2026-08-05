// Package work implements HOR-254's durable customer work domain and the
// single-active-node graph execution service.
package work

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound          = errors.New("work: not found")
	ErrConflict          = errors.New("work: conflict")
	ErrInvalidTransition = errors.New("work: invalid transition")
	ErrInvalidInput      = errors.New("work: invalid input")
	ErrConfirmation      = errors.New("work: consequential action confirmation required")
)

const (
	StateTodo       = "todo"
	StateInProgress = "in_progress"
	StateBlocked    = "blocked"
	StateDone       = "done"
	StateFailed     = "failed"

	NodePending   = "pending"
	NodeRunning   = "running"
	NodeBlocked   = "blocked"
	NodeSucceeded = "succeeded"
	NodeFailed    = "failed"
)

// ArtifactRef is an opaque immutable reference owned by HOR-399.
type ArtifactRef struct {
	ArtifactID string          `json:"artifactId"`
	Role       string          `json:"role,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

// StartInput is the idempotent workflow-start command used by adapters.
type StartInput struct {
	ActorIdentityID    string
	WorkflowKey        string
	WorkflowVersion    string
	IdempotencyKey     string
	Title              string
	Source             json.RawMessage
	SourcePresentation SourcePresentation
	ArtifactRefs       []ArtifactRef
}

// LocalizedText carries approved customer copy. Callers render their requested
// locale and fall back to the other value when customer-authored copy is
// available in only one language.
type LocalizedText struct {
	EN string `json:"en,omitempty"`
	PT string `json:"pt,omitempty"`
}

// PresentationField is one safe business datum shown as source evidence.
type PresentationField struct {
	Label LocalizedText `json:"label"`
	Value string        `json:"value"`
}

// SourcePresentation is the immutable customer-safe source projection. Source
// remains private trigger context and is never serialized by customer APIs.
type SourcePresentation struct {
	Kind        string              `json:"kind"`
	Title       string              `json:"title"`
	Subtitle    string              `json:"subtitle,omitempty"`
	OriginalURL string              `json:"originalUrl,omitempty"`
	Evidence    []PresentationField `json:"evidence,omitempty"`
}

// RevisionInput creates a new attempt on an existing work item.
type RevisionInput struct {
	WorkItemID             string
	ActorIdentityID        string
	FeedbackID             string
	ActionableGuidance     string
	ConfirmedInvocationIDs []string
}

// BlockerResponseInput resolves a human gate or generated consequence gate.
type BlockerResponseInput struct {
	BlockerID              string
	ActorIdentityID        string
	Outcome                string
	Response               json.RawMessage
	ArtifactRefs           []ArtifactRef
	ConfirmedInvocationIDs []string
}

// FeedbackInput saves feedback without starting work.
type FeedbackInput struct {
	WorkItemID      string
	AttemptID       string
	ActorIdentityID string
	Category        string
	Explanation     string
	CorrectedResult json.RawMessage
}

// CompletionReport is the validated complete_step control payload.
type CompletionReport struct {
	Outcome      string          `json:"outcome"`
	Summary      string          `json:"summary"`
	Output       json.RawMessage `json:"output"`
	ArtifactRefs []ArtifactRef   `json:"artifactRefs,omitempty"`
}

// WorkItemFilter selects the relevant Dashboard period and text/state scope.
type WorkItemFilter struct {
	State  string
	Search string
	From   *time.Time
	To     *time.Time
	Limit  int
}

// WorkItem is the customer-safe current-state projection.
type WorkItem struct {
	ID                 string             `json:"id"`
	WorkflowKey        string             `json:"workflowKey"`
	ScopeIdentityID    string             `json:"-"`
	Title              string             `json:"title"`
	Source             json.RawMessage    `json:"-"`
	SourcePresentation SourcePresentation `json:"source"`
	Presentation       WorkPresentation   `json:"presentation"`
	CurrentStep        *BusinessStep      `json:"currentStep,omitempty"`
	Blocker            *BlockerSummary    `json:"blocker,omitempty"`
	CurrentAttemptID   string             `json:"currentAttemptId"`
	State              string             `json:"state"`
	RuntimeState       string             `json:"-"`
	CreatedAt          time.Time          `json:"createdAt"`
	UpdatedAt          time.Time          `json:"updatedAt"`
	StartedAt          *time.Time         `json:"startedAt,omitempty"`
	FinishedAt         *time.Time         `json:"finishedAt,omitempty"`
	ValueConfigured    bool               `json:"valueConfigured"`
	ValueModel         json.RawMessage    `json:"valueModel,omitempty"`
	EstimatedValue     *string            `json:"estimatedValue,omitempty"`
	ValueCurrency      *string            `json:"valueCurrency,omitempty"`
	ValueDisputed      bool               `json:"valueDisputed"`
	FailureSummary     json.RawMessage    `json:"failureSummary,omitempty"`
}

// WorkPresentation is snapshotted from the immutable workflow definition for
// the attempt, so cards never depend on mutable operator configuration.
type WorkPresentation struct {
	WorkflowTitle string `json:"workflowTitle"`
	PersonaName   string `json:"personaName"`
	PersonaAvatar string `json:"personaAvatar,omitempty"`
	Locale        string `json:"locale,omitempty"`
}

// BusinessStep is the current workflow stage without prompt/model details.
type BusinessStep struct {
	Key       string        `json:"key"`
	Label     LocalizedText `json:"label"`
	State     string        `json:"state"`
	StartedAt *time.Time    `json:"startedAt,omitempty"`
}

// BlockerSummary provides enough customer-safe context for a board card.
type BlockerSummary struct {
	ID    string          `json:"id"`
	Kind  string          `json:"kind"`
	Title json.RawMessage `json:"title"`
}

// Attempt is one immutable graph/version snapshot and one runtime run.
type Attempt struct {
	ID                      string          `json:"id"`
	WorkItemID              string          `json:"workItemId"`
	Number                  int             `json:"number"`
	DefinitionKey           string          `json:"definitionKey"`
	DefinitionVersion       string          `json:"definitionVersion"`
	DefinitionDigest        string          `json:"-"`
	GraphSnapshot           json.RawMessage `json:"-"`
	ModelsSnapshot          json.RawMessage `json:"-"`
	PresentationSnapshot    json.RawMessage `json:"-"`
	RevisedFromAttemptID    *string         `json:"revisedFromAttemptId,omitempty"`
	ActionableGuidance      *string         `json:"actionableGuidance,omitempty"`
	ConsequenceConfirmation json.RawMessage `json:"-"`
	CreatedAt               time.Time       `json:"createdAt"`
	FinishedAt              *time.Time      `json:"finishedAt,omitempty"`
}

// NodeExecution is one append-only visit to a graph node.
type NodeExecution struct {
	ID                   string          `json:"id"`
	AttemptID            string          `json:"attemptId"`
	NodeKey              string          `json:"nodeKey"`
	BusinessLabel        json.RawMessage `json:"businessLabel"`
	Visit                int             `json:"visit"`
	ExecutionSeq         int             `json:"executionSeq"`
	Kind                 string          `json:"kind"`
	Prompt               *string         `json:"-"`
	Context              json.RawMessage `json:"-"`
	ModelSnapshot        json.RawMessage `json:"-"`
	SkillsSnapshot       json.RawMessage `json:"-"`
	CapabilitiesSnapshot json.RawMessage `json:"-"`
	WorkspaceTools       bool            `json:"-"`
	TimeoutMS            *int64          `json:"-"`
	State                string          `json:"state"`
	CompletionOutcome    *string         `json:"outcome,omitempty"`
	CompletionSummary    *string         `json:"summary,omitempty"`
	Output               json.RawMessage `json:"output,omitempty"`
	ArtifactRefs         json.RawMessage `json:"artifactRefs,omitempty"`
	CompletionReportedAt *time.Time      `json:"-"`
	StartedAt            *time.Time      `json:"startedAt,omitempty"`
	FinishedAt           *time.Time      `json:"finishedAt,omitempty"`
	CreatedAt            time.Time       `json:"createdAt"`
}

// Blocker is an actionable human or consequence-confirmation request.
type Blocker struct {
	ID                   string          `json:"id"`
	WorkItemID           string          `json:"workItemId"`
	AttemptID            string          `json:"attemptId"`
	NodeExecutionID      *string         `json:"nodeExecutionId,omitempty"`
	Kind                 string          `json:"kind"`
	Title                json.RawMessage `json:"title"`
	Description          json.RawMessage `json:"description"`
	ResponseSchema       json.RawMessage `json:"responseSchema"`
	AllowedOutcomes      json.RawMessage `json:"allowedOutcomes"`
	RequiredConsequences json.RawMessage `json:"requiredConsequences"`
	State                string          `json:"state"`
	ResponseOutcome      *string         `json:"responseOutcome,omitempty"`
	Response             json.RawMessage `json:"response,omitempty"`
	CreatedAt            time.Time       `json:"createdAt"`
	ResolvedAt           *time.Time      `json:"resolvedAt,omitempty"`
}

// Feedback is persisted against the original attempt and starts no work.
type Feedback struct {
	ID               string          `json:"id"`
	WorkItemID       string          `json:"workItemId"`
	AttemptID        string          `json:"attemptId"`
	Category         string          `json:"category"`
	Explanation      *string         `json:"explanation,omitempty"`
	CorrectedResult  json.RawMessage `json:"correctedResult,omitempty"`
	CreatedBy        string          `json:"createdBy"`
	CreatedAt        time.Time       `json:"createdAt"`
	RevisedAttemptID *string         `json:"revisedAttemptId,omitempty"`
}

// WorkArtifact is an immutable artifact linked to the work history. Name and
// other customer labels come only from safe link metadata, never storage keys.
type WorkArtifact struct {
	ArtifactID      string          `json:"artifactId"`
	AttemptID       string          `json:"attemptId"`
	NodeExecutionID *string         `json:"nodeExecutionId,omitempty"`
	Role            string          `json:"role"`
	Metadata        json.RawMessage `json:"metadata"`
	MIMEType        string          `json:"mimeType"`
	SizeBytes       *int64          `json:"sizeBytes,omitempty"`
	Digest          *string         `json:"digest,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
}

// TimelineEvent is a customer-safe, resumable business event.
type TimelineEvent struct {
	Cursor          int64           `json:"cursor"`
	ID              string          `json:"id"`
	WorkItemID      string          `json:"workItemId"`
	AttemptID       *string         `json:"attemptId,omitempty"`
	NodeExecutionID *string         `json:"nodeExecutionId,omitempty"`
	Code            string          `json:"code"`
	Params          json.RawMessage `json:"params"`
	ArtifactRefs    json.RawMessage `json:"artifactRefs"`
	ActorIdentityID *string         `json:"actorIdentityId,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
}

// AssignmentContext is the exact non-secret graph-node context dispatch sends
// to a worker and persists in the active assignment.
type AssignmentContext struct {
	WorkItemID       string
	AttemptID        string
	ScopeIdentityID  string
	AgentPoolKey     string
	Persona          string
	Node             NodeExecution
	AllowedOutcomes  []string
	OutputSchema     json.RawMessage
	Skills           []SkillSnapshot
	ToolPins         json.RawMessage
	Materializations []ArtifactMaterialization
}

// ArtifactMaterialization is one canonical, authorized input copied into the
// session workspace before the child starts.
type ArtifactMaterialization struct {
	ArtifactID   string
	MIMEType     string
	SizeBytes    int64
	Digest       string
	RelativePath string
}

// SkillSnapshot is one immutable skill exposed to the node.
type SkillSnapshot struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

// Consequence is the customer-safe description of a prior external write that
// requires exact confirmation. Summary is trusted localized text rendered from
// the immutable tool descriptor before dispatch; raw arguments/results remain
// excluded. InvocationID is only the exact confirmation token.
type Consequence struct {
	InvocationID string          `json:"invocationId"`
	Summary      json.RawMessage `json:"summary"`
	State        string          `json:"state"`
}
