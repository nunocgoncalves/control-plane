package work

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

// Store owns durable customer work and graph execution state.
type Store struct {
	pool      *pgxpool.Pool
	workflows *workflow.Store
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, workflows: workflow.NewStore(pool)}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Start creates one work item + initial graph attempt atomically. Repeating an
// identity/workflow/idempotency key with the same canonical payload returns the
// existing item; a different payload is a conflict.
//
//nolint:gocyclo // Idempotency, immutable resolution, and all initial snapshots must commit as one command.
func (s *Store) Start(ctx context.Context, in StartInput) (WorkItem, bool, error) {
	if in.ActorIdentityID == "" || in.WorkflowKey == "" || in.IdempotencyKey == "" || in.Title == "" {
		return WorkItem{}, false, fmt.Errorf("%w: actor, workflowKey, idempotency key, and title are required", ErrInvalidInput)
	}
	if err := validateSourcePresentation(in.SourcePresentation); err != nil {
		return WorkItem{}, false, err
	}
	in.ArtifactRefs = append([]ArtifactRef(nil), in.ArtifactRefs...)
	for i := range in.ArtifactRefs {
		if in.ArtifactRefs[i].ArtifactID == "" {
			return WorkItem{}, false, fmt.Errorf("%w: artifactId is required", ErrInvalidInput)
		}
		if in.ArtifactRefs[i].Role != "" && in.ArtifactRefs[i].Role != "source" {
			return WorkItem{}, false, fmt.Errorf("%w: work-item start artifacts must use role source", ErrInvalidInput)
		}
		in.ArtifactRefs[i].Role = "source"
	}
	payloadHash, err := startPayloadHash(in)
	if err != nil {
		return WorkItem{}, false, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return WorkItem{}, false, fmt.Errorf("begin start: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize one logical idempotency key before checking/inserting.
	lockSum := sha256.Sum256([]byte(in.ActorIdentityID + "\n" + in.WorkflowKey + "\n" + in.IdempotencyKey))
	lockKey := hex.EncodeToString(lockSum[:])
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return WorkItem{}, false, fmt.Errorf("lock start idempotency key: %w", err)
	}
	var existingID, existingHash string
	err = tx.QueryRow(ctx, `
		SELECT id::text, start_payload_hash FROM work.work_items
		WHERE start_identity_id = $1 AND workflow_key = $2 AND start_idempotency_key = $3`,
		in.ActorIdentityID, in.WorkflowKey, in.IdempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingHash != payloadHash {
			return WorkItem{}, false, fmt.Errorf("%w: idempotency key reused with different payload", ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return WorkItem{}, false, err
		}
		item, err := s.GetWorkItem(ctx, existingID)
		return item, false, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return WorkItem{}, false, fmt.Errorf("check start idempotency: %w", err)
	}

	// Resolve only after the idempotency lookup. An omitted version is part of
	// the caller's payload, so replay still returns the original item even if a
	// newer workflow version has since become eligible.
	resolved, err := s.workflows.ResolveForAttempt(ctx, in.WorkflowKey, in.WorkflowVersion)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return WorkItem{}, false, fmt.Errorf("%w: workflow is unavailable", ErrInvalidInput)
		}
		return WorkItem{}, false, fmt.Errorf("resolve workflow: %w", err)
	}
	itemID := uuid.NewString()
	attemptID := uuid.NewString()
	sourcePresentation, _ := json.Marshal(in.SourcePresentation)
	if _, err := tx.Exec(ctx, `
		INSERT INTO work.work_items
			(id, workflow_key, scope_identity_id, title, source, source_presentation, start_identity_id,
			 start_idempotency_key, start_payload_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		itemID, in.WorkflowKey, resolved.Definition.ScopeIdentityID, in.Title, jsonOrObject(in.Source),
		sourcePresentation, in.ActorIdentityID, in.IdempotencyKey, payloadHash); err != nil {
		return WorkItem{}, false, fmt.Errorf("insert work item: %w", err)
	}
	if err := s.createAttemptTx(ctx, tx, createAttemptInput{
		ID: attemptID, WorkItemID: itemID, Number: 1, ActorIdentityID: in.ActorIdentityID,
		Resolved: resolved, Source: in.Source, SourceArtifacts: in.ArtifactRefs,
	}); err != nil {
		return WorkItem{}, false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE work.work_items SET current_attempt_id = $2 WHERE id = $1`, itemID, attemptID); err != nil {
		return WorkItem{}, false, fmt.Errorf("set current attempt: %w", err)
	}
	if err := appendTimelineTx(ctx, tx, itemID, attemptID, "", "work_item_created",
		map[string]any{"title": in.Title, "workflowKey": in.WorkflowKey}, in.ArtifactRefs, in.ActorIdentityID); err != nil {
		return WorkItem{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkItem{}, false, fmt.Errorf("commit start: %w", err)
	}
	item, err := s.GetWorkItem(ctx, itemID)
	return item, true, err
}

type createAttemptInput struct {
	ID                      string
	WorkItemID              string
	Number                  int
	ActorIdentityID         string
	Resolved                workflow.ResolvedDefinition
	Source                  json.RawMessage
	SourceArtifacts         []ArtifactRef
	RevisedFromAttemptID    string
	RevisionFeedbackID      string
	ActionableGuidance      string
	ConsequenceConfirmation []string
}

//nolint:gocyclo // Attempt/run/node/config/tool creation is intentionally one atomic initialization path.
func (s *Store) createAttemptTx(ctx context.Context, tx pgx.Tx, in createAttemptInput) error {
	def := in.Resolved.Definition
	graphJSON, _ := json.Marshal(in.Resolved.Spec.Graph)
	skillsJSON, _ := json.Marshal(in.Resolved.Skills)
	capsJSON, _ := json.Marshal(in.Resolved.RequestedCapabilities)
	confirmationJSON, _ := json.Marshal(in.ConsequenceConfirmation)

	poolID, err := resolvePoolTx(ctx, tx, workflow.DefinitionKey(def.Key, def.Version))
	if err != nil {
		return err
	}
	models, err := resolveModelsTx(ctx, tx, def.PoolKey, in.Resolved.Spec)
	if err != nil {
		return err
	}
	modelsJSON, _ := json.Marshal(models)
	var valueModelID string
	var valueSnapshot []byte
	if in.RevisedFromAttemptID == "" {
		valueModelID, valueSnapshot, err = resolveValueModelTx(ctx, tx, in.Resolved.Spec.ValueModelRef)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT COALESCE(value_model_id::text,''), value_model_snapshot
			FROM work.attempts WHERE work_item_id=$1 AND number=1`, in.WorkItemID).
			Scan(&valueModelID, &valueSnapshot)
	}
	if err != nil {
		return fmt.Errorf("resolve item value model: %w", err)
	}

	// One graph attempt is one runtime run/session. Graph nodes are created
	// independently and turns bind to exact node-execution visits.
	if _, err := tx.Exec(ctx, `
		INSERT INTO runtime.workflow_runs
			(id, kind, definition_key, scope_identity_id, session_id, session_dir, trigger)
		VALUES ($1::uuid, 'workflow', $2, $3, $4, $4, $5)`,
		in.ID, workflow.DefinitionKey(def.Key, def.Version), def.ScopeIdentityID, in.ID, jsonOrObject(in.Source)); err != nil {
		return fmt.Errorf("insert runtime run: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO work.attempts
			(id, work_item_id, number, definition_id, definition_key, definition_version,
			 definition_digest, graph_snapshot, skills_snapshot, capabilities_snapshot,
			 models_snapshot, presentation_snapshot, value_model_id, value_model_snapshot, revised_from_attempt_id,
			 revision_feedback_id, actionable_guidance, consequence_confirmation)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		in.ID, in.WorkItemID, in.Number, def.ID, def.Key, def.Version, def.Digest,
		graphJSON, skillsJSON, capsJSON, modelsJSON, jsonOrObject(def.Presentation), nullable(valueModelID), nullableJSON(valueSnapshot),
		nullable(in.RevisedFromAttemptID), nullable(in.RevisionFeedbackID), nullable(in.ActionableGuidance), confirmationJSON); err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO runtime.run_pool_assignments (run_id, pool_id) VALUES ($1,$2)`, in.ID, poolID); err != nil {
		return fmt.Errorf("assign attempt pool: %w", err)
	}
	if err := snapshotToolsTx(ctx, tx, in.ID, poolID, in.Resolved.RequestedCapabilities); err != nil {
		return err
	}

	contextValue := map[string]any{
		"source":              jsonValue(in.Source),
		"sourceArtifacts":     append([]ArtifactRef{}, in.SourceArtifacts...),
		"latestByNode":        map[string]any{},
		"executionHistoryRef": map[string]any{"attemptId": in.ID, "throughExecutionSeq": 0},
	}
	if in.ActionableGuidance != "" {
		contextValue["revisionGuidance"] = in.ActionableGuidance
	}
	entry, ok := findNode(in.Resolved.Spec.Graph, in.Resolved.Spec.Graph.EntryNode)
	if !ok {
		return fmt.Errorf("%w: graph entry node disappeared from immutable snapshot", ErrInvalidInput)
	}
	execID, err := insertNodeExecutionTx(ctx, tx, in.ID, entry, 1, 1, contextValue, models, in.Resolved.Spec)
	if err != nil {
		return err
	}
	for _, ref := range in.SourceArtifacts {
		if ref.ArtifactID == "" {
			return fmt.Errorf("%w: artifactId is required", ErrInvalidInput)
		}
		role := ref.Role
		if role == "" {
			role = "source"
		}
		ct, err := tx.Exec(ctx, `
			INSERT INTO work.artifact_links
				(artifact_id, work_item_id, attempt_id, node_execution_id, role, metadata)
			SELECT id,$2,$3,$4,$5,$6 FROM artifact.artifacts
			WHERE id=$1 AND state='available'`,
			ref.ArtifactID, in.WorkItemID, in.ID, execID, role, jsonOrObject(ref.Metadata))
		if err != nil {
			return fmt.Errorf("link source artifact: %w", err)
		}
		if ct.RowsAffected() != 1 {
			return fmt.Errorf("%w: source artifact is not available", ErrInvalidInput)
		}
	}
	if err := appendTimelineTx(ctx, tx, in.WorkItemID, in.ID, execID, "attempt_created",
		map[string]any{"attemptNumber": in.Number, "definitionVersion": def.Version}, nil, in.ActorIdentityID); err != nil {
		return err
	}
	return nil
}

const workItemSelect = `
	SELECT c.id, c.workflow_key, c.scope_identity_id, c.title, c.source, wi.source_presentation,
	       a.presentation_snapshot, c.current_attempt_id, c.customer_state, c.runtime_state,
	       c.created_at, c.updated_at, c.started_at, c.finished_at,
	       a.customer_failure_summary, a.value_model_id IS NOT NULL, a.value_model_snapshot,
	       v.amount::text, v.currency,
	       EXISTS (SELECT 1 FROM work.value_ledger d WHERE d.work_item_id=c.id AND d.kind='feedback_deduction'),
	       step.node_key, step.business_label, step.state, step.started_at,
	       blocker.id::text, blocker.kind, blocker.title
	FROM work.current_work_items c
	JOIN work.work_items wi ON wi.id=c.id
	JOIN work.attempts a ON a.id=c.current_attempt_id
	LEFT JOIN LATERAL (
		SELECT SUM(amount) amount, MIN(currency) currency FROM work.value_ledger WHERE work_item_id=c.id
	) v ON true
	LEFT JOIN LATERAL (
		SELECT node_key,business_label,state,started_at FROM runtime.node_executions
		WHERE attempt_id=c.current_attempt_id ORDER BY execution_seq DESC LIMIT 1
	) step ON true
	LEFT JOIN LATERAL (
		SELECT id,kind,title FROM work.blockers WHERE work_item_id=c.id AND state='open'
	) blocker ON true`

// GetWorkItem returns the customer-safe current projection and value status.
func (s *Store) GetWorkItem(ctx context.Context, id string) (WorkItem, error) {
	out, err := scanWorkItem(s.pool.QueryRow(ctx, workItemSelect+` WHERE c.id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkItem{}, ErrNotFound
	}
	return out, err
}

// ListWorkItems returns customer work ordered newest first for the selected
// Dashboard period. Search is restricted to customer-safe presentation data.
func (s *Store) ListWorkItems(ctx context.Context, filter WorkItemFilter) ([]WorkItem, error) {
	if filter.Limit <= 0 || filter.Limit > 200 {
		filter.Limit = 200
	}
	rows, err := s.pool.Query(ctx, workItemSelect+`
		WHERE ($1='' OR c.customer_state=$1)
		  AND ($2='' OR c.title ILIKE '%' || $2 || '%' OR wi.source_presentation::text ILIKE '%' || $2 || '%')
		  AND (c.customer_state NOT IN ('done','failed') OR $3::timestamptz IS NULL OR COALESCE(c.finished_at,c.created_at) >= $3)
		  AND (c.customer_state NOT IN ('done','failed') OR $4::timestamptz IS NULL OR COALESCE(c.finished_at,c.created_at) < $4)
		ORDER BY c.created_at DESC, c.id DESC LIMIT $5`, filter.State, filter.Search, filter.From, filter.To, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]WorkItem, 0)
	for rows.Next() {
		item, err := scanWorkItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func scanWorkItem(row pgx.Row) (WorkItem, error) {
	var out WorkItem
	var sourcePresentation, presentation, failure []byte
	var stepKey, stepState, blockerID, blockerKind *string
	var stepLabel, blockerTitle []byte
	var stepStartedAt *time.Time
	if err := row.Scan(&out.ID, &out.WorkflowKey, &out.ScopeIdentityID, &out.Title, &out.Source,
		&sourcePresentation, &presentation, &out.CurrentAttemptID, &out.State, &out.RuntimeState,
		&out.CreatedAt, &out.UpdatedAt, &out.StartedAt, &out.FinishedAt, &failure,
		&out.ValueConfigured, &out.ValueModel, &out.EstimatedValue, &out.ValueCurrency, &out.ValueDisputed,
		&stepKey, &stepLabel, &stepState, &stepStartedAt, &blockerID, &blockerKind, &blockerTitle); err != nil {
		return WorkItem{}, err
	}
	if err := json.Unmarshal(sourcePresentation, &out.SourcePresentation); err != nil {
		return WorkItem{}, err
	}
	if err := json.Unmarshal(presentation, &out.Presentation); err != nil {
		return WorkItem{}, err
	}
	if stepKey != nil {
		step := BusinessStep{Key: *stepKey, State: *stepState, StartedAt: stepStartedAt}
		if err := json.Unmarshal(stepLabel, &step.Label); err != nil {
			return WorkItem{}, err
		}
		out.CurrentStep = &step
	}
	if blockerID != nil {
		out.Blocker = &BlockerSummary{ID: *blockerID, Kind: *blockerKind, Title: blockerTitle}
	}
	out.FailureSummary = failure
	return out, nil
}

func (s *Store) ListAttempts(ctx context.Context, itemID string) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, work_item_id, number, definition_key, definition_version, definition_digest,
		       graph_snapshot, models_snapshot, presentation_snapshot, revised_from_attempt_id, actionable_guidance,
		       consequence_confirmation, created_at, finished_at
		FROM work.attempts WHERE work_item_id=$1 ORDER BY number`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Attempt, 0)
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.WorkItemID, &a.Number, &a.DefinitionKey, &a.DefinitionVersion,
			&a.DefinitionDigest, &a.GraphSnapshot, &a.ModelsSnapshot, &a.PresentationSnapshot,
			&a.RevisedFromAttemptID, &a.ActionableGuidance, &a.ConsequenceConfirmation,
			&a.CreatedAt, &a.FinishedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) ListNodeExecutions(ctx context.Context, attemptID string) ([]NodeExecution, error) {
	rows, err := s.pool.Query(ctx, nodeExecutionSelect+` WHERE attempt_id=$1 ORDER BY execution_seq`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]NodeExecution, 0)
	for rows.Next() {
		n, err := scanNodeExecution(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// ListArtifacts returns immutable item-scoped links with safe metadata needed
// for result/source downloads.
func (s *Store) ListArtifacts(ctx context.Context, itemID string) ([]WorkArtifact, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM work.work_items WHERE id=$1)`, itemID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `
		SELECT l.artifact_id,l.attempt_id::text,l.node_execution_id::text,l.role,l.metadata,
		       a.mime_type,a.size_bytes,a.digest,l.created_at
		FROM work.artifact_links l JOIN artifact.artifacts a ON a.id=l.artifact_id
		WHERE l.work_item_id=$1 AND a.state='available'
		ORDER BY l.created_at,l.id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]WorkArtifact, 0)
	for rows.Next() {
		var a WorkArtifact
		if err := rows.Scan(&a.ArtifactID, &a.AttemptID, &a.NodeExecutionID, &a.Role, &a.Metadata,
			&a.MIMEType, &a.SizeBytes, &a.Digest, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) ListTimeline(ctx context.Context, itemID string, after int64, limit int) ([]TimelineEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT cursor, id, work_item_id, attempt_id, node_execution_id, code, params,
		       artifact_refs, actor_identity_id, created_at
		FROM work.timeline_events
		WHERE work_item_id=$1 AND cursor>$2 ORDER BY cursor LIMIT $3`, itemID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimeline(rows)
}

func (s *Store) TimelineSince(ctx context.Context, after int64, limit int) ([]TimelineEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT cursor, id, work_item_id, attempt_id, node_execution_id, code, params,
		       artifact_refs, actor_identity_id, created_at
		FROM work.timeline_events WHERE cursor>$1 ORDER BY cursor LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimeline(rows)
}

func scanTimeline(rows pgx.Rows) ([]TimelineEvent, error) {
	out := make([]TimelineEvent, 0)
	for rows.Next() {
		var e TimelineEvent
		if err := rows.Scan(&e.Cursor, &e.ID, &e.WorkItemID, &e.AttemptID, &e.NodeExecutionID,
			&e.Code, &e.Params, &e.ArtifactRefs, &e.ActorIdentityID, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

const nodeExecutionSelect = `SELECT id, attempt_id, node_key, business_label, visit, execution_seq, kind, prompt,
	context, model_snapshot, skills_snapshot, capabilities_snapshot, workspace_tools, timeout_ms,
	state, completion_outcome, completion_summary, output, artifact_refs, completion_reported_at,
	started_at, finished_at, created_at FROM runtime.node_executions`

func scanNodeExecution(row pgx.Row) (NodeExecution, error) {
	var n NodeExecution
	err := row.Scan(&n.ID, &n.AttemptID, &n.NodeKey, &n.BusinessLabel, &n.Visit, &n.ExecutionSeq, &n.Kind,
		&n.Prompt, &n.Context, &n.ModelSnapshot, &n.SkillsSnapshot, &n.CapabilitiesSnapshot,
		&n.WorkspaceTools, &n.TimeoutMS, &n.State, &n.CompletionOutcome, &n.CompletionSummary,
		&n.Output, &n.ArtifactRefs, &n.CompletionReportedAt, &n.StartedAt, &n.FinishedAt, &n.CreatedAt)
	return n, err
}

func validateSourcePresentation(in SourcePresentation) error {
	if strings.TrimSpace(in.Kind) == "" || strings.TrimSpace(in.Title) == "" {
		return fmt.Errorf("%w: sourcePresentation.kind and title are required", ErrInvalidInput)
	}
	if in.OriginalURL != "" && !strings.HasPrefix(in.OriginalURL, "https://") {
		return fmt.Errorf("%w: sourcePresentation.originalUrl must use https", ErrInvalidInput)
	}
	for i, field := range in.Evidence {
		if (field.Label.EN == "" && field.Label.PT == "") || strings.TrimSpace(field.Value) == "" {
			return fmt.Errorf("%w: sourcePresentation.evidence[%d] requires a label and value", ErrInvalidInput, i)
		}
	}
	return nil
}

func startPayloadHash(in StartInput) (string, error) {
	var source any = map[string]any{}
	if len(in.Source) > 0 {
		if err := json.Unmarshal(in.Source, &source); err != nil {
			return "", fmt.Errorf("%w: source must be valid JSON", ErrInvalidInput)
		}
	}
	type canonicalArtifact struct {
		ArtifactID string `json:"artifactId"`
		Role       string `json:"role,omitempty"`
		Metadata   any    `json:"metadata,omitempty"`
	}
	artifacts := make([]canonicalArtifact, 0, len(in.ArtifactRefs))
	for _, ref := range in.ArtifactRefs {
		var metadata any
		if len(ref.Metadata) > 0 {
			if err := json.Unmarshal(ref.Metadata, &metadata); err != nil {
				return "", fmt.Errorf("%w: artifact metadata must be valid JSON", ErrInvalidInput)
			}
		}
		artifacts = append(artifacts, canonicalArtifact{ArtifactID: ref.ArtifactID, Role: ref.Role, Metadata: metadata})
	}
	canonical := struct {
		WorkflowKey        string              `json:"workflowKey"`
		Version            string              `json:"version"`
		Title              string              `json:"title"`
		Source             any                 `json:"source"`
		SourcePresentation SourcePresentation  `json:"sourcePresentation"`
		Artifacts          []canonicalArtifact `json:"artifacts,omitempty"`
	}{in.WorkflowKey, in.WorkflowVersion, in.Title, source, in.SourcePresentation, artifacts}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("canonical start payload: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func resolvePoolTx(ctx context.Context, tx pgx.Tx, definitionKey string) (string, error) {
	var poolID string
	if err := tx.QueryRow(ctx, `
		SELECT pool_id::text FROM toolgateway.workflow_pool_bindings
		WHERE workflow_definition_key=$1 AND deleted_at IS NULL`, definitionKey).Scan(&poolID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("%w: workflow has no active pool binding", ErrInvalidInput)
		}
		return "", fmt.Errorf("resolve workflow pool: %w", err)
	}
	return poolID, nil
}

type modelSnapshot struct {
	Key             string          `json:"key"`
	ID              string          `json:"id"`
	API             string          `json:"api"`
	ContextWindow   int             `json:"contextWindow"`
	MaxOutputTokens int             `json:"maxOutputTokens,omitempty"`
	ThinkingLevel   string          `json:"thinkingLevel,omitempty"`
	DefaultParams   json.RawMessage `json:"defaultParams,omitempty"`
	ReasoningConfig json.RawMessage `json:"reasoningConfig,omitempty"`
}

func resolveModelsTx(ctx context.Context, tx pgx.Tx, poolKey string, spec workflow.CanonicalSpec) (map[string]modelSnapshot, error) {
	namespace := strings.SplitN(poolKey, "/", 2)[0]
	refs := map[string]struct{}{}
	for _, n := range spec.Graph.Nodes {
		if n.Kind != workflow.NodeAgentTask {
			continue
		}
		ref := n.ModelRef
		if ref == "" {
			ref = spec.DefaultModelRef
		}
		refs[ref] = struct{}{}
	}
	byRef := map[string]modelSnapshot{}
	for ref := range refs {
		key := namespace + "/" + ref
		var m modelSnapshot
		var defaultParams, reasoning []byte
		err := tx.QueryRow(ctx, `
			SELECT model_key, model_id, context_length, default_params, reasoning_config
			FROM catalog.effective_catalog WHERE model_key=$1 AND available`, key).
			Scan(&m.Key, &m.ID, &m.ContextWindow, &defaultParams, &reasoning)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: model %q is unavailable", ErrInvalidInput, key)
			}
			return nil, fmt.Errorf("resolve model %q: %w", key, err)
		}
		m.API = "openai-completions"
		m.DefaultParams, m.ReasoningConfig = defaultParams, reasoning
		var defaults struct {
			MaxTokens *int `json:"max_tokens"`
		}
		_ = json.Unmarshal(defaultParams, &defaults)
		if defaults.MaxTokens != nil {
			m.MaxOutputTokens = *defaults.MaxTokens
		}
		var rc struct {
			EnableThinking *bool `json:"enable_thinking"`
		}
		_ = json.Unmarshal(reasoning, &rc)
		if rc.EnableThinking != nil {
			if *rc.EnableThinking {
				m.ThinkingLevel = "medium"
			} else {
				m.ThinkingLevel = "off"
			}
		}
		byRef[ref] = m
	}
	return byRef, nil
}

func resolveValueModelTx(ctx context.Context, tx pgx.Tx, ref string) (string, []byte, error) {
	if ref == "" {
		return "", nil, nil
	}
	var id, version, formula, currency string
	var baseline int64
	var hourly string
	var assumptions, explanation []byte
	err := tx.QueryRow(ctx, `
		SELECT id::text, version, formula, currency, baseline_seconds,
		       loaded_hourly_cost::text, assumptions, explanation
		FROM work.value_models WHERE ref=$1 ORDER BY created_at DESC, id DESC LIMIT 1`, ref).
		Scan(&id, &version, &formula, &currency, &baseline, &hourly, &assumptions, &explanation)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, nil
	} // explicitly unconfigured
	if err != nil {
		return "", nil, fmt.Errorf("resolve value model: %w", err)
	}
	snapshot, _ := json.Marshal(map[string]any{
		"ref": ref, "version": version, "formula": formula, "currency": currency,
		"baselineSeconds": baseline, "loadedHourlyCost": hourly,
		"assumptions": json.RawMessage(assumptions), "explanation": json.RawMessage(explanation),
	})
	return id, snapshot, nil
}

func snapshotToolsTx(ctx context.Context, tx pgx.Tx, attemptID, poolID string, capabilities []workflow.CanonicalCapability) error {
	if len(capabilities) == 0 {
		return nil
	}
	unique := map[string]string{}
	for _, capability := range capabilities {
		unique[capability.Tool] = capability.MaxEffectClass
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var digest string
		err := tx.QueryRow(ctx, `
			SELECT tv.digest
			FROM toolgateway.available_tool_versions tv
			JOIN toolgateway.pool_grants pg
			  ON pg.tool_name=tv.name AND pg.pool_id=$1 AND pg.deleted_at IS NULL
			WHERE tv.name=$2
			  AND CASE tv.effect_class WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END
			      <= CASE pg.max_effect_class WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END
			  AND CASE tv.effect_class WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END
			      <= CASE $3 WHEN 'read_only' THEN 1 WHEN 'idempotent_write' THEN 2 ELSE 3 END
			ORDER BY tv.created_at DESC, tv.id DESC LIMIT 1`, poolID, name, unique[name]).Scan(&digest)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: requested tool %q has no eligible healthy version", ErrInvalidInput, name)
		}
		if err != nil {
			return fmt.Errorf("resolve tool %q: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO toolgateway.attempt_tool_pins (attempt_id, tool_name, tool_version_digest)
			VALUES ($1,$2,$3)`, attemptID, name, digest); err != nil {
			return fmt.Errorf("pin tool %q: %w", name, err)
		}
	}
	return nil
}

func findNode(graph workflow.CanonicalGraph, key string) (workflow.CanonicalNode, bool) {
	for _, n := range graph.Nodes {
		if n.Key == key {
			return n, true
		}
	}
	return workflow.CanonicalNode{}, false
}

func insertNodeExecutionTx(ctx context.Context, tx pgx.Tx, attemptID string, node workflow.CanonicalNode,
	visit, seq int, contextValue map[string]any, models map[string]modelSnapshot, spec workflow.CanonicalSpec) (string, error) {
	contextJSON, _ := json.Marshal(contextValue)
	modelRef := node.ModelRef
	if modelRef == "" {
		modelRef = spec.DefaultModelRef
	}
	var modelJSON any
	if node.Kind == workflow.NodeAgentTask {
		b, _ := json.Marshal(models[modelRef])
		modelJSON = b
	}
	skills := make([]workflow.CanonicalSkill, 0, len(node.Skills))
	for _, name := range node.Skills {
		for _, skill := range spec.Skills {
			if skill.Name == name {
				skills = append(skills, skill)
				break
			}
		}
	}
	caps := make([]workflow.CanonicalCapability, 0, len(node.Capabilities))
	for _, name := range node.Capabilities {
		for _, cap := range spec.RequestedCapabilities {
			if cap.Tool == name {
				caps = append(caps, cap)
				break
			}
		}
	}
	skillsJSON, _ := json.Marshal(skills)
	capsJSON, _ := json.Marshal(caps)
	var timeoutMS any
	if node.Timeout != "" {
		d, err := time.ParseDuration(node.Timeout)
		if err != nil || d <= 0 {
			return "", fmt.Errorf("%w: invalid node timeout %q", ErrInvalidInput, node.Timeout)
		}
		timeoutMS = d.Milliseconds()
	}
	id := uuid.NewString()
	labelJSON, _ := json.Marshal(node.Label)
	if _, err := tx.Exec(ctx, `
		INSERT INTO runtime.node_executions
			(id, attempt_id, node_key, business_label, visit, execution_seq, kind, prompt, context,
			 model_snapshot, skills_snapshot, capabilities_snapshot, workspace_tools, timeout_ms)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		id, attemptID, node.Key, labelJSON, visit, seq, node.Kind, nullable(node.Prompt), contextJSON,
		modelJSON, skillsJSON, capsJSON, node.WorkspaceTools, timeoutMS); err != nil {
		return "", fmt.Errorf("insert node execution: %w", err)
	}
	return id, nil
}

func appendTimelineTx(ctx context.Context, tx pgx.Tx, itemID, attemptID, nodeID, code string,
	params any, refs []ArtifactRef, actorID string) error {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal timeline params: %w", err)
	}
	refsJSON, _ := json.Marshal(append([]ArtifactRef(nil), refs...))
	if refsJSON == nil || string(refsJSON) == "null" {
		refsJSON = []byte(`[]`)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO work.timeline_events
			(work_item_id, attempt_id, node_execution_id, code, params, artifact_refs, actor_identity_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, itemID, nullable(attemptID), nullable(nodeID), code,
		paramsJSON, refsJSON, nullable(actorID)); err != nil {
		return fmt.Errorf("append business timeline: %w", err)
	}
	return nil
}

func jsonOrObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}
func jsonValue(raw json.RawMessage) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return map[string]any{}
	}
	return v
}
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func nullableJSON(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return v
}
