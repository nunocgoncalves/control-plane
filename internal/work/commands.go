package work

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	runtimestore "github.com/nunocgoncalves/control-plane/internal/runtime"
	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

// RespondBlocker validates and persists one customer action. Human-gate
// responses complete the node and follow its declared outcome; consequence
// confirmation resumes the already-created target node.
//
//nolint:gocyclo // One transaction validates and applies both human and consequence blocker variants.
func (s *Store) RespondBlocker(ctx context.Context, in BlockerResponseInput) (Blocker, error) {
	if in.BlockerID == "" || in.ActorIdentityID == "" || in.Outcome == "" {
		return Blocker{}, fmt.Errorf("%w: blocker, actor, and outcome are required", ErrInvalidInput)
	}
	if len(in.Response) == 0 {
		in.Response = json.RawMessage(`{}`)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Blocker{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	b, err := scanBlocker(tx.QueryRow(ctx, `
		SELECT id,work_item_id,attempt_id,node_execution_id,kind,title,description,response_schema,
		       allowed_outcomes,required_consequences,state,response_outcome,response,created_at,resolved_at
		FROM work.blockers WHERE id=$1 FOR UPDATE`, in.BlockerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Blocker{}, ErrNotFound
	}
	if err != nil {
		return Blocker{}, err
	}
	if b.State != "open" {
		return Blocker{}, fmt.Errorf("%w: blocker is %s", ErrInvalidTransition, b.State)
	}
	var allowed []string
	if err := json.Unmarshal(b.AllowedOutcomes, &allowed); err != nil {
		return Blocker{}, err
	}
	if !contains(allowed, in.Outcome) {
		return Blocker{}, fmt.Errorf("%w: outcome %q is not allowed", ErrInvalidInput, in.Outcome)
	}
	if err := validateInstance(in.Response, b.ResponseSchema); err != nil {
		return Blocker{}, fmt.Errorf("%w: response: %v", ErrInvalidInput, err)
	}
	if b.Kind == "consequence_confirmation" {
		if b.NodeExecutionID == nil {
			return Blocker{}, fmt.Errorf("%w: blocker has no node execution", ErrInvalidTransition)
		}
		var nodeKey string
		if err := tx.QueryRow(ctx, `SELECT node_key FROM runtime.node_executions WHERE id=$1`, *b.NodeExecutionID).Scan(&nodeKey); err != nil {
			return Blocker{}, err
		}
		current, err := consequencesForNodeTx(ctx, tx, b.AttemptID, nodeKey, *b.NodeExecutionID)
		if err != nil {
			return Blocker{}, err
		}
		var disclosed []Consequence
		if err := json.Unmarshal(b.RequiredConsequences, &disclosed); err != nil {
			return Blocker{}, err
		}
		if !equalStrings(sortedIDs(disclosed), sortedIDs(current)) {
			updated, _ := json.Marshal(current)
			if _, err := tx.Exec(ctx, `UPDATE work.blockers SET required_consequences=$2 WHERE id=$1 AND state='open'`, b.ID, updated); err != nil {
				return Blocker{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return Blocker{}, err
			}
			return Blocker{}, fmt.Errorf("%w: consequential action set changed; review the refreshed blocker", ErrConfirmation)
		}
		required := sortedIDs(current)
		confirmed := append([]string(nil), in.ConfirmedInvocationIDs...)
		sort.Strings(confirmed)
		if !equalStrings(required, confirmed) {
			return Blocker{}, fmt.Errorf("%w: exact invocation set required", ErrConfirmation)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work.blockers SET state='resolved',response_outcome=$2,response=$3,responded_by=$4,resolved_at=now()
		WHERE id=$1`, b.ID, in.Outcome, in.Response, in.ActorIdentityID); err != nil {
		return Blocker{}, err
	}
	if b.NodeExecutionID == nil {
		return Blocker{}, fmt.Errorf("%w: blocker has no node execution", ErrInvalidTransition)
	}
	if b.Kind == "consequence_confirmation" {
		if _, err := tx.Exec(ctx, `UPDATE runtime.node_executions SET state='pending' WHERE id=$1 AND state='blocked'`, *b.NodeExecutionID); err != nil {
			return Blocker{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='running' WHERE id=$1 AND state='awaiting_approval'`, b.AttemptID); err != nil {
			return Blocker{}, err
		}
		if err := appendTimelineTx(ctx, tx, b.WorkItemID, b.AttemptID, *b.NodeExecutionID, "consequence_repetition_confirmed", map[string]any{"invocationIds": in.ConfirmedInvocationIDs}, nil, in.ActorIdentityID); err != nil {
			return Blocker{}, err
		}
	} else {
		if err := s.completeHumanNodeTx(ctx, tx, b, *b.NodeExecutionID, in.Outcome, in.Response, in.ActorIdentityID); err != nil {
			return Blocker{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Blocker{}, err
	}
	b.State = "resolved"
	b.ResponseOutcome = &in.Outcome
	b.Response = in.Response
	return b, nil
}

//nolint:gocyclo // Human completion and graph advancement must remain one atomic state transition.
func (s *Store) completeHumanNodeTx(ctx context.Context, tx pgx.Tx, b Blocker, nodeID, outcome string, response json.RawMessage, actorID string) error {
	node, err := scanNodeExecution(tx.QueryRow(ctx, nodeExecutionSelect+` WHERE id=$1 FOR UPDATE`, nodeID))
	if err != nil {
		return err
	}
	if node.State != NodeBlocked {
		return fmt.Errorf("%w: human node state is %s", ErrInvalidTransition, node.State)
	}
	var graphJSON, modelsJSON, specJSON []byte
	if err := tx.QueryRow(ctx, `
		SELECT a.graph_snapshot,a.models_snapshot,d.spec_json
		FROM work.attempts a JOIN workflow.definitions d ON d.id=a.definition_id WHERE a.id=$1`, b.AttemptID).Scan(&graphJSON, &modelsJSON, &specJSON); err != nil {
		return err
	}
	var graph workflow.CanonicalGraph
	var spec workflow.CanonicalSpec
	var models map[string]modelSnapshot
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return err
	}
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return err
	}
	if err := json.Unmarshal(modelsJSON, &models); err != nil {
		return err
	}
	def, ok := findNode(graph, node.NodeKey)
	if !ok || def.Kind != workflow.NodeHumanGate {
		return fmt.Errorf("human node definition missing")
	}
	if !contains(def.Outcomes, outcome) {
		return fmt.Errorf("%w: outcome %q is not declared", ErrInvalidInput, outcome)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE runtime.node_executions SET state='succeeded',completion_outcome=$2,completion_summary='Human response received',
		output=$3,completion_reported_at=now(),finished_at=now() WHERE id=$1`, nodeID, outcome, response); err != nil {
		return err
	}
	if err := appendTimelineTx(ctx, tx, b.WorkItemID, b.AttemptID, nodeID, "blocker_resolved", map[string]any{"nodeKey": node.NodeKey, "outcome": outcome}, nil, actorID); err != nil {
		return err
	}
	target, terminal, ok := route(graph, node.NodeKey, outcome)
	if !ok {
		return fmt.Errorf("graph route missing")
	}
	if terminal {
		if _, err := tx.Exec(ctx, `INSERT INTO runtime.graph_transitions(attempt_id,from_execution_id,outcome,terminal)VALUES($1,$2,$3,true)`, b.AttemptID, nodeID, outcome); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='succeeded',finished_at=now() WHERE id=$1`, b.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE work.attempts SET finished_at=now() WHERE id=$1`, b.AttemptID); err != nil {
			return err
		}
		if err := creditValueTx(ctx, tx, b.WorkItemID, b.AttemptID); err != nil {
			return err
		}
		return appendTimelineTx(ctx, tx, b.WorkItemID, b.AttemptID, nodeID, "work_completed", map[string]any{}, nil, actorID)
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM runtime.graph_transitions WHERE attempt_id=$1 AND NOT terminal`, b.AttemptID).Scan(&count); err != nil {
		return err
	}
	if count >= int(graph.MaxTransitions) {
		if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='failed',finished_at=now() WHERE id=$1`, b.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE work.attempts SET finished_at=now(),customer_failure_summary='{"code":"transition_limit"}' WHERE id=$1`, b.AttemptID); err != nil {
			return err
		}
		return appendTimelineTx(ctx, tx, b.WorkItemID, b.AttemptID, nodeID, "work_failed", map[string]any{"reason": "transition_limit"}, nil, "")
	}
	targetDef, ok := findNode(graph, target)
	if !ok {
		return fmt.Errorf("target node missing")
	}
	var visit, seq int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(visit),0)+1 FROM runtime.node_executions WHERE attempt_id=$1 AND node_key=$2`, b.AttemptID, target).Scan(&visit); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(execution_seq),0)+1 FROM runtime.node_executions WHERE attempt_id=$1`, b.AttemptID).Scan(&seq); err != nil {
		return err
	}
	node.CompletionOutcome = &outcome
	node.CompletionSummary = strPtr("Human response received")
	node.Output = response
	contextValue, err := buildContextTx(ctx, tx, b.AttemptID, node)
	if err != nil {
		return err
	}
	nextID, err := insertNodeExecutionTx(ctx, tx, b.AttemptID, targetDef, visit, seq, contextValue, models, spec)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO runtime.graph_transitions(attempt_id,from_execution_id,outcome,to_execution_id,terminal)VALUES($1,$2,$3,$4,false)`, b.AttemptID, nodeID, outcome, nextID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='running' WHERE id=$1 AND state='awaiting_approval'`, b.AttemptID)
	return err
}

// SaveFeedback records feedback and its conservative value deduction without
// creating an attempt or starting any work.
func (s *Store) SaveFeedback(ctx context.Context, in FeedbackInput) (Feedback, error) {
	if in.WorkItemID == "" || in.AttemptID == "" || in.ActorIdentityID == "" || in.Category == "" {
		return Feedback{}, fmt.Errorf("%w: item, attempt, actor, and category are required", ErrInvalidInput)
	}
	if len(in.CorrectedResult) > 0 && !json.Valid(in.CorrectedResult) {
		return Feedback{}, fmt.Errorf("%w: correctedResult must be valid JSON", ErrInvalidInput)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Feedback{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var runState string
	if err := tx.QueryRow(ctx, `SELECT wr.state FROM work.attempts a JOIN runtime.workflow_runs wr ON wr.id=a.id WHERE a.id=$1 AND a.work_item_id=$2`, in.AttemptID, in.WorkItemID).Scan(&runState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Feedback{}, ErrNotFound
		}
		return Feedback{}, err
	}
	if runState != runtimestore.RunSucceeded {
		return Feedback{}, fmt.Errorf("%w: feedback requires a completed attempt", ErrInvalidTransition)
	}
	var f Feedback
	err = tx.QueryRow(ctx, `
		INSERT INTO work.feedback(work_item_id,attempt_id,category,explanation,corrected_result,created_by)
		VALUES($1,$2,$3,$4,$5,$6)
		RETURNING id,work_item_id,attempt_id,category,explanation,corrected_result,created_by,created_at`,
		in.WorkItemID, in.AttemptID, in.Category, nullable(in.Explanation), nullableJSON(in.CorrectedResult), in.ActorIdentityID).
		Scan(&f.ID, &f.WorkItemID, &f.AttemptID, &f.Category, &f.Explanation, &f.CorrectedResult, &f.CreatedBy, &f.CreatedAt)
	if err != nil {
		return Feedback{}, err
	}
	if err := deductValueTx(ctx, tx, f); err != nil {
		return Feedback{}, err
	}
	if err := appendTimelineTx(ctx, tx, in.WorkItemID, in.AttemptID, "", "feedback_recorded", map[string]any{"category": in.Category}, nil, in.ActorIdentityID); err != nil {
		return Feedback{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Feedback{}, err
	}
	return f, nil
}

func deductValueTx(ctx context.Context, tx pgx.Tx, f Feedback) error {
	var amount, currency, modelID string
	var formula []byte
	err := tx.QueryRow(ctx, `
		SELECT amount::text,currency,value_model_id::text,formula_snapshot FROM work.value_ledger
		WHERE work_item_id=$1 AND kind='completion_credit'`, f.WorkItemID).Scan(&amount, &currency, &modelID, &formula)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	cmd, err := tx.Exec(ctx, `
		INSERT INTO work.value_ledger(work_item_id,attempt_id,feedback_id,value_model_id,kind,amount,currency,formula_snapshot)
		VALUES($1,$2,$3,$4,'feedback_deduction',-($5::numeric),$6,$7) ON CONFLICT DO NOTHING`, f.WorkItemID, f.AttemptID, f.ID, modelID, amount, currency, formula)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return nil
	}
	return appendTimelineTx(ctx, tx, f.WorkItemID, f.AttemptID, "", "value_deducted", map[string]any{"estimated": true, "currency": currency}, nil, f.CreatedBy)
}

// CreateRevision starts an explicit new attempt after exact consequence
// confirmation. It reuses the original immutable workflow version but resolves
// current healthy model/tool versions.
//
//nolint:gocyclo // Revision validation, stale-confirmation checks, and creation are one fail-closed command.
func (s *Store) CreateRevision(ctx context.Context, in RevisionInput) (WorkItem, error) {
	if in.WorkItemID == "" || in.ActorIdentityID == "" || in.FeedbackID == "" || in.ActionableGuidance == "" {
		return WorkItem{}, fmt.Errorf("%w: item, actor, feedback, and actionable guidance are required", ErrInvalidInput)
	}
	var key, version, currentAttempt string
	if err := s.pool.QueryRow(ctx, `
		SELECT a.definition_key,a.definition_version,wi.current_attempt_id::text
		FROM work.feedback f JOIN work.attempts a ON a.id=f.attempt_id
		JOIN work.work_items wi ON wi.id=f.work_item_id
		WHERE f.id=$1 AND f.work_item_id=$2 AND f.attempt_id=wi.current_attempt_id`, in.FeedbackID, in.WorkItemID).Scan(&key, &version, &currentAttempt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkItem{}, ErrNotFound
		}
		return WorkItem{}, err
	}
	resolved, err := s.workflows.ResolveForAttempt(ctx, key, version)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return WorkItem{}, fmt.Errorf("%w: workflow is unavailable", ErrInvalidInput)
		}
		return WorkItem{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return WorkItem{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var source []byte
	var lockedCurrent string
	if err := tx.QueryRow(ctx, `SELECT source,current_attempt_id::text FROM work.work_items WHERE id=$1 FOR UPDATE`, in.WorkItemID).Scan(&source, &lockedCurrent); err != nil {
		return WorkItem{}, err
	}
	if lockedCurrent != currentAttempt {
		return WorkItem{}, fmt.Errorf("%w: current attempt changed", ErrConflict)
	}
	var runState string
	if err := tx.QueryRow(ctx, `SELECT state FROM runtime.workflow_runs WHERE id=$1`, currentAttempt).Scan(&runState); err != nil {
		return WorkItem{}, err
	}
	if runState != runtimestore.RunSucceeded {
		return WorkItem{}, fmt.Errorf("%w: revision requires current item Done", ErrInvalidTransition)
	}
	// Re-read the exact consequential-action set after locking the item. A
	// caller that confirmed an older set receives a conflict rather than
	// silently carrying a newly settled/unknown action into the revision.
	consequences, err := consequencesForItemQuery(ctx, tx, in.WorkItemID)
	if err != nil {
		return WorkItem{}, err
	}
	required := sortedIDs(consequences)
	confirmed := append([]string(nil), in.ConfirmedInvocationIDs...)
	sort.Strings(confirmed)
	if !equalStrings(required, confirmed) {
		return WorkItem{}, fmt.Errorf("%w: exact invocation set required", ErrConfirmation)
	}
	var number int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(number),0)+1 FROM work.attempts WHERE work_item_id=$1`, in.WorkItemID).Scan(&number); err != nil {
		return WorkItem{}, err
	}
	refs, err := sourceArtifactsTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return WorkItem{}, err
	}
	attemptID := uuid.NewString()
	if err := s.createAttemptTx(ctx, tx, createAttemptInput{ID: attemptID, WorkItemID: in.WorkItemID, Number: number, ActorIdentityID: in.ActorIdentityID, Resolved: resolved, Source: source, SourceArtifacts: refs, RevisedFromAttemptID: currentAttempt, ActionableGuidance: in.ActionableGuidance, ConsequenceConfirmation: confirmed}); err != nil {
		return WorkItem{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE work.work_items SET current_attempt_id=$2 WHERE id=$1`, in.WorkItemID, attemptID); err != nil {
		return WorkItem{}, err
	}
	if err := appendTimelineTx(ctx, tx, in.WorkItemID, attemptID, "", "revision_requested", map[string]any{"feedbackId": in.FeedbackID, "attemptNumber": number}, nil, in.ActorIdentityID); err != nil {
		return WorkItem{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkItem{}, err
	}
	return s.GetWorkItem(ctx, in.WorkItemID)
}

type consequenceQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// ConsequencesForItem returns the exact customer-safe external-action tokens
// that must be confirmed before a revised attempt can start.
func (s *Store) ConsequencesForItem(ctx context.Context, itemID string) ([]Consequence, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM work.work_items WHERE id=$1)`, itemID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	return consequencesForItemQuery(ctx, s.pool, itemID)
}

func consequencesForItemQuery(ctx context.Context, q consequenceQuerier, itemID string) ([]Consequence, error) {
	rows, err := q.Query(ctx, `
		SELECT i.id::text,COALESCE(NULLIF(tv.description,''),'External system update'),i.state
		FROM toolgateway.invocations i JOIN runtime.turns t ON t.id::text=i.caller_scope_id AND i.caller_scope='turn'
		JOIN runtime.node_executions ne ON ne.id=t.node_execution_id JOIN work.attempts a ON a.id=ne.attempt_id
		LEFT JOIN toolgateway.tool_versions tv ON tv.digest=i.tool_version_digest AND tv.name=i.tool_name
		WHERE a.work_item_id=$1 AND i.effect_class IN('idempotent_write','non_idempotent_write') AND i.state IN('succeeded','outcome_unknown')
		ORDER BY i.created_at,i.id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Consequence, 0)
	for rows.Next() {
		var c Consequence
		if err := rows.Scan(&c.InvocationID, &c.Action, &c.State); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func sourceArtifactsTx(ctx context.Context, tx pgx.Tx, itemID string) ([]ArtifactRef, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT artifact_id,metadata FROM work.artifact_links WHERE work_item_id=$1 AND role='source' ORDER BY artifact_id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ArtifactRef, 0)
	for rows.Next() {
		var r ArtifactRef
		r.Role = "source"
		if err := rows.Scan(&r.ArtifactID, &r.Metadata); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanBlocker(row pgx.Row) (Blocker, error) {
	var b Blocker
	err := row.Scan(&b.ID, &b.WorkItemID, &b.AttemptID, &b.NodeExecutionID, &b.Kind, &b.Title, &b.Description, &b.ResponseSchema, &b.AllowedOutcomes, &b.RequiredConsequences, &b.State, &b.ResponseOutcome, &b.Response, &b.CreatedAt, &b.ResolvedAt)
	return b, err
}
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func strPtr(s string) *string { return &s }
