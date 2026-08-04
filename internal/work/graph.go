package work

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/santhosh-tekuri/jsonschema/v5"

	runtimestore "github.com/nunocgoncalves/control-plane/internal/runtime"
	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

// IsGraphAttempt reports whether runID is a HOR-254 work attempt.
func (s *Store) IsGraphAttempt(ctx context.Context, runID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM work.attempts WHERE id=$1)`, runID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// ActiveNode returns the attempt's one pending/running/blocked execution.
func (s *Store) ActiveNode(ctx context.Context, attemptID string) (NodeExecution, error) {
	n, err := scanNodeExecution(s.pool.QueryRow(ctx, nodeExecutionSelect+`
		WHERE attempt_id=$1 AND state IN ('pending','running','blocked')`, attemptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return NodeExecution{}, ErrNotFound
	}
	return n, err
}

// PrepareNode makes the active graph node executable. Agent nodes become
// running and get exactly one turn; human/reentry nodes become durable blockers.
// dispatch=false means no worker assignment is required.
//
//nolint:gocyclo // Preparation atomically covers re-entry safety, human gates, and agent turn creation.
func (s *Store) PrepareNode(ctx context.Context, attemptID string) (NodeExecution, runtimestore.Turn, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	node, err := scanNodeExecution(tx.QueryRow(ctx, nodeExecutionSelect+`
		WHERE attempt_id=$1 AND state IN ('pending','running','blocked') FOR UPDATE`, attemptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return NodeExecution{}, runtimestore.Turn{}, false, ErrNotFound
	}
	if err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	if node.State == NodeBlocked {
		return node, runtimestore.Turn{}, false, tx.Commit(ctx)
	}

	var itemID, runState string
	var graphJSON []byte
	if err := tx.QueryRow(ctx, `
		SELECT work_item_id::text, graph_snapshot, wr.state
		FROM work.attempts a JOIN runtime.workflow_runs wr ON wr.id=a.id
		WHERE a.id=$1 FOR UPDATE OF a,wr`, attemptID).Scan(&itemID, &graphJSON, &runState); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	if runState != runtimestore.RunPending && runState != runtimestore.RunRunning {
		return NodeExecution{}, runtimestore.Turn{}, false, fmt.Errorf("%w: attempt state is %s", ErrInvalidTransition, runState)
	}
	var graph workflow.CanonicalGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	def, ok := findNode(graph, node.NodeKey)
	if !ok {
		return NodeExecution{}, runtimestore.Turn{}, false, fmt.Errorf("graph node %q missing from snapshot", node.NodeKey)
	}

	if node.State == NodePending && node.Visit > 1 && def.Kind == workflow.NodeAgentTask {
		consequences, err := consequencesForNodeTx(ctx, tx, attemptID, node.NodeKey, node.ID)
		if err != nil {
			return NodeExecution{}, runtimestore.Turn{}, false, err
		}
		if len(consequences) > 0 {
			confirmed, err := consequencesConfirmedTx(ctx, tx, node.ID, consequences)
			if err != nil {
				return NodeExecution{}, runtimestore.Turn{}, false, err
			}
			if !confirmed {
				if err := createConsequenceBlockerTx(ctx, tx, itemID, attemptID, node.ID, consequences); err != nil {
					return NodeExecution{}, runtimestore.Turn{}, false, err
				}
				if _, err := tx.Exec(ctx, `UPDATE runtime.node_executions SET state='blocked' WHERE id=$1`, node.ID); err != nil {
					return NodeExecution{}, runtimestore.Turn{}, false, err
				}
				if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='awaiting_approval' WHERE id=$1 AND state IN ('pending','running')`, attemptID); err != nil {
					return NodeExecution{}, runtimestore.Turn{}, false, err
				}
				node.State = NodeBlocked
				if err := appendTimelineTx(ctx, tx, itemID, attemptID, node.ID, "consequence_confirmation_required",
					map[string]any{"nodeKey": node.NodeKey, "consequences": consequences}, nil, ""); err != nil {
					return NodeExecution{}, runtimestore.Turn{}, false, err
				}
				return node, runtimestore.Turn{}, false, tx.Commit(ctx)
			}
		}
	}

	if def.Kind == workflow.NodeHumanGate {
		if node.State == NodePending {
			if err := createHumanBlockerTx(ctx, tx, itemID, attemptID, node.ID, def); err != nil {
				return NodeExecution{}, runtimestore.Turn{}, false, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runtime.node_executions SET state='blocked', started_at=COALESCE(started_at,now()) WHERE id=$1`, node.ID); err != nil {
				return NodeExecution{}, runtimestore.Turn{}, false, err
			}
			if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='awaiting_approval', started_at=COALESCE(started_at,now()) WHERE id=$1 AND state IN ('pending','running')`, attemptID); err != nil {
				return NodeExecution{}, runtimestore.Turn{}, false, err
			}
			if err := appendTimelineTx(ctx, tx, itemID, attemptID, node.ID, "work_blocked", map[string]any{"nodeKey": node.NodeKey, "type": def.HumanGate.Type}, nil, ""); err != nil {
				return NodeExecution{}, runtimestore.Turn{}, false, err
			}
			node.State = NodeBlocked
		}
		return node, runtimestore.Turn{}, false, tx.Commit(ctx)
	}

	// Existing running node already owns one turn; return it for assignment.
	if node.State == NodeRunning {
		turn, err := scanGraphTurn(tx.QueryRow(ctx, `
			SELECT id, run_id, step_id, node_execution_id, session_id, model, state,
			       started_at, settled_at, created_at, updated_at
			FROM runtime.turns WHERE node_execution_id=$1`, node.ID))
		if err != nil {
			return NodeExecution{}, runtimestore.Turn{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return NodeExecution{}, runtimestore.Turn{}, false, err
		}
		return node, turn, true, nil
	}

	var model struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(node.ModelSnapshot, &model); err != nil || model.ID == "" {
		return NodeExecution{}, runtimestore.Turn{}, false, fmt.Errorf("node %s has no valid model snapshot", node.ID)
	}
	var sessionID string
	if err := tx.QueryRow(ctx, `SELECT session_id FROM runtime.workflow_runs WHERE id=$1`, attemptID).Scan(&sessionID); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	turn, err := scanGraphTurn(tx.QueryRow(ctx, `
		INSERT INTO runtime.turns (run_id, node_execution_id, session_id, model, state, started_at)
		VALUES ($1,$2,$3,$4,'running',now())
		RETURNING id, run_id, step_id, node_execution_id, session_id, model, state,
		          started_at, settled_at, created_at, updated_at`, attemptID, node.ID, sessionID, model.ID))
	if err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, fmt.Errorf("start graph turn: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime.node_executions SET state='running', started_at=COALESCE(started_at,now()) WHERE id=$1 AND state='pending'`, node.ID); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='running', started_at=COALESCE(started_at,now()) WHERE id=$1 AND state IN ('pending','awaiting_approval')`, attemptID); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	if err := appendRuntimeEventTx(ctx, tx, attemptID, turn.ID, node.ID, runtimestore.EvTurnStarted, map[string]any{"model": model.ID}); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	if err := appendTimelineTx(ctx, tx, itemID, attemptID, node.ID, "node_started", map[string]any{"nodeKey": node.NodeKey, "visit": node.Visit}, nil, ""); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return NodeExecution{}, runtimestore.Turn{}, false, err
	}
	node.State = NodeRunning
	now := time.Now()
	node.StartedAt = &now
	return node, turn, true, nil
}

// RecordCompletionReport validates and persists one complete_step candidate.
// It does not advance the graph; a clean WorkerOutcome must follow.
//
//nolint:gocyclo // The reserved control payload is fully validated before any durable projection commits.
func (s *Store) RecordCompletionReport(ctx context.Context, turnID string, report CompletionReport) error {
	if report.Outcome == "" || report.Summary == "" {
		return fmt.Errorf("%w: outcome and summary are required", ErrInvalidInput)
	}
	if len(report.Output) == 0 {
		report.Output = json.RawMessage(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var nodeID, attemptID, itemID, nodeKey, state string
	var graphJSON []byte
	if err := tx.QueryRow(ctx, `
		SELECT ne.id::text, ne.attempt_id::text, a.work_item_id::text, ne.node_key, ne.state, a.graph_snapshot
		FROM runtime.turns t
		JOIN runtime.node_executions ne ON ne.id=t.node_execution_id
		JOIN work.attempts a ON a.id=ne.attempt_id
		WHERE t.id=$1 FOR UPDATE OF ne`, turnID).Scan(&nodeID, &attemptID, &itemID, &nodeKey, &state, &graphJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if state != NodeRunning {
		return fmt.Errorf("%w: node state is %s", ErrInvalidTransition, state)
	}
	var graph workflow.CanonicalGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return err
	}
	def, ok := findNode(graph, nodeKey)
	if !ok || def.Kind != workflow.NodeAgentTask {
		return fmt.Errorf("%w: completion report is not for an agent node", ErrInvalidTransition)
	}
	if !contains(def.Outcomes, report.Outcome) {
		return fmt.Errorf("%w: outcome %q is not declared by node %q", ErrInvalidInput, report.Outcome, nodeKey)
	}
	if err := validateInstance(report.Output, def.OutputSchema); err != nil {
		return fmt.Errorf("%w: output: %v", ErrInvalidInput, err)
	}
	for i := range report.ArtifactRefs {
		ref := &report.ArtifactRefs[i]
		if ref.ArtifactID == "" {
			return fmt.Errorf("%w: artifactId is required", ErrInvalidInput)
		}
		if ref.Role == "" {
			ref.Role = "output"
		}
		if ref.Role != "output" && ref.Role != "evidence" {
			return fmt.Errorf("%w: artifact role %q is not allowed for complete_step", ErrInvalidInput, ref.Role)
		}
		if len(ref.Metadata) > 0 && !json.Valid(ref.Metadata) {
			return fmt.Errorf("%w: artifact metadata must be valid JSON", ErrInvalidInput)
		}
		var authorized bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM artifact.artifacts ar
				JOIN work.artifact_links l ON l.artifact_id=ar.id
				WHERE ar.id=$1 AND ar.state='available'
				  AND ar.source_type IN ('sandbox_publish','tool_output','workflow')
				  AND l.attempt_id=$2 AND l.node_execution_id=$3
			)`, ref.ArtifactID, attemptID, nodeID).Scan(&authorized); err != nil {
			return err
		}
		if !authorized {
			return fmt.Errorf("%w: artifact is not an output published by this node", ErrInvalidInput)
		}
	}
	refsJSON, _ := json.Marshal(append([]ArtifactRef{}, report.ArtifactRefs...))
	cmd, err := tx.Exec(ctx, `
		UPDATE runtime.node_executions
		SET completion_outcome=$2, completion_summary=$3, output=$4, artifact_refs=$5,
		    completion_reported_at=now()
		WHERE id=$1 AND state='running' AND completion_reported_at IS NULL`,
		nodeID, report.Outcome, report.Summary, report.Output, refsJSON)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() != 1 {
		return fmt.Errorf("%w: complete_step already reported", ErrConflict)
	}
	for _, ref := range report.ArtifactRefs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO work.artifact_links (artifact_id, work_item_id, attempt_id, node_execution_id, role, metadata)
			VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (artifact_id,work_item_id,attempt_id,node_execution_id,role) DO NOTHING`,
			ref.ArtifactID, itemID, attemptID, nodeID, ref.Role, jsonOrObject(ref.Metadata)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// CompleteTurn atomically terminalizes a graph turn/node and either follows one
// declared edge or completes/fails the attempt. It returns the resulting run
// state for dispatch's SessionEnd sequencing.
//
//nolint:gocyclo // Turn settlement and every graph/run terminal or advancement branch are deliberately atomic.
func (s *Store) CompleteTurn(ctx context.Context, turnID, reason string, customerFailure, operatorFailure json.RawMessage) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var turnState, attemptID, nodeID, itemID string
	if err := tx.QueryRow(ctx, `
		SELECT t.state, t.run_id::text, ne.id::text, a.work_item_id::text
		FROM runtime.turns t JOIN runtime.node_executions ne ON ne.id=t.node_execution_id
		JOIN work.attempts a ON a.id=ne.attempt_id
		WHERE t.id=$1 FOR UPDATE OF t,ne,a`, turnID).Scan(&turnState, &attemptID, &nodeID, &itemID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	if turnState != runtimestore.TurnRunning {
		return "", fmt.Errorf("%w: turn state is %s", ErrInvalidTransition, turnState)
	}
	node, err := scanNodeExecution(tx.QueryRow(ctx, nodeExecutionSelect+` WHERE id=$1 FOR UPDATE`, nodeID))
	if err != nil {
		return "", err
	}
	if reason != "completed" || node.CompletionOutcome == nil || node.CompletionReportedAt == nil {
		to := runtimestore.TurnFailed
		runTo := runtimestore.RunFailed
		if reason == "aborted" {
			to, runTo = runtimestore.TurnAborted, runtimestore.RunAborted
		}
		if _, err := tx.Exec(ctx, `UPDATE runtime.turns SET state=$2, settled_at=now() WHERE id=$1`, turnID, to); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `UPDATE runtime.node_executions SET state='failed', finished_at=now() WHERE id=$1`, nodeID); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state=$2, finished_at=now() WHERE id=$1`, attemptID, runTo); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `UPDATE work.attempts SET finished_at=now(), customer_failure_summary=$2, operator_failure_detail=$3 WHERE id=$1`, attemptID, nullableJSON(customerFailure), nullableJSON(operatorFailure)); err != nil {
			return "", err
		}
		if err := appendTimelineTx(ctx, tx, itemID, attemptID, nodeID, "work_failed", map[string]any{"nodeKey": node.NodeKey}, nil, ""); err != nil {
			return "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", err
		}
		return runTo, nil
	}

	if _, err := tx.Exec(ctx, `UPDATE runtime.turns SET state='succeeded', settled_at=now() WHERE id=$1`, turnID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime.node_executions SET state='succeeded', finished_at=now() WHERE id=$1`, nodeID); err != nil {
		return "", err
	}
	runState, err := s.advanceGraphTx(ctx, tx, graphAdvanceInput{ItemID: itemID, AttemptID: attemptID, Node: node})
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return runState, nil
}

type graphAdvanceInput struct {
	ItemID    string
	AttemptID string
	Node      NodeExecution
	ActorID   string
}

// advanceGraphTx is the single transaction-scoped graph router used after
// both agent-task completion and human-gate resolution. Callers terminalize
// their source node first, then this routine records identical customer events,
// transition-limit failures, routing, visit numbering, context, and terminal
// completion semantics regardless of node kind.
//
//nolint:gocyclo // Every graph route remains in one transaction-scoped semantic routine for agent and human nodes.
func (s *Store) advanceGraphTx(ctx context.Context, tx pgx.Tx, in graphAdvanceInput) (string, error) {
	if in.Node.CompletionOutcome == nil {
		return "", fmt.Errorf("%w: node completion outcome is missing", ErrInvalidTransition)
	}
	var graphJSON, modelsJSON, specJSON []byte
	var definitionKey, definitionVersion string
	if err := tx.QueryRow(ctx, `
		SELECT a.graph_snapshot,a.models_snapshot,a.definition_key,a.definition_version,d.spec_json
		FROM work.attempts a JOIN workflow.definitions d ON d.id=a.definition_id WHERE a.id=$1`, in.AttemptID).
		Scan(&graphJSON, &modelsJSON, &definitionKey, &definitionVersion, &specJSON); err != nil {
		return "", err
	}
	var graph workflow.CanonicalGraph
	var models map[string]modelSnapshot
	var spec workflow.CanonicalSpec
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return "", err
	}
	if err := json.Unmarshal(modelsJSON, &models); err != nil {
		return "", err
	}
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return "", err
	}
	def, ok := findNode(graph, in.Node.NodeKey)
	if !ok || def.Kind != in.Node.Kind {
		return "", fmt.Errorf("immutable graph node %q is missing or changed kind", in.Node.NodeKey)
	}
	if !contains(def.Outcomes, *in.Node.CompletionOutcome) {
		return "", fmt.Errorf("%w: outcome %q is not declared", ErrInvalidInput, *in.Node.CompletionOutcome)
	}
	var completedArtifacts []ArtifactRef
	if len(in.Node.ArtifactRefs) > 0 {
		if err := json.Unmarshal(in.Node.ArtifactRefs, &completedArtifacts); err != nil {
			return "", err
		}
	}
	if err := appendTimelineTx(ctx, tx, in.ItemID, in.AttemptID, in.Node.ID, "node_completed",
		map[string]any{"nodeKey": in.Node.NodeKey, "visit": in.Node.Visit, "outcome": *in.Node.CompletionOutcome, "summary": deref(in.Node.CompletionSummary)}, completedArtifacts, in.ActorID); err != nil {
		return "", err
	}

	target, terminal, ok := route(graph, in.Node.NodeKey, *in.Node.CompletionOutcome)
	if !ok {
		return "", fmt.Errorf("immutable graph has no route for %s/%s", in.Node.NodeKey, *in.Node.CompletionOutcome)
	}
	if terminal {
		if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='succeeded', finished_at=now() WHERE id=$1`, in.AttemptID); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `UPDATE work.attempts SET finished_at=now() WHERE id=$1`, in.AttemptID); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runtime.graph_transitions (attempt_id,from_execution_id,outcome,terminal) VALUES ($1,$2,$3,true)`, in.AttemptID, in.Node.ID, *in.Node.CompletionOutcome); err != nil {
			return "", err
		}
		if err := creditValueTx(ctx, tx, in.ItemID, in.AttemptID); err != nil {
			return "", err
		}
		if err := appendTimelineTx(ctx, tx, in.ItemID, in.AttemptID, in.Node.ID, "work_completed", map[string]any{"definitionKey": definitionKey, "definitionVersion": definitionVersion}, nil, in.ActorID); err != nil {
			return "", err
		}
		return runtimestore.RunSucceeded, nil
	}
	var transitionCount int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM runtime.graph_transitions WHERE attempt_id=$1 AND NOT terminal`, in.AttemptID).Scan(&transitionCount); err != nil {
		return "", err
	}
	if transitionCount >= int(graph.MaxTransitions) {
		if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='failed', finished_at=now() WHERE id=$1`, in.AttemptID); err != nil {
			return "", err
		}
		failure, _ := json.Marshal(map[string]any{"code": "transition_limit", "message": "The workflow could not complete within its configured transition limit."})
		if _, err := tx.Exec(ctx, `UPDATE work.attempts SET finished_at=now(), customer_failure_summary=$2 WHERE id=$1`, in.AttemptID, failure); err != nil {
			return "", err
		}
		if err := appendTimelineTx(ctx, tx, in.ItemID, in.AttemptID, in.Node.ID, "work_failed", map[string]any{"reason": "transition_limit"}, nil, in.ActorID); err != nil {
			return "", err
		}
		return runtimestore.RunFailed, nil
	}

	targetDef, ok := findNode(graph, target)
	if !ok {
		return "", fmt.Errorf("target node %q missing", target)
	}
	var visit, seq int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(visit),0)+1 FROM runtime.node_executions WHERE attempt_id=$1 AND node_key=$2`, in.AttemptID, target).Scan(&visit); err != nil {
		return "", err
	}
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(execution_seq),0)+1 FROM runtime.node_executions WHERE attempt_id=$1`, in.AttemptID).Scan(&seq); err != nil {
		return "", err
	}
	contextValue, err := buildContextTx(ctx, tx, in.AttemptID, in.Node)
	if err != nil {
		return "", err
	}
	nextID, err := insertNodeExecutionTx(ctx, tx, in.AttemptID, targetDef, visit, seq, contextValue, models, spec)
	if err != nil {
		return "", err
	}
	// The next node receives only the current node's explicit inputs plus
	// artifacts selected by complete_step. Publishing an artifact creates a
	// node-scoped output link, but does not implicitly propagate it: selection
	// in runtime.node_executions.artifact_refs is the durable handoff decision.
	if _, err := tx.Exec(ctx, `
		INSERT INTO work.artifact_links
			(artifact_id,work_item_id,attempt_id,node_execution_id,role,metadata)
		SELECT DISTINCT ON (l.artifact_id,l.work_item_id,l.attempt_id)
			l.artifact_id,l.work_item_id,l.attempt_id,$2,'input',l.metadata
		FROM work.artifact_links l
		JOIN runtime.node_executions ne ON ne.id=$3
		WHERE l.attempt_id=$1 AND l.node_execution_id=$3
		  AND (l.role IN ('source','input') OR EXISTS (
			SELECT 1 FROM jsonb_array_elements(ne.artifact_refs) ref
			WHERE ref->>'artifactId'=l.artifact_id::text
		  ))
		ORDER BY l.artifact_id,l.work_item_id,l.attempt_id,l.created_at
		ON CONFLICT (artifact_id,work_item_id,attempt_id,node_execution_id,role) DO NOTHING`, in.AttemptID, nextID, in.Node.ID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO runtime.graph_transitions (attempt_id,from_execution_id,outcome,to_execution_id,terminal) VALUES ($1,$2,$3,$4,false)`, in.AttemptID, in.Node.ID, *in.Node.CompletionOutcome, nextID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE runtime.workflow_runs SET state='running' WHERE id=$1 AND state IN ('running','awaiting_approval')`, in.AttemptID); err != nil {
		return "", err
	}
	return runtimestore.RunRunning, nil
}

func buildContextTx(ctx context.Context, tx pgx.Tx, attemptID string, previous NodeExecution) (map[string]any, error) {
	var source []byte
	var guidance *string
	var itemID string
	if err := tx.QueryRow(ctx, `
		SELECT wi.id::text, wi.source, a.actionable_guidance FROM work.attempts a JOIN work.work_items wi ON wi.id=a.work_item_id WHERE a.id=$1`, attemptID).
		Scan(&itemID, &source, &guidance); err != nil {
		return nil, err
	}
	sourceArtifacts, err := sourceArtifactsTx(ctx, tx, itemID)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT ON (node_key) id::text, node_key, completion_outcome, completion_summary, output, artifact_refs, visit
		FROM runtime.node_executions WHERE attempt_id=$1 AND state='succeeded'
		ORDER BY node_key, visit DESC`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	latest := map[string]any{}
	for rows.Next() {
		var id, key string
		var outcome, summary *string
		var output, refs []byte
		var visit int
		if err := rows.Scan(&id, &key, &outcome, &summary, &output, &refs, &visit); err != nil {
			return nil, err
		}
		latest[key] = map[string]any{"executionId": id, "visit": visit, "outcome": outcome, "summary": summary, "output": jsonValue(output), "artifactRefs": jsonValue(refs)}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	contextValue := map[string]any{
		"source":          jsonValue(source),
		"sourceArtifacts": sourceArtifacts,
		"executionHistoryRef": map[string]any{
			"attemptId": attemptID, "throughExecutionSeq": previous.ExecutionSeq,
		},
		"previous": map[string]any{"executionId": previous.ID, "nodeKey": previous.NodeKey, "visit": previous.Visit,
			"outcome": previous.CompletionOutcome, "summary": previous.CompletionSummary,
			"output": jsonValue(previous.Output), "artifactRefs": jsonValue(previous.ArtifactRefs)},
		"latestByNode": latest,
	}
	if guidance != nil {
		contextValue["revisionGuidance"] = *guidance
	}
	return contextValue, nil
}

func route(graph workflow.CanonicalGraph, node, outcome string) (target string, terminal, ok bool) {
	for _, t := range graph.TerminalOutcomes {
		if t.Node == node && t.Outcome == outcome {
			return "", true, true
		}
	}
	for _, e := range graph.Edges {
		if e.From == node && e.Outcome == outcome {
			return e.To, false, true
		}
	}
	return "", false, false
}

func createHumanBlockerTx(ctx context.Context, tx pgx.Tx, itemID, attemptID, nodeID string, node workflow.CanonicalNode) error {
	title, _ := json.Marshal(node.HumanGate.Title)
	description, _ := json.Marshal(node.HumanGate.Description)
	outcomes, _ := json.Marshal(node.Outcomes)
	_, err := tx.Exec(ctx, `
		INSERT INTO work.blockers
			(work_item_id,attempt_id,node_execution_id,kind,title,description,response_schema,allowed_outcomes)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, itemID, attemptID, nodeID, node.HumanGate.Type,
		title, description, jsonOrObject(node.HumanGate.ResponseSchema), outcomes)
	return err
}

func createConsequenceBlockerTx(ctx context.Context, tx pgx.Tx, itemID, attemptID, nodeID string, consequences []Consequence) error {
	title, _ := json.Marshal(map[string]string{"en": "Confirm repeated external actions", "pt": "Confirmar ações externas repetidas"})
	description, _ := json.Marshal(map[string]string{"en": "This step may repeat external actions from an earlier visit.", "pt": "Este passo pode repetir ações externas de uma visita anterior."})
	required, _ := json.Marshal(consequences)
	outcomes := []byte(`["confirmed"]`)
	_, err := tx.Exec(ctx, `
		INSERT INTO work.blockers
			(work_item_id,attempt_id,node_execution_id,kind,title,description,response_schema,allowed_outcomes,required_consequences)
		VALUES ($1,$2,$3,'consequence_confirmation',$4,$5,'{}',$6,$7)`, itemID, attemptID, nodeID, title, description, outcomes, required)
	return err
}

func consequencesForNodeTx(ctx context.Context, tx pgx.Tx, attemptID, nodeKey, excludeNodeID string) ([]Consequence, error) {
	rows, err := tx.Query(ctx, `
		SELECT i.id::text, i.consequence_summary, i.state
		FROM toolgateway.invocations i
		JOIN runtime.turns t ON t.id::text=i.caller_scope_id AND i.caller_scope='turn'
		JOIN runtime.node_executions ne ON ne.id=t.node_execution_id
		WHERE ne.attempt_id=$1 AND ne.node_key=$2 AND ne.id<>$3
		  AND i.effect_class IN ('idempotent_write','non_idempotent_write')
		  AND i.state IN ('succeeded','outcome_unknown')
		ORDER BY i.created_at, i.id`, attemptID, nodeKey, excludeNodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Consequence, 0)
	for rows.Next() {
		var c Consequence
		if err := rows.Scan(&c.InvocationID, &c.Summary, &c.State); err != nil {
			return nil, err
		}
		if err := validateConsequenceSummary(c.Summary); err != nil {
			return nil, fmt.Errorf("invocation %s: %w", c.InvocationID, err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func validateConsequenceSummary(raw json.RawMessage) error {
	var localized map[string]string
	if err := json.Unmarshal(raw, &localized); err != nil {
		return fmt.Errorf("invalid customer-safe consequence summary: %w", err)
	}
	if localized["en"] == "" || localized["pt"] == "" {
		return fmt.Errorf("customer-safe consequence summary requires en and pt text")
	}
	return nil
}

func consequencesConfirmedTx(ctx context.Context, tx pgx.Tx, nodeID string, current []Consequence) (bool, error) {
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT required_consequences FROM work.blockers
		WHERE node_execution_id=$1 AND kind='consequence_confirmation' AND state='resolved'
		ORDER BY resolved_at DESC LIMIT 1`, nodeID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var confirmed []Consequence
	if err := json.Unmarshal(raw, &confirmed); err != nil {
		return false, err
	}
	return equalStrings(sortedIDs(confirmed), sortedIDs(current)), nil
}

func validateInstance(raw, schema []byte) error {
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if len(bytes.TrimSpace(schema)) == 0 || bytes.Equal(bytes.TrimSpace(schema), []byte(`{}`)) {
		return nil
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", bytes.NewReader(schema)); err != nil {
		return err
	}
	compiled, err := compiler.Compile("schema.json")
	if err != nil {
		return err
	}
	return compiled.Validate(value)
}

func scanGraphTurn(row pgx.Row) (runtimestore.Turn, error) {
	var t runtimestore.Turn
	var stepID, nodeID, model *string
	err := row.Scan(&t.ID, &t.RunID, &stepID, &nodeID, &t.SessionID, &model, &t.State, &t.StartedAt, &t.SettledAt, &t.CreatedAt, &t.UpdatedAt)
	t.StepID = stepID
	t.NodeExecutionID = nodeID
	t.Model = model
	return t, err
}

func appendRuntimeEventTx(ctx context.Context, tx pgx.Tx, runID, turnID, nodeID, kind string, payload any) error {
	b, _ := json.Marshal(payload)
	_, err := tx.Exec(ctx, `
		INSERT INTO runtime.events (run_id,turn_id,node_execution_id,seq,kind,payload)
		VALUES ($1,$2,$3,(SELECT COALESCE(MAX(seq),0)+1 FROM runtime.events WHERE run_id=$1),$4,$5)`, runID, nullable(turnID), nullable(nodeID), kind, b)
	return err
}

func creditValueTx(ctx context.Context, tx pgx.Tx, itemID, attemptID string) error {
	var number int
	var modelID *string
	var snapshot []byte
	if err := tx.QueryRow(ctx, `SELECT number,value_model_id,value_model_snapshot FROM work.attempts WHERE id=$1`, attemptID).Scan(&number, &modelID, &snapshot); err != nil {
		return err
	}
	if number != 1 || modelID == nil || len(snapshot) == 0 {
		return nil
	}
	var v struct {
		Currency string `json:"currency"`
		Baseline int64  `json:"baselineSeconds"`
		Hourly   string `json:"loadedHourlyCost"`
	}
	if err := json.Unmarshal(snapshot, &v); err != nil {
		return err
	}
	cmd, err := tx.Exec(ctx, `
		INSERT INTO work.value_ledger (work_item_id,attempt_id,value_model_id,kind,amount,currency,formula_snapshot)
		VALUES ($1,$2,$3,'completion_credit',($4::numeric*$5::numeric/3600),$6,$7)
		ON CONFLICT DO NOTHING`, itemID, attemptID, *modelID, v.Baseline, v.Hourly, v.Currency, snapshot)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return nil
	}
	return appendTimelineTx(ctx, tx, itemID, attemptID, "", "value_credited", map[string]any{"estimated": true, "currency": v.Currency}, nil, "")
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
func deref(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
func sortedIDs(cs []Consequence) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.InvocationID)
	}
	sort.Strings(out)
	return out
}
