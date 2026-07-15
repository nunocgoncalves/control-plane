// Package runtime implements the control-plane durable turn runtime store
// (HOR-246): the Postgres-backed turn/workflow state machine + append-only
// event/audit log + orchestration state.
//
// One runtime (Platform Direction §4/§6): a workflow_run composes agent tasks
// (a deterministic plan -> steps = agent_task | tool_call | approval_gate);
// chat is a degenerate run (kind=chat, one freeform agent_task step). One run =
// one pi session (session_id/session_dir); all of a run's turns share it. The
// control-plane owns the run -> session.id mapping; HOR-249 generates the id
// and passes it here, HOR-351's harness resumes-or-creates that session.
//
// State columns are authoritative current state; runtime.events is the
// append-only history (audit/replay/usage). Every transition appends its audit
// event in the same tx (state + history move atomically); harness-streamed
// events (assistant_message/tool_result/error) go through AppendEvent. Replay =
// fold ListEvents in per-run `seq` order. The approval-gate step type is
// supported in schema now; its execution (HITL) is deferred.
//
// This package is storage + state-machine validation only. It does NOT drive
// the harness (HOR-249), provision sandboxes (HOR-245), or define workflows
// (HOR-252). Mirrors the identity (HOR-242) / permissions (HOR-243) / catalog
// (HOR-306) stores.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors.
var (
	// ErrNotFound is returned when no row matches the given id.
	ErrNotFound = errors.New("runtime: not found")
	// ErrInvalidTransition is returned when a row exists but is not in a state
	// the requested transition allows. The actual current state is included.
	ErrInvalidTransition = errors.New("runtime: invalid transition")
)

// Run kind + state, step kind + state, turn state, and event kinds. They mirror
// the CHECK constraints in migration 000009 and the harness Event set
// (proto/iterabase/harness/v1/harness.proto).
const (
	KindChat     = "chat"
	KindWorkflow = "workflow"

	StepKindAgentTask    = "agent_task"
	StepKindToolCall     = "tool_call"
	StepKindApprovalGate = "approval_gate"

	RunPending          = "pending"
	RunRunning          = "running"
	RunAwaitingApproval = "awaiting_approval"
	RunSucceeded        = "succeeded"
	RunFailed           = "failed"
	RunAborted          = "aborted"

	StepPending         = "pending"
	StepRunning         = "running"
	StepPendingApproval = "pending_approval"
	StepSucceeded       = "succeeded"
	StepFailed          = "failed"
	StepSkipped         = "skipped"

	TurnPending   = "pending"
	TurnRunning   = "running"
	TurnSucceeded = "succeeded"
	TurnFailed    = "failed"
	TurnAborted   = "aborted"

	// Turn-level events (mirror harness Event).
	EvTurnStarted      = "turn_started"
	EvAssistantMessage = "assistant_message"
	EvToolResult       = "tool_result"
	EvError            = "error"
	EvSettled          = "settled"
	// Step-level events.
	EvStepStarted       = "step_started"
	EvStepSucceeded     = "step_succeeded"
	EvStepFailed        = "step_failed"
	EvStepSkipped       = "step_skipped"
	EvApprovalRequested = "approval_requested"
	EvApprovalResolved  = "approval_resolved"
	// Run-level events.
	EvRunStarted          = "run_started"
	EvRunSucceeded        = "run_succeeded"
	EvRunFailed           = "run_failed"
	EvRunAborted          = "run_aborted"
	EvRunAwaitingApproval = "run_awaiting_approval"
	EvRunResumed          = "run_resumed"
)

// Approval decision (approval_resolved event payload; ResolveApproval input).
const (
	DecisionApproved = "approved"
	DecisionRejected = "rejected"
)

// Run is a row from runtime.workflow_runs: an execution instance.
type Run struct {
	ID              string
	Kind            string  // chat | workflow
	DefinitionKey   *string // HOR-252 Workflow key; nil for chat
	ScopeIdentityID string
	SessionID       string
	SessionDir      string
	Trigger         json.RawMessage // opaque source descriptor
	State           string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	StartedAt       *time.Time
	FinishedAt      *time.Time
}

// Step is a row from runtime.run_steps: one step of a run's snapshotted plan.
type Step struct {
	ID         string
	RunID      string
	Seq        int
	Kind       string          // agent_task | tool_call | approval_gate
	Config     json.RawMessage // opaque
	State      string
	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Turn is a row from runtime.turns: one agent invocation (Prompt->Settled).
type Turn struct {
	ID        string
	RunID     string
	StepID    *string
	SessionID string
	Model     *string
	State     string
	StartedAt *time.Time
	SettledAt *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Event is a row from runtime.events: one append-only audit/replay entry.
type Event struct {
	ID      string
	RunID   string
	TurnID  *string
	StepID  *string
	Seq     int
	Kind    string
	Payload json.RawMessage
	TS      time.Time
}

// StepInput is a step to snapshot at run creation (CreateRun).
type StepInput struct {
	Seq    int
	Kind   string          // agent_task | tool_call | approval_gate
	Config json.RawMessage // opaque; {} when empty
}

// CreateRunInput is the input to CreateRun.
type CreateRunInput struct {
	Kind            string // chat | workflow
	DefinitionKey   string // "" for chat
	ScopeIdentityID string
	SessionID       string
	SessionDir      string
	Trigger         json.RawMessage // opaque; {} when empty
	Steps           []StepInput
}

// Store reads and writes the runtime schema via a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool for runtime operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// CreateRun inserts a run (state=pending) and snapshots its step plan in one tx,
// so a run always exists with its full plan. `trigger` and each step's `config`
// are opaque JSONB. Returns the created run.
func (s *Store) CreateRun(ctx context.Context, in CreateRunInput) (Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		INSERT INTO runtime.workflow_runs (kind, definition_key, scope_identity_id, session_id, session_dir, trigger)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, kind, definition_key, scope_identity_id, session_id, session_dir, trigger, state,
		          created_at, updated_at, started_at, finished_at`,
		in.Kind, nullable(in.DefinitionKey), in.ScopeIdentityID, in.SessionID, in.SessionDir, jsonB(in.Trigger))
	run, err := scanRun(row)
	if err != nil {
		return Run{}, fmt.Errorf("insert run: %w", err)
	}

	for _, st := range in.Steps {
		if _, err := tx.Exec(ctx, `
			INSERT INTO runtime.run_steps (run_id, seq, kind, config)
			VALUES ($1, $2, $3, $4)`,
			run.ID, st.Seq, st.Kind, jsonB(st.Config)); err != nil {
			return Run{}, fmt.Errorf("insert step seq=%d: %w", st.Seq, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("commit: %w", err)
	}
	return run, nil
}

// GetRun fetches a run by id.
func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, kind, definition_key, scope_identity_id, session_id, session_dir, trigger, state,
		       created_at, updated_at, started_at, finished_at
		FROM runtime.workflow_runs WHERE id = $1`, id)
	run, err := scanRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return run, err
}

// ListRunsByState returns runs in the given state (e.g. "running" for resume,
// skipping "awaiting_approval"). Ordered by created_at.
func (s *Store) ListRunsByState(ctx context.Context, state string) ([]Run, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, kind, definition_key, scope_identity_id, session_id, session_dir, trigger, state,
		       created_at, updated_at, started_at, finished_at
		FROM runtime.workflow_runs WHERE state = $1
		ORDER BY created_at`, state)
	if err != nil {
		return nil, fmt.Errorf("list runs by state: %w", err)
	}
	defer rows.Close()

	var out []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

// transitionRun performs a CAS state transition on a run, appending `eventKind`
// (run-level) in the same tx. `from` is the set of allowed current states
// (matched via `state = ANY($2)`). Returns ErrNotFound if the run is absent,
// ErrInvalidTransition if it is in a disallowed state.
func (s *Store) transitionRun(ctx context.Context, id string, from []string, to, eventKind string, payload json.RawMessage) (Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	finishedExpr := "NULL"
	if isTerminal(to) {
		finishedExpr = "now()"
	}
	startedExpr := "NULL"
	if to == RunRunning {
		startedExpr = "now()"
	}

	row := tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE runtime.workflow_runs
		   SET state = $3, started_at = COALESCE(started_at, %s), finished_at = %s
		 WHERE id = $1 AND state = ANY($2)
		RETURNING id, kind, definition_key, scope_identity_id, session_id, session_dir, trigger, state,
		          created_at, updated_at, started_at, finished_at`, startedExpr, finishedExpr),
		id, from, to)
	run, err := scanRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, s.runTransitionErr(ctx, tx, id)
	}
	if err != nil {
		return Run{}, fmt.Errorf("update run: %w", err)
	}

	if _, err := appendEventTx(ctx, tx, run.ID, "", "", eventKind, payload); err != nil {
		return Run{}, fmt.Errorf("append %s: %w", eventKind, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("commit: %w", err)
	}
	return run, nil
}

// runTransitionErr distinguishes ErrNotFound (no row) from ErrInvalidTransition
// (row present, wrong state) by reading the current state.
func (s *Store) runTransitionErr(ctx context.Context, tx pgx.Tx, id string) error {
	var state string
	err := tx.QueryRow(ctx, `SELECT state FROM runtime.workflow_runs WHERE id = $1`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: run state is %s", ErrInvalidTransition, state)
}

// StartRun moves a run pending -> running and appends run_started.
func (s *Store) StartRun(ctx context.Context, id string) (Run, error) {
	return s.transitionRun(ctx, id, []string{RunPending}, RunRunning, EvRunStarted, nil)
}

// SucceedRun moves a run running -> succeeded and appends run_succeeded.
func (s *Store) SucceedRun(ctx context.Context, id string) (Run, error) {
	return s.transitionRun(ctx, id, []string{RunRunning}, RunSucceeded, EvRunSucceeded, nil)
}

// FailRun moves a run (running|awaiting_approval) -> failed and appends run_failed.
func (s *Store) FailRun(ctx context.Context, id string) (Run, error) {
	return s.transitionRun(ctx, id, []string{RunRunning, RunAwaitingApproval}, RunFailed, EvRunFailed, nil)
}

// AbortRun moves a run (running|awaiting_approval) -> aborted and appends run_aborted.
func (s *Store) AbortRun(ctx context.Context, id string) (Run, error) {
	return s.transitionRun(ctx, id, []string{RunRunning, RunAwaitingApproval}, RunAborted, EvRunAborted, nil)
}

// AwaitApproval moves a run running -> awaiting_approval and appends
// run_awaiting_approval. (Called internally by RequestApproval; exposed for
// admin/override paths.)
func (s *Store) AwaitApproval(ctx context.Context, id string) (Run, error) {
	return s.transitionRun(ctx, id, []string{RunRunning}, RunAwaitingApproval, EvRunAwaitingApproval, nil)
}

// ResumeFromApproval moves a run awaiting_approval -> running and appends
// run_resumed. (Called internally by ResolveApproval on approval; exposed for
// admin/override paths.)
func (s *Store) ResumeFromApproval(ctx context.Context, id string) (Run, error) {
	return s.transitionRun(ctx, id, []string{RunAwaitingApproval}, RunRunning, EvRunResumed, nil)
}

// --- steps ---

// ListSteps returns a run's steps ordered by seq.
func (s *Store) ListSteps(ctx context.Context, runID string) ([]Step, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, seq, kind, config, state, started_at, finished_at, created_at, updated_at
		FROM runtime.run_steps WHERE run_id = $1 ORDER BY seq`, runID)
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	defer rows.Close()

	var out []Step
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// GetStep fetches a step by id.
func (s *Store) GetStep(ctx context.Context, id string) (Step, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, run_id, seq, kind, config, state, started_at, finished_at, created_at, updated_at
		FROM runtime.run_steps WHERE id = $1`, id)
	st, err := scanStep(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Step{}, ErrNotFound
	}
	return st, err
}

// NextPendingStep returns the oldest pending step of a run (what HOR-249 drives
// next), or ErrNotFound if none remain.
func (s *Store) NextPendingStep(ctx context.Context, runID string) (Step, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, run_id, seq, kind, config, state, started_at, finished_at, created_at, updated_at
		FROM runtime.run_steps
		WHERE run_id = $1 AND state = $2
		ORDER BY seq
		LIMIT 1`, runID, StepPending)
	st, err := scanStep(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Step{}, ErrNotFound
	}
	return st, err
}

// transitionStep performs a CAS state transition on a step, appending the
// step-level `eventKind` in the same tx. `from` is the allowed current states.
func (s *Store) transitionStep(ctx context.Context, id string, from []string, to, eventKind string, payload json.RawMessage) (Step, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Step{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	finishedExpr := "NULL"
	if isStepTerminal(to) {
		finishedExpr = "now()"
	}
	startedExpr := "NULL"
	if to == StepRunning {
		startedExpr = "now()"
	}

	row := tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE runtime.run_steps
		   SET state = $3, started_at = COALESCE(started_at, %s), finished_at = %s
		 WHERE id = $1 AND state = ANY($2)
		RETURNING id, run_id, seq, kind, config, state, started_at, finished_at, created_at, updated_at`,
		startedExpr, finishedExpr),
		id, from, to)
	st, err := scanStep(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Step{}, s.stepTransitionErr(ctx, tx, id)
	}
	if err != nil {
		return Step{}, fmt.Errorf("update step: %w", err)
	}

	if _, err := appendEventTx(ctx, tx, st.RunID, "", st.ID, eventKind, payload); err != nil {
		return Step{}, fmt.Errorf("append %s: %w", eventKind, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Step{}, fmt.Errorf("commit: %w", err)
	}
	return st, nil
}

func (s *Store) stepTransitionErr(ctx context.Context, tx pgx.Tx, id string) error {
	var state string
	err := tx.QueryRow(ctx, `SELECT state FROM runtime.run_steps WHERE id = $1`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: step state is %s", ErrInvalidTransition, state)
}

// StartStep moves a step pending -> running and appends step_started.
func (s *Store) StartStep(ctx context.Context, id string) (Step, error) {
	return s.transitionStep(ctx, id, []string{StepPending}, StepRunning, EvStepStarted, nil)
}

// SucceedStep moves a step running -> succeeded and appends step_succeeded.
func (s *Store) SucceedStep(ctx context.Context, id string) (Step, error) {
	return s.transitionStep(ctx, id, []string{StepRunning}, StepSucceeded, EvStepSucceeded, nil)
}

// FailStep moves a step running -> failed and appends step_failed.
func (s *Store) FailStep(ctx context.Context, id string) (Step, error) {
	return s.transitionStep(ctx, id, []string{StepRunning}, StepFailed, EvStepFailed, nil)
}

// SkipStep moves a step pending -> skipped (branch not taken) and appends
// step_skipped.
func (s *Store) SkipStep(ctx context.Context, id string) (Step, error) {
	return s.transitionStep(ctx, id, []string{StepPending}, StepSkipped, EvStepSkipped, nil)
}

// RequestApproval moves an approval_gate step pending -> pending_approval AND
// the run running -> awaiting_approval atomically, appending approval_requested
// (step) + run_awaiting_approval (run). The two are inseparable: a step awaiting
// a human <=> the run is blocked.
func (s *Store) RequestApproval(ctx context.Context, stepID string) (Step, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Step{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		UPDATE runtime.run_steps
		   SET state = $2, started_at = COALESCE(started_at, now())
		 WHERE id = $1 AND state = $3 AND kind = $4
		RETURNING id, run_id, seq, kind, config, state, started_at, finished_at, created_at, updated_at`,
		stepID, StepPendingApproval, StepPending, StepKindApprovalGate)
	st, err := scanStep(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Step{}, s.stepTransitionErr(ctx, tx, stepID)
	}
	if err != nil {
		return Step{}, fmt.Errorf("update step: %w", err)
	}

	// Run running -> awaiting_approval (CAS; if it is not running, the step
	// transition is rolled back via the deferred rollback).
	var runState string
	if err := tx.QueryRow(ctx, `
		UPDATE runtime.workflow_runs SET state = $2
		 WHERE id = $1 AND state = $3
		RETURNING state`, st.RunID, RunAwaitingApproval, RunRunning).Scan(&runState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Step{}, fmt.Errorf("%w: run is not running (cannot await approval)", ErrInvalidTransition)
		}
		return Step{}, fmt.Errorf("update run to awaiting_approval: %w", err)
	}

	if _, err := appendEventTx(ctx, tx, st.RunID, "", st.ID, EvApprovalRequested, nil); err != nil {
		return Step{}, fmt.Errorf("append %s: %w", EvApprovalRequested, err)
	}
	if _, err := appendEventTx(ctx, tx, st.RunID, "", "", EvRunAwaitingApproval, nil); err != nil {
		return Step{}, fmt.Errorf("append %s: %w", EvRunAwaitingApproval, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Step{}, fmt.Errorf("commit: %w", err)
	}
	return st, nil
}

// ResolveApproval resolves an approval gate: approved -> step succeeded + run
// resumed; rejected -> step failed + run failed. Appends approval_resolved
// (payload: approver_identity_id + decision) plus the run-level event. Atomic.
func (s *Store) ResolveApproval(ctx context.Context, stepID, approverIdentityID, decision string) (Step, error) {
	switch decision {
	case DecisionApproved, DecisionRejected:
	default:
		return Step{}, fmt.Errorf("invalid decision %q", decision)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Step{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	stepTo := StepSucceeded
	runTo := RunRunning
	runEvent := EvRunResumed
	if decision == DecisionRejected {
		stepTo = StepFailed
		runTo = RunFailed
		runEvent = EvRunFailed
	}

	finishedExpr := "NULL"
	if stepTo == StepFailed {
		finishedExpr = "now()"
	}
	row := tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE runtime.run_steps
		   SET state = $2, finished_at = %s
		 WHERE id = $1 AND state = $3
		RETURNING id, run_id, seq, kind, config, state, started_at, finished_at, created_at, updated_at`,
		finishedExpr),
		stepID, stepTo, StepPendingApproval)
	st, err := scanStep(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Step{}, s.stepTransitionErr(ctx, tx, stepID)
	}
	if err != nil {
		return Step{}, fmt.Errorf("update step: %w", err)
	}

	runFinishedExpr := "NULL"
	if isTerminal(runTo) {
		runFinishedExpr = "now()"
	}
	var runState string
	if err := tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE runtime.workflow_runs
		   SET state = $2, finished_at = %s
		 WHERE id = $1 AND state = $3
		RETURNING state`, runFinishedExpr),
		st.RunID, runTo, RunAwaitingApproval).Scan(&runState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Step{}, fmt.Errorf("%w: run is not awaiting approval", ErrInvalidTransition)
		}
		return Step{}, fmt.Errorf("update run on approval: %w", err)
	}

	payload, _ := json.Marshal(map[string]string{
		"approver_identity_id": approverIdentityID,
		"decision":             decision,
	})
	if _, err := appendEventTx(ctx, tx, st.RunID, "", st.ID, EvApprovalResolved, payload); err != nil {
		return Step{}, fmt.Errorf("append %s: %w", EvApprovalResolved, err)
	}
	if _, err := appendEventTx(ctx, tx, st.RunID, "", "", runEvent, nil); err != nil {
		return Step{}, fmt.Errorf("append %s: %w", runEvent, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Step{}, fmt.Errorf("commit: %w", err)
	}
	return st, nil
}

// --- turns ---

// StartTurn creates a turn (state=pending) for a run + step, moves it pending ->
// running, and appends turn_started. The partial unique index
// idx_turns_one_active_per_session enforces one active turn per session: a
// concurrent StartTurn on the same session fails the insert (HOR-249 treats the
// conflict as "busy"). `model` rides in the turn_started payload.
func (s *Store) StartTurn(ctx context.Context, runID, stepID, model string) (Turn, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Turn{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Carry the run's session_id onto the turn (denormalized for the unique
	// index). The run must exist.
	var sessionID string
	if err := tx.QueryRow(ctx, `SELECT session_id FROM runtime.workflow_runs WHERE id = $1`, runID).Scan(&sessionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Turn{}, ErrNotFound
		}
		return Turn{}, fmt.Errorf("read run session: %w", err)
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO runtime.turns (run_id, step_id, session_id, model, state)
		VALUES ($1, $2, $3, $4, 'running')
		RETURNING id, run_id, step_id, session_id, model, state, started_at, settled_at, created_at, updated_at`,
		runID, nullable(stepID), sessionID, nullable(model))
	t, err := scanTurn(row)
	if err != nil {
		return Turn{}, fmt.Errorf("insert turn: %w", err)
	}

	payload, _ := json.Marshal(map[string]string{"model": model})
	if _, err := appendEventTx(ctx, tx, runID, t.ID, stepID, EvTurnStarted, payload); err != nil {
		return Turn{}, fmt.Errorf("append %s: %w", EvTurnStarted, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Turn{}, fmt.Errorf("commit: %w", err)
	}
	return t, nil
}

// SettleTurn moves a turn running -> succeeded|failed|aborted and appends
// settled (payload: reason + message_count). It does NOT cascade step/run
// transitions — HOR-249 decides whether to SucceedStep/FailStep/StartStep next.
// Maps 1:1 to the harness Settled.Reason: completed->succeeded, failed->failed,
// aborted->aborted.
func (s *Store) SettleTurn(ctx context.Context, id, reason string, messageCount int) (Turn, error) {
	to, ok := turnStateForReason(reason)
	if !ok {
		return Turn{}, fmt.Errorf("invalid reason %q", reason)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Turn{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		UPDATE runtime.turns SET state = $2, settled_at = now()
		 WHERE id = $1 AND state = $3
		RETURNING id, run_id, step_id, session_id, model, state, started_at, settled_at, created_at, updated_at`,
		id, to, TurnRunning)
	t, err := scanTurn(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Turn{}, s.turnTransitionErr(ctx, tx, id)
	}
	if err != nil {
		return Turn{}, fmt.Errorf("update turn: %w", err)
	}

	payload, _ := json.Marshal(map[string]any{"reason": reason, "message_count": messageCount})
	if _, err := appendEventTx(ctx, tx, t.RunID, t.ID, strPtr(t.StepID), EvSettled, payload); err != nil {
		return Turn{}, fmt.Errorf("append %s: %w", EvSettled, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Turn{}, fmt.Errorf("commit: %w", err)
	}
	return t, nil
}

func (s *Store) turnTransitionErr(ctx context.Context, tx pgx.Tx, id string) error {
	var state string
	err := tx.QueryRow(ctx, `SELECT state FROM runtime.turns WHERE id = $1`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: turn state is %s", ErrInvalidTransition, state)
}

// ActiveTurn returns the run's pending/running turn (if any) for reattach/resume.
func (s *Store) ActiveTurn(ctx context.Context, runID string) (Turn, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, run_id, step_id, session_id, model, state, started_at, settled_at, created_at, updated_at
		FROM runtime.turns
		WHERE run_id = $1 AND state = ANY($2)
		ORDER BY started_at DESC
		LIMIT 1`, runID, []string{TurnPending, TurnRunning})
	t, err := scanTurn(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Turn{}, ErrNotFound
	}
	return t, err
}

// --- events ---

// AppendEvent records a raw (non-transition) event — the harness-streamed
// assistant_message/tool_result/error kinds — computing a gapless per-run seq.
// `turnID`/`stepID` may be "". State transitions use the internal appendEventTx
// directly (with their own tx); callers that already hold a turn use this.
func (s *Store) AppendEvent(ctx context.Context, runID, turnID, stepID, kind string, payload json.RawMessage) (Event, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Event{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ev, err := appendEventTx(ctx, tx, runID, turnID, stepID, kind, payload)
	if err != nil {
		return Event{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Event{}, fmt.Errorf("commit: %w", err)
	}
	return ev, nil
}

// EventsSince returns a run's events with seq > `after`, ordered by seq (for
// HOR-249 to resume appending / stream to a surface).
func (s *Store) EventsSince(ctx context.Context, runID string, after int) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, turn_id, step_id, seq, kind, payload, ts
		FROM runtime.events WHERE run_id = $1 AND seq > $2 ORDER BY seq`, runID, after)
	if err != nil {
		return nil, fmt.Errorf("events since: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// ListEvents returns all of a run's events ordered by seq (audit / replay fold).
func (s *Store) ListEvents(ctx context.Context, runID string) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, turn_id, step_id, seq, kind, payload, ts
		FROM runtime.events WHERE run_id = $1 ORDER BY seq`, runID)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// LastEventSeq returns the max seq for a run (where HOR-249 resumes appending),
// or 0 if the run has no events yet.
func (s *Store) LastEventSeq(ctx context.Context, runID string) (int, error) {
	var seq *int
	if err := s.pool.QueryRow(ctx, `SELECT MAX(seq) FROM runtime.events WHERE run_id = $1`, runID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("last event seq: %w", err)
	}
	if seq == nil {
		return 0, nil
	}
	return *seq, nil
}

// --- helpers ---

// appendEventTx inserts an event with a gapless per-run seq (COALESCE(MAX+1))
// within the caller's tx and returns the scanned row. `turnID`/`stepID` may be
// "" (stored NULL). `payload` defaults to '{}'. The UNIQUE(run_id, seq)
// constraint is the collision backstop under the sole-writer-per-turn invariant.
func appendEventTx(ctx context.Context, tx pgx.Tx, runID, turnID, stepID, kind string, payload json.RawMessage) (Event, error) {
	row := tx.QueryRow(ctx, `
		INSERT INTO runtime.events (run_id, turn_id, step_id, seq, kind, payload)
		VALUES ($1, $2, $3, (SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime.events WHERE run_id = $1), $4, $5)
		RETURNING id, run_id, turn_id, step_id, seq, kind, payload, ts`,
		runID, nullable(turnID), nullable(stepID), kind, jsonB(payload))
	var e Event
	var turnIDStr, stepIDStr *string
	if err := row.Scan(&e.ID, &e.RunID, &turnIDStr, &stepIDStr, &e.Seq, &e.Kind, &e.Payload, &e.TS); err != nil {
		return Event{}, fmt.Errorf("insert event: %w", err)
	}
	e.TurnID = turnIDStr
	e.StepID = stepIDStr
	return e, nil
}

func scanEvents(rows pgx.Rows) ([]Event, error) {
	var out []Event
	for rows.Next() {
		var e Event
		var turnIDStr, stepIDStr *string
		if err := rows.Scan(&e.ID, &e.RunID, &turnIDStr, &stepIDStr, &e.Seq, &e.Kind, &e.Payload, &e.TS); err != nil {
			return nil, err
		}
		e.TurnID = turnIDStr
		e.StepID = stepIDStr
		out = append(out, e)
	}
	return out, rows.Err()
}

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	var defKey *string
	err := row.Scan(&r.ID, &r.Kind, &defKey, &r.ScopeIdentityID, &r.SessionID, &r.SessionDir,
		&r.Trigger, &r.State, &r.CreatedAt, &r.UpdatedAt, &r.StartedAt, &r.FinishedAt)
	r.DefinitionKey = defKey
	return r, err
}

func scanStep(row pgx.Row) (Step, error) {
	var s Step
	err := row.Scan(&s.ID, &s.RunID, &s.Seq, &s.Kind, &s.Config, &s.State,
		&s.StartedAt, &s.FinishedAt, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

func scanTurn(row pgx.Row) (Turn, error) {
	var t Turn
	var stepID *string
	var model *string
	err := row.Scan(&t.ID, &t.RunID, &stepID, &t.SessionID, &model, &t.State,
		&t.StartedAt, &t.SettledAt, &t.CreatedAt, &t.UpdatedAt)
	t.StepID = stepID
	t.Model = model
	return t, err
}

// isTerminal reports whether a run state is terminal (sets finished_at).
func isTerminal(state string) bool {
	switch state {
	case RunSucceeded, RunFailed, RunAborted:
		return true
	}
	return false
}

// isStepTerminal reports whether a step state is terminal (sets finished_at).
func isStepTerminal(state string) bool {
	switch state {
	case StepSucceeded, StepFailed, StepSkipped:
		return true
	}
	return false
}

// turnStateForReason maps a harness Settled.Reason to a turn terminal state.
func turnStateForReason(reason string) (string, bool) {
	switch reason {
	case "completed":
		return TurnSucceeded, true
	case "failed":
		return TurnFailed, true
	case "aborted":
		return TurnAborted, true
	}
	return "", false
}

// nullable returns nil for an empty string (so pgx stores NULL, not "").
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// strPtr dereferences a *string for passing as a plain string arg (empty if nil).
func strPtr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// jsonB normalizes a RawMessage to '{}' so nullable text columns don't get NULL
// over the NOT NULL jsonb defaults.
func jsonB(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("{}")
	}
	return b
}
