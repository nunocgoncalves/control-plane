// Package dispatch implements the control-plane durable dispatch + active
// assignment context + worker fencing (HOR-249): the native-gRPC Harness Work
// server (the bidi stream warm workers connect to), the worker connection pool,
// one-credit dispatch, durable TurnEvent ACK/dedup, cancellation and
// worker-loss semantics, generation fencing, and the dispatch reconciler that
// drives pending runs/steps/turns to eligible idle workers.
//
// It is the production writer of runtime.turn_assignments (the active-assignment
// context) and runtime.run_pool_assignments (run -> pool). The tool gateway
// (this repo, HOR-392) and the inference gateway (cross-repo, HOR-398) read
// turn_assignments fail-closed to bind authorization to the specific verified
// worker (cert SAN pod name) + current fencing generation for a running turn —
// closing the HOR-398 / DEC-041 interim residual.
//
// Scope (ARCH-004/006/007/010/014/018): trigger sources (email HOR-356, UI
// HOR-396, operator-artifact HOR-393), workflow definitions (HOR-252), and the
// work-item/attempt model (HOR-254) are NOT implemented here. The runtime DB is
// the durable handoff: HOR-252/254 write run/step/attempt rows +
// run_pool_assignments + attempt_tool_pins; this package consumes pending runs
// and drives them to workers. A Go Dispatcher API + the reconciler cover
// explicit/synthetic starts and integration tests.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nunocgoncalves/control-plane/internal/runtime"
)

// Sentinel errors.
var (
	// ErrNotFound is returned when no row matches.
	ErrNotFound = errors.New("dispatch: not found")
	// ErrAlreadyAssigned is returned when a turn already has an active assignment.
	ErrAlreadyAssigned = errors.New("dispatch: turn already actively assigned")
	// ErrAssignmentNotActive is returned when an assignment exists but is not
	// active (fenced/terminal) — gateways treat this as denial.
	ErrAssignmentNotActive = errors.New("dispatch: assignment not active")
)

// AssignmentState is the active-assignment lifecycle.
type AssignmentState string

const (
	AssignmentActive   AssignmentState = "active"
	AssignmentTerminal AssignmentState = "terminal"
	AssignmentFenced   AssignmentState = "fenced"
)

// Assignment is a row from runtime.turn_assignments: the durable active
// assignment context (ARCH-004/010). One row per actively-dispatched turn.
type Assignment struct {
	TurnID                 string
	RunID                  string
	PoolID                 string
	WorkerID               string // verified cert SAN pod name
	FencingGeneration      int64
	AttemptID              string // run id (v1 attempt identity)
	ScopeIdentityID        string
	AgentPoolKey           string
	ModelPermission        json.RawMessage
	CapabilityRequest      json.RawMessage
	ToolVersionSnapshot    json.RawMessage
	State                  AssignmentState
	HighestAppliedSequence uint64
	AssignedAt             time.Time
	TerminalizedAt         *time.Time
}

// AssignmentInput is the input to CreateAssignment (the active-assignment
// context captured at AssignTurn).
type AssignmentInput struct {
	TurnID              string
	RunID               string
	PoolID              string
	WorkerID            string
	FencingGeneration   int64
	AttemptID           string
	ScopeIdentityID     string
	AgentPoolKey        string
	ModelPermission     json.RawMessage
	CapabilityRequest   json.RawMessage
	ToolVersionSnapshot json.RawMessage
}

// Store reads/writes the dispatch assignment state via a pgx pool. It composes
// the runtime store for run/step/turn state-machine transitions.
type Store struct {
	pool    *pgxpool.Pool
	runtime *runtime.Store
}

// NewStore wraps a pool + runtime store for dispatch operations.
func NewStore(pool *pgxpool.Pool, rt *runtime.Store) *Store {
	return &Store{pool: pool, runtime: rt}
}

// Runtime returns the composed runtime store (SM transitions).
func (s *Store) Runtime() *runtime.Store { return s.runtime }

// CreateAssignment records the active assignment for a turn (state=active). A
// turn may have at most one active assignment; a conflict (the turn is already
// actively assigned) returns ErrAlreadyAssigned. Called by dispatch on
// AssignTurn, atomically with consuming the worker's dispatch credit.
func (s *Store) CreateAssignment(ctx context.Context, in AssignmentInput) (Assignment, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO runtime.turn_assignments
			(turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
			 scope_identity_id, agent_pool_key, model_permission, capability_request,
			 tool_version_snapshot, state)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'active')
		RETURNING turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
		          scope_identity_id, agent_pool_key, model_permission, capability_request,
		          tool_version_snapshot, state, highest_applied_sequence, assigned_at, terminalized_at`,
		in.TurnID, in.RunID, in.PoolID, in.WorkerID, in.FencingGeneration, in.AttemptID,
		in.ScopeIdentityID, in.AgentPoolKey, jsonB(in.ModelPermission), jsonB(in.CapabilityRequest),
		jsonB(in.ToolVersionSnapshot))
	a, err := scanAssignment(row)
	if err != nil {
		if isUniqueViolation(err) {
			return Assignment{}, ErrAlreadyAssigned
		}
		return Assignment{}, fmt.Errorf("create assignment: %w", err)
	}
	return a, nil
}

// ResolveActiveAssignment returns the active assignment for a turn, or
// ErrAssignmentNotActive if the turn has no active assignment (fenced/terminal
// or never assigned). Gateways read this fail-closed (ARCH-004/010).
func (s *Store) ResolveActiveAssignment(ctx context.Context, turnID string) (Assignment, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
		       scope_identity_id, agent_pool_key, model_permission, capability_request,
		       tool_version_snapshot, state, highest_applied_sequence, assigned_at, terminalized_at
		FROM runtime.turn_assignments WHERE turn_id = $1 AND state = 'active'`, turnID)
	a, err := scanAssignment(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrAssignmentNotActive
	}
	return a, err
}

// ActiveAssignmentForWorker returns the active assignment currently bound to a
// (pool, worker), or ErrNotFound. A worker holds at most one active assignment
// (one dispatch credit). Used by the Work server for fencing and cancellation.
func (s *Store) ActiveAssignmentForWorker(ctx context.Context, poolID, workerID string) (Assignment, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
		       scope_identity_id, agent_pool_key, model_permission, capability_request,
		       tool_version_snapshot, state, highest_applied_sequence, assigned_at, terminalized_at
		FROM runtime.turn_assignments
		WHERE pool_id = $1 AND worker_id = $2 AND state = 'active'`, poolID, workerID)
	a, err := scanAssignment(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	return a, err
}

// FenceWorkerGeneration fences the active assignment (if any) bound to a
// (pool, worker) — used when a worker reconnects (new generation fences the old)
// or is deemed lost. The assignment moves active -> fenced; the caller
// terminalizes the turn separately. Returns ErrNotFound if no active assignment.
func (s *Store) FenceWorkerGeneration(ctx context.Context, poolID, workerID string) (Assignment, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE runtime.turn_assignments SET state = 'fenced'
		WHERE pool_id = $1 AND worker_id = $2 AND state = 'active'
		RETURNING turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
		          scope_identity_id, agent_pool_key, model_permission, capability_request,
		          tool_version_snapshot, state, highest_applied_sequence, assigned_at, terminalized_at`,
		poolID, workerID)
	a, err := scanAssignment(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("fence worker generation: %w", err)
	}
	return a, nil
}

// TerminalizeAssignment moves a turn's assignment active -> terminal. Called
// after the turn SM is terminalized (worker outcome committed, cancellation, or
// worker-loss). Idempotent: a fenced assignment is also moved to terminal.
func (s *Store) TerminalizeAssignment(ctx context.Context, turnID string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE runtime.turn_assignments
		   SET state = 'terminal', terminalized_at = now()
		 WHERE turn_id = $1 AND state IN ('active', 'fenced')`, turnID)
	if err != nil {
		return fmt.Errorf("terminalize assignment: %w", err)
	}
	if ct.RowsAffected() == 0 {
		// Already terminal or absent — not an error (idempotent terminalization).
		return nil
	}
	return nil
}

// AppendTurnEvent applies a durable worker TurnEvent with per-turn sequence
// dedup, atomically appending the runtime audit event and advancing the
// cumulative ACK watermark. It returns applied=false (no-op) when the worker
// sequence is <= the highest already applied (a replayed/resent event); the
// caller MUST still ACK through the current watermark so the worker clears its
// retained outbox. Monotonic per-turn sequences make a high watermark
// sufficient (no per-event idempotency table).
func (s *Store) AppendTurnEvent(ctx context.Context, turnID string, workerSeq uint64, kind string, payload json.RawMessage) (applied bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID string
	var highest uint64
	if err := tx.QueryRow(ctx, `
		SELECT run_id::text, highest_applied_sequence
		FROM runtime.turn_assignments WHERE turn_id = $1::uuid AND state = 'active'
		FOR UPDATE`, turnID).Scan(&runID, &highest); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrAssignmentNotActive
		}
		return false, fmt.Errorf("lock assignment: %w", err)
	}
	if workerSeq <= highest {
		return false, nil // dedup: already applied (replayed tail).
	}

	// Append the runtime audit event with a gapless per-run seq (mirrors
	// runtime.appendEventTx). The event's turn_id is set; step_id is left NULL
	// (the dispatch path does not pin a step_id on streamed events).
	if _, err := tx.Exec(ctx, `
		INSERT INTO runtime.events (run_id, turn_id, seq, kind, payload)
		VALUES ($1::uuid, $2::uuid,
			(SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime.events WHERE run_id = $1::uuid),
			$3, $4)`,
		runID, turnID, kind, jsonB(payload)); err != nil {
		return false, fmt.Errorf("insert event: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE runtime.turn_assignments SET highest_applied_sequence = $2
		WHERE turn_id = $1::uuid`, turnID, workerSeq); err != nil {
		return false, fmt.Errorf("advance watermark: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

// AckWatermark returns the current highest-applied worker sequence for a turn
// (the cumulative ACK value to send on reconnect/heartbeat). Returns
// ErrAssignmentNotActive if the turn has no active assignment.
func (s *Store) AckWatermark(ctx context.Context, turnID string) (uint64, error) {
	var highest uint64
	err := s.pool.QueryRow(ctx, `
		SELECT highest_applied_sequence FROM runtime.turn_assignments
		WHERE turn_id = $1::uuid AND state = 'active'`, turnID).Scan(&highest)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrAssignmentNotActive
	}
	if err != nil {
		return 0, err
	}
	return highest, nil
}

// --- pool resolution (toolgateway.pools read) ---

// Pool is a row from toolgateway.pools (the AgentPool registry, ARCH-016/018).
// The dispatch resolves the pool from the worker's verified SPIFFE id (prefix
// match) to bind the cert SAN pool UID to the pool UUID used in
// run_pool_assignments / turn_assignments.
type Pool struct {
	ID             string
	Key            string
	Name           string
	SpiffeIDPrefix string
}

// ResolvePoolBySpiffePrefix finds the pool whose spiffe_id_prefix is a prefix of
// the worker's verified SPIFFE id (the pool is derived from the workload
// identity, not caller-supplied -- ARCH-004/010).
func (s *Store) ResolvePoolBySpiffePrefix(ctx context.Context, spiffeID string) (Pool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id::text, key, name, spiffe_id_prefix
		FROM toolgateway.pools
		WHERE $1 LIKE spiffe_id_prefix || '%' AND deleted_at IS NULL
		ORDER BY length(spiffe_id_prefix) DESC LIMIT 1`, spiffeID)
	var p Pool
	if err := row.Scan(&p.ID, &p.Key, &p.Name, &p.SpiffeIDPrefix); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Pool{}, ErrNotFound
		}
		return Pool{}, err
	}
	return p, nil
}

// --- run -> pool assignment (runtime.run_pool_assignments writer) ---

// AssignRunToPool records the durable run -> pool binding (ARCH-004). The
// dispatch reconciler reads this to know which pool a pending run executes
// under; the gateways read it fail-closed to validate caller scope. Idempotent.
func (s *Store) AssignRunToPool(ctx context.Context, runID, poolID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO runtime.run_pool_assignments (run_id, pool_id)
		VALUES ($1::uuid, $2::uuid)
		ON CONFLICT (run_id) DO UPDATE SET pool_id = EXCLUDED.pool_id, assigned_at = now()`,
		runID, poolID)
	if err != nil {
		return fmt.Errorf("assign run to pool: %w", err)
	}
	return nil
}

// PoolForRun returns the pool a run is assigned to, or ErrNotFound.
func (s *Store) PoolForRun(ctx context.Context, runID string) (string, error) {
	var poolID string
	err := s.pool.QueryRow(ctx, `
		SELECT pool_id::text FROM runtime.run_pool_assignments WHERE run_id = $1::uuid`, runID).Scan(&poolID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return poolID, nil
}

// --- helpers ---

func scanAssignment(row pgx.Row) (Assignment, error) {
	var a Assignment
	var termAt *time.Time
	err := row.Scan(&a.TurnID, &a.RunID, &a.PoolID, &a.WorkerID, &a.FencingGeneration,
		&a.AttemptID, &a.ScopeIdentityID, &a.AgentPoolKey,
		&a.ModelPermission, &a.CapabilityRequest, &a.ToolVersionSnapshot,
		&a.State, &a.HighestAppliedSequence, &a.AssignedAt, &termAt)
	a.TerminalizedAt = termAt
	return a, err
}

// jsonB normalizes a RawMessage to '{}' so NOT NULL jsonb columns get the
// default rather than NULL.
func jsonB(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("{}")
	}
	return b
}

// isUniqueViolation reports whether err is a Postgres unique_violation.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
