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
	// ErrOutOfOrderSequence is returned when a worker presents a TurnEvent
	// sequence with a gap (greater than highest+1). The HOR-381 source-order
	// contract is strictly monotonic, one-based and gapless; a gap is a sender
	// bug and the event is rejected without advancing the watermark
	// (fail-closed).
	ErrOutOfOrderSequence = errors.New("dispatch: out-of-order turn event sequence")
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

// FenceWorkerGenerationIf fences the active assignment bound to a (pool,
// worker) ONLY if its fencing_generation matches expectedGen (a CAS). This is
// the fencing safety fence (HOR-249): a prior-generation handler whose stream
// is finally tearing down must not fence a newer-generation assignment already
// owned by the replacement connection. Returns ErrNotFound if no active
// assignment matches (already fenced/terminal, or a different generation).
func (s *Store) FenceWorkerGenerationIf(ctx context.Context, poolID, workerID string, expectedGen int64) (Assignment, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE runtime.turn_assignments SET state = 'fenced'
		WHERE pool_id = $1 AND worker_id = $2 AND state = 'active' AND fencing_generation = $3
		RETURNING turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
	          scope_identity_id, agent_pool_key, model_permission, capability_request,
	          tool_version_snapshot, state, highest_applied_sequence, assigned_at, terminalized_at`,
		poolID, workerID, expectedGen)
	a, err := scanAssignment(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("fence worker generation (cas): %w", err)
	}
	return a, nil
}

// PriorAssignmentForWorker returns the most recent assignment (any state) bound
// to a (pool, worker), or ErrNotFound. Used on reconnect to send a cumulative
// EventAck for the prior assignment's committed watermark so a reconnected
// worker clears its retained outbox before advertising Ready (HOR-381).
func (s *Store) PriorAssignmentForWorker(ctx context.Context, poolID, workerID string) (Assignment, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
	       scope_identity_id, agent_pool_key, model_permission, capability_request,
	       tool_version_snapshot, state, highest_applied_sequence, assigned_at, terminalized_at
		FROM runtime.turn_assignments
		WHERE pool_id = $1 AND worker_id = $2
		ORDER BY assigned_at DESC LIMIT 1`, poolID, workerID)
	a, err := scanAssignment(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	return a, err
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
		// Dedup: a replayed/resent event already applied (source-order contract
		// allows <= highest replay). The caller still ACKs through the watermark
		// so the worker clears its retained outbox.
		return false, nil
	}
	// HOR-381 source-order contract: strictly monotonic, one-based, gapless.
	// A sequence greater than highest+1 is a gap (sender bug); reject without
	// advancing the watermark so 1..highest+1-1 cannot be silently ACKed away.
	if workerSeq != highest+1 {
		return false, ErrOutOfOrderSequence
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

// AppendAfterTerminalEvent durably records a late worker observation for a
// turn whose assignment is no longer active (fenced/terminal) — after-terminal
// audit (HOR-381: durable observations are never dropped). It is one atomic
// transaction that dedups by (turn, sequence) against the assignment's
// committed watermark, appends the runtime audit event, and advances the
// watermark before returning, so the caller may ACK cumulatively only after
// Postgres commit. An ACK lost after this commit is safe: the next replay sees
// the advanced watermark and dedups (sequence <= highest). Returns applied=false
// for a replayed (already-applied) sequence. Returns ErrNotFound if the turn
// has no assignment row at all (gone/never assigned). The HOR-381 source-order
// contract (strictly monotonic, one-based, gapless) is enforced: a gap is a
// sender bug and is rejected without advancing the watermark (fail-closed).
func (s *Store) AppendAfterTerminalEvent(ctx context.Context, turnID string, workerSeq uint64, kind string, payload json.RawMessage) (applied bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID string
	var highest uint64
	// Lock the assignment row in ANY state (active rows reach here only via a
	// concurrent terminalization race; fenced/terminal rows are the common case).
	// Unlike AppendTurnEvent, no state='active' filter: the turn is terminal.
	if err := tx.QueryRow(ctx, `
		SELECT run_id::text, highest_applied_sequence
		FROM runtime.turn_assignments WHERE turn_id = $1::uuid
		FOR UPDATE`, turnID).Scan(&runID, &highest); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound // turn gone; nothing to audit against.
		}
		return false, fmt.Errorf("lock assignment: %w", err)
	}
	if workerSeq <= highest {
		// Dedup: a replayed/resent event already applied. The caller still ACKs
		// through the watermark so the worker clears its retained outbox.
		return false, nil
	}
	// HOR-381 source-order contract: strictly monotonic, one-based, gapless. A
	// gap is a sender bug; reject without advancing the watermark so the
	// intermediate sequences cannot be silently ACKed away.
	if workerSeq != highest+1 {
		return false, ErrOutOfOrderSequence
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO runtime.events (run_id, turn_id, seq, kind, payload)
		VALUES ($1::uuid, $2::uuid,
			(SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime.events WHERE run_id = $1::uuid),
			$3, $4)`,
		runID, turnID, kind, jsonB(payload)); err != nil {
		return false, fmt.Errorf("insert after-terminal event: %w", err)
	}

	// Advance the cumulative ACK watermark so an ACK lost after this commit
	// dedups the event on the next replay (HOR-381 durable ACK/dedup).
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

// SessionIDForRun returns the session_id of a run (the sandbox_id), or
// ErrNotFound. Used by dispatch to send SessionEnd after a run terminates.
func (s *Store) SessionIDForRun(ctx context.Context, runID string) (string, error) {
	var sessionID string
	err := s.pool.QueryRow(ctx, `SELECT session_id FROM runtime.workflow_runs WHERE id = $1`, runID).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return sessionID, nil
}

// --- session UID allocator (HOR-245 reuse-safety floor, HOR-249 owner) ---

// ErrUIDExhausted is returned when no UID is available in the configured range
// (all are in use or within their non-recycling reap grace). Dispatch fails
// closed (the run waits) rather than silently sharing a UID.
var ErrUIDExhausted = errors.New("dispatch: session UID range exhausted")

// AllocateSessionUID allocates a stable, unique UID (gid = uid) for a session,
// idempotent per session. A UID is reusable only after it has been released
// (SessionEnd reaped) AND a bounded grace exceeding max reap latency has
// elapsed; in-use and within-grace UIDs are never recycled — including for the
// owning session, because the prior sandbox may still be reaping (HOR-245
// reuse-safety floor: the (sandbox_id, uid, gid) triple is non-recyclable until
// reaping is confirmed). Fail-closed on exhaustion. base..base+range-1 is the
// UID space.
func (s *Store) AllocateSessionUID(ctx context.Context, sessionID string, base, n uint32, grace time.Duration) (uint32, error) {
	if n == 0 {
		return 0, ErrUIDExhausted
	}
	graceBefore := time.Now().Add(-grace)
	// Idempotent fast path: this session already holds an IN-USE UID (a retry
	// or concurrent allocation for the same session). Reclaim it. A FREED row
	// is NOT reactivated here: even for the owning session, a freed UID is
	// non-recyclable until freed_at + grace, because the prior sandbox may
	// still be reaping and re-provisioning the same (uid, session) triple would
	// let a new child collide with the live sandbox being reaped (HOR-245).
	var uid uint32
	err := s.pool.QueryRow(ctx, `
		UPDATE runtime.session_uid_allocations
		   SET state = 'in_use', freed_at = NULL
		 WHERE session_id = $1 AND state = 'in_use'
		RETURNING uid`, sessionID).Scan(&uid)
	if err == nil {
		return uid, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("read session uid: %w", err)
	}
	// The session has a FREED row. Past grace the reap is complete and the
	// same (uid, session) triple may be reactivated (ownership-safe); within
	// grace the UID is non-recyclable and the allocation must fail closed so a
	// new sandbox is not provisioned on a UID still being reaped.
	err = s.pool.QueryRow(ctx, `
		UPDATE runtime.session_uid_allocations
		   SET state = 'in_use', freed_at = NULL
		 WHERE session_id = $1 AND state = 'freed' AND freed_at < $2
		RETURNING uid`, sessionID, graceBefore).Scan(&uid)
	if err == nil {
		return uid, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("reclaim freed session uid: %w", err)
	}
	// No in-use row and no freed-past-grace row. If a freed-within-grace row
	// exists, the prior sandbox is still reaping: fail closed (non-recyclable).
	// Otherwise this is a genuinely new session with no row at all.
	var blocked bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM runtime.session_uid_allocations
		               WHERE session_id = $1 AND state = 'freed' AND freed_at >= $2)`,
		sessionID, graceBefore).Scan(&blocked); err != nil {
		return 0, fmt.Errorf("check freed session uid: %w", err)
	}
	if blocked {
		return 0, ErrUIDExhausted // prior sandbox still reaping; non-recyclable.
	}
	// New session (no row): claim the lowest free UID. Retry on a uid PK
	// conflict (two sessions racing the same recyclable candidate) or a
	// session_id conflict (concurrent allocation for the same session); the
	// retry re-selects now that the winner is recorded.
	for attempt := 0; attempt < 8; attempt++ {
		claimed, err := s.tryAllocSessionUID(ctx, sessionID, base, n, grace)
		if err == nil {
			return claimed, nil
		}
		if errors.Is(err, ErrUIDExhausted) {
			return 0, err
		}
		if !isUniqueViolation(err) && !errors.Is(err, errUIDRace) {
			return 0, fmt.Errorf("allocate session uid: %w", err)
		}
	}
	return 0, fmt.Errorf("allocate session uid: retries exhausted for session %s", sessionID)
}

// errUIDRace is returned by tryAllocSessionUID when a recyclable candidate was
// selected but a concurrent allocation claimed it between the candidate select
// and the upsert (the ON CONFLICT guard no longer matched). It is retryable.
var errUIDRace = errors.New("dispatch: uid allocation race, retry")

// tryAllocSessionUID claims the lowest free UID for the session in one tx. A
// UID is free when no row exists for it OR its row is freed past the grace
// window. Recycling reclaims an expired freed row by UPSERTing on uid (the uid
// PK), not by inserting a duplicate — the prior scheme could never recycle a
// freed row because the uid PK rejected the new session's insert. A unique
// violation means a concurrent allocation won a candidate; the caller retries.
func (s *Store) tryAllocSessionUID(ctx context.Context, sessionID string, base, n uint32, grace time.Duration) (uint32, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// A UID is non-recyclable while in_use OR freed within the grace window.
	// Pass the grace threshold as a timestamp to avoid interval-literal parsing.
	graceBefore := time.Now().Add(-grace)

	// Select the lowest free UID (never allocated, or freed past grace). This
	// distinguishes genuine exhaustion (no candidate) from a lost race on a
	// candidate that existed when selected.
	var candidate int
	err = tx.QueryRow(ctx, `
		SELECT g.uid FROM generate_series($1::int, $2::int) AS g(uid)
		LEFT JOIN runtime.session_uid_allocations a ON a.uid = g.uid
		WHERE a.uid IS NULL
		   OR (a.state = 'freed' AND a.freed_at < $3)
		ORDER BY g.uid LIMIT 1`, int(base), int(base+n-1), graceBefore).Scan(&candidate)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrUIDExhausted // no free UID in range.
	}
	if err != nil {
		return 0, fmt.Errorf("select free uid: %w", err)
	}

	// Claim the candidate. INSERT if no row exists for this uid, or UPSERT the
	// freed+expired row to the new session. The ON CONFLICT (uid) guard's WHERE
	// ensures we only reclaim a row that is still freed-past-grace; if a
	// concurrent allocation claimed it in between, the update is a no-op and
	// RETURNING yields no row (errUIDRace — caller retries the next candidate).
	var uid uint32
	err = tx.QueryRow(ctx, `
		INSERT INTO runtime.session_uid_allocations (uid, session_id, state)
		VALUES ($1, $2, 'in_use')
		ON CONFLICT (uid) DO UPDATE
		   SET session_id = EXCLUDED.session_id,
		       state = 'in_use',
		       freed_at = NULL,
		       updated_at = now()
		 WHERE session_uid_allocations.state = 'freed'
		   AND session_uid_allocations.freed_at < $3
		RETURNING uid`, candidate, sessionID, graceBefore).Scan(&uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The candidate was claimable when selected but a concurrent
			// allocation won it before the upsert (ON CONFLICT guard no longer
			// matched). Retry the next candidate.
			return 0, errUIDRace
		}
		if isUniqueViolation(err) {
			return 0, err // session_id conflict (concurrent alloc for same session)
		}
		return 0, fmt.Errorf("claim uid: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return uid, nil
}

// SessionUID returns the in-use UID allocated for a session, or ErrNotFound.
func (s *Store) SessionUID(ctx context.Context, sessionID string) (uint32, error) {
	var uid uint32
	err := s.pool.QueryRow(ctx, `
		SELECT uid FROM runtime.session_uid_allocations
		WHERE session_id = $1 AND state = 'in_use'`, sessionID).Scan(&uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return uid, nil
}

// ReleaseSessionUID marks a session's UID freed (non-recyclable until grace
// elapses), so the allocator does not recycle it while the supervisor reaps the
// sandbox. Called after dispatch sends SessionEnd. Idempotent.
func (s *Store) ReleaseSessionUID(ctx context.Context, sessionID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE runtime.session_uid_allocations
		   SET state = 'freed', freed_at = now()
		 WHERE session_id = $1 AND state = 'in_use'`, sessionID)
	if err != nil {
		return fmt.Errorf("release session uid: %w", err)
	}
	return nil
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
