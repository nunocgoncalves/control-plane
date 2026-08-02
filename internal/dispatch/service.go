package dispatch

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	connect "connectrpc.com/connect"
	v1 "github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1"
	"github.com/nunocgoncalves/control-plane/internal/runtime"
	"github.com/nunocgoncalves/control-plane/internal/spiffe"
)

// Config configures the dispatch Work server + reconciler.
type Config struct {
	TrustDomain       string        // SPIFFE trust domain (default iterabase.local)
	HeartbeatInterval time.Duration // advertised to workers in Welcome
	LeaseInterval     time.Duration // worker availability freshness; any message renews the lease
	ReconcileInterval time.Duration // dispatch poll interval
	ProtocolVersion   string        // advertised in Welcome

	// SessionUIDBase/Range derive a stable per-session sandbox UID for the
	// AssignTurn SandboxRef. Per-session isolation requires unique UIDs; v1
	// derives a deterministic UID from the session id within [Base, Base+Range).
	// Collision-free non-recycling allocation is a HOR-245 sandbox-provisioning
	// hardening item (the supervisor's fail-closed reap + uid/gid ownership
	// check is the v1 safety floor). Defaults: Base 10000, Range 9000.
	SessionUIDBase  uint32
	SessionUIDRange uint32

	// DefaultModel is the ModelConfig sent on AssignTurn when the run/step does
	// not carry one. Per-workflow model selection is HOR-252 (workflow
	// definitions); v1 uses a configured default.
	DefaultModel *v1.ModelConfig
}

func (c Config) defaults() Config {
	if c.TrustDomain == "" {
		c.TrustDomain = spiffe.DefaultTrustDomain
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.LeaseInterval == 0 {
		c.LeaseInterval = 30 * time.Second
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = 500 * time.Millisecond
	}
	if c.ProtocolVersion == "" {
		c.ProtocolVersion = "1"
	}
	if c.SessionUIDBase == 0 {
		c.SessionUIDBase = 10000
	}
	if c.SessionUIDRange == 0 {
		c.SessionUIDRange = 9000
	}
	return c
}

// Service implements the Harness Work gRPC handler (HOR-249): the warm-worker
// bidi stream, worker fencing, one-credit dispatch, durable TurnEvent ACK/dedup,
// cancellation and worker-loss semantics, and the dispatch reconciler.
type Service struct {
	store       *Store
	cfg         Config
	pool        *workerPool
	gen         atomic.Uint64 // global monotonic fencing-generation counter
	log         *slog.Logger
	reconcileCh chan struct{}
}

// NewService builds a dispatch Service. cfg is defaulted.
func NewService(store *Store, cfg Config, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, cfg: cfg.defaults(), pool: newWorkerPool(), log: log, reconcileCh: make(chan struct{}, 1)}
}

// --- identity middleware (stamps the mTLS-verified SPIFFE identity into context) ---

type identityKey struct{}

// IdentityMiddleware extracts the SPIFFE identity from the request's TLS peer
// certificate and stamps it into the request context. It runs AFTER tls verifies
// the chain (RequireAndVerifyClientCert); a missing/invalid identity yields 403.
func IdentityMiddleware(trustDomain string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, err := spiffe.IdentityFromConnState(r.TLS, trustDomain)
			if err != nil {
				http.Error(w, "unauthenticated: "+err.Error(), http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, id)))
		})
	}
}

func identityFromContext(ctx context.Context) (spiffe.Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(spiffe.Identity)
	return id, ok
}

// --- Harness.Work bidi handler ---

// Work is the one long-lived bidi stream per warm worker. Worker->CP: Hello,
// Ready, Heartbeat, TurnEvent, TokenDelta. CP->worker: Welcome, AssignTurn,
// AbortTurn, EventAck, SessionEnd.
//
//nolint:gocyclo // the bidi receive loop is naturally branchy; kept flat.
func (s *Service) Work(ctx context.Context, st *connect.BidiStream[v1.WorkerMessage, v1.ControlMessage]) error {
	id, ok := identityFromContext(ctx)
	if !ok {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	if id.Kind != spiffe.KindSupervisor {
		return connect.NewError(connect.CodePermissionDenied, errors.New("only warm workers may connect to the Work stream"))
	}

	// Hello is the first message; the authenticated cert is authoritative.
	hello, err := st.Receive()
	if err != nil {
		return err
	}
	h := hello.GetHello()
	if h == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first worker message must be Hello"))
	}
	// Bind the verified cert SAN (pool UID + pod name) to Hello. A mismatch
	// terminates the stream fail-closed (ARCH-010 identity contract amendment:
	// worker_id is the pod name, verified by the cert SAN).
	if h.PoolId != id.PoolUID || h.WorkerId != id.WorkerID {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("hello identity (pool=%s worker=%s) does not match verified cert SAN (pool=%s worker=%s)",
				h.PoolId, h.WorkerId, id.PoolUID, id.WorkerID))
	}

	// Resolve the pool UUID from the verified SPIFFE id (prefix match). The
	// pool UUID binds run_pool_assignments / turn_assignments; the cert SAN
	// pool UID is only the identity segment (ARCH-004/010).
	pool, err := s.store.ResolvePoolBySpiffePrefix(ctx, id.SPIFFEID)
	if err != nil {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("worker pool not registered: %w", err))
	}

	gen := int64(s.gen.Add(1)) //nolint:gosec // G115: generation counter

	// Fence the prior generation for this (pool, worker): a reconnect closes
	// the old stream and fences its active assignment as worker loss.
	s.fenceOldGeneration(ctx, pool.ID, id.WorkerID)

	w := &workerConn{
		poolID:   pool.ID,
		workerID: id.WorkerID,
		gen:      gen,
		stream:   st,
		lastSeen: time.Now(),
	}
	s.pool.add(w)
	defer func() {
		w.markClosed()
		s.pool.remove(w)
		s.handleWorkerLoss(ctx, w)
	}()

	if err := st.Send(s.welcome(gen)); err != nil {
		return fmt.Errorf("welcome send: %w", err)
	}
	s.log.Info("worker connected", "pool", pool.ID, "worker", id.WorkerID, "gen", gen,
		"build", h.BuildVersion, "protocol", h.ProtocolVersion)

	// Ack any in-flight assignment the worker may be replaying after a
	// reconnect (cumulative ACK through the current watermark so the worker
	// clears its retained outbox before advertising Ready).
	s.ackReconnectWatermark(ctx, st, id.PoolUID, id.WorkerID)

	for {
		msg, err := st.Receive()
		if err != nil {
			return nil // stream closed (client disconnect / ctx cancel / fenced)
		}
		w.mu.Lock()
		w.lastSeen = time.Now()
		w.mu.Unlock()

		switch m := msg.Kind.(type) {
		case *v1.WorkerMessage_Ready:
			// One credit. Legal only when no turn is active; a Ready while busy
			// is a protocol violation (stream closed fail-closed).
			if w.activeTurn != "" {
				return connect.NewError(connect.CodeFailedPrecondition,
					errors.New("ready while a turn is active is a protocol violation (one-credit dispatch)"))
			}
			w.grantCredit()
			s.kickReconciler()
		case *v1.WorkerMessage_Heartbeat:
			// Any message renews the lease; nothing else to do.
		case *v1.WorkerMessage_TurnEvent:
			if err := s.handleTurnEvent(ctx, m.TurnEvent, w); err != nil {
				s.log.Warn("turn event handling failed", "turn", m.TurnEvent.GetTurnId(), "error", err)
			}
		case *v1.WorkerMessage_TokenDelta:
			// Ephemeral live token streaming; not durable, not ACKed. Surfaced
			// to a UI later; ignored by the dispatch path.
		}
	}
}

// welcome builds the Welcome control message for a fencing generation.
func (s *Service) welcome(gen int64) *v1.ControlMessage {
	return &v1.ControlMessage{Kind: &v1.ControlMessage_Welcome{Welcome: &v1.Welcome{
		ProtocolVersion:     s.cfg.ProtocolVersion,
		FencingGeneration:   uint64(gen),                                       //nolint:gosec // G115
		HeartbeatIntervalMs: int32(s.cfg.HeartbeatInterval / time.Millisecond), //nolint:gosec // G115
		LeaseTimeoutMs:      int32(s.cfg.LeaseInterval / time.Millisecond),     //nolint:gosec // G115
	}}}
}

// fenceOldGeneration closes any prior live connection for this (pool, worker)
// and fences its active assignment as worker loss. The old connection's receive
// loop observes the closed stream and returns; its deferred handleWorkerLoss is
// a no-op because the assignment is already fenced here.
func (s *Service) fenceOldGeneration(ctx context.Context, poolID, workerID string) {
	if old := s.pool.get(poolID, workerID); old != nil {
		old.markClosed()
	}
	// Fence the active assignment (if any) bound to this worker. The turn is
	// terminalized as worker loss (aborted); a late WorkerOutcome from the old
	// generation is after-terminal audit only.
	if a, err := s.store.FenceWorkerGeneration(ctx, poolID, workerID); err == nil {
		s.log.Info("fenced prior generation on reconnect", "pool", poolID, "worker", workerID, "turn", a.TurnID)
		s.terminalizeTurnLoss(ctx, a)
	}
}

// ackReconnectWatermark sends a cumulative EventAck for the worker's prior
// active assignment (if any survived the fence as already-committed events) so
// the worker clears its retained outbox. After fencing there is no active
// assignment, so this is a best-effort no-op when none exists.
func (s *Service) ackReconnectWatermark(ctx context.Context, st *connect.BidiStream[v1.WorkerMessage, v1.ControlMessage], poolID, workerID string) {
	// After fenceOldGeneration the worker has no active assignment; nothing to
	// ACK. Kept as an explicit seam for a future durable replay-ack contract.
}

// kickReconciler pokes the dispatch loop to attempt assignment promptly when a
// worker advertises credit. Best-effort; the loop also polls on a ticker.
func (s *Service) kickReconciler() {
	select {
	case s.reconcileCh <- struct{}{}:
	default:
	}
}

// --- TurnEvent handling (durable ACK/dedup + terminal outcome) ---

// handleTurnEvent commits a durable worker observation with per-turn sequence
// dedup, ACKs cumulatively, and terminalizes the turn on a WorkerOutcome. A
// late outcome after the CP has terminalized is after-terminal audit (appended
// as an event, never overwrites state).
//
//nolint:gocyclo // branchy by nature; kept flat.
func (s *Service) handleTurnEvent(ctx context.Context, te *v1.TurnEvent, w *workerConn) error {
	turnID := te.GetTurnId()
	seq := te.GetSequence()
	if turnID == "" {
		return errors.New("turn_event without turn_id")
	}

	// Fencing: ignore events from a conn whose generation no longer owns the
	// active assignment (a fenced/old-generation stream). The assignment's
	// recorded generation is authoritative.
	a, err := s.store.ResolveActiveAssignment(ctx, turnID)
	if err != nil {
		// No active assignment: the turn was already terminalized (CP
		// first-terminal-writer) or never assigned. A late outcome is
		// after-terminal audit — append it as an event without mutating state.
		if wo := te.GetWorkerOutcome(); wo != nil {
			return s.appendAfterTerminal(ctx, turnID, te)
		}
		return nil
	}
	if a.WorkerID != w.workerID || a.FencingGeneration != w.gen {
		// Stale generation: ignore (fenced). Do not ACK — the old stream will
		// be closed by fencing and the worker reconnects under the new gen.
		return nil
	}

	// Map the proto event kind to a runtime event kind + payload.
	kind, payload, isOutcome := turnEventToRuntime(te)
	if kind == "" && !isOutcome {
		// Unknown/ignored event kind (e.g. an event type with no durable
		// representation). ACK through the current watermark so the worker
		// progresses.
		return s.ack(ctx, w, turnID, seq)
	}

	applied, err := s.store.AppendTurnEvent(ctx, turnID, seq, kind, payload)
	if err != nil {
		if errors.Is(err, ErrAssignmentNotActive) {
			// Terminalized concurrently; late outcome = after-terminal audit.
			if isOutcome {
				return s.appendAfterTerminal(ctx, turnID, te)
			}
			return nil
		}
		return err
	}
	if !applied {
		// Dedup: already applied (replayed tail). ACK through the watermark.
		return s.ack(ctx, w, turnID, seq)
	}

	if err := s.ack(ctx, w, turnID, seq); err != nil {
		return err
	}

	// Terminal outcome: the worker is done. First-terminal-writer: only the
	// first terminal outcome commits the turn SM; a duplicate (deduped above)
	// never re-terminalizes.
	if isOutcome {
		s.terminalizeTurnOutcome(ctx, a, te.GetWorkerOutcome())
	}
	return nil
}

// ack sends a cumulative EventAck through the worker's stream.
func (s *Service) ack(ctx context.Context, w *workerConn, turnID string, through uint64) error {
	return w.stream.Send(&v1.ControlMessage{Kind: &v1.ControlMessage_EventAck{EventAck: &v1.EventAck{
		TurnId: turnID, ThroughSequence: through,
	}}})
}

// appendAfterTerminal records a late worker observation as after-terminal audit
// (appended to the run's event log without mutating turn/assignment state). The
// turn must already exist; if it does not, the event is dropped (audit only).
func (s *Service) appendAfterTerminal(ctx context.Context, turnID string, te *v1.TurnEvent) error {
	kind, payload, _ := turnEventToRuntime(te)
	if kind == "" {
		return nil
	}
	// Resolve the run for the turn to append the audit event.
	var runID string
	err := s.store.pool.QueryRow(ctx, `SELECT run_id::text FROM runtime.turns WHERE id = $1::uuid`, turnID).Scan(&runID)
	if err != nil {
		return nil // turn gone; nothing to audit against.
	}
	_, _ = s.store.Runtime().AppendEvent(ctx, runID, turnID, "", kind, payload)
	s.log.Info("after-terminal audit", "turn", turnID, "kind", kind)
	return nil
}

// terminalizeTurnOutcome commits the turn SM terminal state from a worker
// WorkerOutcome (first-terminal-writer), then terminalizes the assignment and
// releases the worker (it will advertise Ready again when ready).
func (s *Service) terminalizeTurnOutcome(ctx context.Context, a Assignment, wo *v1.WorkerOutcome) {
	var reason string
	runAbort := false
	switch wo.GetOutcome() {
	case v1.Outcome_OUTCOME_COMPLETED:
		reason = "completed"
	case v1.Outcome_OUTCOME_FAILED:
		reason = "failed"
	case v1.Outcome_OUTCOME_ABORTED:
		reason = "aborted"
		runAbort = true
	default:
		reason = "failed"
	}
	s.commitTurnTerminal(ctx, a, reason, runAbort)
	if w := s.pool.get(a.PoolID, a.WorkerID); w != nil {
		w.releaseTurn()
	}
	s.kickReconciler()
}

// terminalizeTurnLoss terminalizes a turn as worker loss (aborted) — used on
// reconnect-fencing and stream-loss. CP is first-terminal-writer; a late worker
// outcome is after-terminal audit.
func (s *Service) terminalizeTurnLoss(ctx context.Context, a Assignment) {
	s.commitTurnTerminal(ctx, a, "aborted", true)
}

// commitTurnTerminal performs the turn/step/run terminal transitions (CAS) and
// terminalizes the assignment. Errors are logged; a CAS miss means the turn was
// already terminalized (first-terminal-writer held by another path) — the
// assignment is still terminalized idempotently.
func (s *Service) commitTurnTerminal(ctx context.Context, a Assignment, reason string, runAbort bool) {
	rt := s.store.Runtime()
	if _, err := rt.SettleTurn(ctx, a.TurnID, reason, 0); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
		s.log.Warn("settle turn", "turn", a.TurnID, "reason", reason, "error", err)
	}
	// Step terminal: succeed on completed, fail otherwise (aborted/failed).
	if reason == "completed" {
		s.succeedRunningStep(ctx, a.RunID)
	} else {
		s.failRunningStep(ctx, a.RunID)
	}
	// Run terminal.
	switch {
	case runAbort:
		if _, err := rt.AbortRun(ctx, a.RunID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
			s.log.Warn("abort run", "run", a.RunID, "error", err)
		}
	case reason == "completed":
		if _, err := rt.SucceedRun(ctx, a.RunID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
			s.log.Warn("succeed run", "run", a.RunID, "error", err)
		}
	default:
		if _, err := rt.FailRun(ctx, a.RunID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
			s.log.Warn("fail run", "run", a.RunID, "error", err)
		}
	}
	if err := s.store.TerminalizeAssignment(ctx, a.TurnID); err != nil {
		s.log.Warn("terminalize assignment", "turn", a.TurnID, "error", err)
	}
}

// failRunningStep/succeedRunningStep transition the run's currently-running step
// (FailStep/SucceedStep take a step id). Used by commitTurnTerminal.
func (s *Service) failRunningStep(ctx context.Context, runID string) {
	st, err := s.runningStep(ctx, runID)
	if err != nil {
		return
	}
	if _, err := s.store.Runtime().FailStep(ctx, st); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
		s.log.Warn("fail step", "step", st, "error", err)
	}
}

func (s *Service) succeedRunningStep(ctx context.Context, runID string) {
	st, err := s.runningStep(ctx, runID)
	if err != nil {
		return
	}
	if _, err := s.store.Runtime().SucceedStep(ctx, st); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
		s.log.Warn("succeed step", "step", st, "error", err)
	}
}

// runningStep returns the id of the run's currently-running step, if any.
func (s *Service) runningStep(ctx context.Context, runID string) (string, error) {
	steps, err := s.store.Runtime().ListSteps(ctx, runID)
	if err != nil {
		return "", err
	}
	for _, st := range steps {
		if st.State == runtime.StepRunning {
			return st.ID, nil
		}
	}
	return "", runtime.ErrNotFound
}

// handleWorkerLoss is invoked on stream close. If the worker held an active
// assignment that is still active (not already fenced/terminalized), fence it
// and terminalize the turn as worker loss (aborted). Idempotent.
func (s *Service) handleWorkerLoss(ctx context.Context, w *workerConn) {
	w.mu.Lock()
	turn := w.activeTurn
	w.mu.Unlock()
	if turn == "" {
		return
	}
	if a, err := s.store.FenceWorkerGeneration(ctx, w.poolID, w.workerID); err == nil {
		s.log.Info("worker loss: terminalize turn", "pool", w.poolID, "worker", w.workerID, "turn", a.TurnID)
		s.terminalizeTurnLoss(ctx, a)
	}
}

// --- cancellation ---

// CancelTurn propagates an AbortTurn to the worker serving the turn and
// terminalizes the turn as aborted (CP first-terminal-writer). Idempotent: a
// terminal turn is a no-op. The caller resolves the active assignment first.
func (s *Service) CancelTurn(ctx context.Context, turnID string, reason v1.AbortReason, msg string) error {
	a, err := s.store.ResolveActiveAssignment(ctx, turnID)
	if err != nil {
		return nil // not active: already terminal or never assigned.
	}
	// Propagate AbortTurn to the serving worker (best-effort).
	if w := s.pool.activeConn(turnID); w != nil {
		_ = w.stream.Send(&v1.ControlMessage{Kind: &v1.ControlMessage_AbortTurn{AbortTurn: &v1.AbortTurn{
			TurnId: turnID, Reason: reason, Message: msg,
		}}})
	}
	// CP first-terminal-writer: fence + terminalize as aborted. A late worker
	// outcome is after-terminal audit.
	if _, ferr := s.store.FenceWorkerGeneration(ctx, a.PoolID, a.WorkerID); ferr == nil {
		s.terminalizeTurnLoss(ctx, a)
	}
	return nil
}

// --- dispatch reconciler ---

// StartReconciler runs the dispatch loop: it polls runtime for pending/running
// runs and assigns turns to eligible idle workers. Run once per process before
// accepting traffic.
func (s *Service) StartReconciler(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.cfg.ReconcileInterval)
		defer ticker.Stop()
		for {
			s.reconcileOnce(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-s.reconcileCh:
			}
		}
	}()
}

// reconcileOnce drives one dispatch pass: start pending runs, then assign
// turns to idle workers.
func (s *Service) reconcileOnce(ctx context.Context) {
	// Start pending runs (pending -> running + start first step).
	pending, err := s.store.Runtime().ListRunsByState(ctx, runtime.RunPending)
	if err != nil {
		s.log.Warn("list pending runs", "error", err)
		return
	}
	for _, run := range pending {
		if _, err := s.store.Runtime().StartRun(ctx, run.ID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
			s.log.Warn("start run", "run", run.ID, "error", err)
			continue
		}
		if err := s.startNextStep(ctx, run.ID); err != nil {
			s.log.Warn("start step", "run", run.ID, "error", err)
		}
	}

	// Assign turns for running runs with a running step and no active turn.
	running, err := s.store.Runtime().ListRunsByState(ctx, runtime.RunRunning)
	if err != nil {
		s.log.Warn("list running runs", "error", err)
		return
	}
	for _, run := range running {
		s.dispatchRun(ctx, run)
	}
}

// startNextStep starts the oldest pending step of a run (pending -> running).
// If no pending step remains, the run is succeeded (chat = one agent_task step).
func (s *Service) startNextStep(ctx context.Context, runID string) error {
	st, err := s.store.Runtime().NextPendingStep(ctx, runID)
	if err != nil {
		if errors.Is(err, runtime.ErrNotFound) {
			if _, err := s.store.Runtime().SucceedRun(ctx, runID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
				return err
			}
			return nil
		}
		return err
	}
	if _, err := s.store.Runtime().StartStep(ctx, st.ID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
		return err
	}
	return nil
}

// dispatchRun ensures a running run has an active, assigned turn. If the run has
// a running step but no active turn, it starts one and assigns it to an idle
// worker in the run's pool. If no idle worker is available, the turn waits
// (retried on the next tick / a Ready).
func (s *Service) dispatchRun(ctx context.Context, run runtime.Run) {
	rt := s.store.Runtime()

	// Must have a running step.
	stID, err := s.runningStepID(ctx, run.ID)
	if err != nil {
		// No running step — try to start the next one.
		if err := s.startNextStep(ctx, run.ID); err != nil {
			return
		}
		stID, err = s.runningStepID(ctx, run.ID)
		if err != nil {
			return
		}
	}

	// Active turn?
	turn, err := rt.ActiveTurn(ctx, run.ID)
	if errors.Is(err, runtime.ErrNotFound) {
		// No active turn: start one. The model is the configured default for v1
		// (per-workflow model selection is HOR-252).
		model := ""
		if s.cfg.DefaultModel != nil {
			model = s.cfg.DefaultModel.Id
		}
		turn, err = rt.StartTurn(ctx, run.ID, stID, model)
		if err != nil {
			s.log.Warn("start turn", "run", run.ID, "error", err)
			return
		}
	} else if err != nil {
		s.log.Warn("active turn", "run", run.ID, "error", err)
		return
	}

	// Already assigned?
	if _, err := s.store.ResolveActiveAssignment(ctx, turn.ID); err == nil {
		return // assigned; nothing to do.
	}

	// Assign to an idle worker in the run's pool.
	poolID, err := s.store.PoolForRun(ctx, run.ID)
	if err != nil {
		s.log.Warn("run has no pool assignment", "run", run.ID, "error", err)
		return
	}
	w := s.pool.pickIdle(poolID, turn.ID)
	if w == nil {
		return // no idle worker; retry on next tick / Ready.
	}
	if err := s.assign(ctx, turn, run, poolID, w); err != nil {
		s.log.Warn("assign turn", "turn", turn.ID, "error", err)
		w.releaseTurn()
		s.kickReconciler()
	}
}

// runningStepID returns the id of the run's currently-running step, if any.
func (s *Service) runningStepID(ctx context.Context, runID string) (string, error) {
	steps, err := s.store.Runtime().ListSteps(ctx, runID)
	if err != nil {
		return "", err
	}
	for _, st := range steps {
		if st.State == runtime.StepRunning {
			return st.ID, nil
		}
	}
	return "", runtime.ErrNotFound
}

// assign records the active assignment and sends AssignTurn to the worker. The
// worker's credit is already consumed by pickIdle; on failure the caller must
// release it.
func (s *Service) assign(ctx context.Context, turn runtime.Turn, run runtime.Run, poolID string, w *workerConn) error {
	model := s.cfg.DefaultModel
	if model == nil {
		model = &v1.ModelConfig{Id: "", Api: "openai-completions"}
	}
	uid, gid := s.sessionUID(run.SessionID)
	msg := &v1.ControlMessage{Kind: &v1.ControlMessage_AssignTurn{AssignTurn: &v1.AssignTurn{
		TurnId:          turn.ID,
		SessionId:       run.SessionID,
		Sandbox:         &v1.SandboxRef{SandboxId: run.SessionID, Uid: uid, Gid: gid, WorkingDir: "workspace"},
		Model:           model,
		WorkspaceTools:  false,
		ScopeIdentityId: run.ScopeIdentityID,
		Message:         "", // the user/task message is sourced by the workflow/trigger (HOR-252/254); v1 dispatch carries the session only.
		RunId:           run.ID,
	}}}
	in := AssignmentInput{
		TurnID:              turn.ID,
		RunID:               run.ID,
		PoolID:              poolID,
		WorkerID:            w.workerID,
		FencingGeneration:   w.gen,
		AttemptID:           run.ID, // v1: attempt identity = run id (HOR-254 may add a first-class attempts table)
		ScopeIdentityID:     run.ScopeIdentityID,
		AgentPoolKey:        poolID, // pool UID; the CR "<ns>/<name>" is resolved by HOR-252
		ModelPermission:     mustJSON(model),
		CapabilityRequest:   []byte("[]"),
		ToolVersionSnapshot: []byte("[]"),
	}
	if _, err := s.store.CreateAssignment(ctx, in); err != nil {
		return err
	}
	if err := w.stream.Send(msg); err != nil {
		// Send failed: the assignment is recorded active but the worker never
		// received it. Fence + terminalize as worker loss (the conn is dead).
		_, _ = s.store.FenceWorkerGeneration(ctx, poolID, w.workerID)
		if a, ferr := s.store.ResolveActiveAssignment(ctx, turn.ID); ferr == nil {
			s.terminalizeTurnLoss(ctx, a)
		}
		return err
	}
	s.log.Info("assigned turn", "turn", turn.ID, "run", run.ID, "pool", poolID, "worker", w.workerID, "gen", w.gen)
	return nil
}

// sessionUID derives a stable per-session sandbox UID/GID within the configured
// range. Per-session isolation requires unique UIDs; v1 derives deterministically
// from the session id. Collision-free non-recycling is a HOR-245 hardening item.
func (s *Service) sessionUID(sessionID string) (uint32, uint32) {
	sum := sha256.Sum256([]byte(sessionID))
	n := binary.BigEndian.Uint32(sum[:4]) % s.cfg.SessionUIDRange
	uid := s.cfg.SessionUIDBase + n
	return uid, uid
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// turnEventToRuntime maps a harness TurnEvent to a runtime event kind + payload.
// Returns isOutcome=true for a terminal WorkerOutcome. An empty kind means the
// event has no durable runtime representation (ignored).
func turnEventToRuntime(te *v1.TurnEvent) (kind string, payload []byte, isOutcome bool) {
	switch k := te.Kind.(type) {
	case *v1.TurnEvent_ExecutionStarted:
		return runtime.EvTurnStarted, mustJSON(k.ExecutionStarted), false
	case *v1.TurnEvent_AssistantMessage:
		return runtime.EvAssistantMessage, mustJSON(k.AssistantMessage), false
	case *v1.TurnEvent_ToolResult:
		return runtime.EvToolResult, mustJSON(k.ToolResult), false
	case *v1.TurnEvent_ModelCallFailed:
		return runtime.EvError, mustJSON(k.ModelCallFailed), false
	case *v1.TurnEvent_HarnessError:
		return runtime.EvError, mustJSON(k.HarnessError), false
	case *v1.TurnEvent_WorkerOutcome:
		return runtime.EvSettled, mustJSON(k.WorkerOutcome), true
	}
	return "", nil, false
}
