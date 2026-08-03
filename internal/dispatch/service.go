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
		done:     make(chan struct{}),
		recvCh:   make(chan recvResult, 1),
	}
	s.pool.add(w)
	w.startReader()
	defer func() {
		w.markClosed()
		s.pool.remove(w)
		// Stream-loss cleanup must run with a detached, bounded context: the
		// request ctx is canceled by the time this defer runs, so fencing /
		// terminalization DB work would fail immediately and leave the
		// assignment active + gateway-authorizable (HOR-249).
		lossCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		s.handleWorkerLoss(lossCtx, w)
		cancel()
	}()

	if err := w.send(s.welcome(gen)); err != nil {
		return fmt.Errorf("welcome send: %w", err)
	}
	s.log.Info("worker connected", "pool", pool.ID, "worker", id.WorkerID, "gen", gen,
		"build", h.BuildVersion, "protocol", h.ProtocolVersion)

	// Ack any in-flight assignment the worker may be replaying after a
	// reconnect (cumulative ACK through the current watermark so the worker
	// clears its retained outbox before advertising Ready).
	s.ackReconnectWatermark(ctx, w, id.PoolUID, id.WorkerID)

	for {
		select {
		case <-w.done:
			return nil
		case r := <-w.recvCh:
			if r.err != nil {
				return nil // stream closed (client disconnect / ctx cancel / fenced)
			}
			msg := r.msg
			w.mu.Lock()
			w.lastSeen = time.Now()
			w.mu.Unlock()

			switch m := msg.Kind.(type) {
			case *v1.WorkerMessage_Ready:
				// One credit. Legal only when no turn is active; a Ready while busy
				// is a protocol violation (stream closed fail-closed). The busy
				// check + credit grant are one locked worker operation so the
				// reconciler cannot race an assignment between them.
				if !w.grantCreditIfIdle() {
					return connect.NewError(connect.CodeFailedPrecondition,
						errors.New("ready while a turn is active is a protocol violation (one-credit dispatch)"))
				}
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
// and fences its active assignment as worker loss. markClosed closes the old
// conn's done channel so its receive loop exits promptly (it does not wait for
// the old stream TCP to tear down); the old handler's deferred handleWorkerLoss
// is a no-op because (a) the assignment is already fenced here and (b)
// handleWorkerLoss uses a generation-qualified CAS that cannot fence the
// replacement generation's assignment.
func (s *Service) fenceOldGeneration(ctx context.Context, poolID, workerID string) {
	if old := s.pool.get(poolID, workerID); old != nil {
		old.markClosed()
	}
	// Fence the active assignment (if any) bound to this worker. At this point
	// the new connection has not created an assignment yet, so the only active
	// assignment is the prior generation's. The turn is terminalized as worker
	// loss (aborted); a late WorkerOutcome from the old generation is
	// after-terminal audit only.
	if a, err := s.store.FenceWorkerGeneration(ctx, poolID, workerID); err == nil {
		s.log.Info("fenced prior generation on reconnect", "pool", poolID, "worker", workerID, "turn", a.TurnID)
		s.terminalizeTurnLoss(ctx, a)
	}
}

// ackReconnectWatermark sends a cumulative EventAck for the worker's prior
// assignment (now fenced by fenceOldGeneration) through its committed
// highest_applied_sequence, so a reconnected worker clears its retained outbox
// and may advertise Ready only after the cumulative ACK (HOR-381 EventAck
// contract). The prior turn was terminalized as worker loss on reconnect; its
// replayed tail is after-terminal audit, but the worker still needs the
// cumulative ACK to drop the retained events. Best-effort: a missing prior
// assignment (clean connect) is a no-op.
func (s *Service) ackReconnectWatermark(ctx context.Context, w *workerConn, poolID, workerID string) {
	a, err := s.store.PriorAssignmentForWorker(ctx, poolID, workerID)
	if err != nil {
		return // no prior assignment; clean connect.
	}
	if err := w.send(&v1.ControlMessage{Kind: &v1.ControlMessage_EventAck{EventAck: &v1.EventAck{
		TurnId: a.TurnID, ThroughSequence: a.HighestAppliedSequence,
	}}}); err != nil {
		s.log.Warn("reconnect watermark ack send", "turn", a.TurnID, "error", err)
	}
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
			_ = s.appendAfterTerminal(ctx, turnID, te)
		}
		// The worker may be replaying its retained outbox after reconnect for a
		// turn the CP has already terminalized. ACK through this event's
		// sequence so the worker clears its outbox and may advertise Ready
		// (HOR-381 cumulative ACK); the replayed events are after-terminal audit,
		// not redelivered work.
		return s.ack(ctx, w, turnID, seq)
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

// ack sends a cumulative EventAck through the worker's stream (serialized).
func (s *Service) ack(ctx context.Context, w *workerConn, turnID string, through uint64) error {
	return w.send(&v1.ControlMessage{Kind: &v1.ControlMessage_EventAck{EventAck: &v1.EventAck{
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
	// Step terminal: succeed on completed, fail otherwise (aborted/failed).
	if reason == "completed" {
		s.succeedRunningStep(ctx, a.RunID)
	} else {
		s.failRunningStep(ctx, a.RunID)
	}
	// Run terminal / advance to the next step.
	switch {
	case runAbort:
		if _, err := rt.AbortRun(ctx, a.RunID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
			s.log.Warn("abort run", "run", a.RunID, "error", err)
		}
	case reason == "completed":
		// Advance to the next pending step; SucceedRun only when none remain.
		// This drives multi-step runs instead of short-circuiting after the
		// first turn (HOR-249 durable workflow/turn dispatch).
		if err := s.startNextStep(ctx, a.RunID); err != nil && !errors.Is(err, runtime.ErrInvalidTransition) {
			s.log.Warn("advance run after turn", "run", a.RunID, "error", err)
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
// assignment that is still active AND still owned by THIS connection's
// generation, fence it (generation-qualified CAS) and terminalize the turn as
// worker loss (aborted). The generation CAS is the fencing safety fence
// (HOR-249): a prior-generation handler tearing down late must not fence a
// newer-generation assignment owned by the replacement connection. Idempotent.
func (s *Service) handleWorkerLoss(ctx context.Context, w *workerConn) {
	w.mu.Lock()
	turn := w.activeTurn
	w.mu.Unlock()
	if turn == "" {
		return
	}
	if a, err := s.store.FenceWorkerGenerationIf(ctx, w.poolID, w.workerID, w.gen); err == nil {
		s.log.Info("worker loss: terminalize turn", "pool", w.poolID, "worker", w.workerID, "turn", a.TurnID, "gen", w.gen)
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
	// Propagate AbortTurn to the serving worker (best-effort, serialized).
	if w := s.pool.activeConn(turnID); w != nil {
		_ = w.send(&v1.ControlMessage{Kind: &v1.ControlMessage_AbortTurn{AbortTurn: &v1.AbortTurn{
			TurnId: turnID, Reason: reason, Message: msg,
		}}})
	}
	// CP first-terminal-writer: fence (generation-qualified CAS) + terminalize
	// as aborted. A late worker outcome is after-terminal audit.
	if _, ferr := s.store.FenceWorkerGenerationIf(ctx, a.PoolID, a.WorkerID, a.FencingGeneration); ferr == nil {
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

// StartLeaseMonitor enforces worker lease expiry (HOR-249 retains
// heartbeats/leases): a connected worker whose lastSeen is older than
// LeaseInterval is deemed lost, fenced with a generation-qualified CAS (so only
// this conn's own generation is fenced) and terminalized as lease-expired
// (aborted). Run once per process alongside the reconciler.
func (s *Service) StartLeaseMonitor(ctx context.Context) {
	interval := s.cfg.LeaseInterval / 2
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.expireLeases(ctx)
			}
		}
	}()
}

// expireLeases fences + terminalizes workers whose lease has expired.
func (s *Service) expireLeases(ctx context.Context) {
	deadline := time.Now().Add(-s.cfg.LeaseInterval)
	for _, w := range s.pool.all() {
		last := w.lastSeenAt()
		if last.After(deadline) {
			continue
		}
		w.mu.Lock()
		turn := w.activeTurn
		w.mu.Unlock()
		if turn == "" {
			continue // idle workers are not terminalized; the conn is simply stale.
		}
		if a, err := s.store.FenceWorkerGenerationIf(ctx, w.poolID, w.workerID, w.gen); err == nil {
			s.log.Info("lease expired: terminalize turn", "pool", w.poolID, "worker", w.workerID,
				"turn", a.TurnID, "gen", w.gen, "last_seen", last)
			// Lease expiry is worker loss; terminalize as aborted. The
			// ABORT_REASON_LEASE_EXPIRED classification is recorded in the log;
			// the runtime turn SM has no lease_expired state.
			s.terminalizeTurnLoss(ctx, a)
			w.markClosed()
		}
	}
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
		// Durable guard against the terminalization race (REQ-009/SCN-008): a
		// step whose turn has just settled but whose step/run succession has
		// not yet committed would otherwise be handed a duplicate turn. v1 is
		// one turn per step with no auto-redelivery, so a step that already has
		// any turn waits for succession instead of starting another.
		if has, gerr := rt.HasTurnForStep(ctx, stID); gerr == nil && has {
			return
		}
		// No active turn: start one. The model is the configured default for v1
		// (per-workflow model selection is HOR-252). Dispatch MUST NOT emit an
		// empty model permission (HOR-249 active-assignment context): if no
		// default is configured, the run waits rather than dispatching an
		// unexecutable assignment.
		if s.cfg.DefaultModel == nil || s.cfg.DefaultModel.Id == "" {
			s.log.Warn("no default model configured; run waits", "run", run.ID)
			return
		}
		turn, err = rt.StartTurn(ctx, run.ID, stID, s.cfg.DefaultModel.Id)
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
	if model == nil || model.Id == "" {
		// Dispatch must not emit an empty model permission (HOR-249). The
		// reconciler gates on this before starting a turn; guard again here.
		return errors.New("no default model configured; cannot assign turn")
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
	if err := w.send(msg); err != nil {
		// Send failed: the assignment is recorded active but the worker never
		// received it. Fence (generation-qualified CAS) + terminalize as worker
		// loss using the assignment returned by the fence (the row is no longer
		// active after fencing, so ResolveActiveAssignment would miss it).
		if a, ferr := s.store.FenceWorkerGenerationIf(ctx, poolID, w.workerID, w.gen); ferr == nil {
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
	case *v1.TurnEvent_ModelCallStarted:
		return runtime.EvModelCallStarted, mustJSON(k.ModelCallStarted), false
	case *v1.TurnEvent_AssistantMessage:
		return runtime.EvAssistantMessage, mustJSON(k.AssistantMessage), false
	case *v1.TurnEvent_ModelCallFailed:
		return runtime.EvModelCallFailed, mustJSON(k.ModelCallFailed), false
	case *v1.TurnEvent_ModelRetryScheduled:
		return runtime.EvModelRetryScheduled, mustJSON(k.ModelRetryScheduled), false
	case *v1.TurnEvent_ModelRetryFinished:
		return runtime.EvModelRetryFinished, mustJSON(k.ModelRetryFinished), false
	case *v1.TurnEvent_ToolCallStarted:
		// The ambiguous side-effect boundary (HOR-381/ARCH-014): durable.
		return runtime.EvToolCallStarted, mustJSON(k.ToolCallStarted), false
	case *v1.TurnEvent_ToolResult:
		return runtime.EvToolResult, mustJSON(k.ToolResult), false
	case *v1.TurnEvent_CompactionStarted:
		return runtime.EvCompactionStarted, mustJSON(k.CompactionStarted), false
	case *v1.TurnEvent_CompactionFinished:
		return runtime.EvCompactionFinished, mustJSON(k.CompactionFinished), false
	case *v1.TurnEvent_HarnessError:
		return runtime.EvError, mustJSON(k.HarnessError), false
	case *v1.TurnEvent_WorkerOutcome:
		return runtime.EvSettled, mustJSON(k.WorkerOutcome), true
	}
	return "", nil, false
}
