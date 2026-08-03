package dispatch

import (
	"context"
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

// errConnClosed is returned by workerConn.send after the conn has been marked
// closed (fenced/lost). Callers treat it as a failed send (stream-lost).
var errConnClosed = errors.New("dispatch: worker conn closed")

// Config configures the dispatch Work server + reconciler.
type Config struct {
	TrustDomain       string        // SPIFFE trust domain (default iterabase.local)
	HeartbeatInterval time.Duration // advertised to workers in Welcome
	LeaseInterval     time.Duration // worker availability freshness; any message renews the lease
	ReconcileInterval time.Duration // dispatch poll interval
	ProtocolVersion   string        // advertised in Welcome

	// SessionUIDBase/Range + SessionUIDGrace derive a stable, collision-free,
	// non-recycling per-session sandbox UID for the AssignTurn SandboxRef
	// (HOR-245 reuse-safety floor). The durable allocator (runtime.session_uid
	// _allocations) assigns a unique UID per session and never recycles one
	// within the reap grace after release. Defaults: Base 10000, Range 50000
	// ([10000,60000)), Grace 5m (must exceed max sandbox reap latency).
	SessionUIDBase  uint32
	SessionUIDRange uint32
	SessionUIDGrace time.Duration

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
		c.SessionUIDRange = 50000
	}
	if c.SessionUIDGrace == 0 {
		c.SessionUIDGrace = 5 * time.Minute
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

	w := &workerConn{
		poolID:   pool.ID,
		workerID: id.WorkerID,
		gen:      gen,
		stream:   st,
		lastSeen: time.Now(),
		done:     make(chan struct{}),
		recvCh:   make(chan recvResult, 1),
	}
	// Atomic replacement: add registers the new conn and returns the prior conn
	// for the same (pool, worker) in one locked step, so there is no window in
	// which two conns are both selectable. The prior conn is fenced below using
	// a generation-qualified CAS (HOR-249).
	old := s.pool.add(w)
	w.startReader()
	defer func() {
		w.markClosed()
		// Drain in-flight sends so no stream.Send is in progress when this
		// handler returns and Connect closes the HTTP/2 response writer. send()
		// refuses to enter stream.Send once closed is set (markClosed just set
		// it); waiting on sendWG lets any in-flight send finish. Same class as
		// the gateway runner-pool teardown race.
		w.sendWG.Wait()
		s.pool.remove(w)
		// Stream-loss cleanup must run with a detached, bounded context: the
		// request ctx is canceled by the time this defer runs, so fencing /
		// terminalization DB work would fail immediately and leave the
		// assignment active + gateway-authorizable (HOR-249).
		lossCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		s.handleWorkerLoss(lossCtx, w)
		cancel()
	}()

	// Fence the prior generation (if any) for this (pool, worker) BEFORE sending
	// Welcome, so the prior turn is fully terminalized before the new generation
	// advertises readiness and a reconnecting worker clears its outbox. Atomic
	// replacement (pool.add returned the prior conn) + a generation-qualified CAS
	// ensure only the prior gen's assignment is fenced, never a replacement's.
	// The prior handler's deferred handleWorkerLoss is also generation-qualified
	// (old.gen) and is a no-op once this fence succeeds. A detached bounded
	// context is used so prior-gen loss cleanup completes even if this new
	// connection's request ctx is canceled.
	if err := s.fenceOldGeneration(ctx, old, pool.ID, id.WorkerID); err != nil {
		// A durable fence / loss-terminalization failure (transient DB error)
		// must NOT advertise the new generation: HOR-249 requires reconnect
		// fencing to prevent old-generation mutation, and the approved HOR-381
		// contract says a newly accepted connection fences any old active
		// assignment before becoming dispatchable. Fail the connection so the
		// worker retries rather than proceeding with a still-active prior turn.
		return fmt.Errorf("fence prior generation: %w", err)
	}

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
// and fences its active assignment as worker loss. The fence+terminalize runs
// BEFORE markClosed and synchronously, so it wins ahead of the old handler's
// deferred handleWorkerLoss (which is then a no-op: its generation-qualified
// CAS finds no active assignment). This avoids a race where the old handler's
// async loss cleanup terminalizes after this handler has already advertised
// the new generation's readiness.
//
// The fence is UNCONDITIONAL — it fences ANY active assignment for (pool,
// worker) — because at connect time the new assignment does not exist yet, so
// any active assignment is necessarily a prior generation's. This closes the
// post-restart race where the in-memory `old.gen` (reset to 0 by the restart)
// does not match a durable prior assignment's generation: a generation-
// qualified CAS would miss it, leaving the durable turn active while a
// simultaneous reconnect advertises its new generation (HOR-249 reconnect
// fencing; HOR-381: a newly accepted connection fences any old active
// assignment). The unconditional UPDATE...RETURNING is atomic in Postgres, so
// simultaneous reconnects serialize: one fences (wins), the other gets
// ErrNotFound (clean).
//
// A detached, bounded context is used so prior-generation loss cleanup
// completes even if the new connection's request ctx is canceled, mirroring
// the stream-loss cleanup path. ErrNotFound (no active assignment) is a clean
// connect. Any other store error — OR a terminalization failure — is returned
// so the caller does NOT advertise the new generation until durable fence +
// loss terminalization succeeds (HOR-249).
func (s *Service) fenceOldGeneration(ctx context.Context, old *workerConn, poolID, workerID string) error {
	_ = ctx
	lossCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Fence ANY active assignment for (pool, worker) before advertising the new
	// generation. See the doc comment for why this is unconditional (not a
	// generation-qualified CAS): a CAS keyed on the in-memory old.gen misses a
	// durable prior-generation assignment after a CP restart.
	a, err := s.store.FenceWorkerGeneration(lossCtx, poolID, workerID)
	if err == nil {
		s.log.Info("fenced prior generation on reconnect", "pool", poolID, "worker", workerID, "turn", a.TurnID, "gen", a.FencingGeneration)
		if tErr := s.terminalizeTurnLoss(lossCtx, a); tErr != nil {
			// The assignment is fenced (no longer active/gateway-authorizable), but
			// the turn SM was not settled. Do NOT advertise the new generation: the
			// worker retries and loss terminalization is re-attempted (HOR-249).
			if old != nil {
				old.markClosed()
			}
			return fmt.Errorf("terminalize fenced prior generation: %w", tErr)
		}
		if old != nil {
			old.markClosed()
		}
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		if old != nil {
			old.markClosed()
		}
		return nil // clean connect; no active prior assignment.
	}
	if old != nil {
		old.markClosed()
	}
	return fmt.Errorf("fence prior generation: %w", err)
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
		// Distinguish a genuinely inactive assignment from a transient store
		// failure. Only ErrAssignmentNotActive (turn already terminalized or
		// never assigned) falls through to after-terminal audit; a store error
		// must NOT be ACKed away — HOR-381 requires cumulative EventAck only
		// after Postgres commit, and ACKing an un-persisted event would let an
		// ACK lost after a failed append duplicate the event on the next replay.
		if !errors.Is(err, ErrAssignmentNotActive) {
			return fmt.Errorf("resolve active assignment: %w", err)
		}
		// No active assignment: the turn was already terminalized (CP
		// first-terminal-writer) or never assigned. HOR-381 makes every durable
		// observation (ModelCallStarted, ToolCallStarted, Compaction*, WorkerOutcome,
		// ...) audit-history that the CP must never silently drop, so persist the
		// late/replayed event as after-terminal audit before ACKing. The worker
		// may be replaying its retained outbox after reconnect for a turn the CP
		// has already terminalized; ACK through this event's sequence so it
		// clears its outbox and may advertise Ready (HOR-381 cumulative ACK).
		// The replayed events are after-terminal audit, not redelivered work.
		if err := s.appendAfterTerminal(ctx, turnID, te, w.poolID, w.workerID); err != nil {
			return fmt.Errorf("after-terminal audit: %w", err)
		}
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
			// Terminalized concurrently between Resolve and Append: the event was
			// not committed. Persist it as after-terminal audit (HOR-381 durable
			// observations are never dropped) and ACK so the worker clears its
			// outbox. The after-terminal append is itself a durable, dedup-by-
			// (turn, sequence) commit; ACK only after it succeeds.
			if err := s.appendAfterTerminal(ctx, turnID, te, w.poolID, w.workerID); err != nil {
				return fmt.Errorf("after-terminal audit: %w", err)
			}
			return s.ack(ctx, w, turnID, seq)
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
// in one durable transaction: it dedups by (turn, sequence) against the
// assignment's committed watermark, appends the runtime audit event, and
// advances the watermark — all before the caller ACKs (HOR-381: cumulative
// EventAck only after Postgres commit). An ACK lost after this commit is safe:
// the next replay sees the advanced watermark and dedups. Unknown/ignored event
// kinds (no durable representation) are a no-op. A turn with no assignment row
// at all (gone/never assigned) is a no-op: there is nothing to audit against,
// and the caller ACKs through the sequence so the worker clears its outbox.
// Any other store error is returned so the caller does NOT ACK.
func (s *Service) appendAfterTerminal(ctx context.Context, turnID string, te *v1.TurnEvent, poolID, workerID string) error {
	kind, payload, _ := turnEventToRuntime(te)
	if kind == "" {
		return nil // unknown/ignored kind: nothing durable to record.
	}
	if _, err := s.store.AppendAfterTerminalEvent(ctx, turnID, te.GetSequence(), kind, payload, poolID, workerID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil // turn gone; nothing to audit against.
		}
		return err
	}
	s.log.Info("after-terminal audit", "turn", turnID, "kind", kind, "seq", te.GetSequence())
	return nil
}

// terminalizeTurnOutcome commits the turn SM terminal state from a worker
// WorkerOutcome (first-terminal-writer), then terminalizes the assignment and
// releases the worker (it will advertise Ready again when ready). When the run
// reaches a terminal state and the worker is still connected, dispatch sends
// SessionEnd so the supervisor reaps the session sandbox (HOR-245 cleanup), and
// releases the session UID into its non-recycling reap grace.
func (s *Service) terminalizeTurnOutcome(ctx context.Context, a Assignment, wo *v1.WorkerOutcome) {
	var reason string
	switch wo.GetOutcome() {
	case v1.Outcome_OUTCOME_COMPLETED:
		reason = "completed"
	case v1.Outcome_OUTCOME_FAILED:
		reason = "failed"
	case v1.Outcome_OUTCOME_ABORTED:
		reason = "aborted"
	default:
		reason = "failed"
	}
	runState, tErr := s.commitTurnTerminal(ctx, a, reason)
	if tErr != nil {
		s.log.Warn("terminalize turn outcome", "turn", a.TurnID, "error", tErr)
	}
	if w := s.pool.get(a.PoolID, a.WorkerID); w != nil {
		w.releaseTurn()
		if runtime.IsTerminalRun(runState) {
			s.sendSessionEnd(ctx, w, a)
		}
	}
	s.kickReconciler()
}

// terminalizeTurnLoss terminalizes a turn as worker loss (aborted) — used on
// reconnect-fencing and stream-loss. CP is first-terminal-writer; a late worker
// outcome is after-terminal audit. The worker is gone, so SessionEnd is not
// sent (v1 leak-and-reconcile); the session UID stays in-use (non-recyclable)
// until a later reaper reaps the leaked sandbox.
func (s *Service) terminalizeTurnLoss(ctx context.Context, a Assignment) error {
	_, err := s.commitTurnTerminal(ctx, a, "aborted")
	return err
}

// commitTurnTerminal settles the turn + advances the step/run ATOMICALLY
// (runtime.SettleTurnAndAdvance, one tx) and terminalizes the assignment
// (idempotent, separate tx — the benign gap between run-terminal and
// assignment-terminal cannot produce a duplicate turn because the run is no
// longer dispatchable once terminal). A turn CAS miss (ErrInvalidTransition)
// means the turn was already terminalized (first-terminal-writer held by
// another path) and is not an error. Returns the run's resulting state so the
// caller can sequence session-end, and an error if durable loss terminalization
// did not succeed — the reconnect path MUST NOT advertise the new generation
// until it does (HOR-249).
func (s *Service) commitTurnTerminal(ctx context.Context, a Assignment, reason string) (runState string, err error) {
	rs, stErr := s.store.Runtime().SettleTurnAndAdvance(ctx, a.TurnID, reason)
	if stErr != nil && !errors.Is(stErr, runtime.ErrInvalidTransition) {
		// A real settle/advance failure leaves the turn SM non-terminal; surface
		// it so loss terminalization is not silently swallowed.
		return "", fmt.Errorf("settle+advance turn %s (%s): %w", a.TurnID, reason, stErr)
	}
	if stErr == nil {
		runState = rs
	}
	if tErr := s.store.TerminalizeAssignment(ctx, a.TurnID); tErr != nil {
		return runState, fmt.Errorf("terminalize assignment %s: %w", a.TurnID, tErr)
	}
	return runState, nil
}

// sendSessionEnd sends SessionEnd {sandbox_id, uid, gid} to the worker serving
// a just-terminated run so the supervisor reaps the session sandbox (HOR-245
// cleanup owner/protocol). The session UID is released into its non-recycling
// reap grace ONLY when the SessionEnd was actually sent: a send failure means
// the worker never received the reap instruction, so releasing would start the
// grace clock against a reap that will not happen — the UID stays in_use and
// the sandbox is left for leak-and-reconcile (v1 leak-and-reconcile posture).
// v1 carries no reap-ack on the wire (founder-approved non-goal); the bounded
// grace exceeding max reap latency is the non-recycling safety floor, and
// reuse is ownership-fenced fail-closed (a stale root owned by a prior session
// is refused, never re-adopted).
func (s *Service) sendSessionEnd(ctx context.Context, w *workerConn, a Assignment) {
	sessionID, err := s.store.SessionIDForRun(ctx, a.RunID)
	if err != nil {
		s.log.Warn("session-end: resolve session", "run", a.RunID, "error", err)
		return
	}
	uid, err := s.store.SessionUID(ctx, sessionID)
	if err != nil {
		s.log.Warn("session-end: resolve uid", "session", sessionID, "error", err)
		return
	}
	if err := w.send(&v1.ControlMessage{Kind: &v1.ControlMessage_SessionEnd{SessionEnd: &v1.SessionEnd{
		SandboxId: sessionID, Uid: uid, Gid: uid,
	}}}); err != nil {
		// Send failed: the worker will not reap. Keep the UID in_use
		// (non-recyclable) so it cannot be handed to a new session while this
		// sandbox lingers; a later reaper reconciles.
		s.log.Warn("session-end send; uid kept in_use for leak-and-reconcile", "session", sessionID, "error", err)
		return
	}
	if err := s.store.ReleaseSessionUID(ctx, sessionID); err != nil {
		s.log.Warn("release session uid", "session", sessionID, "error", err)
	}
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
		if tErr := s.terminalizeTurnLoss(ctx, a); tErr != nil {
			s.log.Warn("worker loss: terminalize turn", "turn", a.TurnID, "error", tErr)
		}
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
		if tErr := s.terminalizeTurnLoss(ctx, a); tErr != nil {
			s.log.Warn("cancel: terminalize turn", "turn", a.TurnID, "error", tErr)
		}
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
			if tErr := s.terminalizeTurnLoss(ctx, a); tErr != nil {
				s.log.Warn("lease expired: terminalize turn", "turn", a.TurnID, "error", tErr)
			}
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
	uid, err := s.store.AllocateSessionUID(ctx, run.SessionID, s.cfg.SessionUIDBase, s.cfg.SessionUIDRange, s.cfg.SessionUIDGrace)
	if err != nil {
		return fmt.Errorf("allocate session uid: %w", err)
	}
	gid := uid
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
			if tErr := s.terminalizeTurnLoss(ctx, a); tErr != nil {
				s.log.Warn("assign send failed: terminalize turn", "turn", a.TurnID, "error", tErr)
			}
		}
		return err
	}
	s.log.Info("assigned turn", "turn", turn.ID, "run", run.ID, "pool", poolID, "worker", w.workerID, "gen", w.gen)
	return nil
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
