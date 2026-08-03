package dispatch_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/nunocgoncalves/control-plane/internal/dispatch"
	v1 "github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1"
	"github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1/harnessv1connect"
	"github.com/nunocgoncalves/control-plane/internal/runtime"
	"github.com/nunocgoncalves/control-plane/internal/spiffe/testca"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

const (
	dispatchTD       = "iterabase.local"
	dispatchSupvSPI  = "spiffe://iterabase.local/pools/pool-1/workers/worker-1"
	dispatchPoolPref = "spiffe://iterabase.local/pools/pool-1/"
)

var dispatchID atomic.Uint64

func dispatchSID() string { return fmt.Sprintf("d%d", dispatchID.Add(1)) }

// dispatchEnv assembles a hermetic dispatch server: Postgres + a seeded pool +
// mTLS HTTP/2 server with the Harness handler + the supervisor cert material.
type dispatchEnv struct {
	store      *dispatch.Store
	rt         *runtime.Store
	pgpool     *pgxpool.Pool
	svc        *dispatch.Service
	srvURL     string
	supervisor tls.Certificate // spiffe://td/pools/pool-1/workers/worker-1
	caPool     *x509.CertPool
	poolID     string
	stop       func()
}

func newDispatchEnv(t *testing.T) *dispatchEnv {
	t.Helper()
	pgpool := testutil.NewPostgresPool(t)
	rt := runtime.NewStore(pgpool)
	store := dispatch.NewStore(pgpool, rt)

	ctx := context.Background()
	var poolID string
	require.NoError(t, pgpool.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ('ns/pool-1', 'pool-1', $1) RETURNING id::text`, dispatchPoolPref).Scan(&poolID))

	ca, err := testca.New()
	require.NoError(t, err)
	supervisor, err := ca.Leaf(testca.LeafOpts{SPIFFEID: dispatchSupvSPI})
	require.NoError(t, err)

	srv := startDispatchServer(t, store, dispatchTD, ca, supervisor)
	return &dispatchEnv{
		store: store, rt: rt, pgpool: pgpool, svc: srv.svc,
		srvURL: srv.srvURL, supervisor: supervisor, caPool: ca.Pool, poolID: poolID, stop: srv.stop,
	}
}

// dispatchServer is a running dispatch HTTP/2 server + its service. startDispatchServer
// is extracted from newDispatchEnv so a test can simulate a control-plane restart:
// build a second server with a fresh in-memory workerPool backed by the SAME
// durable store (same Postgres / pool row / CA), so a reconnecting worker hits
// the `old == nil` path with a durable active assignment still present.
type dispatchServer struct {
	svc    *dispatch.Service
	srvURL string
	stop   func()
}

// startDispatchServer builds a dispatch service + mTLS HTTP/2 server backed by
// the given store, with the reconciler + lease monitor running, and the given
// supervisor cert material (issued by ca). The caller seeds the pool row and
// CA before calling.
func startDispatchServer(t *testing.T, store *dispatch.Store, td string, ca *testca.CA, supervisor tls.Certificate) *dispatchServer {
	t.Helper()
	serverCert, err := ca.Leaf(testca.LeafOpts{SPIFFEID: "spiffe://" + td + "/control-plane/dispatch", DNSNames: []string{"localhost", "127.0.0.1"}, IsServer: true})
	require.NoError(t, err)

	svc := dispatch.NewService(store, dispatch.Config{
		TrustDomain:       td,
		ReconcileInterval: 30 * time.Millisecond,
		DefaultModel:      &v1.ModelConfig{Id: "gpt-4o", Api: "openai-completions"},
	}, nil)
	recCtx, cancelRec := context.WithCancel(context.Background())
	svc.StartReconciler(recCtx)
	svc.StartLeaseMonitor(recCtx)

	mux := http.NewServeMux()
	idmw := dispatch.IdentityMiddleware(td)
	path, handler := harnessv1connect.NewHarnessHandler(svc)
	mux.Handle(path, idmw(handler))

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.Pool,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	httpSrv := &http.Server{Handler: mux, TLSConfig: tlsCfg, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = httpSrv.ServeTLS(ln, "", "") }()
	stop := func() { cancelRec(); _ = httpSrv.Shutdown(context.Background()) }
	t.Cleanup(stop)

	return &dispatchServer{svc: svc, srvURL: fmt.Sprintf("https://localhost:%d", port), stop: stop}
}

func (e *dispatchEnv) mTLSClient(cert tls.Certificate) *http.Client {
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: e.caPool, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}}
	return &http.Client{Transport: &http2.Transport{TLSClientConfig: tlsCfg}}
}

// seedPendingRun creates a chat run (state=pending) + run->pool assignment. The
// dispatch reconciler drives it pending->running and assigns a turn.
func (e *dispatchEnv) seedPendingRun(t *testing.T) (runID string) {
	t.Helper()
	ctx := context.Background()
	sid := dispatchSID()
	identID := insertIdentity(t, e.pgpool, "ident-"+sid)
	run, err := e.rt.CreateRun(ctx, runtime.CreateRunInput{
		Kind: runtime.KindChat, ScopeIdentityID: identID,
		SessionID: sid, SessionDir: "/sessions/" + sid,
		Steps: []runtime.StepInput{{Seq: 1, Kind: runtime.StepKindAgentTask, Config: json.RawMessage(`{"prompt":"hi"}`)}},
	})
	require.NoError(t, err)
	require.NoError(t, e.store.AssignRunToPool(ctx, run.ID, e.poolID))
	return run.ID
}

// fakeWorker is a Go Work-stream client driving Hello/Ready/TurnEvent.
type fakeWorker struct {
	stream *connect.BidiStreamForClient[v1.WorkerMessage, v1.ControlMessage]
	cancel context.CancelFunc
}

func (e *dispatchEnv) connectWorker(t *testing.T) *fakeWorker {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := harnessv1connect.NewHarnessClient(e.mTLSClient(e.supervisor), e.srvURL, connect.WithGRPC())
	stream := client.Work(ctx)
	// Hello.
	require.NoError(t, stream.Send(&v1.WorkerMessage{Kind: &v1.WorkerMessage_Hello{Hello: &v1.Hello{
		WorkerId: "worker-1", PoolId: "pool-1", BuildVersion: "test", ProtocolVersion: "1",
	}}}))
	w := &fakeWorker{stream: stream, cancel: cancel}
	// Await Welcome.
	require.Eventually(t, func() bool {
		ctrl, err := stream.Receive()
		if err != nil {
			return false
		}
		return ctrl.GetWelcome() != nil
	}, 2*time.Second, 5*time.Millisecond, "welcome not received")
	return w
}

func (w *fakeWorker) ready() error {
	return w.stream.Send(&v1.WorkerMessage{Kind: &v1.WorkerMessage_Ready{Ready: &v1.Ready{}}})
}

func (w *fakeWorker) sendEvent(te *v1.TurnEvent) error {
	return w.stream.Send(&v1.WorkerMessage{Kind: &v1.WorkerMessage_TurnEvent{TurnEvent: te}})
}

// recvControl receives a control message within a timeout, skipping Welcome.
func (w *fakeWorker) recvControl(t *testing.T, want func(*v1.ControlMessage) bool) *v1.ControlMessage {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ctrl, err := w.stream.Receive()
		if err != nil {
			require.Fail(t, "receive control", err)
			return nil
		}
		if want(ctrl) {
			return ctrl
		}
	}
	require.Fail(t, "timed out waiting for control message")
	return nil
}

func (w *fakeWorker) close() {
	w.cancel()
	_ = w.stream.CloseRequest()
}

// TestDispatch_AssignEventAckOutcome: a pending run is dispatched to a ready
// worker; durable events are ACKed cumulatively; a terminal WorkerOutcome
// terminalizes the turn/run/assignment (first-terminal-writer).
func TestDispatch_AssignEventAckOutcome(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()
	runID := env.seedPendingRun(t)

	w := env.connectWorker(t)
	defer w.close()
	require.NoError(t, w.ready())

	// Await AssignTurn.
	at := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	turnID := at.GetTurnId()
	require.Equal(t, runID, at.GetRunId())
	assert.NotEmpty(t, at.GetSessionId())

	// assistant_message (seq 1) -> ACK through 1.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 1,
		Kind: &v1.TurnEvent_AssistantMessage{AssistantMessage: &v1.AssistantMessage{Text: "hello"}}}))
	ack1 := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil }).GetEventAck()
	assert.Equal(t, uint64(1), ack1.GetThroughSequence())
	assert.Equal(t, turnID, ack1.GetTurnId())

	// WorkerOutcome COMPLETED (seq 2) -> ACK through 2 + terminalization.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 2,
		Kind: &v1.TurnEvent_WorkerOutcome{WorkerOutcome: &v1.WorkerOutcome{Outcome: v1.Outcome_OUTCOME_COMPLETED}}}))
	ack2 := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil }).GetEventAck()
	assert.Equal(t, uint64(2), ack2.GetThroughSequence())

	// Verify durable state: turn succeeded, run succeeded, assignment terminal.
	require.Eventually(t, func() bool {
		_, err := env.rt.ActiveTurn(ctx, runID)
		if err == nil {
			return false // active turn gone after terminalization
		}
		return true
	}, 2*time.Second, 20*time.Millisecond, "turn not terminalized")
	run, err := env.rt.GetRun(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, runtime.RunSucceeded, run.State)
	// Assignment is terminal (not active).
	_, err = env.store.ResolveActiveAssignment(ctx, turnID)
	assert.ErrorIs(t, err, dispatch.ErrAssignmentNotActive)
}

// TestDispatch_EventDedupOnReplay: a replayed (already-applied) event is a
// no-op but still ACKed through the watermark.
func TestDispatch_EventDedupOnReplay(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()
	runID := env.seedPendingRun(t)

	w := env.connectWorker(t)
	defer w.close()
	require.NoError(t, w.ready())
	at := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	turnID := at.GetTurnId()

	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 1,
		Kind: &v1.TurnEvent_AssistantMessage{AssistantMessage: &v1.AssistantMessage{Text: "a"}}}))
	w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil })

	// Replay seq 1 (dedup) -> still ACKed through 1, no duplicate audit event.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 1,
		Kind: &v1.TurnEvent_AssistantMessage{AssistantMessage: &v1.AssistantMessage{Text: "a"}}}))
	ack := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil }).GetEventAck()
	assert.Equal(t, uint64(1), ack.GetThroughSequence())

	evs, err := env.rt.ListEvents(ctx, runID)
	require.NoError(t, err)
	count := 0
	for _, e := range evs {
		if e.Kind == runtime.EvAssistantMessage {
			count++
		}
	}
	assert.Equal(t, 1, count, "dedup: no duplicate assistant_message event")
}

// TestDispatch_FencingOnReconnect: a second connection for the same worker
// fences the prior generation; the prior active turn is terminalized as worker
// loss (aborted). The prior connection observes a closed stream.
func TestDispatch_FencingOnReconnect(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()
	runID := env.seedPendingRun(t)

	w1 := env.connectWorker(t)
	defer w1.close()
	require.NoError(t, w1.ready())
	at := w1.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	turnID := at.GetTurnId()
	_ = runID

	// Reconnect: a new connection for the same (pool, worker) fences gen 1.
	w2 := env.connectWorker(t)
	defer w2.close()

	// The first turn is terminalized as worker loss (aborted); the run is aborted.
	require.Eventually(t, func() bool {
		_, err := env.store.ResolveActiveAssignment(ctx, turnID)
		return errors.Is(err, dispatch.ErrAssignmentNotActive)
	}, 2*time.Second, 20*time.Millisecond, "prior assignment not fenced")
	run, err := env.rt.GetRun(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, runtime.RunAborted, run.State)
}

// TestDispatch_FencesDurablePriorGenOnRestart: a control-plane restart empties
// the in-memory workerPool while runtime.turn_assignments still holds the
// worker's active assignment. A newly-accepted connection (no in-memory prior
// conn, so `old == nil`) MUST still resolve and fence the durable prior
// assignment before Welcome is sent, and terminalize the prior turn as worker
// loss (HOR-249 reconnect fencing; HOR-381: a newly accepted connection fences
// any old active assignment). The new generation is advertised only after the
// durable fence + loss terminalization succeeds.
//
// The post-restart durable state — an active assignment row with no in-memory
// conn bound to it — is seeded directly; the env's workerPool is empty, so the
// reconnecting worker hits the `old == nil` path. (The full in-memory
// old!=nil reconnect fence is covered by TestDispatch_FencingOnReconnect.)
func TestDispatch_FencesDurablePriorGenOnRestart(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()

	// Seed a running run + running step + running turn and bind it to worker-1
	// via a durable active assignment (generation 5) with NO connected worker —
	// exactly the durable state left behind by a CP restart.
	runID, _, turnID := seedRunTurn(t, env.rt, env.pgpool, dispatchSID())
	identID := insertIdentity(t, env.pgpool, "ident-restart")
	_, err := env.store.CreateAssignment(ctx, dispatch.AssignmentInput{
		TurnID: turnID, RunID: runID, PoolID: env.poolID, WorkerID: "worker-1",
		FencingGeneration: 5, AttemptID: runID, ScopeIdentityID: identID, AgentPoolKey: "ns/pool-1",
		ModelPermission: json.RawMessage(`{"id":"gpt-4o"}`),
	})
	require.NoError(t, err)
	prior, err := env.store.ResolveActiveAssignment(ctx, turnID)
	require.NoError(t, err)
	require.Equal(t, int64(5), prior.FencingGeneration)

	// A fresh service (empty in-memory workerPool) accepts the worker. The
	// reconnect hits `old == nil` and must fence the durable prior assignment.
	w := env.connectWorker(t)
	defer w.close()

	// The durable prior assignment was fenced + terminalized as worker loss
	// before Welcome was sent; the run is aborted.
	require.Eventually(t, func() bool {
		_, err := env.store.ResolveActiveAssignment(ctx, turnID)
		return errors.Is(err, dispatch.ErrAssignmentNotActive)
	}, 2*time.Second, 20*time.Millisecond, "durable prior assignment not fenced on restart")
	run, err := env.rt.GetRun(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, runtime.RunAborted, run.State, "prior turn terminalized as worker loss")
}

// TestDispatch_Cancel: CancelTurn propagates AbortTurn and terminalizes the
// turn as aborted (CP first-terminal-writer).
func TestDispatch_Cancel(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()
	runID := env.seedPendingRun(t)

	w := env.connectWorker(t)
	defer w.close()
	require.NoError(t, w.ready())
	at := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	turnID := at.GetTurnId()

	// Cancel the turn (as the control plane, e.g. a workflow/user cancel).
	require.NoError(t, env.svc.CancelTurn(ctx, turnID, v1.AbortReason_ABORT_REASON_USER_CANCEL, "user cancelled"))

	// Worker receives AbortTurn.
	ab := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAbortTurn() != nil }).GetAbortTurn()
	assert.Equal(t, turnID, ab.GetTurnId())

	// Turn is aborted; run is aborted; assignment not active.
	require.Eventually(t, func() bool {
		run, err := env.rt.GetRun(ctx, runID)
		return err == nil && run.State == runtime.RunAborted
	}, 2*time.Second, 20*time.Millisecond, "run not aborted after cancel")
	_, err := env.store.ResolveActiveAssignment(ctx, turnID)
	assert.ErrorIs(t, err, dispatch.ErrAssignmentNotActive)
}

// TestDispatch_SequenceGapRejected: the HOR-381 source-order contract is
// strictly monotonic, one-based and gapless. A gap (seq jumps from 1 to 100) is
// rejected without advancing the watermark; the subsequent in-order event (seq
// 2) is still applied and ACKed.
func TestDispatch_SequenceGapRejected(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()
	_ = env.seedPendingRun(t)

	w := env.connectWorker(t)
	defer w.close()
	require.NoError(t, w.ready())
	at := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	turnID := at.GetTurnId()

	// seq 1 applied + ACKed through 1.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 1,
		Kind: &v1.TurnEvent_AssistantMessage{AssistantMessage: &v1.AssistantMessage{Text: "a"}}}))
	ack1 := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil }).GetEventAck()
	assert.Equal(t, uint64(1), ack1.GetThroughSequence())

	// seq 100 (gap): rejected. No ACK is sent for the gap; the watermark stays at 1.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 100,
		Kind: &v1.TurnEvent_AssistantMessage{AssistantMessage: &v1.AssistantMessage{Text: "gap"}}}))

	// seq 2 (in-order) is still applied and ACKed through 2.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 2,
		Kind: &v1.TurnEvent_AssistantMessage{AssistantMessage: &v1.AssistantMessage{Text: "b"}}}))
	ack2 := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil }).GetEventAck()
	assert.Equal(t, uint64(2), ack2.GetThroughSequence())

	// The gap payload was not persisted.
	high, err := env.store.AckWatermark(ctx, turnID)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), high, "watermark must not advance past the applied in-order sequence")
}

// TestDispatch_MultiStepRunSuccession: a run with two agent_task steps is not
// short-circuited after its first turn. Completing turn 1 succeeds step 1 and
// starts step 2 + a second turn; completing turn 2 succeeds the run.
func TestDispatch_MultiStepRunSuccession(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()

	sid := dispatchSID()
	identID := insertIdentity(t, env.pgpool, "ident-"+sid)
	run, err := env.rt.CreateRun(ctx, runtime.CreateRunInput{
		Kind: runtime.KindWorkflow, DefinitionKey: "wf-multi",
		ScopeIdentityID: identID, SessionID: sid, SessionDir: "/sessions/" + sid,
		Steps: []runtime.StepInput{
			{Seq: 1, Kind: runtime.StepKindAgentTask, Config: json.RawMessage(`{"prompt":"a"}`)},
			{Seq: 2, Kind: runtime.StepKindAgentTask, Config: json.RawMessage(`{"prompt":"b"}`)},
		},
	})
	require.NoError(t, err)
	require.NoError(t, env.store.AssignRunToPool(ctx, run.ID, env.poolID))

	w := env.connectWorker(t)
	defer w.close()
	require.NoError(t, w.ready())

	// First AssignTurn (step 1).
	at1 := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	require.Equal(t, run.ID, at1.GetRunId())
	// Complete turn 1.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: at1.GetTurnId(), Sequence: 1,
		Kind: &v1.TurnEvent_WorkerOutcome{WorkerOutcome: &v1.WorkerOutcome{Outcome: v1.Outcome_OUTCOME_COMPLETED}}}))
	w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil })

	// Worker re-advertises credit; a second AssignTurn (step 2) must arrive.
	require.NoError(t, w.ready())
	at2 := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	assert.NotEqual(t, at1.GetTurnId(), at2.GetTurnId(), "second step must get a new turn")

	// Complete turn 2 -> run succeeds.
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: at2.GetTurnId(), Sequence: 1,
		Kind: &v1.TurnEvent_WorkerOutcome{WorkerOutcome: &v1.WorkerOutcome{Outcome: v1.Outcome_OUTCOME_COMPLETED}}}))
	require.Eventually(t, func() bool {
		r, err := env.rt.GetRun(ctx, run.ID)
		return err == nil && r.State == runtime.RunSucceeded
	}, 2*time.Second, 20*time.Millisecond, "multi-step run should succeed after both turns complete")
}

// TestDispatch_SessionEndAndUIDAllocation: completing a run's turn sends
// SessionEnd {sandbox_id, uid, gid} to the worker (HOR-245 cleanup), and the
// durable allocator assigns a unique in-range UID per session (no collisions).
func TestDispatch_SessionEndAndUIDAllocation(t *testing.T) {
	env := newDispatchEnv(t)

	// Two sessions -> two distinct UIDs (collision-free allocator, F8).
	uid1 := mustSessionUID(t, env, "sess-A")
	uid2 := mustSessionUID(t, env, "sess-B")
	assert.NotEqual(t, uid1, uid2, "allocator must not collide across sessions")
	assert.GreaterOrEqual(t, uid1, uint32(10000))
	assert.Less(t, uid1, uint32(60000))

	_ = env.seedPendingRun(t)
	w := env.connectWorker(t)
	defer w.close()
	require.NoError(t, w.ready())
	at := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetAssignTurn() != nil }).GetAssignTurn()
	turnID := at.GetTurnId()

	// AssignTurn carried an allocated UID (sandbox_id == session id).
	assert.GreaterOrEqual(t, at.GetSandbox().GetUid(), uint32(10000))
	assert.Equal(t, at.GetSandbox().GetSandboxId(), at.GetSessionId())

	// Complete the turn -> SessionEnd for the session (F9).
	require.NoError(t, w.sendEvent(&v1.TurnEvent{TurnId: turnID, Sequence: 1,
		Kind: &v1.TurnEvent_WorkerOutcome{WorkerOutcome: &v1.WorkerOutcome{Outcome: v1.Outcome_OUTCOME_COMPLETED}}}))
	w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetEventAck() != nil })
	se := w.recvControl(t, func(c *v1.ControlMessage) bool { return c.GetSessionEnd() != nil }).GetSessionEnd()
	assert.Equal(t, at.GetSessionId(), se.GetSandboxId())
	assert.Equal(t, at.GetSandbox().GetUid(), se.GetUid())
	assert.Equal(t, se.GetUid(), se.GetGid())
}

func mustSessionUID(t *testing.T, env *dispatchEnv, sessionID string) uint32 {
	t.Helper()
	uid, err := env.store.AllocateSessionUID(context.Background(), sessionID, 10000, 50000, 5*time.Minute)
	require.NoError(t, err)
	return uid
}

// TestDispatch_SessionUIDRecyclesAfterGrace: a freed UID becomes recyclable
// only after the non-recycling reap grace elapses, and a new session then
// reclaims the expired freed row (UPSERT on uid, not a rejected insert). This
// locks in the HOR-245 reuse-safety floor: no live-session collisions, and the
// allocator does not exhaust the range as freed rows accumulate.
func TestDispatch_SessionUIDRecyclesAfterGrace(t *testing.T) {
	env := newDispatchEnv(t)
	ctx := context.Background()

	const tinyGrace = 50 * time.Millisecond
	const base, n uint32 = 10000, 4 // small range so recycling is observable

	uidA, err := env.store.AllocateSessionUID(ctx, "sess-A", base, n, tinyGrace)
	require.NoError(t, err)
	// Within grace: a new session must NOT recycle sess-A's freed UID.
	require.NoError(t, env.store.ReleaseSessionUID(ctx, "sess-A"))
	uidB, err := env.store.AllocateSessionUID(ctx, "sess-B", base, n, tinyGrace)
	require.NoError(t, err)
	assert.NotEqual(t, uidA, uidB, "freed UID within grace must not be recycled")

	// Exhaust the remaining range so recycling is the only way to satisfy a
	// new allocation once the grace elapses.
	_, err = env.store.AllocateSessionUID(ctx, "sess-C", base, n, tinyGrace)
	require.NoError(t, err)
	_, err = env.store.AllocateSessionUID(ctx, "sess-D", base, n, tinyGrace)
	require.NoError(t, err)
	// Range is now fully allocated (4 UIDs); a new session must fail closed.
	_, err = env.store.AllocateSessionUID(ctx, "sess-E", base, n, tinyGrace)
	require.ErrorIs(t, err, dispatch.ErrUIDExhausted)

	// Release sess-A and wait past the grace; its UID must now recycle to a
	// new session (the allocator reclaims the expired freed row).
	require.NoError(t, env.store.ReleaseSessionUID(ctx, "sess-A"))
	time.Sleep(2 * tinyGrace)
	uidE, err := env.store.AllocateSessionUID(ctx, "sess-E", base, n, tinyGrace)
	require.NoError(t, err)
	assert.Equal(t, uidA, uidE, "freed UID past grace must be recycled to a new session")

	// A freed UID is non-recyclable even for the owning session until the grace
	// elapses (HOR-245: the (sandbox_id, uid, gid) triple is non-recyclable until
	// reaping is confirmed; re-provisioning the same UID while the prior sandbox
	// may still be reaping would let a new child collide with the live sandbox).
	require.NoError(t, env.store.ReleaseSessionUID(ctx, "sess-E"))
	_, err = env.store.AllocateSessionUID(ctx, "sess-E", base, n, tinyGrace)
	require.ErrorIs(t, err, dispatch.ErrUIDExhausted, "freed UID within grace must not be reactivated, even for the same session")

	// Past grace, the same session reclaims its freed UID (reap complete).
	time.Sleep(2 * tinyGrace)
	uidE2, err := env.store.AllocateSessionUID(ctx, "sess-E", base, n, tinyGrace)
	require.NoError(t, err)
	assert.Equal(t, uidE, uidE2, "freed UID past grace is reclaimed by the same session")
}
