package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/dispatch"
	"github.com/nunocgoncalves/control-plane/internal/runtime"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

func newTestStore(t *testing.T) (*dispatch.Store, *runtime.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.NewPostgresPool(t)
	rt := runtime.NewStore(pool)
	return dispatch.NewStore(pool, rt), rt, pool
}

func insertIdentity(t *testing.T, pool *pgxpool.Pool, key string) string {
	t.Helper()
	var id string
	require.NoError(t, pool.QueryRow(context.Background(), `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ($1, 'workflow', 'local', $1) RETURNING id`, key).Scan(&id))
	return id
}

// seedRunTurn creates a chat run + running step + active turn and returns the
// run id, step id, and turn id. The run is left in 'running' state.
func seedRunTurn(t *testing.T, rt *runtime.Store, pool *pgxpool.Pool, sessionID string) (runID, stepID, turnID string) {
	t.Helper()
	ctx := context.Background()
	identID := insertIdentity(t, pool, "ident-"+sessionID)
	run, err := rt.CreateRun(ctx, runtime.CreateRunInput{
		Kind:            runtime.KindChat,
		ScopeIdentityID: identID,
		SessionID:       sessionID,
		SessionDir:      "/sessions/" + sessionID,
		Steps: []runtime.StepInput{
			{Seq: 1, Kind: runtime.StepKindAgentTask, Config: json.RawMessage(`{"prompt":"hi"}`)},
		},
	})
	require.NoError(t, err)
	_, err = rt.StartRun(ctx, run.ID)
	require.NoError(t, err)
	steps, err := rt.ListSteps(ctx, run.ID)
	require.NoError(t, requireLen1(steps))
	stepID = steps[0].ID
	_, err = rt.StartStep(ctx, stepID)
	require.NoError(t, err)
	turn, err := rt.StartTurn(ctx, run.ID, stepID, "m1")
	require.NoError(t, err)
	return run.ID, stepID, turn.ID
}

func requireLen1[T any](s []T) error {
	if len(s) != 1 {
		return errors.New("expected exactly 1 element")
	}
	return nil
}

func TestStore_AssignRunToPoolAndResolve(t *testing.T) {
	store, rt, pool := newTestStore(t)
	ctx := context.Background()

	// Seed a pool in toolgateway.pools (the AgentPool registry).
	var poolID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ('ns/pool-1', 'pool-1', 'spiffe://iterabase.local/pools/pool-1/') RETURNING id::text`).Scan(&poolID))

	runID, _, _ := seedRunTurn(t, rt, pool, "sess-1")
	require.NoError(t, store.AssignRunToPool(ctx, runID, poolID))

	got, err := store.PoolForRun(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, poolID, got)

	// Idempotent.
	require.NoError(t, store.AssignRunToPool(ctx, runID, poolID))

	// Resolve by spiffe prefix.
	p, err := store.ResolvePoolBySpiffePrefix(ctx, "spiffe://iterabase.local/pools/pool-1/workers/worker-1")
	require.NoError(t, err)
	assert.Equal(t, poolID, p.ID)
	assert.Equal(t, "ns/pool-1", p.Key)

	_, err = store.PoolForRun(ctx, "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, dispatch.ErrNotFound)
}

func TestStore_CreateAssignmentAndResolve(t *testing.T) {
	store, rt, pool := newTestStore(t)
	ctx := context.Background()
	var poolID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ('ns/pool-1', 'pool-1', 'spiffe://iterabase.local/pools/pool-1/') RETURNING id::text`).Scan(&poolID))
	identID := insertIdentity(t, pool, "ident-a")
	runID, _, turnID := seedRunTurn(t, rt, pool, "sess-a")

	in := dispatch.AssignmentInput{
		TurnID: turnID, RunID: runID, PoolID: poolID, WorkerID: "worker-1",
		FencingGeneration: 7, AttemptID: runID, ScopeIdentityID: identID,
		AgentPoolKey: "ns/pool-1", ModelPermission: json.RawMessage(`{"id":"m1"}`),
	}
	a, err := store.CreateAssignment(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, dispatch.AssignmentActive, a.State)
	assert.Equal(t, "worker-1", a.WorkerID)
	assert.Equal(t, int64(7), a.FencingGeneration)

	// Resolve active.
	got, err := store.ResolveActiveAssignment(ctx, turnID)
	require.NoError(t, err)
	assert.Equal(t, turnID, got.TurnID)
	assert.Equal(t, runID, got.AttemptID)

	// Active assignment for the worker.
	got, err = store.ActiveAssignmentForWorker(ctx, poolID, "worker-1")
	require.NoError(t, err)
	assert.Equal(t, turnID, got.TurnID)

	// Duplicate create -> ErrAlreadyAssigned.
	_, err = store.CreateAssignment(ctx, in)
	assert.ErrorIs(t, err, dispatch.ErrAlreadyAssigned)

	// A different same-pool worker has no active assignment.
	_, err = store.ActiveAssignmentForWorker(ctx, poolID, "worker-2")
	assert.ErrorIs(t, err, dispatch.ErrNotFound)
}

func TestStore_FenceAndTerminalize(t *testing.T) {
	store, rt, pool := newTestStore(t)
	ctx := context.Background()
	var poolID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ('ns/pool-1', 'pool-1', 'spiffe://iterabase.local/pools/pool-1/') RETURNING id::text`).Scan(&poolID))
	identID := insertIdentity(t, pool, "ident-b")
	runID, _, turnID := seedRunTurn(t, rt, pool, "sess-b")
	_, err := store.CreateAssignment(ctx, dispatch.AssignmentInput{
		TurnID: turnID, RunID: runID, PoolID: poolID, WorkerID: "worker-1",
		FencingGeneration: 1, AttemptID: runID, ScopeIdentityID: identID, AgentPoolKey: "ns/pool-1",
	})
	require.NoError(t, err)

	// Fence the worker (reconnect/loss): active -> fenced. The turn is no longer
	// actively assigned.
	fenced, err := store.FenceWorkerGeneration(ctx, poolID, "worker-1")
	require.NoError(t, err)
	assert.Equal(t, dispatch.AssignmentFenced, fenced.State)
	_, err = store.ResolveActiveAssignment(ctx, turnID)
	assert.ErrorIs(t, err, dispatch.ErrAssignmentNotActive)

	// Terminalize (idempotent): fenced -> terminal.
	require.NoError(t, store.TerminalizeAssignment(ctx, turnID))
	require.NoError(t, store.TerminalizeAssignment(ctx, turnID)) // idempotent
	_, err = store.ResolveActiveAssignment(ctx, turnID)
	assert.ErrorIs(t, err, dispatch.ErrAssignmentNotActive)

	// Fencing a worker with no active assignment -> ErrNotFound.
	_, err = store.FenceWorkerGeneration(ctx, poolID, "worker-1")
	assert.ErrorIs(t, err, dispatch.ErrNotFound)
}

func TestStore_AppendTurnEventDedupAndWatermark(t *testing.T) {
	store, rt, pool := newTestStore(t)
	ctx := context.Background()
	var poolID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ('ns/pool-1', 'pool-1', 'spiffe://iterabase.local/pools/pool-1/') RETURNING id::text`).Scan(&poolID))
	identID := insertIdentity(t, pool, "ident-c")
	runID, _, turnID := seedRunTurn(t, rt, pool, "sess-c")
	_, err := store.CreateAssignment(ctx, dispatch.AssignmentInput{
		TurnID: turnID, RunID: runID, PoolID: poolID, WorkerID: "worker-1",
		FencingGeneration: 1, AttemptID: runID, ScopeIdentityID: identID, AgentPoolKey: "ns/pool-1",
	})
	require.NoError(t, err)

	// Apply seq 1 + 2.
	applied, err := store.AppendTurnEvent(ctx, turnID, 1, runtime.EvAssistantMessage, json.RawMessage(`{"text":"a"}`))
	require.NoError(t, err)
	assert.True(t, applied)
	applied, err = store.AppendTurnEvent(ctx, turnID, 2, runtime.EvAssistantMessage, json.RawMessage(`{"text":"b"}`))
	require.NoError(t, err)
	assert.True(t, applied)

	// Watermark is 2.
	wm, err := store.AckWatermark(ctx, turnID)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), wm)

	// Replay seq 2 (dedup) -> not applied.
	applied, err = store.AppendTurnEvent(ctx, turnID, 2, runtime.EvAssistantMessage, json.RawMessage(`{"text":"b"}`))
	require.NoError(t, err)
	assert.False(t, applied)

	// Events were appended to the run's audit log (turn_started from StartTurn +
	// 2 assistant_message).
	evs, err := rt.ListEvents(ctx, runID)
	require.NoError(t, err)
	kinds := make(map[string]int)
	for _, e := range evs {
		kinds[e.Kind]++
	}
	assert.Equal(t, 2, kinds[runtime.EvAssistantMessage], "two assistant_message events appended")
	assert.Equal(t, 1, kinds[runtime.EvTurnStarted])

	// After fencing, AppendTurnEvent -> ErrAssignmentNotActive (late events are
	// after-terminal audit, handled by the service).
	_, err = store.FenceWorkerGeneration(ctx, poolID, "worker-1")
	require.NoError(t, err)
	_, err = store.AppendTurnEvent(ctx, turnID, 3, runtime.EvAssistantMessage, json.RawMessage(`{}`))
	assert.ErrorIs(t, err, dispatch.ErrAssignmentNotActive)
}

// TestStore_AppendAfterTerminalEventDedup: after a turn is fenced/terminal, late
// worker observations are durably appended as after-terminal audit with
// (turn, sequence) dedup + watermark advance in one transaction, so an ACK lost
// after the commit dedups the event on the next replay (HOR-381 cumulative ACK
// only after Postgres commit).
func TestStore_AppendAfterTerminalEventDedup(t *testing.T) {
	store, rt, pool := newTestStore(t)
	ctx := context.Background()
	var poolID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO toolgateway.pools (key, name, spiffe_id_prefix)
		VALUES ('ns/pool-1', 'pool-1', 'spiffe://iterabase.local/pools/pool-1/') RETURNING id::text`).Scan(&poolID))
	identID := insertIdentity(t, pool, "ident-at")
	runID, _, turnID := seedRunTurn(t, rt, pool, "sess-at")
	_, err := store.CreateAssignment(ctx, dispatch.AssignmentInput{
		TurnID: turnID, RunID: runID, PoolID: poolID, WorkerID: "worker-1",
		FencingGeneration: 1, AttemptID: runID, ScopeIdentityID: identID, AgentPoolKey: "ns/pool-1",
	})
	require.NoError(t, err)
	// Apply seq 1 while active, then fence (turn becomes non-active).
	_, err = store.AppendTurnEvent(ctx, turnID, 1, runtime.EvAssistantMessage, json.RawMessage(`{"text":"a"}`))
	require.NoError(t, err)
	_, err = store.FenceWorkerGeneration(ctx, poolID, "worker-1")
	require.NoError(t, err)

	// Late seq 2 (after-terminal audit): applied + watermark advances to 2.
	applied, err := store.AppendAfterTerminalEvent(ctx, turnID, 2, runtime.EvAssistantMessage, json.RawMessage(`{"text":"b"}`))
	require.NoError(t, err)
	assert.True(t, applied)

	// Replay seq 2 (ACK lost after commit): deduped, not re-appended.
	applied, err = store.AppendAfterTerminalEvent(ctx, turnID, 2, runtime.EvAssistantMessage, json.RawMessage(`{"text":"b"}`))
	require.NoError(t, err)
	assert.False(t, applied)

	// Late seq 3 applied; watermark is now 3.
	applied, err = store.AppendAfterTerminalEvent(ctx, turnID, 3, runtime.EvAssistantMessage, json.RawMessage(`{"text":"c"}`))
	require.NoError(t, err)
	assert.True(t, applied)

	// Exactly two after-terminal audit events were appended (seq 2, 3); no
	// duplicate for the replayed seq 2.
	evs, err := rt.ListEvents(ctx, runID)
	require.NoError(t, err)
	count := 0
	for _, e := range evs {
		if e.Kind == runtime.EvAssistantMessage {
			count++
		}
	}
	assert.Equal(t, 3, count, "one active + two after-terminal assistant_message events, no duplicate")

	// Gap (seq jumps to 5 while highest is 3): rejected without advancing.
	_, err = store.AppendAfterTerminalEvent(ctx, turnID, 5, runtime.EvAssistantMessage, json.RawMessage(`{}`))
	assert.ErrorIs(t, err, dispatch.ErrOutOfOrderSequence)

	// A turn with no assignment row at all -> ErrNotFound (nothing to audit).
	_, err = store.AppendAfterTerminalEvent(ctx, "00000000-0000-0000-0000-000000000000", 1, runtime.EvAssistantMessage, json.RawMessage(`{}`))
	assert.ErrorIs(t, err, dispatch.ErrNotFound)
}
