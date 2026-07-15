package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/runtime"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

func newTestStore(t *testing.T) (*runtime.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.NewPostgresPool(t)
	return runtime.NewStore(pool), pool
}

func insertIdentity(t *testing.T, pool *pgxpool.Pool, key string) string {
	t.Helper()
	var id string
	require.NoError(t, pool.QueryRow(context.Background(), `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ($1, 'workflow', 'local', $1) RETURNING id`, key).Scan(&id))
	return id
}

func mustCreateChatRun(t *testing.T, store *runtime.Store, identityID, sessionID string) (runtime.Run, string) {
	t.Helper()
	run, err := store.CreateRun(context.Background(), runtime.CreateRunInput{
		Kind:            runtime.KindChat,
		ScopeIdentityID: identityID,
		SessionID:       sessionID,
		SessionDir:      "/sessions/" + sessionID,
		Trigger:         json.RawMessage(`{"type":"test"}`),
		Steps: []runtime.StepInput{
			{Seq: 1, Kind: runtime.StepKindAgentTask, Config: json.RawMessage(`{"prompt":"hi"}`)},
		},
	})
	require.NoError(t, err)
	steps, err := store.ListSteps(context.Background(), run.ID)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	return run, steps[0].ID
}

func eventKinds(evs []runtime.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}

func findByKind(evs []runtime.Event, kind string) runtime.Event {
	for _, e := range evs {
		if e.Kind == kind {
			return e
		}
	}
	return runtime.Event{}
}

func TestStore_CreateRunSnapshotsPlan(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	ident := insertIdentity(t, pool, "wf/quotation")

	run, err := store.CreateRun(ctx, runtime.CreateRunInput{
		Kind:            runtime.KindWorkflow,
		DefinitionKey:   "default/quotation",
		ScopeIdentityID: ident,
		SessionID:       "sess-1",
		SessionDir:      "/sessions/sess-1",
		Trigger:         json.RawMessage(`{"type":"graph_email_webhook","inbox":"rfq"}`),
		Steps: []runtime.StepInput{
			{Seq: 1, Kind: runtime.StepKindAgentTask},
			{Seq: 2, Kind: runtime.StepKindToolCall},
			{Seq: 3, Kind: runtime.StepKindApprovalGate},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, runtime.KindWorkflow, run.Kind)
	require.NotNil(t, run.DefinitionKey)
	assert.Equal(t, "default/quotation", *run.DefinitionKey)
	assert.Equal(t, runtime.RunPending, run.State)
	assert.Equal(t, "sess-1", run.SessionID)

	chat, _ := mustCreateChatRun(t, store, ident, "sess-2")
	assert.Nil(t, chat.DefinitionKey)

	steps, err := store.ListSteps(ctx, run.ID)
	require.NoError(t, err)
	require.Len(t, steps, 3)
	assert.Equal(t, runtime.StepPending, steps[0].State)
	assert.Equal(t, []int{1, 2, 3}, []int{steps[0].Seq, steps[1].Seq, steps[2].Seq})
	assert.Equal(t, []string{runtime.StepKindAgentTask, runtime.StepKindToolCall, runtime.StepKindApprovalGate},
		[]string{steps[0].Kind, steps[1].Kind, steps[2].Kind})
}
func TestStore_RunStateMachine(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	run, _ := mustCreateChatRun(t, store, insertIdentity(t, pool, "u/1"), "s1")

	// Legal: pending -> running -> succeeded.
	r, err := store.StartRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, runtime.RunRunning, r.State)
	require.NotNil(t, r.StartedAt)
	assert.Nil(t, r.FinishedAt)

	r, err = store.SucceedRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, runtime.RunSucceeded, r.State)
	require.NotNil(t, r.FinishedAt)

	// Illegal: succeeded -> succeeded (already terminal).
	_, err = store.SucceedRun(ctx, run.ID)
	assert.ErrorIs(t, err, runtime.ErrInvalidTransition)

	// Abort from running; awaiting_approval -> aborted.
	run2, _ := mustCreateChatRun(t, store, insertIdentity(t, pool, "u/2"), "s2")
	_, err = store.StartRun(ctx, run2.ID)
	require.NoError(t, err)
	_, err = store.AwaitApproval(ctx, run2.ID)
	require.NoError(t, err)
	r, err = store.AbortRun(ctx, run2.ID)
	require.NoError(t, err)
	assert.Equal(t, runtime.RunAborted, r.State)

	// Resume from approval.
	run3, _ := mustCreateChatRun(t, store, insertIdentity(t, pool, "u/3"), "s3")
	_, _ = store.StartRun(ctx, run3.ID)
	_, err = store.AwaitApproval(ctx, run3.ID)
	require.NoError(t, err)
	r, err = store.ResumeFromApproval(ctx, run3.ID)
	require.NoError(t, err)
	assert.Equal(t, runtime.RunRunning, r.State)

	// Unknown id -> ErrNotFound.
	_, err = store.StartRun(ctx, "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, runtime.ErrNotFound)

	// Each transition appended its run-level event.
	evs, err := store.ListEvents(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{runtime.EvRunStarted, runtime.EvRunSucceeded}, eventKinds(evs))
}

func TestStore_StepTransitions(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	run, step := mustCreateChatRun(t, store, insertIdentity(t, pool, "u/4"), "s4")
	_, _ = store.StartRun(ctx, run.ID)

	st, err := store.StartStep(ctx, step)
	require.NoError(t, err)
	assert.Equal(t, runtime.StepRunning, st.State)
	require.NotNil(t, st.StartedAt)

	st, err = store.SucceedStep(ctx, step)
	require.NoError(t, err)
	assert.Equal(t, runtime.StepSucceeded, st.State)
	require.NotNil(t, st.FinishedAt)

	// Illegal: succeeded -> succeed.
	_, err = store.SucceedStep(ctx, step)
	assert.ErrorIs(t, err, runtime.ErrInvalidTransition)

	// Skip a pending step (branch not taken). Create a workflow run with 2 steps.
	wf, err := store.CreateRun(ctx, runtime.CreateRunInput{
		Kind: runtime.KindWorkflow, ScopeIdentityID: insertIdentity(t, pool, "u/5"),
		SessionID: "s5", SessionDir: "/s5",
		Steps: []runtime.StepInput{{Seq: 1, Kind: runtime.StepKindAgentTask}, {Seq: 2, Kind: runtime.StepKindAgentTask}},
	})
	require.NoError(t, err)
	steps, _ := store.ListSteps(ctx, wf.ID)
	st, err = store.SkipStep(ctx, steps[1].ID)
	require.NoError(t, err)
	assert.Equal(t, runtime.StepSkipped, st.State)

	// Unknown step -> ErrNotFound.
	_, err = store.StartStep(ctx, "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, runtime.ErrNotFound)
}
func TestStore_TurnOneActivePerSession(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	ident := insertIdentity(t, pool, "u/6")
	run, step := mustCreateChatRun(t, store, ident, "s6")
	_, _ = store.StartRun(ctx, run.ID)
	_, _ = store.StartStep(ctx, step)

	// First turn starts.
	turn, err := store.StartTurn(ctx, run.ID, step, "iterabase-inference/qwen")
	require.NoError(t, err)
	assert.Equal(t, runtime.TurnRunning, turn.State)
	assert.Equal(t, "s6", turn.SessionID)

	// A second active turn on the SAME session is rejected (unique index).
	_, err = store.StartTurn(ctx, run.ID, step, "iterabase-inference/qwen")
	require.Error(t, err)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		assert.Equal(t, "23505", pgErr.Code, "expected unique_violation")
	}

	// After settling, a new turn on the same session starts.
	_, err = store.SettleTurn(ctx, turn.ID, "completed", 2)
	require.NoError(t, err)
	turn2, err := store.StartTurn(ctx, run.ID, step, "iterabase-inference/qwen")
	require.NoError(t, err)
	assert.Equal(t, runtime.TurnRunning, turn2.State)

	// A different session (different run) is independent.
	run2, step2 := mustCreateChatRun(t, store, ident, "s7")
	_, _ = store.StartRun(ctx, run2.ID)
	_, _ = store.StartStep(ctx, step2)
	_, err = store.StartTurn(ctx, run2.ID, step2, "iterabase-inference/qwen")
	require.NoError(t, err)

	// ActiveTurn returns the run's active turn.
	got, err := store.ActiveTurn(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, turn2.ID, got.ID)

	// No active turn -> ErrNotFound.
	_, _ = store.SettleTurn(ctx, turn2.ID, "completed", 1)
	_, err = store.ActiveTurn(ctx, run.ID)
	assert.ErrorIs(t, err, runtime.ErrNotFound)

	// Settling a non-running turn is invalid.
	_, err = store.SettleTurn(ctx, turn.ID, "completed", 1)
	assert.ErrorIs(t, err, runtime.ErrInvalidTransition)
}

func TestStore_ApprovalGate(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	ident := insertIdentity(t, pool, "u/7")
	run, err := store.CreateRun(ctx, runtime.CreateRunInput{
		Kind: runtime.KindWorkflow, ScopeIdentityID: ident,
		SessionID: "s8", SessionDir: "/s8",
		Steps: []runtime.StepInput{{Seq: 1, Kind: runtime.StepKindApprovalGate}},
	})
	require.NoError(t, err)
	steps, _ := store.ListSteps(ctx, run.ID)
	gate := steps[0].ID

	_, _ = store.StartRun(ctx, run.ID)

	// RequestApproval: step pending -> pending_approval + run -> awaiting_approval.
	st, err := store.RequestApproval(ctx, gate)
	require.NoError(t, err)
	assert.Equal(t, runtime.StepPendingApproval, st.State)
	r, _ := store.GetRun(ctx, run.ID)
	assert.Equal(t, runtime.RunAwaitingApproval, r.State)

	// RequestApproval on a non-gate step is invalid (kind guard).
	run2, _ := mustCreateChatRun(t, store, ident, "s9")
	steps2, _ := store.ListSteps(ctx, run2.ID)
	_, _ = store.StartRun(ctx, run2.ID)
	_, err = store.RequestApproval(ctx, steps2[0].ID)
	assert.ErrorIs(t, err, runtime.ErrInvalidTransition)

	// Resolve approved -> step succeeded + run resumed.
	approver := insertIdentity(t, pool, "u/admin")
	st, err = store.ResolveApproval(ctx, gate, approver, runtime.DecisionApproved)
	require.NoError(t, err)
	assert.Equal(t, runtime.StepSucceeded, st.State)
	r, _ = store.GetRun(ctx, run.ID)
	assert.Equal(t, runtime.RunRunning, r.State)

	// Rejected path: a second gate fails the run.
	run3, err := store.CreateRun(ctx, runtime.CreateRunInput{
		Kind: runtime.KindWorkflow, ScopeIdentityID: ident,
		SessionID: "s10", SessionDir: "/s10",
		Steps: []runtime.StepInput{{Seq: 1, Kind: runtime.StepKindApprovalGate}},
	})
	require.NoError(t, err)
	gate3, _ := store.ListSteps(ctx, run3.ID)
	_, _ = store.StartRun(ctx, run3.ID)
	_, _ = store.RequestApproval(ctx, gate3[0].ID)
	_, err = store.ResolveApproval(ctx, gate3[0].ID, approver, runtime.DecisionRejected)
	require.NoError(t, err)
	r, _ = store.GetRun(ctx, run3.ID)
	assert.Equal(t, runtime.RunFailed, r.State)
	st3, _ := store.GetStep(ctx, gate3[0].ID)
	assert.Equal(t, runtime.StepFailed, st3.State)

	// Invalid decision.
	_, err = store.ResolveApproval(ctx, gate, approver, "maybe")
	require.Error(t, err)
}

func TestStore_EventsGaplessAndReplay(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()
	run, step := mustCreateChatRun(t, store, insertIdentity(t, pool, "u/8"), "s11")
	_, _ = store.StartRun(ctx, run.ID)                                                                                 // seq 1: run_started
	_, _ = store.StartStep(ctx, step)                                                                                  // seq 2: step_started
	turn, _ := store.StartTurn(ctx, run.ID, step, "m1")                                                                // seq 3: turn_started
	_, _ = store.AppendEvent(ctx, run.ID, turn.ID, step, runtime.EvAssistantMessage, json.RawMessage(`{"text":"hi"}`)) // seq 4
	_, _ = store.AppendEvent(ctx, run.ID, turn.ID, step, runtime.EvToolResult, json.RawMessage(`{"tool":"excel"}`))    // seq 5
	_, _ = store.SettleTurn(ctx, turn.ID, "completed", 2)                                                              // seq 6: settled
	_, _ = store.SucceedStep(ctx, step)                                                                                // seq 7: step_succeeded
	_, _ = store.SucceedRun(ctx, run.ID)                                                                               // seq 8: run_succeeded

	evs, err := store.ListEvents(ctx, run.ID)
	require.NoError(t, err)
	require.Len(t, evs, 8)

	// Gapless per-run seq 1..8.
	for i, e := range evs {
		assert.Equal(t, i+1, e.Seq, "seq should be gapless 1..N")
	}

	// Expected audit trail (replay fold).
	wantKinds := []string{
		runtime.EvRunStarted, runtime.EvStepStarted, runtime.EvTurnStarted,
		runtime.EvAssistantMessage, runtime.EvToolResult, runtime.EvSettled,
		runtime.EvStepSucceeded, runtime.EvRunSucceeded,
	}
	assert.Equal(t, wantKinds, eventKinds(evs))

	// turn_started/assistant_message/tool_result/settled carry the turn id.
	for _, k := range []string{runtime.EvTurnStarted, runtime.EvAssistantMessage, runtime.EvToolResult, runtime.EvSettled} {
		e := findByKind(evs, k)
		require.NotNil(t, e.TurnID)
		assert.Equal(t, turn.ID, *e.TurnID)
	}

	// LastEventSeq + EventsSince.
	last, err := store.LastEventSeq(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, 8, last)
	since, err := store.EventsSince(ctx, run.ID, 5)
	require.NoError(t, err)
	assert.Equal(t, []string{runtime.EvSettled, runtime.EvStepSucceeded, runtime.EvRunSucceeded}, eventKinds(since))

	// LastEventSeq for an eventless run is 0.
	empty, _ := mustCreateChatRun(t, store, insertIdentity(t, pool, "u/9"), "s12")
	last, err = store.LastEventSeq(ctx, empty.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, last)
}

func TestStore_SettleTurnMapsReasons(t *testing.T) {
	store, pool := newTestStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		reason, want string
	}{
		{"completed", runtime.TurnSucceeded},
		{"failed", runtime.TurnFailed},
		{"aborted", runtime.TurnAborted},
	} {
		run, step := mustCreateChatRun(t, store, insertIdentity(t, pool, "u/"+tc.reason), "s-"+tc.reason)
		_, _ = store.StartRun(ctx, run.ID)
		_, _ = store.StartStep(ctx, step)
		turn, _ := store.StartTurn(ctx, run.ID, step, "m")
		settled, err := store.SettleTurn(ctx, turn.ID, tc.reason, 1)
		require.NoError(t, err)
		assert.Equal(t, tc.want, settled.State, tc.reason)
		require.NotNil(t, settled.SettledAt)

		// settled event carries the reason.
		evs, _ := store.ListEvents(ctx, run.ID)
		se := findByKind(evs, runtime.EvSettled)
		require.Contains(t, string(se.Payload), tc.reason)
	}
}
