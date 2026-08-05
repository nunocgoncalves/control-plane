package work_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/testutil"
	workstore "github.com/nunocgoncalves/control-plane/internal/work"
	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

func TestGraphValidation_CycleAndCoverage(t *testing.T) {
	spec := workflow.CanonicalSpec{Key: "dev/review", ScopeIdentityKey: "workflow:default/dev", DefaultModelRef: "model-one",
		Graph: workflow.CanonicalGraph{EntryNode: "review", MaxTransitions: 20,
			Nodes: []workflow.CanonicalNode{
				{Key: "review", Label: workflow.CanonicalLocalizedText{EN: "Review", PT: "Rever"}, Kind: workflow.NodeAgentTask, Prompt: "review", Outcomes: []string{"approved", "changes"}},
				{Key: "address", Label: workflow.CanonicalLocalizedText{EN: "Address feedback", PT: "Tratar feedback"}, Kind: workflow.NodeAgentTask, Prompt: "address", Outcomes: []string{"addressed"}},
			},
			Edges:            []workflow.CanonicalEdge{{From: "review", Outcome: "changes", To: "address"}, {From: "address", Outcome: "addressed", To: "review"}},
			TerminalOutcomes: []workflow.CanonicalTerminalOutcome{{Node: "review", Outcome: "approved"}},
		}}
	require.NoError(t, workflow.ValidateGraph(spec))
	bad := spec
	bad.Graph.TerminalOutcomes = nil
	require.Error(t, workflow.ValidateGraph(bad))
	bad = spec
	bad.Graph.Edges = bad.Graph.Edges[:1]
	assert.ErrorContains(t, workflow.ValidateGraph(bad), "no edge or terminal")
	bad = spec
	bad.Graph.Nodes = append([]workflow.CanonicalNode(nil), spec.Graph.Nodes...)
	bad.Graph.Nodes[0].Timeout = "-1s"
	assert.ErrorContains(t, workflow.ValidateGraph(bad), "positive duration")
	bad = spec
	bad.RequestedCapabilities = []workflow.CanonicalCapability{{Tool: "complete_step", MaxEffectClass: "read_only"}}
	assert.ErrorContains(t, workflow.ValidateGraph(bad), "reserved")
}

func TestWorkGraphLifecycle_CycleBlockerFeedbackRevisionAndValue(t *testing.T) {
	pool := testutil.NewPostgresPool(t)
	ctx := context.Background()
	store := workstore.NewStore(pool)
	actorID, scopeID, poolID := seedFoundation(t, ctx, pool)
	_ = poolID
	artifactID := "11111111-1111-4111-8111-111111111111"
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO artifact.artifacts
			(id,storage_key,source_type,created_by_identity_id,mime_type,size_bytes,digest,state,available_at)
		VALUES ($1,$2,'user_upload',$3,'text/plain',4,$4,'available',now())
		RETURNING id`, artifactID, "artifacts/11/"+artifactID, actorID,
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").Scan(&artifactID))
	sourceArtifacts := []workstore.ArtifactRef{{ArtifactID: artifactID, Role: "source"}}
	sourcePresentation := workstore.SourcePresentation{Kind: "outlook", Title: "Quote request", Subtitle: "requests@acme.example", Evidence: []workstore.PresentationField{{Label: workstore.LocalizedText{EN: "Customer", PT: "Cliente"}, Value: "ACME"}}}
	valueInput := workstore.ValueModelInput{Ref: "quotation-value", Version: "1", Currency: "EUR", BaselineSeconds: 1200, LoadedHourlyCost: "30.00", Assumptions: json.RawMessage(`{"source":"customer"}`), Explanation: json.RawMessage(`{"en":"20 minutes at EUR 30/hour"}`)}
	_, err := store.CreateValueModel(ctx, valueInput)
	require.NoError(t, err)
	_, err = store.CreateValueModel(ctx, valueInput)
	assert.ErrorIs(t, err, workstore.ErrConflict)
	unreferenced, err := store.Dashboard(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, unreferenced.Value.Configured, "an unreferenced registry row is not Dashboard configuration")

	item, created, err := store.Start(ctx, workstore.StartInput{ActorIdentityID: actorID, WorkflowKey: "walter/quotation", IdempotencyKey: "notification-1", Title: "Quotation — ACME", Source: json.RawMessage(`{"messageId":"m-1","tenant":"acme"}`), SourcePresentation: sourcePresentation, ArtifactRefs: sourceArtifacts})
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, workstore.StateTodo, item.State)
	assert.True(t, item.ValueConfigured)
	assert.Equal(t, "Quote request", item.SourcePresentation.Title)
	assert.Equal(t, "Marco", item.Presentation.PersonaName)
	require.NotNil(t, item.CurrentStep)
	assert.Equal(t, "Processing quotation", item.CurrentStep.Label.EN)
	customerJSON, err := json.Marshal(item)
	require.NoError(t, err)
	assert.NotContains(t, string(customerJSON), "messageId", "private trigger context must not reach customer APIs")
	listed, err := store.ListWorkItems(ctx, workstore.WorkItemFilter{Search: "ACME", From: timePtr(time.Now().Add(-time.Hour)), To: timePtr(time.Now().Add(time.Hour))})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, item.ID, listed[0].ID)
	dashboard, err := store.Dashboard(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, dashboard.Value.Configured)
	assert.True(t, dashboard.Value.Estimated)
	require.Len(t, dashboard.Value.Models, 1)
	assert.Equal(t, "labor_time_saved", dashboard.Value.Models[0].Formula)
	assert.Equal(t, int64(1200), dashboard.Value.Models[0].BaselineSeconds)
	assert.Equal(t, "30.000000", dashboard.Value.Models[0].LoadedHourlyCost)
	assert.JSONEq(t, `{"source":"customer"}`, string(dashboard.Value.Models[0].Assumptions))
	assert.JSONEq(t, `{"en":"20 minutes at EUR 30/hour"}`, string(dashboard.Value.Models[0].Explanation))

	// The selected-period board keeps all active work and selects terminal work
	// by finished_at. Dashboard counts must use the same reassurance projection.
	_, err = pool.Exec(ctx, `UPDATE work.work_items SET created_at=now()-interval '30 days' WHERE id=$1`, item.ID)
	require.NoError(t, err)
	periodFrom, periodTo := time.Now().Add(-7*24*time.Hour), time.Now().Add(time.Hour)
	listed, err = store.ListWorkItems(ctx, workstore.WorkItemFilter{From: &periodFrom, To: &periodTo})
	require.NoError(t, err)
	require.Len(t, listed, 1, "older active work remains visible")
	dashboard, err = store.Dashboard(ctx, periodFrom, periodTo)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dashboard.Counts[workstore.StateTodo])
	assert.True(t, dashboard.Value.Configured, "visible active work contributes its snapshotted value configuration")

	var valueModel map[string]any
	require.NoError(t, json.Unmarshal(item.ValueModel, &valueModel))
	assert.Equal(t, "labor_time_saved", valueModel["formula"])
	attemptID := item.CurrentAttemptID

	// Publish a newer eligible version after creation. An unversioned replay is
	// still the same caller payload and must return the original item rather
	// than re-resolve latest and conflict. Canonical JSON key ordering is also
	// ignored by the payload hash.
	var specJSON, presentation, caps []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT spec_json,presentation FROM workflow.definitions WHERE key='walter/quotation' AND version='1'`).Scan(&specJSON, &presentation))
	v2, err := workflow.NewStore(pool).RegisterDefinition(ctx, workflow.Definition{Key: "walter/quotation", Version: "2", Digest: "sha256:wf-v2", SpecJSON: specJSON, ValidationStatus: workflow.ValidationValid, ScopeIdentityID: scopeID, SourceType: "graph_email", PoolKey: "default/pool", Presentation: presentation})
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT permitted_tools FROM toolgateway.workflow_pool_bindings WHERE workflow_definition_key='walter/quotation:1'`).Scan(&caps))
	_, err = pool.Exec(ctx, `INSERT INTO toolgateway.workflow_pool_bindings(workflow_definition_key,pool_id,permitted_tools)VALUES($1,$2,$3)`, workflow.DefinitionKey(v2.Key, v2.Version), poolID, caps)
	require.NoError(t, err)

	same, created, err := store.Start(ctx, workstore.StartInput{ActorIdentityID: actorID, WorkflowKey: "walter/quotation", IdempotencyKey: "notification-1", Title: "Quotation — ACME", Source: json.RawMessage(`{"tenant":"acme","messageId":"m-1"}`), SourcePresentation: sourcePresentation, ArtifactRefs: sourceArtifacts})
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, item.ID, same.ID)
	_, _, err = store.Start(ctx, workstore.StartInput{ActorIdentityID: actorID, WorkflowKey: "walter/quotation", IdempotencyKey: "notification-1", Title: "Different", Source: json.RawMessage(`{"messageId":"m-1","tenant":"acme"}`), SourcePresentation: sourcePresentation, ArtifactRefs: sourceArtifacts})
	assert.ErrorIs(t, err, workstore.ErrConflict)

	first, turn, dispatch, err := store.PrepareNode(ctx, attemptID)
	require.NoError(t, err)
	assert.True(t, dispatch)
	assert.Equal(t, "process", first.NodeKey)
	// A succeeded external write makes cyclic re-entry consequential.
	var invocationID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO toolgateway.invocations
		(attempt_id,caller_scope,caller_scope_id,tool_call_id,tool_name,tool_version_digest,idempotency_key,effect_class,pool_id,consequence_summary,state,result_json,finished_at)
		VALUES($1,'turn',$2,'call-write','graph.excel.write','sha256:excel-v1','call-write','idempotent_write',$3,
		       '{"en":"Add quotation ACME to workbook Quotations 2026","pt":"Adicionar cotação ACME ao livro Quotations 2026"}',
		       'succeeded','{"row":184}',now()) RETURNING id::text`, attemptID, turn.ID, poolID).Scan(&invocationID))
	selectedArtifactID := "22222222-2222-4222-8222-222222222222"
	unselectedArtifactID := "33333333-3333-4333-8333-333333333333"
	_, err = pool.Exec(ctx, `
		INSERT INTO artifact.artifacts
			(id,storage_key,source_type,created_by_identity_id,mime_type,size_bytes,digest,state,available_at)
		VALUES ($1,'artifacts/selected','sandbox_publish',$3,'text/plain',8,'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','available',now()),
		       ($2,'artifacts/unselected','sandbox_publish',$3,'text/plain',10,'sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc','available',now())`, selectedArtifactID, unselectedArtifactID, actorID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO work.artifact_links (artifact_id,work_item_id,attempt_id,node_execution_id,role)
		VALUES ($1,$3,$4,$5,'output'),($2,$3,$4,$5,'output')`, selectedArtifactID, unselectedArtifactID, item.ID, attemptID, first.ID)
	require.NoError(t, err)
	require.NoError(t, store.RecordCompletionReport(ctx, turn.ID, workstore.CompletionReport{
		Outcome: "needs_information", Summary: "Destination is missing", Output: json.RawMessage(`{"missing":"destination"}`),
		ArtifactRefs: []workstore.ArtifactRef{{ArtifactID: selectedArtifactID, Role: "output"}},
	}))
	state, err := store.CompleteTurn(ctx, turn.ID, "completed", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "running", state)
	var propagated int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM work.artifact_links l
		JOIN runtime.node_executions n ON n.id=l.node_execution_id
		WHERE l.attempt_id=$1 AND l.artifact_id IN ($2,$3) AND l.role='input' AND n.node_key='information'`,
		attemptID, artifactID, selectedArtifactID).Scan(&propagated))
	assert.Equal(t, 2, propagated, "the current inputs and explicitly selected output propagate")
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM work.artifact_links l
		JOIN runtime.node_executions n ON n.id=l.node_execution_id
		WHERE l.attempt_id=$1 AND l.artifact_id=$2 AND n.node_key='information'`,
		attemptID, unselectedArtifactID).Scan(&propagated))
	assert.Zero(t, propagated, "an unselected published output must not widen the next node's artifact scope")

	// The graph reaches a human node and remains Blocked across store restart.
	_, _, dispatch, err = store.PrepareNode(ctx, attemptID)
	require.NoError(t, err)
	assert.False(t, dispatch)
	restarted := workstore.NewStore(pool)
	blocked, err := restarted.GetWorkItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, workstore.StateBlocked, blocked.State)
	human, err := restarted.OpenBlockerForItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, "artifact", human.Kind)
	assert.JSONEq(t, `{"outcomes":[{"en":"Provide information","pt":"Fornecer informação"}],"fields":[{"key":"information","label":{"en":"Information","pt":"Informação"}}]}`, string(human.ResponsePresentation))
	responseArtifactID := "44444444-4444-4444-8444-444444444444"
	_, err = pool.Exec(ctx, `
		INSERT INTO artifact.artifacts
			(id,storage_key,source_type,created_by_identity_id,mime_type,size_bytes,digest,state,available_at)
		VALUES ($1,'artifacts/human-response','user_upload',$2,'text/csv',12,'sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd','available',now())`, responseArtifactID, actorID)
	require.NoError(t, err)
	_, err = restarted.RespondBlocker(ctx, workstore.BlockerResponseInput{BlockerID: human.ID, ActorIdentityID: actorID, Outcome: "information_provided", Response: json.RawMessage(`{"information":"See attached CSV"}`), ArtifactRefs: []workstore.ArtifactRef{{ArtifactID: responseArtifactID, Metadata: json.RawMessage(`{"name":"destination.csv"}`)}}})
	require.NoError(t, err)

	// Re-entering process is intercepted before dispatch because it previously
	// performed a write. Exact confirmation resumes the same node visit.
	reentered, _, dispatch, err := restarted.PrepareNode(ctx, attemptID)
	require.NoError(t, err)
	assert.False(t, dispatch)
	assert.Equal(t, workstore.NodeBlocked, reentered.State)
	consequence, err := restarted.OpenBlockerForItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, "consequence_confirmation", consequence.Kind)
	assert.NotContains(t, string(consequence.RequiredConsequences), "result")
	assert.NotContains(t, string(consequence.RequiredConsequences), "error")
	assert.NotContains(t, string(consequence.RequiredConsequences), "toolName")
	assert.NotContains(t, string(consequence.RequiredConsequences), "arguments")
	assert.Contains(t, string(consequence.RequiredConsequences), "Add quotation ACME to workbook Quotations 2026")
	_, err = restarted.RespondBlocker(ctx, workstore.BlockerResponseInput{BlockerID: consequence.ID, ActorIdentityID: actorID, Outcome: "confirmed", Response: json.RawMessage(`{}`), ConfirmedInvocationIDs: []string{invocationID}})
	require.NoError(t, err)
	second, turn2, dispatch, err := restarted.PrepareNode(ctx, attemptID)
	require.NoError(t, err)
	assert.True(t, dispatch)
	assert.Equal(t, 2, second.Visit)
	assignment, err := restarted.GetAssignmentContext(ctx, second.ID)
	require.NoError(t, err)
	require.Len(t, assignment.Materializations, 3)
	assert.ElementsMatch(t, []string{artifactID, selectedArtifactID, responseArtifactID}, []string{
		assignment.Materializations[0].ArtifactID, assignment.Materializations[1].ArtifactID,
		assignment.Materializations[2].ArtifactID,
	})
	var handoff map[string]any
	require.NoError(t, json.Unmarshal(second.Context, &handoff))
	assert.NotEmpty(t, handoff["executionHistoryRef"])
	assert.Contains(t, handoff["previous"], "executionId")
	require.NoError(t, restarted.RecordCompletionReport(ctx, turn2.ID, workstore.CompletionReport{Outcome: "completed", Summary: "Quotation processed", Output: json.RawMessage(`{"classification":"pricing"}`)}))
	state, err = restarted.CompleteTurn(ctx, turn2.ID, "completed", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "succeeded", state)
	done, err := restarted.GetWorkItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, workstore.StateDone, done.State)
	require.NotNil(t, done.EstimatedValue)
	assert.Equal(t, "10.000000", *done.EstimatedValue)
	listed, err = restarted.ListWorkItems(ctx, workstore.WorkItemFilter{From: &periodFrom, To: &periodTo})
	require.NoError(t, err)
	require.Len(t, listed, 1, "recently completed work is selected by finished_at")
	dashboard, err = restarted.Dashboard(ctx, periodFrom, periodTo)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dashboard.Counts[workstore.StateDone])
	artifacts, err := restarted.ListArtifacts(ctx, item.ID)
	require.NoError(t, err)
	artifactIDs := make([]string, 0, len(artifacts))
	for _, artifact := range artifacts {
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
	}
	assert.Contains(t, artifactIDs, responseArtifactID, "the customer-supplied blocker artifact remains linked")

	feedback, err := restarted.SaveFeedback(ctx, workstore.FeedbackInput{WorkItemID: item.ID, AttemptID: attemptID, ActorIdentityID: actorID, Category: "incorrect_classification", Explanation: "Should be engineering", CorrectedResult: json.RawMessage(`{"classification":"engineering"}`)})
	require.NoError(t, err)
	feedbackHistory, err := restarted.ListFeedback(ctx, item.ID)
	require.NoError(t, err)
	require.Len(t, feedbackHistory, 1)
	assert.Equal(t, "incorrect_classification", feedbackHistory[0].Category)
	assert.Equal(t, "Should be engineering", *feedbackHistory[0].Explanation)
	assert.JSONEq(t, `{"classification":"engineering"}`, string(feedbackHistory[0].CorrectedResult))
	assert.Equal(t, attemptID, feedbackHistory[0].AttemptID)
	assert.Equal(t, actorID, feedbackHistory[0].CreatedBy)
	assert.Nil(t, feedbackHistory[0].RevisedAttemptID)
	persistedFeedback, err := restarted.GetFeedback(ctx, item.ID, feedback.ID)
	require.NoError(t, err)
	assert.Equal(t, feedback.ID, persistedFeedback.ID)
	attempts, err := restarted.ListAttempts(ctx, item.ID)
	require.NoError(t, err)
	assert.Len(t, attempts, 1, "feedback must not start work")
	customerAttempt, err := json.Marshal(attempts[0])
	require.NoError(t, err)
	assert.NotContains(t, string(customerAttempt), "graphSnapshot")
	assert.NotContains(t, string(customerAttempt), "modelsSnapshot")
	assert.NotContains(t, string(customerAttempt), "Process quotation")
	disputed, err := restarted.GetWorkItem(ctx, item.ID)
	require.NoError(t, err)
	assert.True(t, disputed.ValueDisputed)
	assert.Equal(t, "0.000000", *disputed.EstimatedValue)
	_, err = pool.Exec(ctx, `UPDATE work.value_ledger SET amount=1 WHERE work_item_id=$1`, item.ID)
	assert.Error(t, err, "value history must be append-only")
	_, err = pool.Exec(ctx, `DELETE FROM work.timeline_events WHERE work_item_id=$1`, item.ID)
	assert.Error(t, err, "business timeline must be append-only")
	consequences, err := restarted.ConsequencesForItem(ctx, item.ID)
	require.NoError(t, err)
	require.Len(t, consequences, 1)
	assert.Equal(t, invocationID, consequences[0].InvocationID)
	assert.JSONEq(t, `{"en":"Add quotation ACME to workbook Quotations 2026","pt":"Adicionar cotação ACME ao livro Quotations 2026"}`, string(consequences[0].Summary))

	revised, err := restarted.CreateRevision(ctx, workstore.RevisionInput{WorkItemID: item.ID, ActorIdentityID: actorID, FeedbackID: feedback.ID, ActionableGuidance: "Classify using the corrected destination rule", ConfirmedInvocationIDs: []string{invocationID}})
	require.NoError(t, err)
	assert.Equal(t, workstore.StateTodo, revised.State)
	assert.NotEqual(t, attemptID, revised.CurrentAttemptID)
	attempts, err = restarted.ListAttempts(ctx, item.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 2)
	assert.Equal(t, attemptID, *attempts[1].RevisedFromAttemptID)
	feedbackHistory, err = restarted.ListFeedback(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, feedbackHistory[0].RevisedAttemptID)
	assert.Equal(t, revised.CurrentAttemptID, *feedbackHistory[0].RevisedAttemptID)

	// Scope identity remains the workflow identity, never the initiating user.
	assert.Equal(t, scopeID, revised.ScopeIdentityID)
}

func timePtr(value time.Time) *time.Time { return &value }

func seedFoundation(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, string, string) {
	t.Helper()
	var actorID, scopeID, poolID string
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO identity.identities(key,kind,source,display_name)VALUES('user/operator','user','local','Operator')RETURNING id`).Scan(&actorID))
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO identity.identities(key,kind,source,display_name)VALUES('workflow:default/walter','workflow','local','Walter')RETURNING id`).Scan(&scopeID))
	_, err := pool.Exec(ctx, `INSERT INTO catalog.backends(key,name,namespace,kind,model,service_url,image,deployed,healthy)VALUES('default/backend','backend','default','vLLM','model/base','http://model','image',true,true)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO catalog.models(key,namespace,model_id,display_name,context_length,backend_ref,default_params,reasoning_config,available)VALUES('default/model-one','default','model-one','Model One',32768,'backend','{"max_tokens":2048}','{"enable_thinking":false}',true)`)
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO toolgateway.pools(key,name,spiffe_id_prefix)VALUES('default/pool','pool','spiffe://iterabase.local/pools/pool/')RETURNING id`).Scan(&poolID))
	_, err = pool.Exec(ctx, `INSERT INTO toolgateway.pool_grants(pool_id,tool_name,max_effect_class)VALUES($1,'graph.read','read_only'),($1,'graph.excel.write','idempotent_write')`, poolID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO toolgateway.tool_versions
		    (name,version,digest,effect_class,timeout_ms,idempotency_proof,consequence_summary_template)
		VALUES
		    ('graph.read','1','sha256:read-v1','read_only',1000,NULL,'{}'),
		    ('graph.excel.write','1','sha256:excel-v1','idempotent_write',1000,
		     '{"strategy":"upstream_key"}',
		     '{"localized_templates":{"en":"Update the configured workbook","pt":"Atualizar o livro configurado"},"argument_paths":{}}')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO toolgateway.runner_registrations(runner_id,spiffe_id,namespace,tool_name,tool_version,tool_digest,fencing_generation)VALUES
		('runner','spiffe://runner','product','graph.read','1','sha256:read-v1',1),
		('runner','spiffe://runner','product','graph.excel.write','1','sha256:excel-v1',1)`)
	require.NoError(t, err)

	spec := workflow.CanonicalSpec{Key: "walter/quotation", ScopeIdentityKey: "workflow:default/walter", PoolRef: "pool", DefaultModelRef: "model-one", ValueModelRef: "quotation-value",
		Source:                workflow.CanonicalSource{Type: "graph_email"},
		RequestedCapabilities: []workflow.CanonicalCapability{{Tool: "graph.read", MaxEffectClass: "read_only"}, {Tool: "graph.excel.write", MaxEffectClass: "idempotent_write"}},
		Graph: workflow.CanonicalGraph{EntryNode: "process", MaxTransitions: 20,
			Nodes: []workflow.CanonicalNode{
				{Key: "process", Label: workflow.CanonicalLocalizedText{EN: "Processing quotation", PT: "A processar cotação"}, Kind: workflow.NodeAgentTask, Prompt: "Process quotation", Capabilities: []string{"graph.read", "graph.excel.write"}, Outcomes: []string{"completed", "needs_information"}, OutputSchema: json.RawMessage(`{"type":"object"}`)},
				{Key: "information", Label: workflow.CanonicalLocalizedText{EN: "Waiting for artifact", PT: "A aguardar artefacto"}, Kind: workflow.NodeHumanGate, Outcomes: []string{"information_provided"}, HumanGate: &workflow.CanonicalHumanGate{Type: "artifact", Title: workflow.CanonicalLocalizedText{EN: "Artifact required", PT: "Artefacto necessário"}, Description: workflow.CanonicalLocalizedText{EN: "Provide the destination file", PT: "Forneça o ficheiro de destino"}, ResponseSchema: json.RawMessage(`{"type":"object","required":["information"],"properties":{"information":{"type":"string"}}}`), Presentation: workflow.CanonicalHumanGatePresentation{Outcomes: []workflow.CanonicalLocalizedText{{EN: "Provide information", PT: "Fornecer informação"}}, Fields: []workflow.CanonicalHumanGateFieldPresentation{{Key: "information", Label: workflow.CanonicalLocalizedText{EN: "Information", PT: "Informação"}}}}}},
			},
			Edges:            []workflow.CanonicalEdge{{From: "process", Outcome: "needs_information", To: "information"}, {From: "information", Outcome: "information_provided", To: "process"}},
			TerminalOutcomes: []workflow.CanonicalTerminalOutcome{{Node: "process", Outcome: "completed"}}},
		Presentation: workflow.CanonicalPresentation{WorkflowTitle: "Quotation", PersonaName: "Marco", Locale: "en"}}
	specJSON, _ := json.Marshal(spec)
	presentation, _ := json.Marshal(spec.Presentation)
	wfStore := workflow.NewStore(pool)
	def, err := wfStore.RegisterDefinition(ctx, workflow.Definition{Key: "walter/quotation", Version: "1", Digest: "sha256:wf-v1", SpecJSON: specJSON, ValidationStatus: workflow.ValidationValid, ScopeIdentityID: scopeID, SourceType: "graph_email", PoolKey: "default/pool", Presentation: presentation})
	require.NoError(t, err)
	caps, _ := json.Marshal(spec.RequestedCapabilities)
	_, err = pool.Exec(ctx, `INSERT INTO toolgateway.workflow_pool_bindings(workflow_definition_key,pool_id,permitted_tools)VALUES($1,$2,$3)`, workflow.DefinitionKey(def.Key, def.Version), poolID, caps)
	require.NoError(t, err)
	return actorID, scopeID, poolID
}
