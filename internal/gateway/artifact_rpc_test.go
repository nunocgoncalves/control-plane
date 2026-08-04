package gateway_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"sync"
	"testing"

	connect "connectrpc.com/connect"
	artifactstore "github.com/nunocgoncalves/control-plane/internal/artifact"
	v1 "github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1"
	"github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1/gatewayv1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memoryObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemoryObjectStore() *memoryObjectStore {
	return &memoryObjectStore{objects: map[string][]byte{}}
}
func (m *memoryObjectStore) Ready(context.Context) error { return nil }
func (m *memoryObjectStore) Put(_ context.Context, key string, r io.Reader, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.objects[key]; exists {
		return errors.New("overwrite")
	}
	b, err := io.ReadAll(r)
	if err == nil {
		m.objects[key] = b
	}
	return err
}
func (m *memoryObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	if !ok {
		return nil, errors.New("missing")
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), b...))), nil
}
func (m *memoryObjectStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func artifactClient(env *testEnv, cert tls.Certificate) gatewayv1connect.ArtifactServiceClient {
	return gatewayv1connect.NewArtifactServiceClient(mTLSClient(cert, env.caPool), env.srvURL, connect.WithGRPC())
}

func TestArtifactRPCSupervisorScopeAndStreaming(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	run1, turn1, work1, node1, creator1 := seedWorkAttempt(t, env)
	run2, turn2, _, _, _ := seedWorkAttempt(t, env)

	input, err := env.artifacts.Upload(ctx, artifactstore.UploadInput{
		SourceType: artifactstore.SourceUserUpload, CreatedByIdentityID: creator1, MIMEType: "text/plain",
		Scope: &artifactstore.Scope{WorkItemID: work1, AttemptID: run1, NodeExecutionID: node1, Role: "input"},
	}, bytes.NewBufferString("authorized input"))
	require.NoError(t, err)

	client := artifactClient(env, env.supervisor)
	context1 := &v1.ArtifactCallerContext{AttemptId: run1, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turn1, FencingGeneration: 1}
	stream, err := client.GetArtifact(ctx, connect.NewRequest(&v1.GetArtifactRequest{Context: context1, ArtifactId: input.ID}))
	require.NoError(t, err)
	var got bytes.Buffer
	metadata := false
	for stream.Receive() {
		msg := stream.Msg()
		if msg.GetMetadata() != nil {
			metadata = true
		}
		got.Write(msg.GetChunk())
	}
	require.NoError(t, stream.Err())
	assert.True(t, metadata)
	assert.Equal(t, "authorized input", got.String())

	// A valid supervisor assignment for another run cannot use a learned id.
	_, err = client.StatArtifact(ctx, connect.NewRequest(&v1.StatArtifactRequest{Context: &v1.ArtifactCallerContext{
		AttemptId: run2, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turn2, FencingGeneration: 1,
	}, ArtifactId: input.ID}))
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	put := client.PutArtifact(ctx)
	require.NoError(t, put.Send(&v1.PutArtifactRequest{Kind: &v1.PutArtifactRequest_Init{Init: &v1.PutArtifactInit{Context: context1, MimeType: "application/json"}}}))
	require.NoError(t, put.Send(&v1.PutArtifactRequest{Kind: &v1.PutArtifactRequest_Chunk{Chunk: []byte(`{"new":true}`)}}))
	resp, err := put.CloseAndReceive()
	require.NoError(t, err)
	ref := resp.Msg.Metadata.Ref
	require.NotNil(t, ref)
	assert.NotEqual(t, input.ID, ref.ArtifactId)
	linked, err := env.artifacts.Store().LinkedToAttempt(ctx, ref.ArtifactId, run1, node1)
	require.NoError(t, err)
	assert.True(t, linked)
}

func seedWorkAttempt(t *testing.T, env *testEnv) (runID, turnID, workItemID, nodeID, identityID string) {
	t.Helper()
	ctx := context.Background()
	sid := shortID()
	require.NoError(t, env.pgpool.QueryRow(ctx, `INSERT INTO identity.identities (key,kind,source,display_name) VALUES ($1,'workflow','local',$1) RETURNING id::text`, "artifact-"+sid).Scan(&identityID))
	var definitionID string
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO workflow.definitions (key,version,digest,spec_json,scope_identity_id,source_type,pool_key)
		VALUES ($1,'1','sha256:test','{}',$2,'operator_artifact',$3) RETURNING id::text`, "artifact-"+sid, identityID, poolKey).Scan(&definitionID))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO runtime.workflow_runs (kind,definition_key,scope_identity_id,session_id,session_dir,state,started_at)
		VALUES ('workflow',$1,$2,$3,$4,'running',now()) RETURNING id::text`, "artifact-"+sid+":1", identityID, sid, "/sessions/"+sid).Scan(&runID))
	require.NoError(t, env.store.UpsertRunPoolAssignment(ctx, runID, env.poolID))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO work.work_items (workflow_key,scope_identity_id,title,start_identity_id,start_idempotency_key,start_payload_hash)
		VALUES ($1,$2,'Artifact work',$2,$3,$3) RETURNING id::text`, "artifact-"+sid, identityID, sid).Scan(&workItemID))
	_, err := env.pgpool.Exec(ctx, `
		INSERT INTO work.attempts (id,work_item_id,number,definition_id,definition_key,definition_version,definition_digest,graph_snapshot)
		VALUES ($1,$2,1,$3,$4,'1','sha256:test','{}')`, runID, workItemID, definitionID, "artifact-"+sid+":1")
	require.NoError(t, err)
	_, err = env.pgpool.Exec(ctx, `UPDATE work.work_items SET current_attempt_id=$1 WHERE id=$2`, runID, workItemID)
	require.NoError(t, err)
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO runtime.node_executions (attempt_id,node_key,visit,execution_seq,kind,state)
		VALUES ($1,'node',1,1,'agent_task','running') RETURNING id::text`, runID).Scan(&nodeID))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO runtime.turns (run_id,session_id,node_execution_id,state,started_at)
		VALUES ($1,$2,$3,'running',now()) RETURNING id::text`, runID, sid, nodeID).Scan(&turnID))
	_, err = env.pgpool.Exec(ctx, `
		INSERT INTO runtime.turn_assignments
		(turn_id,run_id,pool_id,worker_id,fencing_generation,attempt_id,scope_identity_id,agent_pool_key,work_item_id,node_execution_id,state)
		VALUES ($1::uuid,$2::uuid,$3::uuid,'worker-1',1,$2::text,$4::uuid,$5,$6::uuid,$7::uuid,'active')`, turnID, runID, env.poolID, identityID, poolKey, workItemID, nodeID)
	require.NoError(t, err)
	return
}
