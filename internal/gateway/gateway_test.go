package gateway_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	artifactstore "github.com/nunocgoncalves/control-plane/internal/artifact"
	"github.com/nunocgoncalves/control-plane/internal/gateway"
	v1 "github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1"
	"github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1/gatewayv1connect"
	"github.com/nunocgoncalves/control-plane/internal/spiffe/testca"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/durationpb"
)

// trustDomain for the hermetic test suite.
const td = "iterabase.local"

// testEnv assembles a hermetic gateway: Postgres + seeded grants/bindings +
// mTLS server with in-memory CA + the cert material for each caller class.
type testEnv struct {
	store      *gateway.Store
	pgpool     *pgxpool.Pool // raw pool for seeding runtime/identity rows
	srvURL     string
	supervisor tls.Certificate // spiffe://td/pools/pool-1/workers/worker-1
	runner     tls.Certificate // spiffe://td/tool-runners/ns-1/runner-1
	wfStep     tls.Certificate // spiffe://td/control-plane/workflow-runtime
	caPool     *x509.CertPool
	poolID     string // the seeded pool's uuid
	artifacts  *artifactstore.Service
	stop       func()
}

const (
	poolKey      = "ns/pool-1"
	poolPrefix   = "spiffe://iterabase.local/pools/pool-1"
	runnerSpiffe = "spiffe://iterabase.local/tool-runners/ns-1/runner-1"
	supvSpiffe   = "spiffe://iterabase.local/pools/pool-1/workers/worker-1"
	wfSpiffe     = "spiffe://iterabase.local/control-plane/workflow-runtime"
	wfKey        = "wf-quote"
)

var idCounter atomic.Uint64

func shortID() string { return fmt.Sprintf("t%d", idCounter.Add(1)) }

func newTestEnv(t *testing.T, secrets *gateway.FakeSecretResolver) *testEnv {
	t.Helper()
	if secrets == nil {
		secrets = gateway.NewFakeSecretResolver()
	}
	pgpool := testutil.NewPostgresPool(t)
	store := gateway.NewStore(pgpool)

	// Seed: pool, grants, credential binding, approved runner, workflow binding.
	ctx := context.Background()
	p, err := store.UpsertPool(ctx, poolKey, "pool-1", poolPrefix)
	require.NoError(t, err)
	require.NoError(t, store.UpsertPoolGrant(ctx, p.ID, "echo", gateway.EffectReadOnly, nil))
	require.NoError(t, store.UpsertPoolGrant(ctx, p.ID, "send_email", gateway.EffectNonIdempotentWrite, nil))
	require.NoError(t, store.UpsertPoolGrant(ctx, p.ID, "upsert_row", gateway.EffectIdempotentWrite, nil))
	// Credential binding for send_email (bearer slot "graph_token").
	require.NoError(t, store.UpsertCredentialBinding(ctx, p.ID, "send_email", "graph_token",
		gateway.CredBearer,
		[]byte(`{"value_ref":{"name":"graph","key":"token"}}`), []byte(`{"mailbox":"walter-inbox"}`)))
	secrets.Set("graph", "token", "sekret-token-value")
	// Approved runner. allowed_tool_namespaces is the explicit set of tool
	// namespaces used across the shared env (echo / send_email / upsert_row
	// have no '.' separator, so their namespace is the full name; graph.* maps
	// to "graph"). ARCH-015 fail-closed: an empty list would deny all
	// registrations, so the shared env declares the permitted set explicitly;
	// TestGateway_ToolNamespaceEnforced / TestGateway_NamespaceDenyOnEmptyApproval
	// cover restriction and denial.
	require.NoError(t, store.UpsertApprovedRunner(ctx, "ns-1", "runner-1", runnerSpiffe, []string{"echo", "send_email", "upsert_row", "graph", "danger"}))
	// Workflow-step binding (workflow "wf-quote" -> pool-1, permits echo+send_email).
	require.NoError(t, store.UpsertWorkflowPoolBinding(ctx, wfKey, p.ID, []gateway.Capability{
		{Tool: "echo", MaxEffectClass: string(gateway.EffectNonIdempotentWrite)},
		{Tool: "send_email", MaxEffectClass: string(gateway.EffectNonIdempotentWrite)},
	}))

	// In-memory CA + certs.
	ca, err := testca.New()
	require.NoError(t, err)
	serverCert, err := ca.Leaf(testca.LeafOpts{SPIFFEID: "spiffe://" + td + "/control-plane/tool-gateway", DNSNames: []string{"localhost", "127.0.0.1"}, IsServer: true})
	require.NoError(t, err)
	supervisor, err := ca.Leaf(testca.LeafOpts{SPIFFEID: supvSpiffe})
	require.NoError(t, err)
	runner, err := ca.Leaf(testca.LeafOpts{SPIFFEID: runnerSpiffe})
	require.NoError(t, err)
	wfStep, err := ca.Leaf(testca.LeafOpts{SPIFFEID: wfSpiffe})
	require.NoError(t, err)

	svc := gateway.NewService(store, secrets, gateway.NewFakeOAuthAcquirer("oauth-token"), gateway.Config{TrustDomain: td, DispatchLease: 2 * time.Second}, nil)
	artifacts := artifactstore.NewService(artifactstore.NewStore(pgpool), newMemoryObjectStore(), artifactstore.Config{MaxSize: 1 << 20}, nil)
	svc.SetArtifactService(artifacts)

	mux := http.NewServeMux()
	idmw := gateway.IdentityMiddleware(td)
	rp, rh := gatewayv1connect.NewRunnerServiceHandler(svc)
	gp, gh := gatewayv1connect.NewGatewayServiceHandler(svc)
	ap, ah := gatewayv1connect.NewArtifactServiceHandler(svc)
	mux.Handle(rp, idmw(rh))
	mux.Handle(gp, idmw(gh))
	mux.Handle(ap, idmw(ah))

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
	stop := func() { _ = httpSrv.Shutdown(context.Background()) }
	t.Cleanup(stop)

	return &testEnv{
		store: store, pgpool: pgpool, srvURL: fmt.Sprintf("https://localhost:%d", port),
		supervisor: supervisor, runner: runner, wfStep: wfStep, caPool: ca.Pool, poolID: p.ID, artifacts: artifacts, stop: stop,
	}
}

// mTLSClient builds an HTTP/2 client carrying the given leaf cert.
func mTLSClient(cert tls.Certificate, caPool *x509.CertPool) *http.Client {
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	}
	return &http.Client{Transport: &http2.Transport{TLSClientConfig: tlsCfg}}
}

func gatewayClient(env *testEnv, cert tls.Certificate) gatewayv1connect.GatewayServiceClient {
	return gatewayv1connect.NewGatewayServiceClient(mTLSClient(cert, env.caPool), env.srvURL, connect.WithGRPC())
}

// seedTurnAttempt creates a chat run + active turn + run->pool assignment and
// snapshots the attempt's tools. Must be called AFTER a runner has registered
// (SnapshotAttemptTools resolves healthy versions). Returns runID (attempt_id)
// and turnID (caller_scope_id).
func seedTurnAttempt(t *testing.T, env *testEnv) (runID, turnID string) {
	t.Helper()
	ctx := context.Background()
	sid := shortID()
	var identID string
	require.NoError(t, env.pgpool.QueryRow(ctx,
		`INSERT INTO identity.identities (key,kind,source,display_name) VALUES ($1,'workflow','local',$1) RETURNING id`,
		"ident-"+sid).Scan(&identID))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO runtime.workflow_runs (kind, definition_key, scope_identity_id, session_id, session_dir, state, started_at)
		VALUES ('chat', NULL, $1, $2, $3, 'running', now()) RETURNING id::text`,
		identID, sid, "/sessions/"+sid).Scan(&runID))
	require.NoError(t, env.store.UpsertRunPoolAssignment(ctx, runID, env.poolID))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO runtime.turns (run_id, session_id, state, started_at)
		VALUES ($1::uuid, $2, 'running', now()) RETURNING id::text`,
		runID, sid).Scan(&turnID))
	// Active assignment (HOR-249): the verified supervisor cert is
	// spiffe://.../workers/worker-1; the gateway cross-checks the turn's active
	// assignment worker_id against it.
	_, err := env.pgpool.Exec(ctx, `
		INSERT INTO runtime.turn_assignments
		    (turn_id, run_id, pool_id, worker_id, fencing_generation, attempt_id,
		     scope_identity_id, agent_pool_key, state)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'worker-1', 1, $2, $4::uuid, 'pool-1', 'active')`,
		turnID, runID, env.poolID, identID)
	require.NoError(t, err)
	require.NoError(t, env.store.SnapshotAttemptTools(ctx, runID, env.poolID, nil))
	return runID, turnID
}

// seedWorkflowStepAttempt creates a workflow run (definition_key=wf-quote) + a
// running run_step + run->pool assignment and snapshots the attempt's tools
// restricted to the workflow-permitted set.
func seedWorkflowStepAttempt(t *testing.T, env *testEnv, permitted []string) (runID, stepID string) {
	t.Helper()
	ctx := context.Background()
	sid := shortID()
	var identID string
	require.NoError(t, env.pgpool.QueryRow(ctx,
		`INSERT INTO identity.identities (key,kind,source,display_name) VALUES ($1,'workflow','local',$1) RETURNING id`,
		"ident-"+sid).Scan(&identID))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO runtime.workflow_runs (kind, definition_key, scope_identity_id, session_id, session_dir, state, started_at)
		VALUES ('workflow', $1, $2, $3, $4, 'running', now()) RETURNING id::text`,
		wfKey, identID, sid, "/sessions/"+sid).Scan(&runID))
	require.NoError(t, env.store.UpsertRunPoolAssignment(ctx, runID, env.poolID))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO runtime.run_steps (run_id, seq, kind, state, started_at)
		VALUES ($1::uuid, 1, 'agent_task', 'running', now()) RETURNING id::text`,
		runID).Scan(&stepID))
	require.NoError(t, env.store.SnapshotAttemptTools(ctx, runID, env.poolID, permitted))
	return runID, stepID
}

// refRunner is the Go reference runner: connects over mTLS, registers a tool,
// and dispatches Invokes to handler. If handler returns drop=true, the runner
// closes its stream without responding (simulating stream loss -> outcome_unknown).
type refRunner struct {
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	welcome *v1.Welcome
	stream  *connect.BidiStreamForClient[v1.RunnerMessage, v1.RunnerControl]
}

func startRefRunner(t *testing.T, env *testEnv, desc *v1.ToolDescriptor,
	handler func(inv *v1.Invoke) (result *v1.InvokeResult, drop bool)) *refRunner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())
	stream := client.RegisterRunner(ctx)
	rr := &refRunner{cancel: cancel, done: make(chan struct{}), stream: stream}
	go func() {
		defer close(rr.done)
		if err := stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{Descriptor_: desc}}}); err != nil {
			return
		}
		ctrl, err := stream.Receive()
		if err != nil {
			return
		}
		if w := ctrl.GetWelcome(); w != nil {
			rr.mu.Lock()
			rr.welcome = w
			rr.mu.Unlock()
		}
		for {
			ctrl, err := stream.Receive()
			if err != nil {
				return
			}
			if inv := ctrl.GetInvoke(); inv != nil {
				result, drop := handler(inv)
				if drop {
					_ = stream.CloseRequest()
					return
				}
				if result != nil {
					result.InvocationId = inv.InvocationId
					_ = stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_InvokeResult{InvokeResult: result}})
				}
			}
		}
	}()
	// Wait for welcome (registration complete -> tool available).
	require.Eventually(t, func() bool { rr.mu.Lock(); defer rr.mu.Unlock(); return rr.welcome != nil }, 2*time.Second, 10*time.Millisecond, "runner welcome not received")
	return rr
}

func (rr *refRunner) close() {
	rr.cancel()
	if rr.stream != nil {
		_ = rr.stream.CloseRequest()
	}
	select {
	case <-rr.done:
	case <-time.After(2 * time.Second):
	}
}

// echoDescriptor is a read_only tool with no credential slots.
func echoDescriptor() *v1.ToolDescriptor {
	return &v1.ToolDescriptor{
		Name: "echo", Version: "1.0.0", Digest: "sha256:echo-1",
		Description: "echo back the args", EffectClass: v1.EffectClass_EFFECT_CLASS_READ_ONLY,
		InputSchema: []byte(`{"type":"object"}`), Timeout: durationPtr(5 * time.Second),
	}
}

func consequenceTemplate(en, pt string, paths map[string]string) *v1.ConsequenceSummaryTemplate {
	return &v1.ConsequenceSummaryTemplate{LocalizedTemplates: map[string]string{"en": en, "pt": pt}, ArgumentPaths: paths}
}

func sendEmailDescriptor() *v1.ToolDescriptor {
	return &v1.ToolDescriptor{
		Name: "send_email", Version: "1.0.0", Digest: "sha256:send-1",
		Description: "send an email", EffectClass: v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE,
		InputSchema: []byte(`{"type":"object"}`), Timeout: durationPtr(5 * time.Second),
		CredentialSlots:            []*v1.CredentialSlot{{Name: "graph_token", Scheme: v1.CredentialScheme_CREDENTIAL_SCHEME_BEARER, Required: true}},
		ConsequenceSummaryTemplate: consequenceTemplate("Send an email to {{recipient}}", "Enviar um email para {{recipient}}", map[string]string{"recipient": "/to"}),
	}
}

func upsertRowDescriptor(proof bool) *v1.ToolDescriptor {
	d := &v1.ToolDescriptor{
		Name: "upsert_row", Version: "1.0.0", Digest: "sha256:upsert-1",
		Description: "idempotent upsert", EffectClass: v1.EffectClass_EFFECT_CLASS_IDEMPOTENT_WRITE,
		InputSchema: []byte(`{"type":"object"}`), Timeout: durationPtr(5 * time.Second),
		ConsequenceSummaryTemplate: consequenceTemplate("Update the configured row", "Atualizar a linha configurada", nil),
	}
	if proof {
		d.IdempotencyProof = &v1.IdempotencyProof{Strategy: "upstream_key", UpstreamKeyHeader: "Idempotency-Key"}
	}
	return d
}

// ---- tests ----

func TestGateway_RegistrationDiscoveryAndInvoke(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: inv.ArgumentsJson}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)

	// Discovery: supervisor sees the registered+pinned echo tool.
	dresp, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
	}))
	require.NoError(t, err)
	var names []string
	for _, d := range dresp.Msg.Descriptors {
		names = append(names, d.Name)
	}
	assert.Contains(t, names, "echo")

	// Invoke echo -> succeeded with the echoed args.
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{"msg":"hi"}`), IdempotencyKey: "k1",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State)
	assert.Equal(t, `{"msg":"hi"}`, string(iresp.Msg.ResultJson))
}

func TestGateway_StaticRunnerApprovalReconciliation(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	staticID := "spiffe://iterabase.local/tool-runners/ns-1/overlay-tools"
	require.NoError(t, env.store.ReconcileStaticApprovedRunners(ctx, []gateway.ApprovedRunner{{
		Namespace: "ns-1", RunnerID: "overlay-tools", SpiffeID: staticID,
		AllowedToolNamespaces: []string{"platform"},
	}}))
	approval, err := env.store.IsApprovedRunner(ctx, staticID)
	require.NoError(t, err)
	require.Equal(t, []string{"platform"}, approval.AllowedToolNamespaces)

	require.NoError(t, env.store.ReconcileStaticApprovedRunners(ctx, nil))
	_, err = env.store.IsApprovedRunner(ctx, staticID)
	require.ErrorIs(t, err, gateway.ErrNotFound)
	// Existing operator-managed test approval is not owned/deactivated by the
	// static deployment reconciler.
	_, err = env.store.IsApprovedRunner(ctx, runnerSpiffe)
	require.NoError(t, err)
}

func TestGateway_GenerationDrainPreservesPinsAndStopsNewSnapshots(t *testing.T) {
	env := newTestEnv(t, nil)
	desc := echoDescriptor()
	runner := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer runner.close()
	runID, _ := seedTurnAttempt(t, env)

	runner.mu.Lock()
	gen := int64(runner.welcome.FencingGeneration) //nolint:gosec // test generation is tiny
	runner.mu.Unlock()
	ref := gateway.ToolRef{Name: desc.Name, Digest: desc.Digest}
	require.NoError(t, env.store.BeginDrain(context.Background(), "runner-1", gen, []gateway.ToolRef{ref}))

	pinned, releasable, err := env.store.DrainingStatus(context.Background(), "runner-1", gen)
	require.NoError(t, err)
	require.Equal(t, []gateway.ToolRef{ref}, pinned)
	require.Empty(t, releasable)

	// Draining registrations are not eligible for a new attempt snapshot.
	err = env.store.SnapshotAttemptTools(context.Background(), "new-attempt", env.poolID, []string{desc.Name})
	require.ErrorContains(t, err, "no eligible healthy version")

	// Once the old attempt releases its pin, the exact version is releasable.
	_, err = env.pgpool.Exec(context.Background(), `DELETE FROM toolgateway.attempt_tool_pins WHERE attempt_id=$1`, runID)
	require.NoError(t, err)
	pinned, releasable, err = env.store.DrainingStatus(context.Background(), "runner-1", gen)
	require.NoError(t, err)
	require.Empty(t, pinned)
	require.Equal(t, []gateway.ToolRef{ref}, releasable)
	require.NoError(t, env.store.RetireVersions(context.Background(), "runner-1", gen, releasable))
}

func TestGateway_ReconnectPreservesDrainingRegistrationState(t *testing.T) {
	env := newTestEnv(t, nil)
	desc := echoDescriptor()
	runner := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer runner.close()
	runner.mu.Lock()
	gen := int64(runner.welcome.FencingGeneration) //nolint:gosec // test generation is tiny
	runner.mu.Unlock()
	ref := gateway.ToolRef{Name: desc.Name, Digest: desc.Digest}
	require.NoError(t, env.store.BeginDrain(context.Background(), "runner-1", gen, []gateway.ToolRef{ref}))

	reconnected, err := env.store.UpsertRunnerRegistration(context.Background(), gateway.RunnerRegistration{
		RunnerID: "runner-1", SpiffeID: runnerSpiffe, Namespace: "ns-1", ToolName: desc.Name,
		ToolVersion: desc.Version, ToolDigest: desc.Digest, FencingGeneration: gen + 1,
	})
	require.NoError(t, err)
	assert.False(t, reconnected.AcceptingNew, "reconnect must not expose a draining version to new snapshots")

	require.NoError(t, env.store.RetireVersions(context.Background(), "runner-1", gen+1, []gateway.ToolRef{ref}))
	reintroduced, err := env.store.UpsertRunnerRegistration(context.Background(), gateway.RunnerRegistration{
		RunnerID: "runner-1", SpiffeID: runnerSpiffe, Namespace: "ns-1", ToolName: desc.Name,
		ToolVersion: desc.Version, ToolDigest: desc.Digest, FencingGeneration: gen + 2,
	})
	require.NoError(t, err)
	assert.True(t, reintroduced.AcceptingNew, "deliberate reintroduction after retirement must accept new snapshots")
	require.NoError(t, env.store.SnapshotAttemptTools(context.Background(), "reactivated-attempt", env.poolID, []string{desc.Name}))
}

func TestGateway_GenerationRollbackReactivatesStillLoadedVersion(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	desc := echoDescriptor()
	runner := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer runner.close()
	runner.mu.Lock()
	gen := int64(runner.welcome.FencingGeneration) //nolint:gosec // test generation is tiny
	runner.mu.Unlock()

	v1 := gateway.ToolRef{Name: desc.Name, Digest: desc.Digest}
	require.NoError(t, env.store.BeginDrain(ctx, "runner-1", gen, []gateway.ToolRef{v1}))
	v2 := gateway.ToolVersion{
		Name: desc.Name, Version: "2.0.0", Digest: "sha256:echo-2", Description: desc.Description,
		InputSchema: desc.InputSchema, EffectClass: gateway.EffectReadOnly,
		CredentialSlots: []byte(`[]`), ArtifactCapabs: []byte(`{}`), TimeoutMS: 5000,
		ConsequenceTemplate: []byte(`{}`),
	}
	_, err := env.store.RegisterToolVersion(ctx, v2)
	require.NoError(t, err)
	_, err = env.store.UpsertRunnerRegistration(ctx, gateway.RunnerRegistration{
		RunnerID: "runner-1", SpiffeID: runnerSpiffe, Namespace: "ns-1", ToolName: v2.Name,
		ToolVersion: v2.Version, ToolDigest: v2.Digest, FencingGeneration: gen,
	})
	require.NoError(t, err)

	// Rolling back while v1 is still loaded makes v1 current and v2 draining.
	// The lifecycle update must flip both states in one transaction.
	require.NoError(t, env.store.BeginDrain(ctx, "runner-1", gen, []gateway.ToolRef{{Name: v2.Name, Digest: v2.Digest}}))
	var v1Accepting, v2Accepting bool
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		SELECT accepting_new FROM toolgateway.runner_registrations
		WHERE runner_id='runner-1' AND fencing_generation=$1 AND tool_digest=$2 AND active`, gen, v1.Digest).Scan(&v1Accepting))
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		SELECT accepting_new FROM toolgateway.runner_registrations
		WHERE runner_id='runner-1' AND fencing_generation=$1 AND tool_digest=$2 AND active`, gen, v2.Digest).Scan(&v2Accepting))
	assert.True(t, v1Accepting, "the deliberately reactivated exact version must accept new snapshots")
	assert.False(t, v2Accepting, "the superseded version must drain")
	require.NoError(t, env.store.SnapshotAttemptTools(ctx, "rollback-attempt", env.poolID, []string{desc.Name}))
}

// TestGateway_FencingGenerationBinding (HOR-249 / DEC-041): the gateway binds
// authorization to the verified worker AND the current fencing generation. A
// request carrying a stale/wrong generation is denied even with a valid
// same-pod supervisor cert; the correct generation is accepted.
func TestGateway_FencingGenerationBinding(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env) // assignment fencing_generation = 1
	gc := gatewayClient(env, env.supervisor)

	// Wrong (stale) generation -> denied.
	_, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 2,
	}))
	assert.Error(t, err, "stale fencing generation must be denied (DEC-041)")

	// Correct generation -> accepted.
	dresp, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, dresp.Msg.Descriptors)
}

func TestGateway_DuplicateInvocationReturnsCommittedResult(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{"ok":true}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	req := &v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "dup-key",
	}
	first, err := gc.InvokeTool(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	require.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, first.Msg.State)

	// Duplicate (same key) -> returns the committed result, not a re-execution.
	second, err := gc.InvokeTool(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, second.Msg.State)
	assert.Equal(t, first.Msg.InvocationId, second.Msg.InvocationId, "duplicate must return the same invocation id")
	assert.JSONEq(t, `{"ok":true}`, string(second.Msg.ResultJson), "duplicate returns the committed result")
}

func TestGateway_PermissionDenialFailsClosed(t *testing.T) {
	env := newTestEnv(t, nil)
	// Register a tool NOT granted to the pool. It will not be pinned (snapshot
	// intersects with grants), so invocation is rejected fail-closed.
	rr := startRefRunner(t, env, &v1.ToolDescriptor{
		Name: "danger", Version: "1.0.0", Digest: "sha256:danger-1",
		EffectClass: v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE, InputSchema: []byte(`{}`),
		ConsequenceSummaryTemplate: consequenceTemplate("Perform the configured dangerous action", "Executar a ação perigosa configurada", nil),
	}, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		t.Error("denied tool must not be dispatched to the runner")
		return nil, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	_, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "danger", ToolVersionDigest: "sha256:danger-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "k",
	}))
	require.Error(t, err, "ungranted/unpinned tool must be rejected fail-closed")
	assert.Contains(t, err.Error(), "not pinned")
}

func TestGateway_ActionDenialFailsClosed(t *testing.T) {
	env := newTestEnv(t, nil)
	// Grant echo with an action allow-list that excludes echo's action.
	ctx := context.Background()
	require.NoError(t, env.store.UpsertPoolGrant(ctx, env.poolID, "echo", gateway.EffectReadOnly, []byte(`["other_action"]`)))
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		t.Error("action-denied tool must not be dispatched")
		return nil, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "k",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_FAILED, iresp.Msg.State)
	assert.Equal(t, "permission_denied", iresp.Msg.Error.Code)
}

func TestGateway_VersionPinningNoSubstitution(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	// Invoke with a digest that differs from the pinned one.
	_, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:does-not-exist",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "k",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match the pinned digest")
}

func TestGateway_CredentialBindingResolution(t *testing.T) {
	env := newTestEnv(t, nil)
	var seenCred *v1.CredentialContext
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		seenCred = inv.CredentialContext
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{"sent":true}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
		ArgumentsJson: []byte(`{"to":"x@y"}`), IdempotencyKey: "send-1",
	}))
	require.NoError(t, err)
	require.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State)
	require.NotNil(t, seenCred)
	assert.Equal(t, "sekret-token-value", seenCred.Slots["graph_token"].BearerValue)
	invocation, err := env.store.GetInvocation(context.Background(), iresp.Msg.InvocationId)
	require.NoError(t, err)
	assert.JSONEq(t, `{"en":"Send an email to x@y","pt":"Enviar um email para x@y"}`, string(invocation.ConsequenceSummary))
	assert.NotContains(t, string(invocation.ConsequenceSummary), "sekret-token-value")
}

func TestGateway_ConsequenceSummariesDistinguishRecipients(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{"sent":true}`)}, false
	})
	defer rr.close()

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	summaries := make([]string, 0, 2)
	for index, recipient := range []string{"buyer-a@example.test", "buyer-b@example.test"} {
		resp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
			AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
			ToolCallId: fmt.Sprintf("call-%d", index), ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
			ArgumentsJson: []byte(fmt.Sprintf(`{"to":%q}`, recipient)), IdempotencyKey: fmt.Sprintf("send-%d", index),
		}))
		require.NoError(t, err)
		invocation, err := env.store.GetInvocation(context.Background(), resp.Msg.InvocationId)
		require.NoError(t, err)
		summaries = append(summaries, string(invocation.ConsequenceSummary))
	}
	assert.NotEqual(t, summaries[0], summaries[1])
	assert.Contains(t, summaries[0], "buyer-a@example.test")
	assert.Contains(t, summaries[1], "buyer-b@example.test")
}

func TestGateway_CredentialSlotMismatchRejected(t *testing.T) {
	env := newTestEnv(t, nil)
	// Add an UNDECLARED binding for send_email (extra slot the descriptor did
	// not declare) -> resolution must fail closed.
	ctx := context.Background()
	require.NoError(t, env.store.UpsertCredentialBinding(ctx, env.poolID, "send_email", "rogue_slot",
		gateway.CredBearer, []byte(`{"value_ref":{"name":"graph","key":"token"}}`), nil))
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		t.Error("slot-mismatched tool must not be dispatched")
		return nil, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
		ArgumentsJson: []byte(`{"to":"x@y"}`), IdempotencyKey: "send-1",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_FAILED, iresp.Msg.State)
	assert.Equal(t, "credential_resolution_failed", iresp.Msg.Error.Code)
}

func TestGateway_AmbiguousOutcomeNotRepeated(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return nil, true // drop stream
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	req := &v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
		ArgumentsJson: []byte(`{"to":"x@y"}`), IdempotencyKey: "send-once",
	}
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN, iresp.Msg.State,
		"a possible effect with no committed result must become outcome_unknown")

	// The duplicate must NOT repeat the effect; it returns outcome_unknown.
	second, err := gc.InvokeTool(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN, second.Msg.State)
	assert.Equal(t, iresp.Msg.InvocationId, second.Msg.InvocationId, "outcome_unknown invocation is not repeated")
	invocation, err := env.store.GetInvocation(context.Background(), iresp.Msg.InvocationId)
	require.NoError(t, err)
	assert.JSONEq(t, `{"en":"Send an email to x@y","pt":"Enviar um email para x@y"}`, string(invocation.ConsequenceSummary), "summary is committed before an ambiguous effect")
}

func TestGateway_ReadOnlyRetryOnStreamLoss(t *testing.T) {
	env := newTestEnv(t, nil)
	var calls int32
	desc := echoDescriptor()
	desc.Timeout = durationPtr(150 * time.Millisecond) // short: a non-response times out -> streamLost -> retry
	rr := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return nil, false // ignore first dispatch; let it time out (retryable for read_only)
		}
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{"ok":true}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "k",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State, "read_only retries after stream loss")
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2))
}

func TestGateway_ProvenIdempotentRetry(t *testing.T) {
	env := newTestEnv(t, nil)
	var calls int32
	desc := upsertRowDescriptor(true)
	desc.Timeout = durationPtr(150 * time.Millisecond)
	rr := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return nil, false // ignore first dispatch; proven idempotent -> retryable
		}
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{"ok":true}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "upsert_row", ToolVersionDigest: "sha256:upsert-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "up-1",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State, "proven idempotent_write retries after stream loss")
}

func TestGateway_IdempotentWriteWithoutProofRejected(t *testing.T) {
	env := newTestEnv(t, nil)
	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())
	stream := client.RegisterRunner(context.Background())
	err := stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{
		Descriptor_: upsertRowDescriptor(false), // no proof
	}}})
	require.NoError(t, err)
	_, err = stream.Receive()
	require.Error(t, err, "idempotent_write without proof must be rejected at registration")
}

func TestGateway_WriteWithoutConsequenceTemplateRejected(t *testing.T) {
	env := newTestEnv(t, nil)
	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())
	stream := client.RegisterRunner(context.Background())
	err := stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{
		Descriptor_: &v1.ToolDescriptor{
			Name: "send_email", Version: "2.0.0", Digest: "sha256:send-2",
			EffectClass: v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE, InputSchema: []byte(`{"type":"object"}`),
		},
	}}})
	require.NoError(t, err)
	_, err = stream.Receive()
	require.Error(t, err, "writes without trusted en/pt consequence templates must be rejected")
	assert.Contains(t, err.Error(), "consequence summary templates")
}

func TestGateway_MigratedWriteDigestRequiresRepublish(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	_, err := env.pgpool.Exec(ctx, `
		INSERT INTO toolgateway.tool_versions
		    (name,version,digest,description,input_schema,effect_class,credential_slots,
		     artifact_capabilities,timeout_ms,consequence_summary_template)
		VALUES ('legacy.write','1.0.0','sha256:legacy-write','Legacy write','{}',
		        'non_idempotent_write','[]','{}',5000,'{}')`)
	require.NoError(t, err)

	_, err = env.store.RegisterToolVersion(ctx, gateway.ToolVersion{
		Name: "legacy.write", Version: "1.0.0", Digest: "sha256:legacy-write",
		Description: "Legacy write", InputSchema: []byte(`{}`),
		EffectClass: gateway.EffectNonIdempotentWrite, CredentialSlots: []byte(`[]`),
		ArtifactCapabs: []byte(`{}`), TimeoutMS: 5000,
		ConsequenceTemplate: []byte(`{"localized_templates":{"en":"Update legacy record","pt":"Atualizar registo legado"},"argument_paths":{}}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish a new version and digest")
}

func TestGateway_EmptyEffectClassRejected(t *testing.T) {
	env := newTestEnv(t, nil)
	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())
	stream := client.RegisterRunner(context.Background())
	err := stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{
		Descriptor_: &v1.ToolDescriptor{
			Name: "bad", Version: "1.0.0", Digest: "sha256:bad-1",
			EffectClass: v1.EffectClass_EFFECT_CLASS_UNSPECIFIED, InputSchema: []byte(`{}`),
		},
	}}})
	require.NoError(t, err)
	_, err = stream.Receive()
	require.Error(t, err, "unspecified effect_class must be rejected at registration")
}

func TestGateway_SchemaValidation(t *testing.T) {
	env := newTestEnv(t, nil)
	desc := &v1.ToolDescriptor{
		Name: "echo", Version: "1.0.0", Digest: "sha256:echo-1",
		EffectClass: v1.EffectClass_EFFECT_CLASS_READ_ONLY,
		InputSchema: []byte(`{"type":"object","required":["msg"],"properties":{"msg":{"type":"string"}},"additionalProperties":false}`),
		Timeout:     durationPtr(5 * time.Second),
	}
	rr := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: inv.ArgumentsJson}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	// Missing required field -> invalid_arguments (pre-effect, not dispatched).
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{"other":1}`), IdempotencyKey: "k",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_FAILED, iresp.Msg.State)
	assert.Equal(t, "invalid_arguments", iresp.Msg.Error.Code)
}

func TestGateway_PostDispatchContextCancel(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		<-time.After(500 * time.Millisecond) // never responds promptly
		return nil, true
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	// Caller cancels mid-dispatch (write): a possible effect -> outcome_unknown.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := gc.InvokeTool(ctx, connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
		ArgumentsJson: []byte(`{"to":"x@y"}`), IdempotencyKey: "cancel-1",
	}))
	// The gateway classifies the post-send cancellation as outcome_unknown and
	// returns a response; the connect client may surface a deadline error. The
	// durable invariant is checked directly below.
	_ = err
	// Wait for the ledger to settle, then assert the row is outcome_unknown.
	time.Sleep(800 * time.Millisecond)
	inv, gerr := env.store.GetInvocationByKey(context.Background(), gateway.InvocationKey{
		AttemptID: runID, CallerScope: gateway.CallerScopeTurn, CallerScopeID: turnID,
		ToolCallID: "call-1", ToolVersionDigest: "sha256:send-1", IdempotencyKey: "cancel-1",
	})
	require.NoError(t, gerr)
	assert.Equal(t, gateway.InvocationOutcomeUnknown, inv.State,
		"post-send cancellation of a write must be outcome_unknown, not failed")
}

func TestGateway_RestartRecovery(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	// Seed an orphaned running write (lease expired) and an orphaned running
	// read (lease expired), as if the process died mid-dispatch.
	runID, _ := seedTurnAttempt(t, env) // pins echo+send_email for this attempt

	now := time.Now()
	writeInv := seedOrphanInvocation(t, env, runID, gateway.EffectNonIdempotentWrite, now.Add(-time.Hour))
	readInv := seedOrphanInvocation(t, env, runID, gateway.EffectReadOnly, now.Add(-time.Hour))

	recovered, err := env.store.RecoverOrphanedInvocations(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, recovered, 2)

	wInv, err := env.store.GetInvocation(ctx, writeInv)
	require.NoError(t, err)
	assert.Equal(t, gateway.InvocationOutcomeUnknown, wInv.State, "orphaned write -> outcome_unknown")

	rInv, err := env.store.GetInvocation(ctx, readInv)
	require.NoError(t, err)
	assert.Equal(t, gateway.InvocationFailed, rInv.State, "orphaned read_only -> failed")
}

// seedOrphanInvocation inserts a non-terminal invocation row with an expired
// lease, simulating a crash mid-dispatch.
func seedOrphanInvocation(t *testing.T, env *testEnv, attemptID string, ec gateway.EffectClass, expiresAt time.Time) string {
	t.Helper()
	ctx := context.Background()
	var id string
	require.NoError(t, env.pgpool.QueryRow(ctx, `
		INSERT INTO toolgateway.invocations
			(attempt_id, caller_scope, caller_scope_id, tool_call_id, tool_name,
			 tool_version_digest, idempotency_key, effect_class, pool_id, arguments_json,
			 consequence_summary, state, dispatch_lease_expires_at, gateway_instance_id)
		VALUES ($1, 'turn', 'orphan-turn', $2, $3, $4, $5, $6, $7, '{}',
		        '{"en":"Orphan update","pt":"Atualização órfã"}', 'running', $8, 'dead-gw')
		RETURNING id::text`,
		attemptID, "call-"+shortID(), "orphan_tool", "sha256:orphan", "k-"+shortID(), ec, env.poolID, expiresAt).Scan(&id))
	return id
}

func TestGateway_CallerScopeValidation(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		t.Error("scope-invalid call must not be dispatched")
		return nil, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, _ := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	// A turn id that does not exist -> scope denied (fail closed).
	_, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "00000000-0000-0000-0000-000000000000",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scope not authorized")

	// A run id that does not match the turn's run -> scope denied.
	_, turnID := seedTurnAttempt(t, env)
	_, err = gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: "00000000-0000-0000-0000-000000000000", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "k",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scope not authorized")
}

func TestGateway_CancelOwnership(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		<-time.After(2 * time.Second) // hold in-flight
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	// Start an in-flight invocation owned by pool-1's supervisor.
	go func() {
		_, _ = gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
			AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
			ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
			ArgumentsJson: []byte(`{"to":"x@y"}`), IdempotencyKey: "cancel-own",
		}))
	}()

	// Wait for the row to exist + be in-flight.
	var inv gateway.Invocation
	require.Eventually(t, func() bool {
		inv, _ = env.store.GetInvocationByKey(context.Background(), gateway.InvocationKey{
			AttemptID: runID, CallerScope: gateway.CallerScopeTurn, CallerScopeID: turnID,
			ToolCallID: "call-1", ToolVersionDigest: "sha256:send-1", IdempotencyKey: "cancel-own",
		})
		return inv.ID != "" && (inv.State == gateway.InvocationDispatching || inv.State == gateway.InvocationRunning)
	}, 2*time.Second, 20*time.Millisecond)

	// A runner identity cannot cancel.
	rc := gatewayv1connect.NewGatewayServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())
	_, err := rc.CancelInvocation(context.Background(), connect.NewRequest(&v1.CancelRequest{InvocationId: inv.ID, Reason: "r"}))
	require.Error(t, err)

	// The owning supervisor can cancel (returns running state).
	cresp, err := gc.CancelInvocation(context.Background(), connect.NewRequest(&v1.CancelRequest{InvocationId: inv.ID, Reason: "r", FencingGeneration: 1}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_RUNNING, cresp.Msg.State)
}

func TestGateway_WorkflowStepCallerDiscovery(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	// The control-plane workflow-step caller discovers tools for workflow wf-quote.
	runID, stepID := seedWorkflowStepAttempt(t, env, []string{"echo"})
	gc := gatewayClient(env, env.wfStep)
	dresp, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_WORKFLOW_STEP, CallerScopeId: stepID,
	}))
	require.NoError(t, err)
	var names []string
	for _, d := range dresp.Msg.Descriptors {
		names = append(names, d.Name)
	}
	assert.Contains(t, names, "echo")
	assert.NotContains(t, names, "upsert_row", "workflow does not permit upsert_row -> not pinned")
}

// TestGateway_WorkflowEffectClassCeilingNarrowsDiscovery verifies ARCH-016 /
// REQ-001 / REQ-010: a workflow that narrows a tool to a lower effect class
// than the pool-grant ceiling must NOT discover that tool's higher-effect
// version. The workflow's maxEffectClass is enforced at discovery so the
// workflow narrowing is not widened back to the pool ceiling at runtime.
func TestGateway_WorkflowEffectClassCeilingNarrowsDiscovery(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	// Narrow the wf-quote binding: send_email is a non_idempotent_write tool,
	// pool-granted at non_idempotent_write, but the workflow requests it at
	// read_only. The pool ceiling would allow it; the workflow ceiling must
	// exclude it from discovery.
	require.NoError(t, env.store.UpsertWorkflowPoolBinding(context.Background(), wfKey, env.poolID, []gateway.Capability{
		{Tool: "send_email", MaxEffectClass: string(gateway.EffectReadOnly)},
	}))

	runID, stepID := seedWorkflowStepAttempt(t, env, []string{"send_email"})
	gc := gatewayClient(env, env.wfStep)
	dresp, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_WORKFLOW_STEP, CallerScopeId: stepID,
	}))
	require.NoError(t, err)
	var names []string
	for _, d := range dresp.Msg.Descriptors {
		names = append(names, d.Name)
	}
	assert.NotContains(t, names, "send_email", "workflow narrowed to read_only must not discover a non_idempotent_write tool (ARCH-016)")
}

// TestGateway_WorkflowActionCeilingNarrowsDiscoveryAndInvocation verifies the
// workflow action set is enforced, not merely persisted. Under the approved v1
// undecomposed descriptor contract echo's effective action is "echo".
func TestGateway_WorkflowActionCeilingNarrowsDiscoveryAndInvocation(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		t.Error("workflow-action-denied tool must not be dispatched")
		return nil, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	// Pool permits echo, but this workflow explicitly permits a different action.
	require.NoError(t, env.store.UpsertWorkflowPoolBinding(context.Background(), wfKey, env.poolID, []gateway.Capability{
		{Tool: "echo", MaxEffectClass: string(gateway.EffectReadOnly), Actions: []string{"other_action"}},
	}))
	runID, stepID := seedWorkflowStepAttempt(t, env, []string{"echo"})
	gc := gatewayClient(env, env.wfStep)

	dresp, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_WORKFLOW_STEP, CallerScopeId: stepID,
	}))
	require.NoError(t, err)
	assert.Empty(t, dresp.Msg.Descriptors, "workflow action narrowing must filter discovery")

	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_WORKFLOW_STEP, CallerScopeId: stepID,
		ToolCallId: "call-action-denied", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "workflow-action-denied",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_FAILED, iresp.Msg.State)
	assert.Equal(t, "permission_denied", iresp.Msg.Error.Code)
	assert.Contains(t, iresp.Msg.Error.Message, "not permitted by workflow capability")
}

func TestGateway_UnapprovedRunnerRejected(t *testing.T) {
	env := newTestEnv(t, nil)
	// Build a runner cert for an unapproved SPIFFE id.
	ca, err := testca.New()
	require.NoError(t, err)
	rogue, err := ca.Leaf(testca.LeafOpts{SPIFFEID: "spiffe://" + td + "/tool-runners/ns-1/rogue"})
	require.NoError(t, err)
	// The rogue cert is signed by a DIFFERENT CA -> chain verification fails.
	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(rogue, env.caPool), env.srvURL, connect.WithGRPC())
	stream := client.RegisterRunner(context.Background())
	err = stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{Descriptor_: echoDescriptor()}}})
	if err == nil {
		_, err = stream.Receive()
	}
	assert.Error(t, err, "unapproved/untrusted runner must be rejected")
}

// TestGateway_ToolNamespaceEnforced verifies ARCH-015: an approved runner may
// only register descriptors whose tool-name namespace is in its
// allowed_tool_namespaces. The shared env has no restriction; this test
// re-seeds the approval with a restrictive list.
func TestGateway_ToolNamespaceEnforced(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	// Restrict the approved runner to the "graph" tool namespace.
	require.NoError(t, env.store.UpsertApprovedRunner(ctx, "ns-1", "runner-1", runnerSpiffe, []string{"graph"}))

	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())

	// graph.read_mail is permitted.
	ok := startRefRunner(t, env, &v1.ToolDescriptor{
		Name: "graph.read_mail", Version: "1.0.0", Digest: "sha256:grm-1",
		EffectClass: v1.EffectClass_EFFECT_CLASS_READ_ONLY, InputSchema: []byte(`{"type":"object"}`),
		Timeout: durationPtr(5 * time.Second),
	}, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	ok.close()

	// fs.delete is rejected (namespace "fs" not permitted).
	stream := client.RegisterRunner(context.Background())
	require.NoError(t, stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{Descriptor_: &v1.ToolDescriptor{
		Name: "fs.delete", Version: "1.0.0", Digest: "sha256:fsd-1",
		EffectClass: v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE, InputSchema: []byte(`{"type":"object"}`),
		ConsequenceSummaryTemplate: consequenceTemplate("Delete the configured file", "Eliminar o ficheiro configurado", nil),
	}}}}))
	_, err := stream.Receive()
	require.Error(t, err, "tool outside the runner's permitted namespace must be rejected")
	_ = stream.CloseRequest()
}

// TestGateway_ResourceConstraintDenial verifies ARCH-008/018: an argument that
// names a constrained resource dimension with a value outside the binding's
// allowed set is denied before the effect boundary.
func TestGateway_ResourceConstraintDenial(t *testing.T) {
	env := newTestEnv(t, nil)
	// The send_email binding constrains {"mailbox":"walter-inbox"} (seeded in
	// newTestEnv). An argument targeting a different mailbox is denied.
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		t.Error("resource-denied tool must not be dispatched")
		return nil, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
		ArgumentsJson: []byte(`{"to":"x@y","mailbox":"other-inbox"}`), IdempotencyKey: "res-1",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_FAILED, iresp.Msg.State)
	assert.Equal(t, "permission_denied", iresp.Msg.Error.Code)
	assert.Contains(t, iresp.Msg.Error.Message, "mailbox")
}

// TestGateway_LedgerScopeFromDurableResolution verifies ARCH-004/014: the
// ledger key uses the identity-derived caller scope, not the caller-supplied
// value. A supervisor that lies about CallerScope=WORKFLOW_STEP still records a
// 'turn' scope row (and dedup works on the durable scope).
func TestGateway_LedgerScopeFromDurableResolution(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	// Lie about the caller scope: claim WORKFLOW_STEP while authenticated as a
	// supervisor. The gateway must resolve scope from the identity, so the
	// invocation still succeeds and records 'turn'.
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_WORKFLOW_STEP, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "scope-1",
	}))
	require.NoError(t, err)
	require.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State)

	// The ledger row records the durable (identity-derived) scope, not the lie.
	inv, gerr := env.store.GetInvocationByKey(context.Background(), gateway.InvocationKey{
		AttemptID: runID, CallerScope: gateway.CallerScopeTurn, CallerScopeID: turnID,
		ToolCallID: "call-1", ToolVersionDigest: "sha256:echo-1", IdempotencyKey: "scope-1",
	})
	require.NoError(t, gerr)
	assert.Equal(t, gateway.CallerScopeTurn, inv.CallerScope,
		"ledger records the identity-derived scope, not the caller-supplied value")
}

// TestGateway_NamespaceDenyOnEmptyApproval verifies ARCH-015 fail-closed: an
// approved runner whose allowed_tool_namespaces is empty (the default approval
// record) may NOT register any tool — empty is deny, not unrestricted.
func TestGateway_NamespaceDenyOnEmptyApproval(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	// Re-seed the approved runner with an empty namespace list (the default).
	require.NoError(t, env.store.UpsertApprovedRunner(ctx, "ns-1", "runner-1", runnerSpiffe, nil))

	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())
	stream := client.RegisterRunner(context.Background())
	require.NoError(t, stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{Descriptor_: echoDescriptor()}}}))
	_, err := stream.Receive()
	require.Error(t, err, "an approved runner with no permitted tool namespace must be rejected (ARCH-015 fail-closed)")
	_ = stream.CloseRequest()
}

// TestGateway_IdempotentWriteUnprovableStrategyRejected verifies ARCH-014
// fail-closed: an idempotent_write descriptor whose idempotency_proof strategy
// is not gateway-provable in v1 (e.g. resource_identity, which the gateway
// cannot verify without a declared resource-identity argument) is rejected at
// registration rather than becoming auto-retryable.
func TestGateway_IdempotentWriteUnprovableStrategyRejected(t *testing.T) {
	env := newTestEnv(t, nil)
	client := gatewayv1connect.NewRunnerServiceClient(mTLSClient(env.runner, env.caPool), env.srvURL, connect.WithGRPC())
	stream := client.RegisterRunner(context.Background())
	desc := upsertRowDescriptor(true)
	desc.IdempotencyProof = &v1.IdempotencyProof{Strategy: "resource_identity"}
	require.NoError(t, stream.Send(&v1.RunnerMessage{Kind: &v1.RunnerMessage_Register{Register: &v1.Register{Descriptor_: desc}}}))
	_, err := stream.Receive()
	require.Error(t, err, "an unprovable idempotency_proof.strategy must be rejected at registration (ARCH-014 fail-closed)")
	_ = stream.CloseRequest()
}

// TestGateway_RetryRenewsRunningLease verifies ARCH-014/SCN-008: a multi-attempt
// retry of an idempotent_write renews the dispatch lease on the already-running
// row so the recovery sweep cannot terminalize live work mid-retry.
func TestGateway_RetryRenewsRunningLease(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	desc := upsertRowDescriptor(true)
	desc.Timeout = durationPtr(150 * time.Millisecond)
	var calls int32
	rr := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		if atomic.AddInt32(&calls, 1) < 2 {
			return nil, false // stream loss -> proven idempotent retry
		}
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "upsert_row", ToolVersionDigest: "sha256:upsert-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "lease-1",
	}))
	require.NoError(t, err)
	require.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2), "expected at least one retry")

	// The invocation must have reached a terminal committed state, not been
	// recovered as outcome_unknown by a racing sweep (the lease was renewed).
	inv, gerr := env.store.GetInvocation(ctx, iresp.Msg.InvocationId)
	require.NoError(t, gerr)
	assert.Equal(t, gateway.InvocationSucceeded, inv.State, "lease renewal kept the live retry from being recovered")
}

// TestGateway_RetryAbortsWhenLeaseNotRenewed verifies ARCH-014/SCN-008 fail-
// closed behavior: if the SCN-008 recovery sweep terminalizes a row while a
// proven-idempotent retry is in flight (the lease could not be renewed because
// the row is no longer dispatching/running), the retry MUST NOT cross the
// side-effect boundary. The runner is not dispatched a second time and the
// caller receives the committed durable outcome (outcome_unknown), never a
// repeated effect.
func TestGateway_RetryAbortsWhenLeaseNotRenewed(t *testing.T) {
	env := newTestEnv(t, nil)
	ctx := context.Background()
	desc := upsertRowDescriptor(true)
	desc.Timeout = durationPtr(150 * time.Millisecond)
	var calls int32
	var firstErr error
	rr := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// Simulate the recovery sweep terminalizing the row mid-dispatch:
			// expire the lease and run SCN-008 recovery while the first
			// (stream-losing) dispatch is still in flight. The gateway will
			// time out, classify stream loss as retryable, and attempt a retry
			// whose lease renewal must then fail closed.
			if _, err := env.pgpool.Exec(ctx,
				`UPDATE toolgateway.invocations SET dispatch_lease_expires_at = now() - interval '1 hour' WHERE id = $1`,
				inv.InvocationId); err != nil {
				firstErr = err
				return nil, true
			}
			if _, err := env.store.RecoverOrphanedInvocations(ctx); err != nil {
				firstErr = err
				return nil, true
			}
			return nil, false // no result -> gateway times out -> streamLost -> retry
		}
		t.Error("retry must not dispatch after recovery terminalized the row (ARCH-014 fail-closed)")
		return nil, true
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "upsert_row", ToolVersionDigest: "sha256:upsert-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "abort-1",
	}))
	require.NoError(t, firstErr, "test harness: recovery terminalization failed")
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN, iresp.Msg.State,
		"a retry whose lease was not renewed must abort fail-closed, returning the recovered outcome_unknown")
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls),
		"the effect boundary must not be crossed a second time")

	// The durable row is the recovered outcome_unknown; a duplicate must not
	// repeat the effect (REQ-009).
	inv, gerr := env.store.GetInvocation(ctx, iresp.Msg.InvocationId)
	require.NoError(t, gerr)
	assert.Equal(t, gateway.InvocationOutcomeUnknown, inv.State)
}

// TestGateway_IdempotentRetryStableUpstreamKey verifies ARCH-014: an
// idempotent_write retry propagates the SAME durable upstream idempotency key
// (the invocation id) across retries, not the caller's dedup key.
func TestGateway_IdempotentRetryStableUpstreamKey(t *testing.T) {
	env := newTestEnv(t, nil)
	var seenKeys []string
	desc := upsertRowDescriptor(true)
	desc.Timeout = durationPtr(150 * time.Millisecond)
	rr := startRefRunner(t, env, desc, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		seenKeys = append(seenKeys, inv.IdempotencyKey)
		if len(seenKeys) < 2 {
			return nil, false // ignore first dispatch; proven idempotent -> retry
		}
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{"ok":true}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	runID, turnID := seedTurnAttempt(t, env)
	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: runID, CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: turnID, FencingGeneration: 1,
		ToolCallId: "call-1", ToolName: "upsert_row", ToolVersionDigest: "sha256:upsert-1",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "stable-1",
	}))
	require.NoError(t, err)
	require.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State)

	require.Len(t, seenKeys, 2, "expected exactly two dispatches (one retry)")
	assert.Equal(t, seenKeys[0], seenKeys[1], "upstream key must be stable across retries")
	assert.Equal(t, iresp.Msg.InvocationId, seenKeys[0], "upstream key is the durable invocation id, not the caller key")
	assert.NotEqual(t, "stable-1", seenKeys[0], "caller-supplied key is not the upstream key")
}

// durationPtr wraps a duration for a proto descriptors timeout.
func durationPtr(d time.Duration) *durationpb.Duration { return durationpb.New(d) }
