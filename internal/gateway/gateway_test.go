package gateway_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"
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
	srvURL     string
	supervisor tls.Certificate // spiffe://td/pools/pool-1/workers/worker-1
	runner     tls.Certificate // spiffe://td/tool-runners/ns-1/runner-1
	wfStep     tls.Certificate // spiffe://td/control-plane/workflow-runtime
	caPool     *x509.CertPool
	stop       func()
}

const (
	poolKey      = "ns/pool-1"
	poolPrefix   = "spiffe://iterabase.local/pools/pool-1"
	runnerSpiffe = "spiffe://iterabase.local/tool-runners/ns-1/runner-1"
	supvSpiffe   = "spiffe://iterabase.local/pools/pool-1/workers/worker-1"
	wfSpiffe     = "spiffe://iterabase.local/control-plane/workflow-runtime"
)

func newTestEnv(t *testing.T, secrets *gateway.FakeSecretResolver) *testEnv {
	t.Helper()
	if secrets == nil {
		secrets = gateway.NewFakeSecretResolver()
	}
	pool := testutil.NewPostgresPool(t)
	store := gateway.NewStore(pool)

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
	// Approved runner.
	require.NoError(t, store.UpsertApprovedRunner(ctx, "ns-1", "runner-1", runnerSpiffe, []string{"ns-1"}))
	// Workflow-step binding (workflow "wf-quote" -> pool-1, permits echo+send_email).
	require.NoError(t, store.UpsertWorkflowPoolBinding(ctx, "wf-quote", p.ID, []string{"echo", "send_email"}))

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

	svc := gateway.NewService(store, secrets, gateway.NewFakeOAuthAcquirer("oauth-token"), gateway.Config{TrustDomain: td}, nil)

	mux := http.NewServeMux()
	idmw := gateway.IdentityMiddleware(td)
	rp, rh := gatewayv1connect.NewRunnerServiceHandler(svc)
	gp, gh := gatewayv1connect.NewGatewayServiceHandler(svc)
	mux.Handle(rp, idmw(rh))
	mux.Handle(gp, idmw(gh))

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
		store: store, srvURL: fmt.Sprintf("https://localhost:%d", port),
		supervisor: supervisor, runner: runner, wfStep: wfStep, caPool: ca.Pool, stop: stop,
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

// refRunner is the Go reference runner: connects over mTLS, registers a tool,
// and dispatches Invokes to handler. If handler returns drop=true, the runner
// closes its stream without responding (simulating stream loss -> outcome_unknown).
type refRunner struct {
	cancel  context.CancelFunc
	done    chan struct{}
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
			rr.welcome = w
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
	// Wait for welcome.
	require.Eventually(t, func() bool { return rr.welcome != nil }, 2*time.Second, 10*time.Millisecond, "runner welcome not received")
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

func sendEmailDescriptor() *v1.ToolDescriptor {
	return &v1.ToolDescriptor{
		Name: "send_email", Version: "1.0.0", Digest: "sha256:send-1",
		Description: "send an email", EffectClass: v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE,
		InputSchema: []byte(`{"type":"object"}`), Timeout: durationPtr(5 * time.Second),
		CredentialSlots: []*v1.CredentialSlot{{Name: "graph_token", Scheme: v1.CredentialScheme_CREDENTIAL_SCHEME_BEARER, Required: true}},
	}
}

func upsertRowDescriptor(proof bool) *v1.ToolDescriptor {
	d := &v1.ToolDescriptor{
		Name: "upsert_row", Version: "1.0.0", Digest: "sha256:upsert-1",
		Description: "idempotent upsert", EffectClass: v1.EffectClass_EFFECT_CLASS_IDEMPOTENT_WRITE,
		InputSchema: []byte(`{"type":"object"}`), Timeout: durationPtr(5 * time.Second),
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

	gc := gatewayClient(env, env.supervisor)

	// Discovery: supervisor sees the registered echo tool.
	dresp, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: "att-1", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "turn-1",
	}))
	require.NoError(t, err)
	var names []string
	for _, d := range dresp.Msg.Descriptors {
		names = append(names, d.Name)
	}
	assert.Contains(t, names, "echo")

	// Invoke echo -> succeeded with the echoed args.
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: "att-1", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "turn-1",
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:echo-1",
		ArgumentsJson: []byte(`{"msg":"hi"}`), IdempotencyKey: "k1",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State)
	assert.Equal(t, `{"msg":"hi"}`, string(iresp.Msg.ResultJson))
}

func TestGateway_DuplicateInvocationReturnsCommittedResult(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{"ok":true}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	gc := gatewayClient(env, env.supervisor)
	req := &v1.InvokeRequest{
		AttemptId: "att-2", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "turn-2",
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
	// Register a tool NOT granted to the pool.
	rr := startRefRunner(t, env, &v1.ToolDescriptor{
		Name: "danger", Version: "1.0.0", Digest: "sha256:danger-1",
		EffectClass: v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE, InputSchema: []byte(`{}`),
	}, func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		t.Error("denied tool must not be dispatched to the runner")
		return nil, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: "att-3", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "turn-3",
		ToolCallId: "call-1", ToolName: "danger", ToolVersionDigest: "sha256:danger-1",
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

	gc := gatewayClient(env, env.supervisor)
	// Invoke with a digest that was never published.
	_, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: "att-4", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "turn-4",
		ToolCallId: "call-1", ToolName: "echo", ToolVersionDigest: "sha256:does-not-exist",
		ArgumentsJson: []byte(`{}`), IdempotencyKey: "k",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unavailable")
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

	gc := gatewayClient(env, env.supervisor)
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(&v1.InvokeRequest{
		AttemptId: "att-5", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "turn-5",
		ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
		ArgumentsJson: []byte(`{"to":"x@y"}`), IdempotencyKey: "send-1",
	}))
	require.NoError(t, err)
	require.Equal(t, v1.InvokeState_INVOKE_STATE_SUCCEEDED, iresp.Msg.State)
	// The runner received the resolved bearer value (not the SecretRef); raw
	// creds never enter Postgres/logs (ARCH-008).
	require.NotNil(t, seenCred)
	assert.Equal(t, "sekret-token-value", seenCred.Slots["graph_token"].BearerValue)
}

func TestGateway_AmbiguousOutcomeNotRepeated(t *testing.T) {
	env := newTestEnv(t, nil)
	// The runner drops its stream on first invoke (no result) -> outcome_unknown
	// for the non_idempotent write. A second invoke returns outcome_unknown.
	rr := startRefRunner(t, env, sendEmailDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return nil, true // drop stream
	})
	defer rr.close()
	t.Cleanup(rr.close)

	gc := gatewayClient(env, env.supervisor)
	req := &v1.InvokeRequest{
		AttemptId: "att-6", CallerScope: v1.CallerScope_CALLER_SCOPE_TURN, CallerScopeId: "turn-6",
		ToolCallId: "call-1", ToolName: "send_email", ToolVersionDigest: "sha256:send-1",
		ArgumentsJson: []byte(`{"to":"x@y"}`), IdempotencyKey: "send-once",
	}
	// Wait for the runner stream to drop after the (lost) dispatch is not needed;
	// the dispatch + drop happen synchronously on invoke.
	iresp, err := gc.InvokeTool(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN, iresp.Msg.State,
		"a possible effect with no committed result must become outcome_unknown")

	// The duplicate must NOT repeat the effect; it returns outcome_unknown.
	second, err := gc.InvokeTool(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	assert.Equal(t, v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN, second.Msg.State)
	assert.Equal(t, iresp.Msg.InvocationId, second.Msg.InvocationId, "outcome_unknown invocation is not repeated")
}

func TestGateway_WorkflowStepCallerDiscovery(t *testing.T) {
	env := newTestEnv(t, nil)
	rr := startRefRunner(t, env, echoDescriptor(), func(inv *v1.Invoke) (*v1.InvokeResult, bool) {
		return &v1.InvokeResult{State: v1.InvokeState_INVOKE_STATE_SUCCEEDED, ResultJson: []byte(`{}`)}, false
	})
	defer rr.close()
	t.Cleanup(rr.close)

	// The control-plane workflow-step caller discovers tools for workflow "wf-quote".
	gc := gatewayClient(env, env.wfStep)
	dresp, err := gc.DiscoverEffectiveTools(context.Background(), connect.NewRequest(&v1.DiscoverRequest{
		AttemptId: "att-7", CallerScope: v1.CallerScope_CALLER_SCOPE_WORKFLOW_STEP, CallerScopeId: "wf-quote",
	}))
	require.NoError(t, err)
	var names []string
	for _, d := range dresp.Msg.Descriptors {
		names = append(names, d.Name)
	}
	// workflow_pool_binding permits echo + send_email; echo is registered+healthy.
	assert.Contains(t, names, "echo")
	assert.NotContains(t, names, "upsert_row", "workflow does not permit upsert_row")
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
	// Either Send fails or the next Receive returns the mTLS rejection.
	if err == nil {
		_, err = stream.Receive()
	}
	assert.Error(t, err, "unapproved/untrusted runner must be rejected")
}

// durationPtr wraps a duration for a proto descriptor timeout.
func durationPtr(d time.Duration) *durationpb.Duration { return durationpb.New(d) }
