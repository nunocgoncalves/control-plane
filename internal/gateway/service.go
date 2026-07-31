package gateway

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
	v1 "github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1"
	"github.com/nunocgoncalves/control-plane/internal/spiffe"
)

// Config configures the gateway runtime.
type Config struct {
	TrustDomain       string        // SPIFFE trust domain (default iterabase.local)
	HeartbeatInterval time.Duration // advertised to runners in Welcome
	LeaseInterval     time.Duration // availability freshness; must match the migration's view interval (30s)
	InlineLimit       int           // max inline args/result bytes (larger -> artifact refs, HOR-399)
	DefaultTimeout    time.Duration // per-invocation timeout when descriptor has none
	RetryMaxAttempts  int           // bounded automatic retry for read_only / proven idempotent_write
	RetryBackoff      time.Duration
}

// Defaults applied when zero.
func (c Config) defaults() Config {
	if c.TrustDomain == "" {
		c.TrustDomain = spiffe.DefaultTrustDomain
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.LeaseInterval == 0 {
		c.LeaseInterval = 30 * time.Second // matches toolgateway.available_tool_versions
	}
	if c.InlineLimit == 0 {
		c.InlineLimit = 64 * 1024
	}
	if c.DefaultTimeout == 0 {
		c.DefaultTimeout = 60 * time.Second
	}
	if c.RetryMaxAttempts == 0 {
		c.RetryMaxAttempts = 3
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	return c
}

// Service implements the tool-gateway gRPC handlers (RunnerService +
// GatewayService). It is served by cmd/gateway over mTLS.
type Service struct {
	store   *Store
	secrets SecretResolver
	oauth   OAuthAcquirer
	cfg     Config
	pool    *runnerPool
	gen     atomic.Uint64 // fencing generation counter
	log     *slog.Logger
}

// NewService builds a gateway Service. store/secrets/oauth are required; cfg is
// defaulted.
func NewService(store *Store, secrets SecretResolver, oauth OAuthAcquirer, cfg Config, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		store:   store,
		secrets: secrets,
		oauth:   oauth,
		cfg:     cfg.defaults(),
		pool:    newRunnerPool(),
		log:     log,
	}
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

// identityFromContext returns the stamped identity (panics-free; the middleware
// guarantees presence on reached handlers).
func identityFromContext(ctx context.Context) (spiffe.Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(spiffe.Identity)
	return id, ok
}

// --- GatewayService ---

// DiscoverEffectiveTools returns only the descriptors permitted for the
// caller's active context (ARCH-006/016/018).
func (s *Service) DiscoverEffectiveTools(ctx context.Context, req *connect.Request[v1.DiscoverRequest]) (*connect.Response[v1.DiscoverResponse], error) {
	id, ok := identityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	pool, permitted, err := s.resolveCallerScope(ctx, id, req.Msg)
	if err != nil {
		return nil, mapErr(err)
	}
	tools, err := s.store.DiscoverEffectiveTools(ctx, pool.ID, permitted)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	descs := make([]*v1.ToolDescriptor, 0, len(tools))
	for _, tv := range tools {
		// Discovery returns a child-safe descriptor: credential slots and
		// idempotency proof are gateway-internal (the child never resolves
		// credentials or decides retry policy).
		descs = append(descs, toolVersionToDescriptor(tv))
	}
	return connect.NewResponse(&v1.DiscoverResponse{Descriptors: descs}), nil
}

// InvokeTool is the ledger-gated execution path (ARCH-014).
func (s *Service) InvokeTool(ctx context.Context, req *connect.Request[v1.InvokeRequest]) (*connect.Response[v1.InvokeResponse], error) {
	id, ok := identityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	msg := req.Msg

	// 1. Resolve the pinned tool version (ARCH-007). An unknown pin fails before
	//    action execution; the gateway never substitutes another version.
	tv, err := s.store.GetToolVersion(ctx, msg.ToolName, msg.ToolVersionDigest)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("pinned tool %s@%s unavailable; no substitution (ARCH-007)", msg.ToolName, msg.ToolVersionDigest))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// 2. Resolve caller scope -> pool + permitted tools (deny-by-default).
	pool, permitted, err := s.resolveCallerScope(ctx, id, &v1.DiscoverRequest{
		AttemptId: msg.AttemptId, CallerScope: msg.CallerScope, CallerScopeId: msg.CallerScopeId,
	})
	if err != nil {
		return nil, mapErr(err)
	}

	// 3. Authorization: the tool must be granted to the pool with an effect-class
	//    ceiling >= the tool's effect (SCN-009). Absence = denied, attributable.
	if !s.authorized(ctx, pool.ID, tv, permitted) {
		s.log.Warn("tool invocation denied (out of scope)",
			"tool", tv.Name, "pool", pool.ID, "attempt", msg.AttemptId, "caller", id.SPIFFEID)
		return connect.NewResponse(&v1.InvokeResponse{
			State: v1.InvokeState_INVOKE_STATE_FAILED,
			Error: &v1.ErrorDetail{Code: "permission_denied", Message: "tool not authorized for this scope", Retryability: v1.Retryability_RETRYABILITY_NON_RETRYABLE},
		}), nil
	}

	// 4. Argument validation (deterministic, pre-effect). v1: valid JSON + inline
	//    size limit. Full JSON-Schema validation is a fast-follow.
	if err := validateArgs(msg.ArgumentsJson, s.cfg.InlineLimit); err != nil {
		return connect.NewResponse(&v1.InvokeResponse{
			State: v1.InvokeState_INVOKE_STATE_FAILED,
			Error: &v1.ErrorDetail{Code: "invalid_arguments", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_NON_RETRYABLE},
		}), nil
	}

	// 5. Commit the ledger row BEFORE the side-effect boundary (ARCH-014). This
	//    MUST precede the runner-availability check so a duplicate of an
	//    already-terminal invocation (e.g. outcome_unknown after a runner drop)
	//    returns its committed result rather than a fresh tool_unavailable. On a
	//    unique-key conflict (duplicate caller) return the existing result or
	//    report in-progress.
	key := InvocationKey{
		AttemptID: msg.AttemptId, CallerScope: callerScopeFromProto(msg.CallerScope),
		CallerScopeID: msg.CallerScopeId, ToolCallID: msg.ToolCallId,
		ToolVersionDigest: tv.Digest, IdempotencyKey: msg.IdempotencyKey,
	}
	inv, inserted, err := s.store.BeginInvocation(ctx, key, tv, &pool.ID, msg.ArgumentsJson)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if !inserted {
		resp := invocationToResponse(inv)
		return connect.NewResponse(resp), nil
	}

	// 6. A live runner must serve the pinned version (ARCH-007). No runner => no
	//    effect possible => fail the committed invocation (not a pre-ledger
	//    error, so a later duplicate still returns this committed failure).
	if !s.pool.toolAvailable(tv.Name, tv.Digest) {
		return connect.NewResponse(s.finishFailed(ctx, inv, &v1.ErrorDetail{
			Code: "tool_unavailable", Message: "no live runner for the pinned tool version", Retryability: v1.Retryability_RETRYABILITY_RETRYABLE,
		})), nil
	}

	// 7. Resolve credential bindings -> CredentialContext (ARCH-008).
	credCtx, err := resolveCredentialContext(ctx, pool.ID, tv.Name, s.store, s.secrets, s.oauth)
	if err != nil {
		return connect.NewResponse(s.finishFailed(ctx, inv, &v1.ErrorDetail{
			Code: "credential_resolution_failed", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_NON_RETRYABLE,
		})), nil
	}

	// 8. Dispatch (with retry classification by effect class).
	resp := s.dispatchWithRetry(ctx, inv, tv, msg, credCtx)
	return connect.NewResponse(resp), nil
}

// dispatchWithRetry executes the invocation over a runner stream, applying the
// effect-class retry policy on stream loss / ambiguity (ARCH-014).
func (s *Service) dispatchWithRetry(ctx context.Context, inv Invocation, tv ToolVersion, msg *v1.InvokeRequest, credCtx *v1.CredentialContext) *v1.InvokeResponse {
	timeout := s.cfg.DefaultTimeout
	if tv.TimeoutMS > 0 {
		timeout = time.Duration(tv.TimeoutMS) * time.Millisecond
	}
	invokeCtrl := &v1.RunnerControl{Kind: &v1.RunnerControl_Invoke{Invoke: &v1.Invoke{
		InvocationId:      inv.ID,
		Descriptor_:       toolVersionToDescriptor(tv),
		ArgumentsJson:     msg.ArgumentsJson,
		IdempotencyKey:    msg.IdempotencyKey,
		ArtifactInputRefs: msg.ArtifactInputRefs,
		CredentialContext: credCtx,
	}}}

	canRetry := tv.EffectClass == EffectReadOnly ||
		(tv.EffectClass == EffectIdempotentWrite && len(tv.IdempotencyProof) > 0)
	attempts := 1
	if canRetry {
		attempts = s.cfg.RetryMaxAttempts
		if attempts < 1 {
			attempts = 1
		}
	}

	for attempt := 0; attempt < attempts; attempt++ {
		// Transition dispatching -> running for this attempt.
		_ = s.store.MarkRunning(ctx, inv.ID, "") // runner_id unknown until picked; audit best-effort

		res, err := s.pool.dispatchToRunner(ctx, tv.Name, tv.Digest, invokeCtrl, inv.ID, timeout)
		if err != nil {
			if errors.Is(err, ErrNoRunner) {
				// Runner vanished between check and dispatch: ambiguous for writes.
				return s.classifyAmbiguous(ctx, inv, tv, "no runner available for dispatch")
			}
			return s.finishFailed(ctx, inv, &v1.ErrorDetail{Code: "dispatch_error", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_RETRYABLE})
		}
		if res.streamLost {
			// Ambiguous: a possible effect occurred with no committed result.
			if attempt < attempts-1 && canRetry {
				time.Sleep(s.cfg.RetryBackoff)
				continue
			}
			return s.classifyAmbiguous(ctx, inv, tv, "runner stream lost before result")
		}
		// Runner reported a terminal result.
		return s.finishFromResult(ctx, inv, res)
	}
	return s.classifyAmbiguous(ctx, inv, tv, "retry budget exhausted")
}

// classifyAmbiguous terminalizes an invocation as outcome_unknown (a possible
// effect with no committed result; never automatically repeated — ARCH-014) for
// writes, or failed for read_only (no effect possible).
func (s *Service) classifyAmbiguous(ctx context.Context, inv Invocation, tv ToolVersion, reason string) *v1.InvokeResponse {
	state := InvocationOutcomeUnknown
	if tv.EffectClass == EffectReadOnly {
		state = InvocationFailed
	}
	errDetail, _ := marshalJSON(map[string]any{
		"code": "outcome_unknown", "message": reason, "retryability": "unknown",
		"effect_class": tv.EffectClass,
	})
	_ = s.store.FinishInvocation(ctx, inv.ID, state, nil, []byte("[]"), errDetail)
	resp := &v1.InvokeResponse{InvocationId: inv.ID}
	switch state {
	case InvocationOutcomeUnknown:
		resp.State = v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN
	case InvocationFailed:
		resp.State = v1.InvokeState_INVOKE_STATE_FAILED
	}
	resp.Error = &v1.ErrorDetail{Code: "outcome_unknown", Message: reason, Retryability: v1.Retryability_RETRYABILITY_UNKNOWN}
	return resp
}

func (s *Service) finishFailed(ctx context.Context, inv Invocation, err *v1.ErrorDetail) *v1.InvokeResponse {
	errJSON, _ := marshalJSON(err)
	_ = s.store.FinishInvocation(ctx, inv.ID, InvocationFailed, nil, []byte("[]"), errJSON)
	return &v1.InvokeResponse{InvocationId: inv.ID, State: v1.InvokeState_INVOKE_STATE_FAILED, Error: err}
}

func (s *Service) finishFromResult(ctx context.Context, inv Invocation, res dispatchResult) *v1.InvokeResponse {
	state := InvocationSucceeded
	if res.state == InvocationFailed {
		state = InvocationFailed
	}
	_ = s.store.FinishInvocation(ctx, inv.ID, state, res.resultJSON, res.artifactRefs, res.errorDetail)
	resp := &v1.InvokeResponse{InvocationId: inv.ID, ArtifactOutputRefs: nil}
	if state == InvocationSucceeded {
		resp.State = v1.InvokeState_INVOKE_STATE_SUCCEEDED
		resp.ResultJson = res.resultJSON
	} else {
		resp.State = v1.InvokeState_INVOKE_STATE_FAILED
		resp.Error = &v1.ErrorDetail{Code: "tool_failed", Message: "tool execution failed", Retryability: v1.Retryability_RETRYABILITY_UNKNOWN}
	}
	// Artifact refs are parsed from the ledger's JSONB for the response.
	refs := parseArtifactRefs(res.artifactRefs)
	resp.ArtifactOutputRefs = refs
	return resp
}

// CancelInvocation propagates cancellation to an in-flight invocation. It
// cannot undo an effect already started (ARCH-014).
func (s *Service) CancelInvocation(ctx context.Context, req *connect.Request[v1.CancelRequest]) (*connect.Response[v1.CancelResponse], error) {
	inv, err := s.store.GetInvocation(ctx, req.Msg.InvocationId)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	// If terminal, return the committed state unchanged.
	if inv.State == InvocationSucceeded || inv.State == InvocationFailed || inv.State == InvocationOutcomeUnknown {
		return connect.NewResponse(&v1.CancelResponse{State: invocationStateToProto(inv.State)}), nil
	}
	// Propagate cancel to the runner serving this invocation (best-effort).
	s.pool.propagateCancel(ctx, inv.ID, req.Msg.Reason)
	return connect.NewResponse(&v1.CancelResponse{State: v1.InvokeState_INVOKE_STATE_RUNNING}), nil
}

// --- RunnerService (bidi) ---

// RegisterRunner is the long-lived runner stream (ARCH-015).
//
//nolint:gocyclo // the bidi receive loop is naturally branchy; kept flat for readability.
func (s *Service) RegisterRunner(ctx context.Context, st *connect.BidiStream[v1.RunnerMessage, v1.RunnerControl]) error {
	id, ok := identityFromContext(ctx)
	if !ok {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	if id.Kind != spiffe.KindRunner {
		return connect.NewError(connect.CodePermissionDenied, errors.New("only tool runners may register"))
	}

	// Deny-by-default runner approval (ARCH-015).
	approved, err := s.store.IsApprovedRunner(ctx, id.SPIFFEID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return connect.NewError(connect.CodePermissionDenied, errors.New("runner identity not approved"))
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	if !namespaceAllowed(approved.AllowedToolNamespaces, id.Namespace) {
		return connect.NewError(connect.CodePermissionDenied, errors.New("runner namespace not permitted"))
	}

	gen := s.gen.Add(1)
	rc := &runnerConn{
		identity: id, runnerID: approved.RunnerID, gen: int64(gen), //nolint:gosec // G115: generation counter
		stream: st, pending: make(map[string]chan dispatchResult), tools: make(map[string]string),
	}
	s.pool.add(rc)
	defer func() {
		s.pool.remove(rc)
		_ = s.store.DeactivateRunnerStream(ctx, rc.runnerID, int64(gen)) //nolint:gosec // G115
		// Any pending dispatchers on this conn observe streamLost.
		rc.mu.Lock()
		rc.closed = true
		for _, ch := range rc.pending {
			select {
			case ch <- dispatchResult{streamLost: true}:
			default:
			}
		}
		rc.mu.Unlock()
	}()

	// Read the first message: must be a Register.
	first, err := st.Receive()
	if err != nil {
		return err
	}
	reg := first.GetRegister()
	if reg == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first runner message must be Register"))
	}
	if err := s.handleRegister(ctx, reg, approved, rc); err != nil {
		return err
	}
	if err := st.Send(welcome(gen, s.cfg)); err != nil {
		return err
	}

	// Receive loop: Register (more tools), Heartbeat, InvokeResult, InvokeError.
	for {
		msg, err := st.Receive()
		if err != nil {
			return nil // stream closed (client disconnect / ctx cancel)
		}
		switch m := msg.Kind.(type) {
		case *v1.RunnerMessage_Register:
			if err := s.handleRegister(ctx, m.Register, approved, rc); err != nil {
				return err
			}
			_ = st.Send(&v1.RunnerControl{Kind: &v1.RunnerControl_Ack{Ack: &v1.Ack{Kind: &v1.Ack_Registered{Registered: m.Register.Descriptor_.Name}}}})
		case *v1.RunnerMessage_Heartbeat:
			_ = s.store.HeartbeatRunner(ctx, rc.runnerID, int64(gen)) //nolint:gosec // G115
			_ = st.Send(&v1.RunnerControl{Kind: &v1.RunnerControl_Ack{Ack: &v1.Ack{Kind: &v1.Ack_Heartbeat{Heartbeat: true}}}})
		case *v1.RunnerMessage_InvokeResult:
			rc.deliver(m.InvokeResult.InvocationId, invokeResultToDispatch(m.InvokeResult))
		case *v1.RunnerMessage_InvokeError:
			rc.deliver(m.InvokeError.InvocationId, dispatchResult{
				state:       InvocationFailed,
				errorDetail: mustMarshalError(m.InvokeError.Error),
			})
		}
	}
}

// handleRegister registers one tool version for the runner + publishes the
// immutable descriptor (idempotent on digest).
func (s *Service) handleRegister(ctx context.Context, reg *v1.Register, approved ApprovedRunner, rc *runnerConn) error {
	desc := reg.Descriptor_
	tv := descriptorToToolVersion(desc)
	tv, err := s.store.RegisterToolVersion(ctx, tv)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	rr, err := s.store.UpsertRunnerRegistration(ctx, RunnerRegistration{
		RunnerID: rc.runnerID, SpiffeID: rc.identity.SPIFFEID, Namespace: rc.identity.Namespace,
		ToolName: tv.Name, ToolVersion: tv.Version, ToolDigest: tv.Digest, FencingGeneration: rc.gen,
	})
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	rc.registerTool(tv.Name, tv.Digest)
	s.log.Info("runner registered tool", "runner", rc.runnerID, "tool", tv.Name, "version", tv.Version, "gen", rc.gen, "reg", rr.ID)
	return nil
}

// --- helpers ---

// resolveCallerScope resolves the pool + workflow-permitted tools for a caller.
// Supervisor: pool from SPIFFE prefix; permitted = {} (all granted; workflow
// narrowing for turns is a follow-up). Workflow-step: pool + permitted from the
// workflow binding (ARCH-012/018).
func (s *Service) resolveCallerScope(ctx context.Context, id spiffe.Identity, req *v1.DiscoverRequest) (Pool, []string, error) {
	switch id.Kind {
	case spiffe.KindSupervisor:
		pool, err := s.store.ResolvePoolBySpiffePrefix(ctx, id.SPIFFEID)
		if err != nil {
			return Pool{}, nil, fmt.Errorf("supervisor pool not bound: %w", err)
		}
		return pool, nil, nil // no workflow narrowing for turns in v1
	case spiffe.KindControlPlaneWorkflow:
		// The workflow-step caller carries the workflow definition key via the
		// attempt context. v1: derive workflow key from the caller_scope_id
		// (run_step_id) -> the binding is looked up by the workflow the step
		// belongs to. For the hermetic path the caller passes the workflow key
		// in AttemptId's place is not ideal; resolve by the step's workflow.
		// Simpler: the workflow-step caller's pool is resolved from a binding
		// keyed by workflow_definition_key passed as CallerScopeId.
		b, err := s.store.GetWorkflowPoolBinding(ctx, req.CallerScopeId)
		if err != nil {
			return Pool{}, nil, fmt.Errorf("workflow pool binding not found: %w", err)
		}
		pool, err := s.getPool(ctx, b.PoolID)
		if err != nil {
			return Pool{}, nil, err
		}
		return pool, b.PermittedTools, nil
	}
	return Pool{}, nil, fmt.Errorf("caller kind %s cannot resolve scope", id.Kind)
}

func (s *Service) getPool(ctx context.Context, poolID string) (Pool, error) {
	// Pools are keyed by id; a direct fetch.
	row := s.store.pool.QueryRow(ctx, `SELECT id, key, name, spiffe_id_prefix FROM toolgateway.pools WHERE id = $1 AND deleted_at IS NULL`, poolID)
	var p Pool
	if err := row.Scan(&p.ID, &p.Key, &p.Name, &p.SpiffeIDPrefix); err != nil {
		return Pool{}, fmt.Errorf("get pool: %w", err)
	}
	return p, nil
}

// authorized checks the pool grant ceiling + workflow-permitted intersection.
func (s *Service) authorized(ctx context.Context, poolID string, tv ToolVersion, permitted []string) bool {
	if len(permitted) > 0 {
		found := false
		for _, t := range permitted {
			if t == tv.Name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	var maxEffect string
	err := s.store.pool.QueryRow(ctx,
		`SELECT max_effect_class FROM toolgateway.pool_grants WHERE pool_id = $1 AND tool_name = $2 AND deleted_at IS NULL`,
		poolID, tv.Name).Scan(&maxEffect)
	if err != nil {
		return false // absence = denied
	}
	return effectRank(tv.EffectClass) <= effectRank(EffectClass(maxEffect))
}

func validateArgs(args []byte, limit int) error {
	if len(args) > limit {
		return fmt.Errorf("arguments exceed inline limit %d (use artifact refs)", limit)
	}
	if len(args) == 0 {
		return nil
	}
	var any json.RawMessage
	return json.Unmarshal(args, &any)
}

// namespaceAllowed reports whether the runner's namespace is in the allowed
// list (empty allowed list = no namespace restriction configured).
func namespaceAllowed(allowed []string, ns string) bool {
	if len(allowed) == 0 {
		return true // no restriction configured
	}
	for _, a := range allowed {
		if a == ns || a == "*" {
			return true
		}
	}
	return false
}

func mapErr(err error) error {
	if errors.Is(err, ErrNotFound) {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("scope not authorized: %w", err))
	}
	return connect.NewError(connect.CodeInternal, err)
}

func welcome(gen uint64, cfg Config) *v1.RunnerControl {
	return &v1.RunnerControl{Kind: &v1.RunnerControl_Welcome{Welcome: &v1.Welcome{
		ProtocolVersion: "1", FencingGeneration: gen,
		HeartbeatIntervalMs: int32(cfg.HeartbeatInterval / time.Millisecond), //nolint:gosec // G115
		LeaseTimeoutMs:      int32(cfg.LeaseInterval / time.Millisecond),     //nolint:gosec // G115
	}}}
}
