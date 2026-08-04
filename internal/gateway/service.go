package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
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
	DispatchLease     time.Duration // crash-recovery lease for in-flight invocations (ARCH-014/SCN-008)
	GatewayInstanceID string        // unique per process; auto-generated if empty
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
	if c.DispatchLease == 0 {
		c.DispatchLease = 60 * time.Second
	}
	if c.GatewayInstanceID == "" {
		c.GatewayInstanceID = randomInstanceID()
	}
	return c
}

func randomInstanceID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "gw-" + hex.EncodeToString(b[:])
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
// defaulted. Crash-recovery reconciliation is started separately via
// StartReconciler (cmd/gateway) so tests control it explicitly.
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

// StartReconciler runs the crash-recovery sweep once at start, then on a ticker
// until ctx is done (SCN-008/ARCH-014). It classifies orphaned in-flight
// invocations (read_only -> failed, writes -> outcome_unknown). Call once per
// process, before accepting traffic.
func (s *Service) StartReconciler(ctx context.Context) {
	recovered, err := s.store.RecoverOrphanedInvocations(ctx)
	if err != nil {
		s.log.Error("initial orphan recovery failed", "error", err)
	} else if recovered > 0 {
		s.log.Info("recovered orphaned invocations", "count", recovered)
	}
	ticker := time.NewTicker(s.cfg.DispatchLease / 2)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n, err := s.store.RecoverOrphanedInvocations(ctx); err != nil {
					s.log.Warn("orphan recovery sweep failed", "error", err)
				} else if n > 0 {
					s.log.Info("recovered orphaned invocations", "count", n)
				}
			}
		}
	}()
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
// caller's active, durably-resolved context (ARCH-004/006/007/016/018). The
// caller's pool/attempt/permitted set is resolved from runtime state + the
// attempt's immutable tool-version pin snapshot; caller-supplied IDs are
// validated, never trusted as scope.
func (s *Service) DiscoverEffectiveTools(ctx context.Context, req *connect.Request[v1.DiscoverRequest]) (*connect.Response[v1.DiscoverResponse], error) {
	id, ok := identityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	if id.Kind == spiffe.KindRunner {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("runners cannot call GatewayService"))
	}
	res, err := s.resolveCallerScope(ctx, id, req.Msg)
	if err != nil {
		return nil, mapErr(err)
	}
	tools, err := s.store.DiscoverEffectiveTools(ctx, res.AttemptID, res.Pool.ID, res.PermittedCapabilities)
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

// InvokeTool is the ledger-gated execution path (ARCH-014). Authorization,
// version pinning, argument validation, and credential resolution all occur
// BEFORE the side-effect boundary; the ledger row is committed before dispatch.
//
//nolint:gocyclo // the pre-effect gate is naturally branchy; kept flat for readability (cf. RegisterRunner).
func (s *Service) InvokeTool(ctx context.Context, req *connect.Request[v1.InvokeRequest]) (*connect.Response[v1.InvokeResponse], error) {
	id, ok := identityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	if id.Kind == spiffe.KindRunner {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("runners cannot call GatewayService"))
	}
	msg := req.Msg

	// 1. Resolve the caller's durable scope (ARCH-004): pool + permitted tools +
	//    attempt id, validated against runtime state. Fail closed.
	res, err := s.resolveCallerScope(ctx, id, &v1.DiscoverRequest{
		AttemptId: msg.AttemptId, CallerScope: msg.CallerScope, CallerScopeId: msg.CallerScopeId,
		FencingGeneration: msg.FencingGeneration,
	})
	if err != nil {
		return nil, mapErr(err)
	}

	// 2. Resolve the pinned tool version from the attempt's immutable snapshot
	//    (ARCH-007). The caller-supplied digest is NOT trusted: if present it
	//    must equal the pin. No pin => fail closed (no substitution).
	pinDigest, err := s.store.GetAttemptToolPin(ctx, res.AttemptID, msg.ToolName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("tool %s is not pinned for attempt %s; no substitution (ARCH-007)", msg.ToolName, res.AttemptID))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if msg.ToolVersionDigest != "" && msg.ToolVersionDigest != pinDigest {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("requested digest %s does not match the pinned digest %s for attempt %s (ARCH-007)", msg.ToolVersionDigest, pinDigest, res.AttemptID))
	}
	tv, err := s.store.GetToolVersion(ctx, msg.ToolName, pinDigest)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("pinned tool %s@%s unavailable; no substitution (ARCH-007)", msg.ToolName, pinDigest))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// 3. Authorization (ARCH-008/016/018): workflow-permitted intersection +
	//    pool grant effect ceiling + action allow-list. Absence = denied,
	//    attributable.
	allowed, err := s.authorize(ctx, res.Pool.ID, tv, res.PermittedCapabilities)
	if err != nil {
		s.log.Warn("tool invocation denied", "tool", tv.Name, "pool", res.Pool.ID,
			"attempt", res.AttemptID, "caller", id.SPIFFEID, "reason", err)
		return connect.NewResponse(&v1.InvokeResponse{
			State: v1.InvokeState_INVOKE_STATE_FAILED,
			Error: &v1.ErrorDetail{Code: "permission_denied", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_NON_RETRYABLE},
		}), nil
	}
	_ = allowed

	// 4. Argument validation against the pinned descriptor's JSON Schema
	//    (REQ-010/ARCH-014) — deterministic, pre-effect.
	if err := validateArguments(msg.ArgumentsJson, tv.InputSchema, s.cfg.InlineLimit); err != nil {
		return connect.NewResponse(&v1.InvokeResponse{
			State: v1.InvokeState_INVOKE_STATE_FAILED,
			Error: &v1.ErrorDetail{Code: "invalid_arguments", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_NON_RETRYABLE},
		}), nil
	}

	// 4b. Resource-scope policy (ARCH-008/018): credential-binding resource
	//     constraints are asserted against the resource target derived from the
	//     validated arguments, before the effect boundary. The pool-level
	//     grants + bindings ARE the v1 customer/identity policy (ARCH-018:
	//     pool-level integration scope; per-identity bindings deferred).
	if err := s.assertResourcePolicy(ctx, res.Pool.ID, tv.Name, msg.ArgumentsJson); err != nil {
		s.log.Warn("tool invocation denied: resource scope", "tool", tv.Name, "pool", res.Pool.ID,
			"attempt", res.AttemptID, "caller", id.SPIFFEID, "reason", err)
		return connect.NewResponse(&v1.InvokeResponse{
			State: v1.InvokeState_INVOKE_STATE_FAILED,
			Error: &v1.ErrorDetail{Code: "permission_denied", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_NON_RETRYABLE},
		}), nil
	}

	// 5. Commit the ledger row BEFORE the side-effect boundary (ARCH-014), with
	//    a crash-recovery lease covering the full dispatch timeout + margin so a
	//    live invocation cannot be recovered while still executing. The caller's
	//    scope is taken from the durable resolution (res), never from caller-
	//    supplied msg fields (ARCH-004). On a unique-key conflict (duplicate
	//    caller) return the existing result or report in-progress.
	leaseExpiresAt := time.Now().Add(s.dispatchTimeout(tv) + s.cfg.DispatchLease)
	key := InvocationKey{
		AttemptID: res.AttemptID, CallerScope: res.CallerScope,
		CallerScopeID: res.CallerScopeID, ToolCallID: msg.ToolCallId,
		ToolVersionDigest: tv.Digest, IdempotencyKey: msg.IdempotencyKey,
	}
	inv, inserted, err := s.store.BeginInvocation(ctx, key, tv, &res.Pool.ID, msg.ArgumentsJson, leaseExpiresAt, s.cfg.GatewayInstanceID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if !inserted {
		return connect.NewResponse(invocationToResponse(inv)), nil
	}

	// 6. A live runner must serve the pinned version (ARCH-007). No runner => no
	//    effect possible => fail the committed invocation (retryable). This is a
	//    post-ledger failure, so a later duplicate returns this committed
	//    failure rather than re-dispatching.
	if !s.pool.toolAvailable(tv.Name, tv.Digest) {
		resp, ferr := s.finishFailed(ctx, inv, &v1.ErrorDetail{
			Code: "tool_unavailable", Message: "no live runner for the pinned tool version", Retryability: v1.Retryability_RETRYABILITY_RETRYABLE,
		})
		if ferr != nil {
			return nil, connect.NewError(connect.CodeInternal, ferr)
		}
		return connect.NewResponse(resp), nil
	}

	// 7. Resolve credential bindings -> CredentialContext, validated against the
	//    pinned descriptor's declared slots (ARCH-008).
	credCtx, err := resolveCredentialContext(ctx, res.Pool.ID, tv.Name, tv, s.store, s.secrets, s.oauth)
	if err != nil {
		resp, ferr := s.finishFailed(ctx, inv, &v1.ErrorDetail{
			Code: "credential_resolution_failed", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_NON_RETRYABLE,
		})
		if ferr != nil {
			return nil, connect.NewError(connect.CodeInternal, ferr)
		}
		return connect.NewResponse(resp), nil
	}

	// 8. Dispatch (with retry classification by effect class).
	resp, derr := s.dispatchWithRetry(ctx, inv, tv, msg, credCtx)
	if derr != nil {
		return nil, connect.NewError(connect.CodeInternal, derr)
	}
	return connect.NewResponse(resp), nil
}

// dispatchWithRetry executes the invocation over a runner stream, applying the
// effect-class retry policy on stream loss / ambiguity (ARCH-014).
func (s *Service) dispatchWithRetry(ctx context.Context, inv Invocation, tv ToolVersion, msg *v1.InvokeRequest, credCtx *v1.CredentialContext) (*v1.InvokeResponse, error) {
	timeout := s.dispatchTimeout(tv)
	// The stable upstream idempotency key for an idempotent_write is the durable
	// invocation id — gateway-derived (not caller-controlled) and identical
	// across retries, so the runner propagates one stable upstream key per
	// ARCH-014 ("propagates an upstream idempotency key or deterministic
	// resource identity"). The caller's idempotency_key remains the gateway
	// dedup key (BeginInvocation); it is not trusted as the upstream key.
	upstreamKey := msg.IdempotencyKey
	if tv.EffectClass == EffectIdempotentWrite {
		upstreamKey = inv.ID
	}
	invokeCtrl := &v1.RunnerControl{Kind: &v1.RunnerControl_Invoke{Invoke: &v1.Invoke{
		InvocationId:      inv.ID,
		Descriptor_:       toolVersionToDescriptor(tv),
		ArgumentsJson:     msg.ArgumentsJson,
		IdempotencyKey:    upstreamKey,
		ArtifactInputRefs: msg.ArtifactInputRefs,
		CredentialContext: credCtx,
	}}}

	// Retry gate (ARCH-014): read_only may bounded-retry; idempotent_write may
	// retry only when the descriptor declares a proven strategy AND a stable
	// identity is available (inv.ID, always present after BeginInvocation).
	canRetry := tv.EffectClass == EffectReadOnly ||
		(tv.EffectClass == EffectIdempotentWrite && len(tv.IdempotencyProof) > 0 && inv.ID != "")
	attempts := 1
	if canRetry {
		attempts = s.cfg.RetryMaxAttempts
		if attempts < 1 {
			attempts = 1
		}
	}

	for attempt := 0; attempt < attempts; attempt++ {
		// Establish/renew the recovery lease for this attempt's full timeout +
		// margin BEFORE crossing the side-effect boundary, so the SCN-008 recovery
		// sweep cannot terminalize a live invocation mid-dispatch AND a retry
		// cannot dispatch after recovery already terminalized the row (ARCH-014).
		// Fail closed: if the lease was not (re-)established, do not dispatch.
		if err := s.renewDispatchLease(ctx, inv.ID, timeout); err != nil {
			s.log.Warn("dispatch lease not established; aborting before effect boundary", "inv", inv.ID, "attempt", attempt, "error", err)
			return s.abortAfterLeaseFailure(ctx, inv, tv, "dispatch lease not established/renewed: "+err.Error())
		}
		res, err := s.pool.dispatchToRunner(ctx, tv.Name, tv.Digest, invokeCtrl, inv.ID, timeout)
		// A streamLost result (Send succeeded, result lost / ctx cancelled)
		// means a possible effect occurred with no committed result. Classify
		// by effect class BEFORE treating a non-nil err as a hard failure — a
		// post-send cancellation is ambiguity, not a plain dispatch error.
		if res.streamLost {
			if canRetry && attempt < attempts-1 {
				time.Sleep(s.cfg.RetryBackoff)
				continue
			}
			return s.classifyAmbiguous(ctx, inv, tv, "runner stream lost / context cancelled after send")
		}
		if err != nil {
			if errors.Is(err, ErrNoRunner) {
				// Runner vanished between check and dispatch: ambiguous for writes.
				return s.classifyAmbiguous(ctx, inv, tv, "no runner available for dispatch")
			}
			resp, ferr := s.finishFailed(ctx, inv, &v1.ErrorDetail{Code: "dispatch_error", Message: err.Error(), Retryability: v1.Retryability_RETRYABILITY_RETRYABLE})
			return resp, ferr
		}
		// Runner reported a terminal result.
		return s.finishFromResult(ctx, inv, tv, res)
	}
	return s.classifyAmbiguous(ctx, inv, tv, "retry budget exhausted")
}

// classifyAmbiguous terminalizes an invocation as outcome_unknown (a possible
// effect with no committed result; never automatically repeated — ARCH-014) for
// writes, or failed for read_only (no effect possible). Returns an error if the
// ledger transition does not commit (the caller must not fabricate a terminal
// response).
func (s *Service) classifyAmbiguous(ctx context.Context, inv Invocation, tv ToolVersion, reason string) (*v1.InvokeResponse, error) {
	state := InvocationOutcomeUnknown
	if tv.EffectClass == EffectReadOnly {
		state = InvocationFailed
	}
	errDetail, _ := marshalJSON(map[string]any{
		"code": "outcome_unknown", "message": reason, "retryability": "unknown",
		"effect_class": tv.EffectClass,
	})
	commitCtx, cancel := s.detachedCtx(ctx)
	defer cancel()
	if ferr := s.store.FinishInvocation(commitCtx, inv.ID, state, nil, []byte("[]"), errDetail); ferr != nil {
		return nil, fmt.Errorf("commit ambiguous outcome: %w", ferr)
	}
	resp := &v1.InvokeResponse{InvocationId: inv.ID}
	switch state {
	case InvocationOutcomeUnknown:
		resp.State = v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN
	case InvocationFailed:
		resp.State = v1.InvokeState_INVOKE_STATE_FAILED
	}
	resp.Error = &v1.ErrorDetail{Code: "outcome_unknown", Message: reason, Retryability: v1.Retryability_RETRYABILITY_UNKNOWN}
	return resp, nil
}

func (s *Service) finishFailed(ctx context.Context, inv Invocation, err *v1.ErrorDetail) (*v1.InvokeResponse, error) {
	errJSON, _ := marshalJSON(err)
	commitCtx, cancel := s.detachedCtx(ctx)
	defer cancel()
	if ferr := s.store.FinishInvocation(commitCtx, inv.ID, InvocationFailed, nil, []byte("[]"), errJSON); ferr != nil {
		return nil, fmt.Errorf("commit failed result: %w", ferr)
	}
	return &v1.InvokeResponse{InvocationId: inv.ID, State: v1.InvokeState_INVOKE_STATE_FAILED, Error: err}, nil
}

// finishFromResult commits a runner-reported terminal result. Runner output is
// bounded + validated before commit (REQ-009/ARCH-014). A succeeded write with
// malformed/oversized output cannot be trusted as a clean success: it is
// classified outcome_unknown (a possible effect with an uncommittable result).
// Never emits a terminal response unless the ledger transition commits.
func (s *Service) finishFromResult(ctx context.Context, inv Invocation, tv ToolVersion, res dispatchResult) (*v1.InvokeResponse, error) {
	state := res.state
	resultJSON := res.resultJSON
	// Bound + validate runner output before committing.
	if state == InvocationSucceeded {
		if len(resultJSON) > s.cfg.InlineLimit {
			// Oversized success: the effect may have happened but the result
			// cannot be stored inline. Classify by effect class.
			return s.classifyAmbiguous(ctx, inv, tv, "runner result exceeds inline limit (use artifact refs)")
		}
		if len(resultJSON) == 0 {
			resultJSON = []byte("{}")
		} else if !jsonValid(resultJSON) {
			return s.classifyAmbiguous(ctx, inv, tv, "runner reported succeeded with malformed JSON result")
		}
	}
	commitCtx, cancel := s.detachedCtx(ctx)
	defer cancel()
	if ferr := s.store.FinishInvocation(commitCtx, inv.ID, state, resultJSON, res.artifactRefs, res.errorDetail); ferr != nil {
		return nil, fmt.Errorf("commit result: %w", ferr)
	}
	resp := &v1.InvokeResponse{InvocationId: inv.ID}
	if state == InvocationSucceeded {
		resp.State = v1.InvokeState_INVOKE_STATE_SUCCEEDED
		resp.ResultJson = resultJSON
	} else {
		resp.State = v1.InvokeState_INVOKE_STATE_FAILED
		resp.Error = &v1.ErrorDetail{Code: "tool_failed", Message: "tool execution failed", Retryability: v1.Retryability_RETRYABILITY_UNKNOWN}
	}
	resp.ArtifactOutputRefs = parseArtifactRefs(res.artifactRefs)
	return resp, nil
}

// CancelInvocation propagates cancellation to an in-flight invocation. The
// caller's durable scope must own the invocation (REQ-010 applies to
// cancellation). It cannot undo an effect already started (ARCH-014).
func (s *Service) CancelInvocation(ctx context.Context, req *connect.Request[v1.CancelRequest]) (*connect.Response[v1.CancelResponse], error) {
	id, ok := identityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	if id.Kind == spiffe.KindRunner {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("runners cannot cancel invocations"))
	}
	inv, err := s.store.GetInvocation(ctx, req.Msg.InvocationId)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	// Ownership: resolve the caller's scope and require the invocation's pool
	// to match. Runner identities are already rejected above.
	if err := s.assertCallerOwnsInvocation(ctx, id, inv, req.Msg.FencingGeneration); err != nil {
		return nil, connect.NewError(connect.CodePermissionDenied, err)
	}
	// If terminal, return the committed state unchanged.
	if inv.State == InvocationSucceeded || inv.State == InvocationFailed || inv.State == InvocationOutcomeUnknown {
		return connect.NewResponse(&v1.CancelResponse{State: invocationStateToProto(inv.State)}), nil
	}
	// Propagate cancel to the runner serving this invocation (best-effort).
	s.pool.propagateCancel(ctx, inv.ID, req.Msg.Reason)
	return connect.NewResponse(&v1.CancelResponse{State: v1.InvokeState_INVOKE_STATE_RUNNING}), nil
}

// assertCallerOwnsInvocation proves the caller owns the SPECIFIC turn/workflow-
// step that owns the invocation, not just the pool (REQ-010). It re-resolves
// the caller's durable scope from the invocation's own recorded scope
// (attempt_id + caller_scope + caller_scope_id) via resolveCallerScope: the
// turn/run_step must still be active and its run must be durably assigned to
// the caller's pool. A supervisor whose pool matches but whose turn does not
// own the invocation is rejected. Runner identities are rejected upstream.
func (s *Service) assertCallerOwnsInvocation(ctx context.Context, id spiffe.Identity, inv Invocation, fencingGeneration uint64) error {
	if inv.PoolID == nil {
		return errors.New("invocation has no owning pool")
	}
	res, err := s.resolveCallerScope(ctx, id, &v1.DiscoverRequest{
		AttemptId:         inv.AttemptID,
		CallerScope:       callerScopeToProto(inv.CallerScope),
		CallerScopeId:     inv.CallerScopeID,
		FencingGeneration: fencingGeneration,
	})
	if err != nil {
		return errors.New("caller scope does not own this invocation")
	}
	if res.Pool.ID != *inv.PoolID || res.AttemptID != inv.AttemptID ||
		res.CallerScopeID != inv.CallerScopeID {
		return errors.New("caller scope does not own this invocation")
	}
	return nil
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
	// The deployment namespace on the wire must match the approval (the
	// SPIFFE id is /tool-runners/<namespace>/<runner-id>; the approval is keyed
	// by spiffe_id, so this is defense-in-depth). Tool-name namespace limits
	// are enforced per-descriptor in handleRegister (ARCH-015).
	if id.Namespace != approved.Namespace {
		return connect.NewError(connect.CodePermissionDenied, errors.New("runner deployment namespace does not match approval (ARCH-015)"))
	}

	gen := s.gen.Add(1)
	rc := &runnerConn{
		identity: id, runnerID: approved.RunnerID, gen: int64(gen), //nolint:gosec // G115: generation counter
		stream: st, pending: make(map[string]chan dispatchResult), tools: make(map[string]string),
	}
	s.pool.add(rc)
	defer func() {
		// Mark closed and drain in-flight sends BEFORE the handler returns so
		// no stream.Send is in progress when Connect closes the HTTP/2 response
		// writer. send() refuses to enter stream.Send once closed is set; waiting
		// on sendWG lets any in-flight send finish. Removal from the pool happens
		// after the closed flag so a dispatch that already picked this conn
		// observes closed and bails rather than racing teardown.
		rc.mu.Lock()
		rc.closed = true
		rc.mu.Unlock()
		rc.sendWG.Wait()
		s.pool.remove(rc)
		_ = s.store.DeactivateRunnerStream(ctx, rc.runnerID, int64(gen)) //nolint:gosec // G115
		// Any pending dispatchers on this conn observe streamLost.
		rc.mu.Lock()
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
	if err := rc.send(welcome(gen, s.cfg)); err != nil {
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
			_ = rc.send(&v1.RunnerControl{Kind: &v1.RunnerControl_Ack{Ack: &v1.Ack{Kind: &v1.Ack_Registered{Registered: m.Register.Descriptor_.Name}}}})
		case *v1.RunnerMessage_Heartbeat:
			_ = s.store.HeartbeatRunner(ctx, rc.runnerID, int64(gen)) //nolint:gosec // G115
			_ = rc.send(&v1.RunnerControl{Kind: &v1.RunnerControl_Ack{Ack: &v1.Ack{Kind: &v1.Ack_Heartbeat{Heartbeat: true}}}})
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
// immutable descriptor (idempotent on digest; fail-closed on bad descriptor).
// Each descriptor's tool-name namespace is validated against the runner's
// approved AllowedToolNamespaces (ARCH-015): an approved runner cannot
// publish or serve tool names outside its authorized namespace.
func (s *Service) handleRegister(ctx context.Context, reg *v1.Register, approved ApprovedRunner, rc *runnerConn) error {
	desc := reg.Descriptor_
	tv, err := descriptorToToolVersion(desc)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if !namespaceAllowed(approved.AllowedToolNamespaces, toolNamespace(tv.Name)) {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("tool %s namespace %q not permitted for runner %s (ARCH-015)", tv.Name, toolNamespace(tv.Name), rc.runnerID))
	}
	tv, err = s.store.RegisterToolVersion(ctx, tv)
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

// resolveCallerScope resolves the pool + permitted tools + attempt id + the
// authoritative caller scope for a caller from durable state (ARCH-004). The
// returned CallerScope/CallerScopeID are identity-derived and validated
// against runtime state; callers MUST use them for the ledger key, never the
// caller-supplied msg fields. Supervisor/turn: pool from SPIFFE, cross-checked
// against the active turn + run assignment. Workflow-step: pool + permitted
// tools from the run's workflow binding, cross-checked against the run
// assignment. Caller-supplied IDs are validated, never trusted as scope.
func (s *Service) resolveCallerScope(ctx context.Context, id spiffe.Identity, req *v1.DiscoverRequest) (CallerResolution, error) {
	switch id.Kind {
	case spiffe.KindSupervisor:
		// Pool is resolved from the verified SPIFFE id (prefix match), then
		// cross-checked against the active turn + run assignment + the active
		// turn-assignment worker + generation binding (HOR-249 / DEC-041).
		pool, err := s.store.ResolvePoolBySpiffePrefix(ctx, id.SPIFFEID)
		if err != nil {
			return CallerResolution{}, ErrScopeDenied
		}
		return s.store.ResolveTurnScope(ctx, pool.ID, req.AttemptId, req.CallerScopeId, id.WorkerID, req.FencingGeneration)
	case spiffe.KindControlPlaneWorkflow:
		// The run_step + run are validated; the workflow binding is derived
		// from the run's definition_key (NOT a caller-supplied key).
		return s.store.ResolveWorkflowStepScope(ctx, req.AttemptId, req.CallerScopeId)
	}
	return CallerResolution{}, ErrScopeDenied
}

// authorize evaluates the durable action/resource policy before the effect
// boundary (ARCH-008/016/018): workflow-permitted intersection + pool grant
// effect ceiling + action allow-list. Returns nil if authorized, an error
// (permission_denied ...) otherwise.
func (s *Service) authorize(ctx context.Context, poolID string, tv ToolVersion, permitted []Capability) (bool, error) {
	// Workflow-requested capability narrowing (ARCH-016). nil = no narrowing
	// (turn path); empty slice = explicitly none (deny all). The workflow's
	// maxEffectClass is enforced here so a workflow narrowed to read_only cannot
	// invoke a tool whose effect exceeds it, even when the pool grant ceiling is
	// higher — the narrowing is not widened back to the pool ceiling at runtime
	// (REQ-001/REQ-010). The workflow action allow-list is enforced using the
	// same approved effective-action rule as pool grants: an undecomposed v1
	// descriptor's action is its tool name; empty means effect-class-only.
	if err := authorizeWorkflowCapability(permitted, tv); err != nil {
		return false, err
	}
	grant, err := s.store.GetPoolGrant(ctx, poolID, tv.Name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, errors.New("tool not granted to pool")
		}
		return false, err
	}
	if effectRank(tv.EffectClass) > effectRank(grant.MaxEffectClass) {
		return false, fmt.Errorf("tool effect_class %s exceeds pool grant ceiling %s", tv.EffectClass, grant.MaxEffectClass)
	}
	// Action allow-list (ARCH-008/018). An empty allowed_actions means
	// effect-class-only (no action narrowing). Otherwise the tool's effective
	// action (the tool name when the descriptor declares no action
	// decomposition) must be in the list.
	if len(grant.AllowedActions) > 0 {
		action := actionForTool(tv)
		ok := false
		for _, a := range grant.AllowedActions {
			if a == action || a == "*" {
				ok = true
				break
			}
		}
		if !ok {
			return false, fmt.Errorf("action %q not permitted by pool grant", action)
		}
	}
	return true, nil
}

// authorizeWorkflowCapability enforces the workflow-requested tool/effect/action
// intersection. nil means no workflow narrowing (turn path); an empty slice
// denies every tool on the workflow-step path.
func authorizeWorkflowCapability(permitted []Capability, tv ToolVersion) error {
	if permitted == nil {
		return nil
	}
	for _, cap := range permitted {
		if cap.Tool != tv.Name {
			continue
		}
		if cap.MaxEffectClass != "" && effectRank(tv.EffectClass) > effectRank(EffectClass(cap.MaxEffectClass)) {
			return fmt.Errorf("tool effect_class %s exceeds workflow-requested ceiling %s", tv.EffectClass, cap.MaxEffectClass)
		}
		if !capabilityAllowsAction(cap, tv) {
			return fmt.Errorf("action %q not permitted by workflow capability", actionForTool(tv))
		}
		return nil
	}
	return errors.New("tool not in workflow-permitted set")
}

// actionForTool derives the effective action a tool invocation targets. v1
// treats an undeclared action as the single action "<tool_name>" (SD-3); tool
// descriptors may declare action decomposition in a later revision.
func actionForTool(tv ToolVersion) string { return tv.Name }

// capabilityAllowsAction applies a workflow capability's action narrowing to a
// concrete tool descriptor. Empty actions means effect-class-only; otherwise
// the approved effective action (v1 undecomposed descriptor = tool name) or a
// wildcard must be explicitly present.
func capabilityAllowsAction(cap Capability, tv ToolVersion) bool {
	if len(cap.Actions) == 0 {
		return true
	}
	action := actionForTool(tv)
	for _, allowed := range cap.Actions {
		if allowed == action || allowed == "*" {
			return true
		}
	}
	return false
}

// toolNamespace returns the namespace prefix of a tool name — the segment
// before the first '.' (e.g. "graph.read_mail" -> "graph"), or the whole name
// when it has no namespace separator. ARCH-015 constrains runner registration
// to the runner's permitted tool namespace.
func toolNamespace(toolName string) string {
	if i := strings.Index(toolName, "."); i > 0 {
		return toolName[:i]
	}
	return toolName
}

// dispatchTimeout returns the per-invocation dispatch timeout for a tool
// version: the descriptor's timeout, or the gateway default.
func (s *Service) dispatchTimeout(tv ToolVersion) time.Duration {
	if tv.TimeoutMS > 0 {
		return time.Duration(tv.TimeoutMS) * time.Millisecond
	}
	return s.cfg.DefaultTimeout
}

// assertResourcePolicy enforces the resource-scope boundary before the effect
// (ARCH-008/018): each credential binding's non-secret resource_constraints
// (tenant/site/mailbox scope) is asserted against the resource target derived
// from the validated arguments. An argument that names a constrained dimension
// with a value outside the binding's allowed set is denied — input content
// cannot widen the durable resource scope. Dimensions absent from the
// arguments are not widened (the runner still receives ResourceConstraintsJson
// and must honor the binding's fixed scope). The pool-level grants + bindings
// are the v1 customer/identity policy (ARCH-018).
func (s *Service) assertResourcePolicy(ctx context.Context, poolID, toolName string, argsJSON []byte) error {
	bindings, err := s.store.ResolveCredentialBindings(ctx, poolID, toolName)
	if err != nil {
		return fmt.Errorf("resolve resource policy: %w", err)
	}
	if len(bindings) == 0 {
		return nil // no bindings -> no resource policy for this tool
	}
	var args map[string]any
	if len(argsJSON) > 0 {
		if err := json.Unmarshal(argsJSON, &args); err != nil {
			return fmt.Errorf("invalid arguments json for resource check: %w", err)
		}
	}
	for _, b := range bindings {
		if len(b.ResourceConstraints) == 0 || string(b.ResourceConstraints) == "{}" {
			continue // no resource narrowing on this binding
		}
		var constraints map[string]any
		if err := json.Unmarshal(b.ResourceConstraints, &constraints); err != nil {
			return fmt.Errorf("invalid resource_constraints for slot %s: %w", b.SlotName, err)
		}
		for dim, allowed := range constraints {
			argVal, present := args[dim]
			if !present {
				continue // argument does not target this dimension; runner honors the fixed scope
			}
			argStr, ok := argVal.(string)
			if !ok {
				continue // non-string arg cannot widen a string scope dimension
			}
			if !resourceAllowed(allowed, argStr) {
				return fmt.Errorf("argument %q=%q exceeds resource constraint on slot %s (ARCH-008/018)", dim, argStr, b.SlotName)
			}
		}
	}
	return nil
}

// resourceAllowed reports whether val is within the allowed scope value (a
// single string or a list of strings; "*" is a wildcard).
func resourceAllowed(allowed any, val string) bool {
	switch a := allowed.(type) {
	case string:
		return a == val || a == "*"
	case []any:
		for _, x := range a {
			if s, ok := x.(string); ok && (s == val || s == "*") {
				return true
			}
		}
	}
	return false
}

// namespaceAllowed reports whether the runner is permitted to register a
// tool in namespace ns (ARCH-015). An empty allowed list is DENY: an approved
// runner must declare an explicit permitted tool namespace, and the default
// approval record (allowed_tool_namespaces = '{}') fails closed rather than
// being treated as unrestricted. "*" is a wildcard.
func namespaceAllowed(allowed []string, ns string) bool {
	if len(allowed) == 0 {
		return false // no permitted namespace -> deny (ARCH-015 fail-closed)
	}
	for _, a := range allowed {
		if a == ns || a == "*" {
			return true
		}
	}
	return false
}

// renewDispatchLease transitions a freshly committed invocation to 'running'
// (first attempt, dispatching -> running) or renews the lease on an
// already-running row (retry attempt), so a multi-attempt retry whose
// cumulative duration exceeds the original lease is not terminalized by the
// SCN-008 recovery sweep mid-dispatch (ARCH-014). It returns a non-nil error
// if the lease could not be established or renewed — including the
// zero-rows case where recovery has already terminalized the row — so the
// caller can fail closed before the effect boundary.
func (s *Service) renewDispatchLease(ctx context.Context, invID string, timeout time.Duration) error {
	leaseExpiresAt := time.Now().Add(timeout + s.cfg.DispatchLease)
	if err := s.store.MarkRunning(ctx, invID, "", leaseExpiresAt, s.cfg.GatewayInstanceID); err != nil {
		if !errors.Is(err, ErrInvalidTransition) {
			return fmt.Errorf("mark running: %w", err)
		}
		// Already 'running' (retry attempt): renew the lease. A zero-rows result
		// (ErrInvalidTransition) means recovery terminalized the row; surface it
		// so the caller does not dispatch.
		if rerr := s.store.RenewDispatchLease(ctx, invID, s.cfg.GatewayInstanceID, leaseExpiresAt); rerr != nil {
			return fmt.Errorf("renew dispatch lease: %w", rerr)
		}
	}
	return nil
}

// abortAfterLeaseFailure handles a dispatch/retry that could not (re-)establish
// its safety lease before the effect boundary (ARCH-014/SCN-008). The
// invocation MUST NOT be dispatched. If the SCN-008 recovery sweep already
// terminalized the row, return that committed durable state (a duplicate must
// reflect the committed outcome, never repeat the effect — REQ-009).
// Otherwise commit outcome_unknown (writes) / failed (read_only) fail-closed.
func (s *Service) abortAfterLeaseFailure(ctx context.Context, inv Invocation, tv ToolVersion, reason string) (*v1.InvokeResponse, error) {
	if cur, err := s.store.GetInvocation(ctx, inv.ID); err == nil && cur.State.IsTerminal() {
		return invocationToResponse(cur), nil
	}
	return s.classifyAmbiguous(ctx, inv, tv, reason)
}

func mapErr(err error) error {
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrScopeDenied) {
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

// detachedCtx returns a context that survives caller cancellation, for
// terminal ledger commits after a possible effect (ARCH-014). If the request
// context is still alive it is returned unchanged; otherwise a fresh
// non-cancellable context (context.WithoutCancel) with a bounded timeout is
// used so the durable outcome always commits even after the caller disconnects.
// The returned cancel must be deferred by the caller.
func (s *Service) detachedCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

// jsonValid reports whether b is valid JSON.
func jsonValid(b []byte) bool { return json.Valid(b) }
