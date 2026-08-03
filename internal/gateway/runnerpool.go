package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	connect "connectrpc.com/connect"
	v1 "github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1"
	"github.com/nunocgoncalves/control-plane/internal/spiffe"
)

// dispatchResult is the outcome of dispatching one Invoke over a runner stream.
// Either state is set (a runner-reported result) or streamLost is true (the
// runner stream dropped before a result arrived, leaving the effect ambiguous).
type dispatchResult struct {
	state        InvocationState // succeeded | failed
	resultJSON   []byte
	artifactRefs []byte // JSONB []ArtifactRef
	errorDetail  []byte // JSONB ErrorDetail
	streamLost   bool
}

// runnerConn is one live runner bidi stream. Invocations are dispatched over it
// and their results arrive on the receive loop, routed to the waiting caller by
// invocation id.
type runnerConn struct {
	identity spiffe.Identity
	runnerID string
	gen      int64
	stream   *connect.BidiStream[v1.RunnerMessage, v1.RunnerControl]

	mu      sync.Mutex
	pending map[string]chan dispatchResult // invocationID -> result chan
	tools   map[string]string              // tool_name -> digest (registered by this conn)
	closed  bool

	// sendMu serializes all runner->stream sends (Invoke dispatch + Cancel).
	// The connect bidi writer shares the HTTP response writer; concurrent
	// dispatches to the same runner would race/corrupt the stream (same class
	// as the dispatch Work stream, F2).
	sendMu sync.Mutex
}

// send serializes a RunnerControl send on the runner stream.
func (rc *runnerConn) send(msg *v1.RunnerControl) error {
	rc.sendMu.Lock()
	defer rc.sendMu.Unlock()
	return rc.stream.Send(msg)
}

// registerTool records a tool version served by this runner.
func (rc *runnerConn) registerTool(name, digest string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.tools[name] = digest
}

// serves reports whether this conn serves the given (tool, digest).
func (rc *runnerConn) serves(tool, digest string) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	d, ok := rc.tools[tool]
	return ok && d == digest
}

// dispatch sends an Invoke over the stream and waits for the result (or stream
// loss / timeout). The caller has already committed the ledger row (dispatching)
// and will transition running/terminal based on the result.
func (rc *runnerConn) dispatch(ctx context.Context, invoke *v1.RunnerControl, invocationID string, timeout time.Duration) (dispatchResult, error) {
	ch := make(chan dispatchResult, 1)
	rc.mu.Lock()
	if rc.closed {
		rc.mu.Unlock()
		return dispatchResult{streamLost: true}, nil
	}
	rc.pending[invocationID] = ch
	rc.mu.Unlock()
	defer func() {
		rc.mu.Lock()
		delete(rc.pending, invocationID)
		rc.mu.Unlock()
	}()

	if err := rc.send(invoke); err != nil {
		// Send failure => stream effectively lost; the receive loop will tear down.
		return dispatchResult{streamLost: true}, nil
	}

	timeoutCh := make(<-chan time.Time)
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timeoutCh = t.C
	}
	select {
	case r := <-ch:
		return r, nil
	case <-timeoutCh:
		// Timeout after dispatch => ambiguous outcome. The caller classifies by
		// effect class (retry or outcome_unknown). The runner may still report
		// late; that result is dropped (the ledger already terminalized).
		return dispatchResult{streamLost: true}, nil
	case <-ctx.Done():
		return dispatchResult{streamLost: true}, ctx.Err()
	}
}

// deliver routes a runner-reported result to the waiting dispatcher.
func (rc *runnerConn) deliver(invocationID string, r dispatchResult) bool {
	rc.mu.Lock()
	ch, ok := rc.pending[invocationID]
	rc.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- r:
	default:
	}
	return true
}

// runnerPool tracks live runner connections keyed by tool, for dispatch.
type runnerPool struct {
	mu    sync.Mutex
	conns []*runnerConn
}

func newRunnerPool() *runnerPool { return &runnerPool{} }

// add registers a runner connection.
func (p *runnerPool) add(rc *runnerConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns = append(p.conns, rc)
}

// remove drops a connection and returns it.
func (p *runnerPool) remove(target *runnerConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, rc := range p.conns {
		if rc == target {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
}

// pick selects a live runner serving (tool, digest). v1: round-robin-ish (first
// match); a disconnected runner is removed from the pool by its receive loop.
func (p *runnerPool) pick(tool, digest string) *runnerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, rc := range p.conns {
		if rc.serves(tool, digest) {
			return rc
		}
	}
	return nil
}

// ErrNoRunner means no live runner serves the requested (tool, digest). For a
// NEW invocation this is an attributable failure; for a pinned active attempt
// the gateway fails rather than substitute (ARCH-007).
var ErrNoRunner = errors.New("gateway: no live runner for the requested tool version")

// hasRunner reports whether any live runner serves (tool, digest).
func (p *runnerPool) hasRunner(tool, digest string) bool { return p.pick(tool, digest) != nil }

// dispatchToRunner selects a runner and dispatches; helper to keep the service
// handler readable.
func (p *runnerPool) dispatchToRunner(ctx context.Context, tool, digest string, invoke *v1.RunnerControl, invocationID string, timeout time.Duration) (dispatchResult, error) {
	rc := p.pick(tool, digest)
	if rc == nil {
		return dispatchResult{}, ErrNoRunner
	}
	return rc.dispatch(ctx, invoke, invocationID, timeout)
}

// toolAvailable reports whether a tool version has any live healthy runner.
func (p *runnerPool) toolAvailable(tool, digest string) bool { return p.hasRunner(tool, digest) }

// propagateCancel sends a Cancel to the runner serving an in-flight invocation
// (best-effort). It cannot undo an effect already started (ARCH-014).
func (p *runnerPool) propagateCancel(ctx context.Context, invocationID, reason string) {
	p.mu.Lock()
	conns := make([]*runnerConn, len(p.conns))
	copy(conns, p.conns)
	p.mu.Unlock()
	for _, rc := range conns {
		rc.mu.Lock()
		_, pending := rc.pending[invocationID]
		rc.mu.Unlock()
		if pending {
			_ = rc.send(&v1.RunnerControl{Kind: &v1.RunnerControl_Cancel{Cancel: &v1.Cancel{
				InvocationId: invocationID, Reason: reason,
			}}})
			return
		}
	}
}
