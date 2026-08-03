package dispatch

import (
	"sync"
	"time"

	connect "connectrpc.com/connect"
	v1 "github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1"
)

// workerConn is one live warm-worker Work bidi stream. The worker is the mTLS
// gRPC client; the control-plane is the server. One connection per (pool, pod
// name) at a time — a reconnect fences the prior generation.
type workerConn struct {
	poolID   string // pool UID (verified cert SAN)
	workerID string // pod name (verified cert SAN)
	gen      int64  // fencing generation (CP-assigned, monotonic)
	stream   *connect.BidiStream[v1.WorkerMessage, v1.ControlMessage]

	mu         sync.Mutex
	closed     bool
	idle       bool   // has an unspent Ready credit
	activeTurn string // turn_id currently assigned ("" when idle)
	lastSeen   time.Time

	// sendMu serializes all server->worker ControlMessage sends. The connect
	// bidi writer ultimately shares the HTTP response writer; concurrent sends
	// from the receive loop (EventAck), the dispatcher (AssignTurn),
	// cancellation (AbortTurn) and session-end (SessionEnd) would race/corrupt
	// the stream. All sends MUST go through send().
	sendMu sync.Mutex

	// done is closed by markClosed to unblock the receive loop (F5: a fenced
	// prior generation must stop acting; its receive loop is blocked on
	// stream.Receive, so a reader goroutine forwards into recvCh and the loop
	// selects on recvCh/done). Closed once.
	done      chan struct{}
	recvCh    chan recvResult
	closeOnce sync.Once
}

// recvResult is one forwarded stream.Receive result.
type recvResult struct {
	msg *v1.WorkerMessage
	err error
}

// startReader launches a goroutine that forwards stream.Receive results into
// recvCh until the stream errors. It is the sole reader of stream.Receive; the
// Work handler selects on recvCh/done so markClosed can unblock the loop
// without waiting for the (possibly lingering) old stream to tear down.
func (w *workerConn) startReader() {
	go func() {
		for {
			msg, err := w.stream.Receive()
			select {
			case w.recvCh <- recvResult{msg: msg, err: err}:
				if err != nil {
					return
				}
			case <-w.done:
				return
			}
		}
	}()
}

// send serializes a ControlMessage send. Safe for concurrent callers.
func (w *workerConn) send(msg *v1.ControlMessage) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	return w.stream.Send(msg)
}

// markClosed marks the conn closed so dispatches/fences stop using it and
// unblocks the receive loop by closing done. Idempotent.
func (w *workerConn) markClosed() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.closeOnce.Do(func() { close(w.done) })
}

// tryConsumeCredit atomically consumes the Ready credit for a turn assignment.
// Returns false if the worker is not idle (no credit) or the conn is closed —
// the dispatcher must pick another worker. One credit => one assignment; a
// second assign before the next Ready is a protocol violation (ARCH: one-credit
// dispatch, fail-closed).
func (w *workerConn) tryConsumeCredit(turnID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || !w.idle || w.activeTurn != "" {
		return false
	}
	w.idle = false
	w.activeTurn = turnID
	return true
}

// releaseTurn clears the active turn after terminalization. The worker is NOT
// idle again until it sends the next Ready (credit is per-assignment).
func (w *workerConn) releaseTurn() {
	w.mu.Lock()
	w.activeTurn = ""
	w.mu.Unlock()
}

// grantCreditIfIdle atomically checks the worker is not busy and grants the
// Ready credit. Returns false (and is a protocol violation) if a turn is
// active; the caller closes the stream fail-closed. The busy check and credit
// grant are one locked operation so the reconciler cannot race a concurrent
// assignment between the check and the grant.
func (w *workerConn) grantCreditIfIdle() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.activeTurn != "" {
		return false
	}
	w.idle = true
	w.lastSeen = time.Now()
	return true
}

// workerPool tracks live worker connections keyed by (pool, worker). At most
// one conn per key; a reconnect fences the prior conn (returned by add).
type workerPool struct {
	mu    sync.Mutex
	conns map[string]*workerConn // key = poolID + "/" + workerID
}

func newWorkerPool() *workerPool { return &workerPool{conns: make(map[string]*workerConn)} }

func workerKey(poolID, workerID string) string { return poolID + "/" + workerID }

// add registers a new connection. If a prior connection exists for the same
// (pool, worker), it is returned for fencing (caller closes it + fences its
// assignment); the new conn replaces it.
func (p *workerPool) add(w *workerConn) *workerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := workerKey(w.poolID, w.workerID)
	old := p.conns[key]
	p.conns[key] = w
	return old
}

// remove drops a connection if it is still the registered one (a fenced conn is
// no longer the registered one and is a no-op).
func (p *workerPool) remove(w *workerConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := workerKey(w.poolID, w.workerID)
	if p.conns[key] == w {
		delete(p.conns, key)
	}
}

// get returns the live connection for (pool, worker), or nil.
func (p *workerPool) get(poolID, workerID string) *workerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conns[workerKey(poolID, workerID)]
}

// pickIdle selects an idle worker in poolID and consumes its credit for
// turnID. Returns nil if no idle worker is available in the pool.
func (p *workerPool) pickIdle(poolID, turnID string) *workerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.conns {
		if w.poolID != poolID {
			continue
		}
		if w.tryConsumeCredit(turnID) {
			return w
		}
	}
	return nil
}

// activeConn returns the conn currently holding an active assignment for the
// given turn (across all pools), or nil. Used to route AbortTurn.
func (p *workerPool) activeConn(turnID string) *workerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.conns {
		w.mu.Lock()
		active := w.activeTurn == turnID && !w.closed
		w.mu.Unlock()
		if active {
			return w
		}
	}
	return nil
}

// all returns a snapshot of all live connections (for lease monitoring).
func (p *workerPool) all() []*workerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*workerConn, 0, len(p.conns))
	for _, w := range p.conns {
		out = append(out, w)
	}
	return out
}

// lastSeenAt returns the worker's last message time under its lock.
func (w *workerConn) lastSeenAt() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSeen
}
