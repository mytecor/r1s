package tunnel

import (
	"bytes"
	"errors"
	"sync"
)

// Registry is the allocator-side in-memory registry of per-execution tunnel
// bindings and active sessions (F22-06). It stores exactly one record per
// execution: a repeat open replaces the outstanding binding (repinning the peer
// key and target list), a re-open immediately after a session close is
// instantaneous, and the single live session per execution is enforced at
// accept, not at bind.
//
// The registry is deliberately not persisted: after an allocator restart the
// client simply re-declares a fresh authenticated open. There is no TTL
// sweeper goroutine because there is no TTL: the binding lives for the
// execution's lifetime and is revoked by record removal via Invalidate (a
// terminal execution) or CloseSession (an ended session). Callers must hold
// their own execution authority; this registry only manages binding and
// session lifecycle.
type Registry struct {
	mu      sync.Mutex
	records map[string]*record
}

// NewRegistry constructs an empty registry.
func NewRegistry() *Registry {
	return &Registry{records: make(map[string]*record)}
}

// Bind records the authenticated owner's tunnel declaration for one execution
// (F22-06): the client's edge node public key and container-port target list
// are bound to the execution, replacing any previously bound values. A bind
// while a session is active replaces the binding without disturbing the
// session — each open stream still resolves its target against the bound list
// recorded at the time it is authorized, and the per-execution cap of one
// session is enforced at accept. The endpoint advertisement is the allocator's
// transport-neutral overlay destination returned in the open ack.
//
// Bind itself is not authorization: the caller (the allocator) must first
// verify the authenticated transport sender is the execution owner. targets is
// the client-supplied container port list bound to the execution.
func (r *Registry) Bind(executionID string, peerKey []byte, targets []Target, endpoint Endpoint) error {
	if executionID == "" {
		return errors.New("tunnel binding: execution ID is required")
	}
	if len(peerKey) == 0 {
		return errors.New("tunnel binding: peer key is required")
	}
	if len(targets) == 0 {
		return errors.New("tunnel binding: at least one target port is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[executionID]
	if rec == nil {
		rec = &record{executionID: executionID}
		r.records[executionID] = rec
	}
	rec.peerKey = append([]byte(nil), peerKey...)
	rec.targets = cloneTargets(targets)
	// Clone the endpoint slices: the caller owns the backing arrays and may
	// reuse them, and the record outlives the Bind call. peerKey is cloned
	// above for the same reason.
	rec.endpoint = Endpoint{
		Address: append([]byte(nil), endpoint.Address...),
		PubKey:  append([]byte(nil), endpoint.PubKey...),
	}
	return nil
}

// Open validates the edge's authenticated mesh peer key against the execution's
// bound peer key and opens the execution's single live session. It rejects:
//
//   - an unknown execution or a binding that was never declared;
//   - a peer key that does not match the execution's bound key;
//   - a second open while the execution already has a live session.
//
// The ordering matters for an unauthenticated peer:
//
//   - the auth checks (binding existence, peer key) both precede the liveness
//     (busy) check, so an unauthenticated client cannot probe whether an
//     execution's session is busy;
//   - execution liveness is validated by the allocator around this call, so the
//     registry itself never needs to know execution phase.
func (r *Registry) Open(executionID string, peerKey []byte) (*Session, error) {
	if executionID == "" {
		return nil, errors.New("invalid tunnel open: execution ID is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[executionID]
	if rec == nil {
		return nil, ErrTunnelNotBound
	}
	// The peer key is validated before the liveness (busy) check so an
	// unauthenticated peer cannot probe whether an execution's session is busy.
	if !bytes.Equal(rec.peerKey, peerKey) {
		return nil, ErrPeerKeyMismatch
	}
	if rec.session != nil {
		return nil, ErrSessionBusy
	}
	session := &Session{
		ExecutionID: executionID,
		done:        make(chan struct{}),
		PeerKey:     append([]byte(nil), peerKey...),
		Targets:     cloneTargets(rec.targets),
		Endpoint: Endpoint{
			Address: append([]byte(nil), rec.endpoint.Address...),
			PubKey:  append([]byte(nil), rec.endpoint.PubKey...),
		},
	}
	rec.session = session
	return session, nil
}

// Session returns the active session for an execution, if any. Used by the
// lifecycle coupling so a terminal execution can close its sessions.
func (r *Registry) Session(executionID string) (*Session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[executionID]
	if rec == nil || rec.session == nil {
		return nil, false
	}
	session := *rec.session
	session.PeerKey = append([]byte(nil), rec.session.PeerKey...)
	session.Targets = cloneTargets(rec.session.Targets)
	return &session, true
}

// CloseSession ends the active session for an execution without cancelling the
// execution and without disturbing the outstanding binding. Losing a tunnel
// never cancels the execution; a later owner connection re-opens a fresh
// session against the same binding.
func (r *Registry) CloseSession(executionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec := r.records[executionID]; rec != nil && rec.session != nil {
		close(rec.session.done)
		rec.session = nil
	}
}

// Invalidate deletes the single record for an execution: it closes any active
// session and drops the binding. It is the terminal cleanup path — when an
// execution becomes terminal the allocator calls this in the same local sweep
// that commits the terminal state. A later connection against the deleted
// binding is rejected (the record no longer exists), so a stale execution can
// never win a tunnel after it ends.
func (r *Registry) Invalidate(executionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec := r.records[executionID]; rec != nil && rec.session != nil {
		close(rec.session.done)
	}
	delete(r.records, executionID)
}
