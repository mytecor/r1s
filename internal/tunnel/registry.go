package tunnel

import (
	"bytes"
	"errors"
	"sync"
	"time"
)

// RegistryConfig controls grant identity generation.
type RegistryConfig struct {
	// NewID generates a unique grant ID. Required.
	NewID func() string
}

// Grant is a minted, single-use, execution-scoped access grant.
type Grant struct {
	ExecutionID string
	ID          string
	ExpiresAt   time.Time
}

// Session is the single live tunnel session for one execution, opened at
// accept and bound to the execution lifecycle. It carries the authenticated
// peer key reported by the edge and the allocator-local target the edge shall
// splice to.
type Session struct {
	ExecutionID string
	PeerKey     []byte
	Target      Target
	Endpoint    Endpoint
}

type grantState struct {
	id        string
	expiresAt time.Time
	consumed  bool
}

// record is the grant-and-session state for one execution.
type record struct {
	executionID string
	peerKey     []byte
	target      Target
	endpoint    Endpoint
	grant       *grantState
	session     *Session
}

// Registry is the allocator-side in-memory registry of per-execution tunnel
// grants and active sessions (F14-01). It stores exactly one record per
// execution: a repeat mint replaces an outstanding unconfirmed grant, a re-mint
// immediately after a session close is instantaneous, and the single live
// session per execution is enforced at accept, not at mint.
//
// The registry is deliberately not persisted: after an allocator restart the
// client simply requests a new grant. Expiry is evaluated lazily at mint and
// at accept; there is no TTL sweeper goroutine. Revocation is record removal
// via Invalidate. Callers must hold their own execution authority; this
// registry only manages grant and session lifecycle.
type Registry struct {
	mu      sync.Mutex
	newID   func() string
	records map[string]*record
}

// NewRegistry constructs an empty registry.
func NewRegistry(config RegistryConfig) (*Registry, error) {
	if config.NewID == nil {
		return nil, errors.New("invalid tunnel registry configuration: ID generator is required")
	}
	return &Registry{
		newID:   config.NewID,
		records: make(map[string]*record),
	}, nil
}

// Mint creates the outstanding grant for one execution, or replaces a
// previously minted, still-unconsumed grant. The peer key from the grant
// request is pinned into the record; a repeat mint with a different key repins
// it. A mint while a session is active replaces the outstanding grant without
// disturbing the session — the per-execution cap of one session is enforced at
// accept. Expiry is granted from now; a TTL is not tracked in the record
// because it is evaluated lazily at accept.
func (r *Registry) Mint(executionID string, peerKey []byte, target Target, endpoint Endpoint, ttl time.Duration, now time.Time) (Grant, error) {
	if executionID == "" {
		return Grant{}, errors.New("invalid tunnel grant: execution ID is required")
	}
	if ttl <= 0 {
		return Grant{}, errors.New("invalid tunnel grant: grant TTL must be positive")
	}
	id := r.newID()
	if id == "" {
		return Grant{}, errors.New("invalid tunnel registry: ID generator returned an empty ID")
	}
	expiresAt := now.Add(ttl)
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[executionID]
	if rec == nil {
		rec = &record{executionID: executionID, target: target, endpoint: endpoint}
		r.records[executionID] = rec
	}
	rec.peerKey = append([]byte(nil), peerKey...)
	// A repeat mint replaces the outstanding grant, keeping any active session
	// intact; the new grant is unconsumed and single-use.
	rec.grant = &grantState{id: id, expiresAt: expiresAt}
	return Grant{ExecutionID: executionID, ID: id, ExpiresAt: expiresAt}, nil
}

// Accept validates the routing preamble (execution ID and grant ID) against
// the authenticated peer key and opens the execution's single live session.
// The grant is consumed only on a successful accept. It rejects:
//
//   - an unknown execution or grant, or an execution with no minted grant;
//   - an expired grant (expiry is evaluated here, lazily);
//   - a reused (already consumed) grant;
//   - a grant pinned to a different peer key than the authenticated one;
//   - a second accept while the execution already has a live session.
//
// A failed validation leaves the grant unconsumed and reusable until its
// expiry.
func (r *Registry) Accept(executionID, grantID string, peerKey []byte, now time.Time) (*Session, error) {
	if executionID == "" || grantID == "" {
		return nil, errors.New("invalid tunnel accept: execution and grant IDs are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[executionID]
	if rec == nil || rec.grant == nil {
		return nil, ErrGrantNotFound
	}
	if rec.session != nil {
		return nil, ErrSessionBusy
	}
	grant := rec.grant
	if grant.id != grantID {
		return nil, ErrGrantNotFound
	}
	if !rec.grant.expiresAt.After(now) {
		return nil, ErrGrantExpired
	}
	if grant.consumed {
		return nil, ErrGrantReused
	}
	if !bytes.Equal(rec.peerKey, peerKey) {
		return nil, ErrPeerKeyMismatch
	}
	grant.consumed = true
	session := &Session{
		ExecutionID: executionID,
		PeerKey:     append([]byte(nil), peerKey...),
		Target:      rec.target,
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
	return &session, true
}

// RejectBeforeSplice is a no-op placeholder for the F14-02 accept-time routing
// hook: a failed validation before splice must leave the grant unconsumed and
// reusable. The registry already enforces that inside Accept; this method
// documents the contract for the edge plumbing and is retained so the core
// lifecycle coupling has a single call site when sessions end without a
// successful splice.
func (r *Registry) RejectBeforeSplice(executionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[executionID]
	if rec == nil || rec.session != nil {
		return
	}
	// Keep the unconsumed grant; it remains reusable until expiry. Nothing to
	// do here in F14-01; F14-02's edge calls this before relaying payload.
}

// CloseSession ends the active session for an execution without cancelling the
// execution and without disturbing any outstanding grant. Losing a tunnel
// never cancels the execution.
func (r *Registry) CloseSession(executionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec := r.records[executionID]; rec != nil {
		rec.session = nil
	}
}

// Invalidate deletes the single record for an execution: it closes any active
// session and drops any outstanding grant. It is the terminal cleanup path —
// when an execution becomes terminal the allocator calls this in the same
// local sweep that commits the terminal state. Reuse of an invalidated grant is
// rejected (the record no longer exists).
func (r *Registry) Invalidate(executionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, executionID)
}
