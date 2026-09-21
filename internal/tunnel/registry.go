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

// Session is the single live tunnel session (mesh connection) for one
// execution, opened at accept and bound to the execution lifecycle. It
// carries the authenticated peer key reported by the edge and the
// client-supplied target slot list the edge may splice streams to. Each
// opening stream references exactly one slot (or the default); every slot
// came from the client-supplied list the allocator bound into the minted
// grant (validated only for well-formedness).
type Session struct {
	ExecutionID   string
	PeerKey       []byte
	Targets       []Target
	DefaultTarget Target
	Endpoint      Endpoint
}

type grantState struct {
	id        string
	expiresAt time.Time
	consumed  bool
}

// record is the grant-and-session state for one execution.
type record struct {
	executionID   string
	peerKey       []byte
	targets       []Target
	defaultTarget Target
	endpoint      Endpoint
	grant         *grantState
	session       *Session
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
// previously minted, still-unconsumed grant. The peer key, target slot list,
// and endpoint from the grant request are pinned into the record; a repeat
// mint with new values repins all three. A mint while a session is active
// replaces the outstanding grant without disturbing the session — the
// per-execution cap of one session is enforced at accept. Expiry is granted
// from now; a TTL is not tracked in the record because it is evaluated lazily
// at accept.
//
// targets is the client-supplied slot list bound into the grant; defaultTarget
// is the slot a stream with no target_slot references is spliced to (the
// interactive pipe).
func (r *Registry) Mint(executionID string, peerKey []byte, targets []Target, defaultTarget Target, endpoint Endpoint, ttl time.Duration, now time.Time) (Grant, error) {
	if executionID == "" {
		return Grant{}, errors.New("invalid tunnel grant: execution ID is required")
	}
	if ttl <= 0 {
		return Grant{}, errors.New("invalid tunnel grant: grant TTL must be positive")
	}
	if len(targets) == 0 {
		return Grant{}, errors.New("invalid tunnel grant: at least one target slot is required")
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
		rec = &record{executionID: executionID}
		r.records[executionID] = rec
	}
	rec.peerKey = append([]byte(nil), peerKey...)
	rec.targets = cloneTargets(targets)
	rec.defaultTarget = defaultTarget
	// Clone the endpoint slices: the caller owns the backing arrays and may
	// reuse them, and the record outlives the Mint call. peerKey is cloned
	// above for the same reason.
	rec.endpoint = Endpoint{
		Address: append([]byte(nil), endpoint.Address...),
		PubKey:  append([]byte(nil), endpoint.PubKey...),
	}
	// A repeat mint replaces the outstanding grant, keeping any active session
	// intact; the new grant is unconsumed and single-use.
	rec.grant = &grantState{id: id, expiresAt: expiresAt}
	return Grant{ExecutionID: executionID, ID: id, ExpiresAt: expiresAt}, nil
}

// Accept validates the routing preamble (execution ID and grant ID) against
// the authenticated peer key and opens the execution's single live session.
// The grant is consumed only on a successful accept. It rejects:
//
//   - an unknown execution or grant;
//   - an expired grant (expiry is evaluated here, lazily);
//   - a reused (already consumed) grant;
//   - a grant pinned to a different peer key than the authenticated one;
//   - a second accept while the execution already has a live session.
//
// The ordering matters for an unauthenticated peer:
//
//   - the auth checks (grant existence, grant ID, expiry, peer key) all
//     precede the liveness (busy) check, so an unauthenticated client cannot
//     probe whether an execution's session is busy;
//   - expiry is evaluated before the peer key so a stale grant fails fast and
//     uniformly; the expiry bit is low-sensitivity (it only reveals that a
//     grant is no longer usable), unlike session liveness, so leaking it to
//     an unauthenticated peer is acceptable.
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
	grant := rec.grant
	if grant.id != grantID {
		return nil, ErrGrantNotFound
	}
	if !rec.grant.expiresAt.After(now) {
		return nil, ErrGrantExpired
	}
	// The peer key is validated before the liveness (busy) check so an
	// unauthenticated peer cannot probe whether an execution's session is busy.
	if !bytes.Equal(rec.peerKey, peerKey) {
		return nil, ErrPeerKeyMismatch
	}
	if rec.session != nil {
		return nil, ErrSessionBusy
	}
	if grant.consumed {
		return nil, ErrGrantReused
	}
	grant.consumed = true
	session := &Session{
		ExecutionID:   executionID,
		PeerKey:       append([]byte(nil), peerKey...),
		Targets:       cloneTargets(rec.targets),
		DefaultTarget: rec.defaultTarget,
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

// cloneTargets deep-copies a target slot list so the caller's backing slice is
// never aliased by a long-lived record or session.
func cloneTargets(targets []Target) []Target {
	if targets == nil {
		return nil
	}
	cloned := make([]Target, len(targets))
	copy(cloned, targets)
	return cloned
}

// ResolveTarget returns the target slot for the given slot ID, or the default
// slot when id is empty. An unknown non-empty id reports false, so the edge
// can reject the stream with ReasonUnauthorized before any payload byte moves.
func (s *Session) ResolveTarget(id string) (Target, bool) {
	if id == "" {
		return s.DefaultTarget, true
	}
	for _, target := range s.Targets {
		if target.ID == id {
			return target, true
		}
	}
	return Target{}, false
}
