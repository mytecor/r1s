package yggdrasil

import (
	"github.com/mytecor/r1s/internal/tunnel"
)

// Listener is the allocator-side overlay listener. It accepts inbound tunnel
// mesh connections (pairs) and exposes each accepted connection's authenticated
// peer key and routing preamble, so the allocator core can validate the grant
// before a single stream (and thus payload byte) is spliced.
//
// The listener is backed by the edge's packet mux: the first packet from a
// previously unseen peer opens a pending pair, and Accept returns it after the
// preamble frame arrived. The caller (the r1sd edge loop) validates the preamble
// against the allocator core, and for an accepted pair then consumes authorized
// streams through AcceptStream, splicing each one to its allocator-resolved
// target slot (F19-01).
type Listener struct {
	edge *edge
}

// NewListener constructs the allocator overlay listener on a started node.
// The returned listener reports its endpoint advertisement (address + public
// key) so minted grants advertise where the client dials and which key to pin.
func NewListener(node *Node) (*Listener, error) {
	e, err := newEdge(node)
	if err != nil {
		return nil, err
	}
	return &Listener{edge: e}, nil
}

// Endpoint returns the transport-neutral advertisement handed to a minted
// grant: the node's overlay address and public key. These are opaque bytes to
// the protocol; the client pins the public key at connect time.
func (l *Listener) Endpoint() tunnel.Endpoint {
	key := l.edge.node.PublicKey()
	return tunnel.Endpoint{
		Address: l.edge.node.AddressBytes(),
		PubKey:  append([]byte(nil), key...),
	}
}

// IncomingSession is one inbound mesh connection whose routing preamble has
// arrived but has not yet been validated. The allocator core validates the
// preamble (execution + grant + peer key) and either promotes the pair (into
// the payload map, then authorized streams flow through AcceptStream) or
// rejects it with a classified reason.
type IncomingSession struct {
	pair     *pairConn
	pending  *pendingPair
	preamble tunnel.Preamble
}

// Preamble returns the routing preamble the client sent as the pair's first
// frame.
func (s *IncomingSession) Preamble() tunnel.Preamble { return s.preamble }

// PeerKey returns the authenticated remote edge node public key. The allocator
// core validates it against the grant's pinned key.
func (s *IncomingSession) PeerKey() []byte { return s.pair.PeerKey() }

// Accept implements tunnel.Listener's accept surface: it blocks until one
// inbound mesh connection's routing preamble has arrived, then returns it for
// validation. A malformed preamble (protocol garbage from an authenticated or
// unauthenticated peer) is rejected and dropped here with ReasonUnauthorized,
// and Accept continues with the next pending connection instead of returning an
// error; the only error path is the edge closing, so the caller's accept loop
// survives any single peer and stays live on ErrEdgeClosed.
func (l *Listener) Accept() (*IncomingSession, error) {
	for {
		pending, err := l.edge.mux.accept()
		if err != nil {
			return nil, err
		}
		preamble, err := pending.pair.readPreamble()
		if err != nil {
			pending.pair.endAll(&tunnel.SessionError{Reason: tunnel.ReasonUnauthorized, Detail: "invalid routing preamble"})
			l.edge.mux.dropPending(pending.remote)
			// Drain and continue: a malformed preamble from one peer must not
			// terminate the allocator's accept loop for every other peer.
			continue
		}
		return &IncomingSession{pair: pending.pair, pending: pending, preamble: preamble}, nil
	}
}

// Promote validates a mesh connection into the payload map after the allocator
// core accepted the grant. It marks the pair authorized (with the session's slot
// list) and writes the accept frame; from here streams may be opened by the
// client and consumed through AcceptStream.
func (s *IncomingSession) Promote(session *tunnel.Session) error {
	if err := s.pair.owner.promote(s.pending); err != nil {
		return err
	}
	return s.pair.Authorize(session)
}

// Reject ends a pending mesh connection with a classified reason. The client's
// dial observes the reason; the mux keeps no record behind.
func (s *IncomingSession) Reject(reason tunnel.Reason, detail string) error {
	s.pair.endAll(&tunnel.SessionError{Reason: reason, Detail: detail})
	return nil
}

// AcceptStream returns the next authorized stream on this (already promoted)
// mesh connection, each carrying the client-supplied target slot the edge
// splices to. It blocks until a stream opens or the pair closes.
func (s *IncomingSession) AcceptStream() (*IncomingStream, error) {
	return s.pair.AcceptStream()
}

// Stale reports whether this session still holds the pending map slot (used
// only for diagnostics in tests).
func (s *IncomingSession) Stale() bool {
	s.pair.owner.mu.Lock()
	defer s.pair.owner.mu.Unlock()
	return s.pair.owner.pending[s.pending.remote] != s.pending
}

// Close implements tunnel.Listener: it stops the edge and the embedded node.
func (l *Listener) Close() error {
	return l.edge.Close()
}

// CloseWithReason closes the mesh connection and all of its streams.
func (s *IncomingSession) CloseWithReason(reason tunnel.Reason, detail string) error {
	return s.pair.CloseWithReason(reason, detail)
}
