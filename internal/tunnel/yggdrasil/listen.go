package yggdrasil

import (
	"github.com/mytecor/r1s/internal/tunnel"
)

// Listener is the allocator-side overlay listener. It accepts inbound tunnel
// connections and exposes each accepted connection's authenticated peer key
// and routing preamble, so the allocator core can validate the grant before a
// single payload byte is relayed.
//
// The listener is backed by the edge's packet mux: the first packet from a
// previously unseen peer opens a pending session, and Accept returns it after
// the preamble frame arrived. The caller (the r1sd edge loop) validates the
// preamble against the allocator core and either promotes the session into
// the payload path or rejects it with a classified reason.
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

// IncomingSession is one inbound tunnel session whose routing preamble has
// arrived but has not yet been validated. The daemon validates the preamble
// against the allocator core and either promotes the session into the payload
// path or rejects it with a classified reason.
type IncomingSession struct {
	pending  *pendingSession
	preamble tunnel.Preamble
}

// Preamble returns the routing preamble the client sent as the first frame.
func (s *IncomingSession) Preamble() tunnel.Preamble { return s.preamble }

// PeerKey returns the authenticated remote edge node public key. The
// allocator core validates it against the grant's pinned key.
func (s *IncomingSession) PeerKey() []byte { return s.pending.conn.PeerKey() }

// Conn returns the session's byte stream. Call Promote first: until then the
// session stays pending (payload frames buffer into the pending conn and are
// carried over at promote, so nothing is lost).
func (s *IncomingSession) Conn() tunnel.Conn { return s.pending.conn }

// Accept implements tunnel.Listener. It blocks until one inbound session's
// routing preamble has arrived, then returns it for validation. A malformed
// preamble (protocol garbage from an authenticated or unauthenticated peer) is
// rejected and dropped here with ReasonUnauthorized, and Accept continues with
// the next pending session instead of returning an error; the only error path
// is the edge closing, so the caller's accept loop survives any single peer
// and stays live on ErrEdgeClosed.
func (l *Listener) Accept() (*IncomingSession, error) {
	for {
		pending, err := l.edge.mux.accept()
		if err != nil {
			return nil, err
		}
		preamble, err := pending.conn.readPreamble()
		if err != nil {
			_ = pending.conn.CloseWithReason(tunnel.ReasonUnauthorized, "invalid routing preamble")
			l.edge.mux.dropPending(pending.remote)
			// Drain and continue: a malformed preamble from one peer must not
			// terminate the allocator's accept loop for every other peer.
			continue
		}
		return &IncomingSession{pending: pending, preamble: preamble}, nil
	}
}

// Promote validates a session into the payload map after the allocator core
// accepted the grant. It marks the preamble as consumed (the allocator may
// write payload from here on) and writes the accept frame; from here the
// splice is live and payload flows.
func (s *IncomingSession) Promote() error {
	if err := s.pending.conn.owner.promote(s.pending); err != nil {
		_ = s.pending.conn.CloseWithReason(tunnel.ReasonSessionFailed, err.Error())
		return err
	}
	s.pending.conn.mu.Lock()
	s.pending.conn.preambleOK = true
	s.pending.conn.mu.Unlock()
	return s.pending.conn.writeAccept()
}

// Reject ends a pending session with a classified reason. The client's dial
// observes the reason; the mux keeps no record behind.
func (s *IncomingSession) Reject(reason tunnel.Reason, detail string) error {
	return s.pending.conn.CloseWithReason(reason, detail)
}

// Close implements tunnel.Listener: it stops the edge and the embedded node.
func (l *Listener) Close() error {
	return l.edge.Close()
}
