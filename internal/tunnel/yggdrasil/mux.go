package yggdrasil

import (
	"os"
	"sync"
	"time"

	iwt "github.com/Arceliar/ironwood/types"

	"github.com/mytecor/r1s/internal/tunnel"
)

// The node's packet interface is process-wide: every embedded node has one
// ReadFrom/WriteTo pair, and packets from every remote peer arrive through it.
// The mux owns ReadFrom and demultiplexes packets by remote key (ironwood's
// types.Addr, the remote's ed25519 public key).
//
// Session identity is the remote node key (Model A, BACKLOG resolved decision
// 16): one live tunnel per pair of node keys. The allocator registry enforces
// one live session per execution and the client holds one tunnel session at a
// time, so the mux maps one remote key to at most one stream.
//
// Inbound sessions: the first packet from a previously unseen peer opens a
// bounded pending session keyed by that peer. The routing preamble rides as
// the first frame; Accept hands the pending session to the listener, whose
// caller validates it against the allocator core and promotes the session
// into the payload map (the same conn, moved between maps). An
// unauthenticated peer can at worst allocate one small pending buffer that
// expires.
type mux struct {
	pkt packetIO

	mu       sync.Mutex
	sessions map[string]*streamConn // payload sessions by remote key
	pending  map[string]*pendingSession
	inbound  chan *pendingSession
	closed   bool
	closeCh  chan struct{}
}

var tracePackets = os.Getenv("R1S_TRACE") != ""

// pendingSession is an inbound session whose first frame (the routing
// preamble) is arriving but has not yet been validated by the allocator core.
// Promote moves the same conn into the payload map after validation.
type pendingSession struct {
	conn     *streamConn
	remote   string // the remote key; also the pending map's key
	deadline time.Time
}

// newMux wraps a packet interface; the caller starts one readLoop per mux.
func newMux(pkt packetIO) *mux {
	return &mux{
		pkt:      pkt,
		sessions: make(map[string]*streamConn),
		pending:  make(map[string]*pendingSession),
		inbound:  make(chan *pendingSession, maxPendingSessions),
		closeCh:  make(chan struct{}),
	}
}

// readLoop owns the packet interface's ReadFrom until the mux closes. Each
// packet is routed to the registered session for its remote key; a packet
// from an unknown peer opens a bounded pending inbound session keyed by that
// peer.
func (m *mux) readLoop() {
	buf := make([]byte, maxPacketPayload)
	for {
		n, from, err := m.pkt.ReadFrom(buf)
		if err != nil {
			m.endAll(&tunnel.SessionError{Reason: tunnel.ReasonMeshUnreachable, Detail: err.Error()})
			return
		}
		if n == 0 {
			continue
		}
		remote, ok := from.(iwt.Addr)
		if !ok {
			continue
		}
		packet := append([]byte(nil), buf[:n]...)

		if tracePackets {
			println("readLoop from=", len(remote), "sess=", len(m.sessions), "pend=", len(m.pending), "n=", n)
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		session := m.sessions[string(remote)]
		pending := m.pending[string(remote)]
		m.mu.Unlock()

		switch {
		case session != nil:
			session.ingest(packet)
		case pending != nil:
			pending.conn.ingest(packet)
		default:
			m.openPending(remote, packet)
		}
	}
}

// openPending registers a pending inbound session for a previously unseen
// remote key, feeds it the first packet, and queues it for Accept. The
// unauthenticated peer can do nothing with it except send a preamble the
// allocator core will validate — or the deadline drops it.
func (m *mux) openPending(remote iwt.Addr, packet []byte) {
	key := string(remote)
	m.mu.Lock()
	if m.closed || len(m.pending) >= maxPendingSessions {
		m.mu.Unlock()
		return
	}
	conn := newStreamConn(m.pkt, remote, remote, m)
	pending := &pendingSession{conn: conn, remote: key, deadline: time.Now().Add(pendingSessionTimeout)}
	m.pending[key] = pending
	m.mu.Unlock()

	pending.conn.ingest(packet)
	select {
	case m.inbound <- pending:
	case <-time.After(pendingSessionTimeout):
		m.dropPending(key)
	case <-m.closeCh:
	}
}

// remove deletes a session or pending record by remote key; it is the onEnd
// hook (wired in end) for every session the mux creates.
func (m *mux) remove(key string) {
	m.mu.Lock()
	delete(m.sessions, key)
	delete(m.pending, key)
	m.mu.Unlock()
}

// dropPending removes and ends one pending session.
func (m *mux) dropPending(key string) {
	m.mu.Lock()
	pending := m.pending[key]
	delete(m.pending, key)
	m.mu.Unlock()
	if pending != nil {
		pending.conn.end(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: "accept timed out"})
	}
}

// promote moves a validated pending session into the payload map. Called by
// the listener's caller after the allocator core validated the grant and
// preamble; the pending conn keeps buffering and its Read serves payload from
// here on. A pending record that was already superseded (dropped for its
// deadline and re-created) rejects the stale validation.
func (m *mux) promote(p *pendingSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrEdgeClosed
	}
	if m.pending[p.remote] != p {
		return tunnel.ErrSessionBusy
	}
	if _, busy := m.sessions[p.remote]; busy {
		return tunnel.ErrSessionBusy
	}
	delete(m.pending, p.remote)
	m.sessions[p.remote] = p.conn
	return nil
}

// accept removes and returns the oldest pending inbound session, or blocks
// until one arrives or the mux closes. Expired pending sessions are dropped
// lazily as they surface.
func (m *mux) accept() (*pendingSession, error) {
	for {
		select {
		case p := <-m.inbound:
			if time.Now().After(p.deadline) {
				m.dropPending(p.remote)
				continue
			}
			return p, nil
		case <-m.closeCh:
			return nil, ErrEdgeClosed
		}
	}
}

// close ends every live and pending session and marks the mux closed. The
// read loop exits on the next packet-interface failure (the node close
// unblocks ReadFrom).
func (m *mux) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	close(m.closeCh)
	m.mu.Unlock()
	m.endAll(&tunnel.SessionError{Reason: tunnel.ReasonMeshUnreachable, Detail: "edge closed"})
}

// endAll ends every live and pending session with a classified reason. Used
// on packet-interface failure and on mux close.
func (m *mux) endAll(sessionErr error) {
	m.mu.Lock()
	sessions := make([]*streamConn, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	pending := make([]*pendingSession, 0, len(m.pending))
	for _, p := range m.pending {
		pending = append(pending, p)
	}
	m.sessions = make(map[string]*streamConn)
	m.pending = make(map[string]*pendingSession)
	m.mu.Unlock()
	for _, session := range sessions {
		session.end(sessionErr)
	}
	for _, p := range pending {
		p.conn.end(sessionErr)
	}
}

// register adds an outbound (client-side) session for a remote peer key. The
// dialing side knows the preamble comes next (the caller writes it through
// PreambleWriter); until then the session receives no packets from unknown
// peers because it is registered up front. It fails if the mux is closing or
// a session for that key already exists (Model A: one mesh path per tunnel).
func (m *mux) register(remote []byte) (*streamConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrEdgeClosed
	}
	if _, busy := m.sessions[string(remote)]; busy {
		return nil, tunnel.ErrSessionBusy
	}
	conn := newStreamConn(m.pkt, remote, remote, m)
	m.sessions[string(remote)] = conn
	return conn, nil
}
