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
// types.Addr, the remote's ed25519 public key) into one pairConn per remote key.
//
// Session identity is the remote node key (Model A, BACKLOG resolved decision
// 16): one authenticated mesh connection per pair of node keys. F19-01 lets a
// single such connection carry many logical streams (a tunnelMux), so the mux
// maps one remote key to exactly one pair, and the pair routes frames by stream
// id to individual streams.
//
// Inbound pairs: the first packet from a previously unseen peer opens a bounded
// pending pair keyed by that peer. The routing preamble rides as the pair's
// first frame; Accept hands the pending pair to the listener, whose caller
// validates it against the allocator core and Authorizes it. An unauthenticated
// peer can at worst allocate one small pending buffer that expires.
type mux struct {
	pkt packetIO

	mu      sync.Mutex
	pairs   map[string]*pairConn // mesh connections by remote key
	pending map[string]*pendingPair
	inbound chan *pendingPair
	closed  bool
	closeCh chan struct{}
}

var tracePackets = os.Getenv("R1S_TRACE") != ""

// pendingPair is an inbound pair whose first frame (the routing preamble) is
// arriving but has not yet been validated by the allocator core. Promote moves
// the same pair into the payload map after validation.
type pendingPair struct {
	pair     *pairConn
	remote   string // the remote key; also the pending map's key
	deadline time.Time
}

// newMux wraps a packet interface; the caller starts one readLoop per mux.
func newMux(pkt packetIO) *mux {
	return &mux{
		pkt:     pkt,
		pairs:   make(map[string]*pairConn),
		pending: make(map[string]*pendingPair),
		inbound: make(chan *pendingPair, maxPendingSessions),
		closeCh: make(chan struct{}),
	}
}

// readLoop owns the packet interface's ReadFrom until the mux closes. Each
// packet is routed to the pair for its remote key; a packet from an unknown
// peer opens a bounded pending inbound pair keyed by that peer.
func (m *mux) readLoop() {
	buf := make([]byte, frameHeaderSize+maxPacketPayload)
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
			println("readLoop from=", len(remote), "pairs=", len(m.pairs), "pend=", len(m.pending), "n=", n)
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		pair := m.pairs[string(remote)]
		pending := m.pending[string(remote)]
		m.mu.Unlock()

		switch {
		case pair != nil:
			pair.ingest(packet)
		case pending != nil:
			pending.pair.ingest(packet)
		default:
			m.openPending(remote, packet)
		}
	}
}

// openPending registers a pending inbound pair for a previously unseen remote
// key, feeds it the first packet, and queues it for Accept.
func (m *mux) openPending(remote iwt.Addr, packet []byte) {
	key := string(remote)
	m.mu.Lock()
	if m.closed || len(m.pending) >= maxPendingSessions {
		m.mu.Unlock()
		return
	}
	pair := newPair(m.pkt, remote, remote, m)
	pending := &pendingPair{pair: pair, remote: key, deadline: time.Now().Add(pendingSessionTimeout)}
	m.pending[key] = pending
	m.mu.Unlock()

	pair.ingest(packet)
	select {
	case m.inbound <- pending:
	case <-time.After(pendingSessionTimeout):
		m.dropPending(key)
	case <-m.closeCh:
	}
}

// removePair deletes a pair or pending record by remote key; it is the onEnd
// hook (wired in endAll) for every pair the mux creates.
func (m *mux) removePair(key string) {
	m.mu.Lock()
	delete(m.pairs, key)
	delete(m.pending, key)
	m.mu.Unlock()
}

// dropPending removes and ends one pending pair.
func (m *mux) dropPending(key string) {
	m.mu.Lock()
	pending := m.pending[key]
	delete(m.pending, key)
	m.mu.Unlock()
	if pending != nil {
		pending.pair.endAll(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: "accept timed out"})
	}
}

// promote moves a validated pending pair into the payload map. Called by the
// listener's caller after the allocator core validated the grant and preamble.
// A pending record already superseded (dropped for its deadline and re-created)
// rejects the stale validation.
func (m *mux) promote(p *pendingPair) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrEdgeClosed
	}
	if m.pending[p.remote] != p {
		return tunnel.ErrSessionBusy
	}
	if _, busy := m.pairs[p.remote]; busy {
		return tunnel.ErrSessionBusy
	}
	delete(m.pending, p.remote)
	m.pairs[p.remote] = p.pair
	return nil
}

// accept removes and returns the oldest pending inbound pair, or blocks until
// one arrives or the mux closes. Expired pending pairs are dropped lazily.
func (m *mux) accept() (*pendingPair, error) {
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

// close ends every pair and marks the mux closed. The read loop exits on the
// next packet-interface failure (the node close unblocks ReadFrom).
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

// endAll ends every pair with a classified reason. Used on packet-interface
// failure and on mux close.
func (m *mux) endAll(sessionErr error) {
	m.mu.Lock()
	pairs := make([]*pairConn, 0, len(m.pairs))
	for _, pair := range m.pairs {
		pairs = append(pairs, pair)
	}
	m.pairs = make(map[string]*pairConn)
	m.pending = make(map[string]*pendingPair)
	m.mu.Unlock()
	for _, pair := range pairs {
		pair.endAll(sessionErr)
	}
}

// register adds an outbound (client-side) pair for a remote peer key. The
// dialing side knows the preamble comes next (the caller writes it through
// WritePreamble); until then the pair receives no packets from unknown peers
// because it is registered up front. It fails if the mux is closing or a pair
// for that key already exists (Model A: one mesh connection per tunnel).
func (m *mux) register(remote []byte) (*pairConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrEdgeClosed
	}
	if _, busy := m.pairs[string(remote)]; busy {
		return nil, tunnel.ErrSessionBusy
	}
	pair := newPair(m.pkt, remote, remote, m)
	m.pairs[string(remote)] = pair
	return pair, nil
}
