package yggdrasil

import (
	"errors"
	"net"
	"time"
)

// packetIO is the packet interface a pair runs over: the embedded yggdrasil
// node in production (its Core satisfies it via ReadFrom/WriteTo), or a fake in
// tests.
type packetIO interface {
	ReadFrom(p []byte) (n int, from net.Addr, err error)
	WriteTo(p []byte, addr net.Addr) (n int, err error)
	MTU() uint64
	Close() error
}

// ErrEdgeClosed reports that the edge (node or mux) is closed.
var ErrEdgeClosed = errors.New("yggdrasil tunnel edge is closed")

// maxPacketPayload is the max payload bytes per frame, hence per packet. The
// embedded Core's MTU (ironwood default peerMaxMessageSize minus overheads) is
// far larger; the adapter clamps to a conservative value so one frame always
// fits one packet with room for mesh overhead, and mux read buffers are this
// size.
const maxPacketPayload = 16 * 1024

const (
	// maxHandshakeWait bounds waiting for one handshake frame (the accept
	// frame after the preamble) and for the mesh path before a dial.
	maxHandshakeWait = 10 * time.Second
	// pendingSessionTimeout bounds how long a pending inbound pair waits for
	// Accept before it is dropped.
	pendingSessionTimeout = 30 * time.Second
	// maxPendingSessions bounds the number of half-open inbound pairs, so a
	// flood of garbage packets from foreign keys cannot grow memory without
	// bound.
	maxPendingSessions = 8
)

// edge is the shared machinery of the allocator listener and the client
// dialer: one embedded node plus one packet mux reading its packet interface.
type edge struct {
	node *Node
	mux  *mux
}

// newEdge starts the shared machinery for a started node: the packet mux's
// read loop owns the node's ReadFrom from here on.
func newEdge(node *Node) (*edge, error) {
	if node == nil || node.core == nil {
		return nil, errors.New("yggdrasil edge: a started node is required")
	}
	e := &edge{node: node, mux: newMux(node.PacketIO())}
	go e.mux.readLoop()
	return e, nil
}

// Close stops the edge: pairs end classified as mesh-unreachable and the
// embedded node is stopped.
func (e *edge) Close() error {
	if e == nil {
		return nil
	}
	e.mux.close()
	return e.node.Close()
}
