package yggdrasil

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/mytecor/r1s/internal/tunnel"
)

// Dialer is the client-side overlay dialer running inside `r1s serve`. It pins
// the allocator's public key from the granted endpoint before returning, so a
// relay or man-in-the-middle has nothing usable even if it saw the
// advertisement.
//
// The routing preamble is written through the Conn's PreambleWriter: Dial
// establishes the mesh session, and WritePreamble sends the one-time routing
// preamble as the first frame and waits for the allocator's accept frame, so
// a rejected or unauthenticated session fails before a single payload byte is
// sent.
type Dialer struct {
	node *Node
	edge *edge
}

// NewDialer constructs the client-side overlay dialer on a started node.
func NewDialer(node *Node) (*Dialer, error) {
	e, err := newEdge(node)
	if err != nil {
		return nil, err
	}
	return &Dialer{node: node, edge: e}, nil
}

// Dial implements tunnel.Dialer: it connects to the remote edge at endpoint
// (whose Address is the allocator's node key and whose PubKey the dialer pins
// against the mesh-authenticated peer) and returns the session in the
// handshake-pending state. The caller writes the routing preamble through
// tunnel.PreambleWriter (the backend has the grant ID); the first payload
// Write after the preamble completes the handshake by waiting for the
// allocator's accept frame, so a rejected or unauthenticated session fails
// before any payload byte is sent.
func (d *Dialer) Dial(ctx context.Context, endpoint tunnel.Endpoint) (tunnel.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(endpoint.Address) == 0 || len(endpoint.PubKey) == 0 {
		return nil, errors.New("tunnel: endpoint advertisement is incomplete")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The dial writes only to the endpoint's advertised key and the session's
	// authenticated PeerKey must match it, so a relay or man-in-the-middle has
	// nothing usable even if it observed the advertisement. The mesh must know
	// a path first: dialing before a path exists would surface as a bare dial
	// timeout, so wait for the path and fail as mesh-unreachable when it never
	// comes.
	if err := d.awaitPath(ctx, endpoint.PubKey); err != nil {
		return nil, err
	}
	return d.edge.mux.register(endpoint.Address)
}

// awaitPath waits until the node knows a path to the remote key, bounded by
// the context. The mesh builds paths asynchronously; dialing before a path
// exists would surface as a bare dial timeout, so the dialer waits for the
// path notification first and fails as mesh-unreachable when it never comes.
func (d *Dialer) awaitPath(ctx context.Context, remote []byte) error {
	deadline := time.NewTimer(maxHandshakeWait)
	defer deadline.Stop()
	// A long-lived ticker drives the poll, so a short dial-out window does not
	// allocate one time.Timer per 100 ms iteration (the hot handshake path).
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		if nodeKnowsPath(d.node, remote) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return tunnel.ErrMeshUnreachable
		case <-poll.C:
		}
	}
}

// nodeKnowsPath reports whether the node knows how to route session traffic
// to the remote key: the tree entry means the node is reachable in the
// spanning tree. The tree converges before the mesh can deliver session
// traffic, so it gates the dial; path discovery itself is triggered by the
// first packet, which is why the handshake retries the preamble instead of
// waiting for the path table.
func nodeKnowsPath(node *Node, remote []byte) bool {
	for _, entry := range node.Core().GetTree() {
		if bytes.Equal(entry.Key, remote) {
			return true
		}
	}
	return false
}

// Close implements tunnel.Dialer: it stops the edge and the embedded node.
func (d *Dialer) Close() error {
	return d.edge.Close()
}
