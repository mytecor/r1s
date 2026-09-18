package yggdrasil

import (
	"context"
	"errors"

	"github.com/mytecor/r1s/internal/tunnel"
)

// Dialer is the client-side overlay dialer running in `r1s serve`. It pins the
// allocator's public key from the granted endpoint before returning, so a relay
// or man-in-the-middle has nothing usable even if it saw the advertisement.
//
// The dial is deferred with the framing/reliability layer (internal/tunnel
// carries the transport-neutral contract; this package supplies the overlay
// mapping in the follow-up). The type pins the shape serve wires against.
type Dialer struct {
	node *Node
}

// NewDialer constructs the client-side overlay dialer for a node.
func NewDialer(node *Node) (*Dialer, error) {
	if node == nil {
		return nil, errors.New("yggdrasil dialer: node is required")
	}
	return &Dialer{node: node}, nil
}

// Dial implements tunnel.Dialer. Deferred with the framing layer, which pins
// the allocator's public key from endpoint.PubKey and fails if the mesh's
// authenticated peer key does not match it.
func (d *Dialer) Dial(ctx context.Context, endpoint tunnel.Endpoint) (tunnel.Conn, error) {
	return nil, ErrorDeferred
}

// Close implements tunnel.Dialer.
func (d *Dialer) Close() error { return nil }
