package yggdrasil

import (
	"errors"

	"github.com/mytecor/r1s/internal/tunnel"
)

// ErrorDeferred is returned by the stubbed edge methods until the
// framing/reliability layer over the yggdrasil packet interface lands (BACKLOG).
var ErrorDeferred = errors.New("yggdrasil tunnel edge is not yet wired (framing/reliability layer deferred)")

// Listener is the allocator-side overlay listener. It accepts inbound tunnel
// connections and exposes each accepted connection's authenticated peer key so
// the allocator core can validate the grant and routing preamble before a
// single payload byte is relayed.
//
// The accept/splice contract is transport-neutral (internal/tunnel.Listener);
// the yggdrasil implementation behind it is deferred while the
// framing/reliability layer over the packet interface is built. This type pins
// the shape the daemon wires against so the plumbing stays stable.
type Listener struct {
	node *Node
}

// NewListener constructs the allocator overlay listener for a node. The node
// must already be started. The returned listener reports its endpoint
// advertisement (address + public key) so the client can dial and pin it.
func NewListener(node *Node) (*Listener, error) {
	if node == nil {
		return nil, errors.New("yggdrasil listener: node is required")
	}
	return &Listener{node: node}, nil
}

// Endpoint returns the transport-neutral advertisement handed to a minted
// grant: the node's overlay address and public key. These are opaque bytes to
// the protocol; the client pins the public key at connect time.
func (l *Listener) Endpoint() tunnel.Endpoint {
	key := l.node.PublicKey()
	return tunnel.Endpoint{
		Address: l.node.AddressBytes(),
		PubKey:  append([]byte(nil), key...),
	}
}

// Accept implements tunnel.Listener. It is deferred together with the
// framing/reliability layer.
func (l *Listener) Accept() (tunnel.Conn, error) {
	return nil, ErrorDeferred
}

// Close implements tunnel.Listener.
func (l *Listener) Close() error { return nil }
