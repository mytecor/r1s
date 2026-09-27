package meshbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidNode is returned when a Node cannot compose its transport and
	// bounded messaging primitives.
	ErrInvalidNode = errors.New("invalid meshbus node")
	// ErrUnknownPeer is returned when direct send has no discovered route for
	// the requested authenticated peer identity.
	ErrUnknownPeer = errors.New("unknown meshbus peer")
	// ErrNodeClosed is returned after the Node has been closed.
	ErrNodeClosed = errors.New("meshbus node is closed")
)

// NodeTransport is the lifecycle, direct-message and discovery surface a Node
// composes. Implementations own transport mechanics; Node owns only
// orchestration. meshbus/rns.Endpoint implements this interface.
type NodeTransport interface {
	Sender
	Start(context.Context) error
	Close() error
	Directory() *PeerDirectory
}

// TransportFactory constructs a transport with the complete inbound handler.
// This removes the circular setup that would otherwise require applications to
// manually wire Bus.Handler into a transport before constructing a Node.
type TransportFactory func(Handler) (NodeTransport, error)

// NodeConfig configures one cohesive meshbus participant. Bus.Sender and
// Bus.Peers are supplied by Node and must be left nil; all other Bus bounds
// retain their existing meanings and defaults.
type NodeConfig struct {
	Transport     TransportFactory
	DirectHandler Handler
	Bus           BusConfig
}

// Node composes one authenticated direct transport, its bounded peer
// directory, and the bounded best-effort event Bus.
type Node struct {
	transport NodeTransport
	bus       *Bus

	mu        sync.RWMutex
	closed    bool
	closeErr  error
	closeOnce sync.Once
}

// NewNode constructs a Node and binds its composed inbound handler to the
// transport. The transport is not started until Start is called.
func NewNode(config NodeConfig) (*Node, error) {
	if config.Transport == nil {
		return nil, fmt.Errorf("%w: transport factory is required", ErrInvalidNode)
	}
	if config.Bus.Sender != nil || config.Bus.Peers != nil {
		return nil, fmt.Errorf("%w: bus sender and peer source are managed by Node", ErrInvalidNode)
	}

	node := new(Node)
	busConfig := config.Bus
	busConfig.Sender = node
	busConfig.Peers = PeerSourceFunc(node.routes)
	bus, err := NewBus(busConfig)
	if err != nil {
		return nil, err
	}
	node.bus = bus
	transport, err := config.Transport(bus.Handler(config.DirectHandler))
	if err != nil {
		_ = bus.Close()
		return nil, fmt.Errorf("%w: construct transport: %w", ErrInvalidNode, err)
	}
	if transport == nil || transport.Directory() == nil {
		if transport != nil {
			_ = transport.Close()
		}
		_ = bus.Close()
		return nil, fmt.Errorf("%w: transport and peer directory are required", ErrInvalidNode)
	}
	node.transport = transport
	return node, nil
}

// Start starts the underlying authenticated transport. The context bounds the
// transport lifetime; Close remains the explicit resource release operation.
func (n *Node) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidNode)
	}
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return ErrNodeClosed
	}
	return n.transport.Start(ctx)
}

// Send resolves an authenticated peer identity through the current directory
// and sends one opaque direct message to its transport route.
func (n *Node) Send(ctx context.Context, peer PeerID, payload []byte) error {
	if peer.IsZero() {
		return ErrInvalidPeerID
	}
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return ErrNodeClosed
	}
	route, ok := n.transport.Directory().Resolve(peer)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, peer.String())
	}
	return n.SendMessage(ctx, route, payload)
}

// SendMessage implements Sender for the composed Bus. Application code should
// normally call Send with a PeerID; event fan-out uses transport routes.
func (n *Node) SendMessage(ctx context.Context, route string, payload []byte) error {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return ErrNodeClosed
	}
	return n.transport.SendMessage(ctx, route, payload)
}

// Subscribe registers one exact-topic event handler.
func (n *Node) Subscribe(topic string, handler EventHandler) (*Subscription, error) {
	return n.bus.Subscribe(topic, handler)
}

// Publish creates an event and fans it out to the Node's current discovered
// peer routes. Callers never supply a separate peer snapshot.
func (n *Node) Publish(ctx context.Context, topic string, payload []byte, options PublishOptions) (EventID, error) {
	return n.bus.Publish(ctx, topic, payload, options)
}

// Peers returns the bounded, immutable snapshot of currently discovered peers.
func (n *Node) Peers() []Peer { return n.transport.Directory().Peers() }

// Close stops subscriptions and the underlying transport. It is idempotent.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closed = true
		n.mu.Unlock()
		n.closeErr = errors.Join(n.bus.Close(), n.transport.Close())
	})
	return n.closeErr
}

func (n *Node) routes() []string {
	if n.transport == nil {
		return nil
	}
	return n.transport.Directory().Routes()
}

var _ Sender = (*Node)(nil)
