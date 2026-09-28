package meshbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultPeerTTL       = 15 * time.Minute
	defaultSweepInterval = time.Minute
)

var (
	ErrInvalidNode        = errors.New("invalid meshbus node")
	ErrUnknownPeer        = errors.New("unknown meshbus peer")
	ErrNodeNotStarted     = errors.New("meshbus node is not started")
	ErrNodeAlreadyStarted = errors.New("meshbus node is already started")
	ErrNodeClosed         = errors.New("meshbus node is closed")
)

// NodeTransport supplies authenticated direct delivery and reports discovery
// separately from successful realm authentication.
type NodeTransport interface {
	Sender
	Identity() PeerID
	SetPeerObserver(PeerObserver) error
	Start(context.Context) error
	Close() error
}

type TransportFactory func(Handler) (NodeTransport, error)

type NodeConfig struct {
	Transport     TransportFactory
	DirectHandler Handler
	Bus           BusConfig
	Directory     DirectoryConfig
	PeerTTL       time.Duration
	SweepInterval time.Duration
	OnPeerError   func(error)
}

type nodeState uint8

const (
	nodeCreated nodeState = iota
	nodeRunning
	nodeClosed
)

// Node owns authenticated peer state and composes it with direct messaging
// and bounded pub/sub. Transport discovery remains advisory until a realm
// proof promotes the candidate into Peers.
type Node struct {
	transport   NodeTransport
	bus         *Bus
	candidates  *PeerDirectory
	peers       *PeerDirectory
	identity    PeerID
	peerTTL     time.Duration
	sweep       time.Duration
	onPeerError func(error)

	mu        sync.RWMutex
	state     nodeState
	cancel    context.CancelFunc
	closeErr  error
	closeOnce sync.Once
}

func NewNode(config NodeConfig) (*Node, error) {
	if config.Transport == nil {
		return nil, fmt.Errorf("%w: transport factory is required", ErrInvalidNode)
	}
	if config.Bus.Sender != nil || config.Bus.Peers != nil {
		return nil, fmt.Errorf("%w: bus sender and peer source are managed by Node", ErrInvalidNode)
	}
	if config.PeerTTL == 0 {
		config.PeerTTL = defaultPeerTTL
	}
	if config.SweepInterval == 0 {
		config.SweepInterval = defaultSweepInterval
	}
	if config.PeerTTL < time.Millisecond || config.SweepInterval < time.Millisecond {
		return nil, fmt.Errorf("%w: peer TTL and sweep interval must be positive", ErrInvalidNode)
	}

	candidates, err := NewPeerDirectory(config.Directory)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidNode, err)
	}
	peers, err := NewPeerDirectory(config.Directory)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidNode, err)
	}
	node := &Node{
		candidates: candidates,
		peers:      peers,
		peerTTL:    config.PeerTTL, sweep: config.SweepInterval, onPeerError: config.OnPeerError,
	}
	busConfig := config.Bus
	busConfig.Sender = node
	busConfig.Peers = PeerSourceFunc(node.peerIDs)
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
	if transport == nil || transport.Identity().IsZero() {
		if transport != nil {
			_ = transport.Close()
		}
		_ = bus.Close()
		return nil, fmt.Errorf("%w: transport identity is required", ErrInvalidNode)
	}
	node.transport = transport
	node.identity = transport.Identity()
	if err := transport.SetPeerObserver(node); err != nil {
		_ = transport.Close()
		_ = bus.Close()
		return nil, fmt.Errorf("%w: bind peer observer: %w", ErrInvalidNode, err)
	}
	return node, nil
}

func (n *Node) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidNode)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.state {
	case nodeRunning:
		return ErrNodeAlreadyStarted
	case nodeClosed:
		return ErrNodeClosed
	}
	runContext, cancel := context.WithCancel(ctx)
	if err := n.transport.Start(runContext); err != nil {
		cancel()
		return err
	}
	n.cancel = cancel
	n.state = nodeRunning
	go n.sweepLoop(runContext)
	return nil
}

func (n *Node) Send(ctx context.Context, peer PeerID, payload []byte) error {
	if peer.IsZero() {
		return ErrInvalidPeerID
	}
	if err := n.requireRunning(); err != nil {
		return err
	}
	if _, ok := n.peers.Get(peer); !ok {
		if _, candidate := n.candidates.Get(peer); !candidate {
			return fmt.Errorf("%w: %s", ErrUnknownPeer, peer.String())
		}
	}
	return n.transport.SendMessage(ctx, peer, payload)
}

// SendMessage implements Sender for the composed Bus. It is equivalent to
// Send and remains peer-addressed.
func (n *Node) SendMessage(ctx context.Context, peer PeerID, payload []byte) error {
	return n.Send(ctx, peer, payload)
}

// Identity returns this node's transport-authenticated identity.
func (n *Node) Identity() PeerID { return n.identity }

func (n *Node) Subscribe(topic string, handler EventHandler) (*Subscription, error) {
	return n.bus.Subscribe(topic, handler)
}

func (n *Node) Publish(ctx context.Context, topic string, payload []byte, options PublishOptions) (PublishResult, error) {
	if err := n.requireRunning(); err != nil {
		return PublishResult{}, err
	}
	if !options.RemoteOnly {
		options.localSender = n.identity
	}
	return n.bus.Publish(ctx, topic, payload, options)
}

// DiscoveredPeers returns advisory presence candidates. They are usable by
// Send to initiate authentication but excluded from Peers and pub/sub fan-out.
func (n *Node) DiscoveredPeers() []Peer { return n.candidates.Peers() }

// Peers returns only identities that completed realm authentication.
func (n *Node) Peers() []Peer { return n.peers.Peers() }

func (n *Node) Discovered(peer Peer) error {
	if authenticated, ok := n.peers.Get(peer.ID); ok {
		authenticated.Metadata = peer.Metadata
		authenticated.Hops = peer.Hops
		return n.recordPeer(n.peers, authenticated)
	}
	return n.recordPeer(n.candidates, peer)
}

func (n *Node) Authenticated(id PeerID) error {
	peer, ok := n.candidates.Get(id)
	if !ok {
		peer = Peer{ID: id}
	}
	if err := n.recordPeer(n.peers, peer); err != nil {
		return err
	}
	n.candidates.Remove(id)
	return nil
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.state = nodeClosed
		if n.cancel != nil {
			n.cancel()
		}
		n.mu.Unlock()
		n.closeErr = errors.Join(n.bus.Close(), n.transport.Close())
	})
	return n.closeErr
}

func (n *Node) requireRunning() error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	switch n.state {
	case nodeRunning:
		return nil
	case nodeClosed:
		return ErrNodeClosed
	default:
		return ErrNodeNotStarted
	}
}

func (n *Node) recordPeer(directory *PeerDirectory, peer Peer) error {
	err := directory.Remember(peer)
	if err != nil && n.onPeerError != nil {
		n.onPeerError(err)
	}
	return err
}

func (n *Node) peerIDs() []PeerID { return n.peers.IDs() }

func (n *Node) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(n.sweep)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.candidates.ExpireStale(n.peerTTL)
			n.peers.ExpireStale(n.peerTTL)
		}
	}
}

var (
	_ Sender       = (*Node)(nil)
	_ PeerObserver = (*Node)(nil)
)
