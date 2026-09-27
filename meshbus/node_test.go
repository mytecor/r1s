package meshbus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryNetwork struct {
	mu        sync.RWMutex
	endpoints map[string]*memoryNodeTransport
}

func newMemoryNetwork() *memoryNetwork {
	return &memoryNetwork{endpoints: make(map[string]*memoryNodeTransport)}
}

type memoryNodeTransport struct {
	network  *memoryNetwork
	id       PeerID
	route    string
	handler  Handler
	observer PeerObserver

	mu      sync.RWMutex
	started bool
	closed  bool
}

func (n *memoryNetwork) factory(identity byte, capture **memoryNodeTransport) TransportFactory {
	return func(handler Handler) (NodeTransport, error) {
		peer, err := NewPeerID([]byte{identity})
		if err != nil {
			return nil, err
		}
		transport := &memoryNodeTransport{
			network: n, id: peer, route: fmt.Sprintf("memory-%02x", identity), handler: handler,
		}
		n.mu.Lock()
		n.endpoints[transport.route] = transport
		n.mu.Unlock()
		*capture = transport
		return transport, nil
	}
}

func (t *memoryNodeTransport) Start(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrNodeClosed
	}
	t.started = true
	return nil
}

func (t *memoryNodeTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()
	t.network.mu.Lock()
	delete(t.network.endpoints, t.route)
	t.network.mu.Unlock()
	return nil
}

func (t *memoryNodeTransport) Identity() PeerID { return t.id }

func (t *memoryNodeTransport) SetPeerObserver(observer PeerObserver) error {
	t.observer = observer
	return nil
}

func (t *memoryNodeTransport) SendMessage(ctx context.Context, route string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.RLock()
	ready := t.started && !t.closed
	t.mu.RUnlock()
	if !ready {
		return errors.New("memory transport is not available")
	}
	t.network.mu.RLock()
	target := t.network.endpoints[route]
	t.network.mu.RUnlock()
	if target == nil {
		return errors.New("memory route is unavailable")
	}
	target.mu.RLock()
	targetReady := target.started && !target.closed
	handler := target.handler
	target.mu.RUnlock()
	if !targetReady {
		return errors.New("memory target is not available")
	}
	if err := t.observer.Authenticated(target.id, target.route); err != nil {
		return err
	}
	if err := target.observer.Authenticated(t.id, t.route); err != nil {
		return err
	}
	message, err := NewReceivedMessage(t.id.Bytes(), bytes.Clone(payload))
	if err != nil {
		return err
	}
	return handler(ctx, message)
}

func (t *memoryNodeTransport) discover(peer *memoryNodeTransport, metadata map[string]string) error {
	return t.observer.Discovered(Peer{ID: peer.id, Route: peer.route, Metadata: metadata})
}

func TestNodeComposesDiscoveryDirectMessagesAndPubSub(t *testing.T) {
	network := newMemoryNetwork()
	var transportA, transportB *memoryNodeTransport
	directB := make(chan ReceivedMessage, 1)
	nodeA, err := NewNode(NodeConfig{
		Transport: network.factory(0xa1, &transportA),
		Bus:       BusConfig{MaxSubscriptions: 1, MaxFanoutPeers: 1, FanoutConcurrency: 1},
		Directory: DirectoryConfig{MaxPeers: 1, MaxMetadataBytes: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Close()
	nodeB, err := NewNode(NodeConfig{
		Transport: network.factory(0xb2, &transportB),
		DirectHandler: func(_ context.Context, message ReceivedMessage) error {
			directB <- message
			return nil
		},
		Bus:       BusConfig{MaxSubscriptions: 1, MaxFanoutPeers: 1, FanoutConcurrency: 1},
		Directory: DirectoryConfig{MaxPeers: 1, MaxMetadataBytes: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeB.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := nodeA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transportA.discover(transportB, map[string]string{"os": "go"}); err != nil {
		t.Fatal(err)
	}
	if err := transportB.discover(transportA, nil); err != nil {
		t.Fatal(err)
	}
	if len(nodeA.DiscoveredPeers()) != 1 || len(nodeB.DiscoveredPeers()) != 1 || len(nodeA.Peers()) != 0 || len(nodeB.Peers()) != 0 {
		t.Fatalf("before auth discovered A=%+v B=%+v authenticated A=%+v B=%+v", nodeA.DiscoveredPeers(), nodeB.DiscoveredPeers(), nodeA.Peers(), nodeB.Peers())
	}

	if err := nodeA.Send(ctx, transportB.id, []byte("direct")); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-directB:
		if message.Sender() != transportA.id || string(message.Payload()) != "direct" {
			t.Fatalf("direct sender=%s payload=%q", message.Sender().String(), message.Payload())
		}
	case <-time.After(time.Second):
		t.Fatal("direct message was not delivered")
	}
	if len(nodeA.Peers()) != 1 || nodeA.Peers()[0].ID != transportB.id || len(nodeB.Peers()) != 1 {
		t.Fatalf("authenticated peer snapshots A=%+v B=%+v", nodeA.Peers(), nodeB.Peers())
	}

	events := make(chan ReceivedEvent, 1)
	localEvents := make(chan ReceivedEvent, 1)
	if _, err := nodeA.Subscribe("node.event", func(_ context.Context, event ReceivedEvent) error {
		localEvents <- event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.Subscribe("node.event", func(_ context.Context, event ReceivedEvent) error {
		events <- event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := nodeA.Publish(ctx, "node.event", []byte("published"), PublishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempted != 1 || result.Delivered != 1 || !result.LocalDelivered || len(result.Failed) != 0 {
		t.Fatalf("publish result = %+v", result)
	}
	select {
	case event := <-events:
		if event.Sender != transportA.id || string(event.Payload) != "published" {
			t.Fatalf("event sender=%s payload=%q", event.Sender.String(), event.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("published event was not delivered")
	}
	select {
	case event := <-localEvents:
		if event.Sender != transportA.id || string(event.Payload) != "published" {
			t.Fatalf("local event sender=%s payload=%q", event.Sender.String(), event.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("published event was not delivered locally")
	}

	extra, _ := NewPeerID([]byte{0xc3})
	if err := nodeA.Authenticated(extra, "extra"); !errors.Is(err, ErrPeerLimit) {
		t.Fatalf("directory bound error=%v, want ErrPeerLimit", err)
	}
	if _, err := nodeB.Subscribe("second.event", func(context.Context, ReceivedEvent) error { return nil }); !errors.Is(err, ErrSubscriptionLimit) {
		t.Fatalf("subscription bound error=%v, want ErrSubscriptionLimit", err)
	}
}

func TestNodeRejectsUnknownPeersAndClosesComposition(t *testing.T) {
	network := newMemoryNetwork()
	var transport *memoryNodeTransport
	node, err := NewNode(NodeConfig{Transport: network.factory(1, &transport)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	unknown, _ := NewPeerID([]byte{2})
	if err := node.Send(ctx, unknown, []byte("payload")); !errors.Is(err, ErrNodeNotStarted) {
		t.Fatalf("Send() before start error=%v", err)
	}
	if err := node.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(ctx); !errors.Is(err, ErrNodeAlreadyStarted) {
		t.Fatalf("second Start() error=%v", err)
	}
	if err := node.Send(ctx, unknown, []byte("payload")); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("unknown peer error=%v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("second Close() error=%v", err)
	}
	if err := node.Start(ctx); !errors.Is(err, ErrNodeClosed) {
		t.Fatalf("Start() after close error=%v", err)
	}
	if err := node.SendMessage(ctx, "route", []byte("payload")); !errors.Is(err, ErrNodeClosed) {
		t.Fatalf("SendMessage() after close error=%v", err)
	}
}

func TestNodeValidatesComposition(t *testing.T) {
	if _, err := NewNode(NodeConfig{}); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("missing transport error=%v", err)
	}
	network := newMemoryNetwork()
	var transport *memoryNodeTransport
	if _, err := NewNode(NodeConfig{
		Transport: network.factory(1, &transport),
		Bus:       BusConfig{Sender: &recordingSender{}},
	}); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("caller-supplied bus sender error=%v", err)
	}
}

func TestNodeExpiresStaleCandidatesAndAuthenticatedPeers(t *testing.T) {
	var nowUnix atomic.Int64
	nowUnix.Store(1_700_000_000)
	network := newMemoryNetwork()
	var transport *memoryNodeTransport
	node, err := NewNode(NodeConfig{
		Transport: network.factory(1, &transport),
		Directory: DirectoryConfig{now: func() time.Time { return time.Unix(nowUnix.Load(), 0) }},
		PeerTTL:   10 * time.Millisecond, SweepInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	peer, _ := NewPeerID([]byte{2})
	if err := node.Discovered(Peer{ID: peer, Route: "candidate"}); err != nil {
		t.Fatal(err)
	}
	if err := node.Authenticated(peer, "candidate"); err != nil {
		t.Fatal(err)
	}
	if len(node.Peers()) != 1 {
		t.Fatal("authenticated peer was not recorded")
	}
	nowUnix.Add(1)
	deadline := time.Now().Add(time.Second)
	for len(node.Peers()) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(node.Peers()) != 0 {
		t.Fatalf("stale authenticated peers = %+v", node.Peers())
	}
}
