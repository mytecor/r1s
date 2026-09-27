package meshbus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
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
	network   *memoryNetwork
	id        PeerID
	route     string
	handler   Handler
	directory *PeerDirectory

	mu      sync.RWMutex
	started bool
	closed  bool
}

func (n *memoryNetwork) factory(identity byte, directoryConfig DirectoryConfig, capture **memoryNodeTransport) TransportFactory {
	return func(handler Handler) (NodeTransport, error) {
		peer, err := NewPeerID([]byte{identity})
		if err != nil {
			return nil, err
		}
		transport := &memoryNodeTransport{
			network: n, id: peer, route: fmt.Sprintf("memory-%02x", identity), handler: handler,
			directory: NewPeerDirectory(directoryConfig),
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

func (t *memoryNodeTransport) Directory() *PeerDirectory { return t.directory }

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
	message, err := NewReceivedMessage(t.id.Bytes(), bytes.Clone(payload))
	if err != nil {
		return err
	}
	return handler(ctx, message)
}

func (t *memoryNodeTransport) discover(peer *memoryNodeTransport, metadata map[string]string) error {
	return t.directory.Remember(Peer{ID: peer.id, Route: peer.route, Metadata: metadata})
}

func TestNodeComposesDiscoveryDirectMessagesAndPubSub(t *testing.T) {
	network := newMemoryNetwork()
	var transportA, transportB *memoryNodeTransport
	directB := make(chan ReceivedMessage, 1)
	nodeA, err := NewNode(NodeConfig{
		Transport: network.factory(0xa1, DirectoryConfig{MaxPeers: 1, MaxMetadataBytes: 8}, &transportA),
		Bus:       BusConfig{MaxSubscriptions: 1, MaxFanoutPeers: 1, FanoutConcurrency: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Close()
	nodeB, err := NewNode(NodeConfig{
		Transport: network.factory(0xb2, DirectoryConfig{MaxPeers: 1, MaxMetadataBytes: 8}, &transportB),
		DirectHandler: func(_ context.Context, message ReceivedMessage) error {
			directB <- message
			return nil
		},
		Bus: BusConfig{MaxSubscriptions: 1, MaxFanoutPeers: 1, FanoutConcurrency: 1},
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
	if len(nodeA.Peers()) != 1 || nodeA.Peers()[0].ID != transportB.id || len(nodeB.Peers()) != 1 {
		t.Fatalf("peer snapshots A=%+v B=%+v", nodeA.Peers(), nodeB.Peers())
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

	events := make(chan ReceivedEvent, 1)
	if _, err := nodeB.Subscribe("node.event", func(_ context.Context, event ReceivedEvent) error {
		events <- event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeA.Publish(ctx, "node.event", []byte("published"), PublishOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Sender != transportA.id || string(event.Payload) != "published" {
			t.Fatalf("event sender=%s payload=%q", event.Sender.String(), event.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("published event was not delivered")
	}

	extra, _ := NewPeerID([]byte{0xc3})
	if err := transportA.directory.Remember(Peer{ID: extra, Route: "extra"}); !errors.Is(err, ErrPeerLimit) {
		t.Fatalf("directory bound error=%v, want ErrPeerLimit", err)
	}
	if _, err := nodeB.Subscribe("second.event", func(context.Context, ReceivedEvent) error { return nil }); !errors.Is(err, ErrSubscriptionLimit) {
		t.Fatalf("subscription bound error=%v, want ErrSubscriptionLimit", err)
	}
}

func TestNodeRejectsUnknownPeersAndClosesComposition(t *testing.T) {
	network := newMemoryNetwork()
	var transport *memoryNodeTransport
	node, err := NewNode(NodeConfig{Transport: network.factory(1, DirectoryConfig{}, &transport)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := node.Start(ctx); err != nil {
		t.Fatal(err)
	}
	unknown, _ := NewPeerID([]byte{2})
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
		Transport: network.factory(1, DirectoryConfig{}, &transport),
		Bus:       BusConfig{Sender: &recordingSender{}},
	}); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("caller-supplied bus sender error=%v", err)
	}
}
