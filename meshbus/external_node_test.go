package meshbus_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mytecor/meshbus"
)

type externalNetwork struct {
	mu    sync.RWMutex
	nodes map[meshbus.PeerID]*externalTransport
}

type externalTransport struct {
	network  *externalNetwork
	id       meshbus.PeerID
	handler  meshbus.Handler
	observer meshbus.PeerObserver
	started  bool
}

func newExternalNetwork() *externalNetwork {
	return &externalNetwork{nodes: make(map[meshbus.PeerID]*externalTransport)}
}

func (n *externalNetwork) factory(identity byte, captured **externalTransport) meshbus.TransportFactory {
	return func(handler meshbus.Handler) (meshbus.NodeTransport, error) {
		id, err := meshbus.NewPeerID([]byte{identity})
		if err != nil {
			return nil, err
		}
		transport := &externalTransport{network: n, id: id, handler: handler}
		n.mu.Lock()
		n.nodes[id] = transport
		n.mu.Unlock()
		*captured = transport
		return transport, nil
	}
}

func (t *externalTransport) Identity() meshbus.PeerID { return t.id }

func (t *externalTransport) SetPeerObserver(observer meshbus.PeerObserver) error {
	t.observer = observer
	return nil
}

func (t *externalTransport) Start(context.Context) error {
	t.started = true
	return nil
}

func (t *externalTransport) Close() error {
	t.started = false
	return nil
}

func (t *externalTransport) SendMessage(ctx context.Context, peer meshbus.PeerID, payload []byte) error {
	t.network.mu.RLock()
	target := t.network.nodes[peer]
	t.network.mu.RUnlock()
	if !t.started || target == nil || !target.started {
		return errors.New("peer unavailable")
	}
	if err := t.observer.Authenticated(target.id); err != nil {
		return err
	}
	if err := target.observer.Authenticated(t.id); err != nil {
		return err
	}
	message, err := meshbus.NewReceivedMessage(t.id.Bytes(), payload)
	if err != nil {
		return err
	}
	return target.handler(ctx, message)
}

func (t *externalTransport) discover(peer *externalTransport) error {
	return t.observer.Discovered(meshbus.Peer{ID: peer.id})
}

func TestPublicNodeAPIFromExternalPackage(t *testing.T) {
	network := newExternalNetwork()
	var transportA, transportB *externalTransport
	nodeA, err := meshbus.NewNode(meshbus.NodeConfig{Transport: network.factory(1, &transportA)})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Close()
	nodeB, err := meshbus.NewNode(meshbus.NodeConfig{Transport: network.factory(2, &transportB)})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeB.Close()
	ctx := context.Background()
	if err := nodeA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transportA.discover(transportB); err != nil {
		t.Fatal(err)
	}
	if err := transportB.discover(transportA); err != nil {
		t.Fatal(err)
	}
	events := make(chan meshbus.ReceivedEvent, 1)
	if _, err := nodeB.Subscribe("example.event", func(_ context.Context, event meshbus.ReceivedEvent) error {
		events <- event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.Send(ctx, nodeB.Identity(), []byte("authenticate")); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeA.Publish(ctx, "example.event", []byte("hello"), meshbus.PublishOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Sender != nodeA.Identity() || string(event.Payload) != "hello" {
			t.Fatalf("event sender=%s payload=%q", event.Sender, event.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("external consumer did not receive event")
	}
}

func ExampleNode() {
	network := newExternalNetwork()
	var transportA, transportB *externalTransport
	nodeA, _ := meshbus.NewNode(meshbus.NodeConfig{Transport: network.factory(1, &transportA)})
	nodeB, _ := meshbus.NewNode(meshbus.NodeConfig{Transport: network.factory(2, &transportB)})
	defer nodeA.Close()
	defer nodeB.Close()
	ctx := context.Background()
	_ = nodeA.Start(ctx)
	_ = nodeB.Start(ctx)
	_ = transportA.discover(transportB)
	_ = transportB.discover(transportA)
	_ = nodeA.Send(ctx, nodeB.Identity(), []byte("authenticate"))

	received := make(chan meshbus.ReceivedEvent, 1)
	_, _ = nodeB.Subscribe("example.event", func(_ context.Context, event meshbus.ReceivedEvent) error {
		received <- event
		return nil
	})
	_, _ = nodeA.Publish(ctx, "example.event", []byte("hello"), meshbus.PublishOptions{})
	event := <-received
	fmt.Printf("%s: %s\n", event.Sender, event.Payload)
	// Output: 01: hello
}
