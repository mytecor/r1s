package rns

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	meshbus "github.com/mytecor/meshbus/core"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

func snapshotEndpoint(t *testing.T) *Endpoint {
	t.Helper()
	e, err := New(Config{EphemeralIdentity: true, ClusterKey: testClusterKey()}, func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func rememberAllocator(t *testing.T, e *Endpoint, name string, slots uint32) meshbus.PeerID {
	t.Helper()
	peer, err := meshbus.NewPeerID([]byte(name))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := newDescriptor(map[string]uint32{"default": slots}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	observer := peerObserver{PeerObserver: e.node, notify: e.discovered, resolve: e.transport.DestinationForIdentity}
	if err := observer.Discovered(meshbus.Peer{ID: peer, Metadata: descriptor.metadata()}); err != nil {
		t.Fatal(err)
	}
	return peer
}

func TestDiscoverySnapshotsSurviveUnreadNotificationsAndPromotion(t *testing.T) {
	e := snapshotEndpoint(t)
	for i := range 96 {
		rememberAllocator(t, e, fmt.Sprintf("allocator-%03d", i), 1)
	}
	if len(e.discovered) != 1 {
		t.Fatal("discovery notifications are not coalesced")
	}
	if got := len(e.Services()); got != 96 {
		t.Fatalf("snapshot lost discovery: %d", got)
	}
	peer := rememberAllocator(t, e, "allocator-000", 3)
	if err := e.node.Authenticated(peer); err != nil {
		t.Fatal(err)
	}
	services := e.Services()
	if len(services) != 96 {
		t.Fatal("promotion lost or duplicated allocator")
	}
	for _, service := range services {
		if service.Identity == peer.String() {
			if service.Descriptor.Capacity["default"] != 3 {
				t.Fatal("snapshot kept stale metadata")
			}
			service.Descriptor.Capacity["default"] = 999
		}
	}
	for _, service := range e.Services() {
		if service.Descriptor.Capacity["default"] == 999 {
			t.Fatal("snapshot mutation leaked into directory")
		}
	}
}

func TestStatusUsesTransportSenderRevisionAndExpiry(t *testing.T) {
	e := snapshotEndpoint(t)
	peer := rememberAllocator(t, e, "allocator", 2)
	apply := func(sender meshbus.PeerID, revision uint64, ready, draining bool, slots uint32) {
		data, _ := json.Marshal(AllocatorStatus{Revision: revision, Capacity: map[string]uint32{"default": slots}, Ready: ready, Draining: draining})
		if err := e.receiveStatus(context.Background(), meshbus.ReceivedEvent{Sender: sender, Event: meshbus.Event{Topic: "r1s.allocator.capacity", Payload: data}}); err != nil {
			t.Fatal(err)
		}
	}
	foreign, _ := meshbus.NewPeerID([]byte("not-an-allocator"))
	apply(foreign, 99, true, false, 100)
	if len(e.advisory) != 0 {
		t.Fatal("event invented an allocator")
	}
	apply(peer, 2, true, false, 0)
	apply(peer, 1, true, false, 100)
	if e.Services()[0].Descriptor.Capacity["default"] != 0 {
		t.Fatal("stale status replaced newest capacity")
	}
	apply(peer, 3, true, true, 100)
	if e.Services()[0].Descriptor.Capacity["default"] != 0 {
		t.Fatal("draining allocator still advertised free capacity")
	}
	e.advisoryMu.Lock()
	status := e.advisory[peer]
	status.receivedAt = time.Now().Add(-2 * statusTTL)
	e.advisory[peer] = status
	e.advisoryMu.Unlock()
	if e.Services()[0].Descriptor.Capacity["default"] != 2 {
		t.Fatal("expired event suppressed presence fallback")
	}
	apply(peer, 1, true, false, 1)
	if e.Services()[0].Descriptor.Capacity["default"] != 1 {
		t.Fatal("expired revision prevented allocator restart")
	}
}

func TestNodeDirectBootstrapAndCapacityEventsOverRNS(t *testing.T) {
	a, b := freeUDPPort(t), freeUDPPort(t)
	for b == a {
		b = freeUDPPort(t)
	}
	root := t.TempDir()
	received := make(chan *r1sv1.Envelope, 1)
	client := newTestEndpointWithCapacity(t, filepath.Join(root, "client"), a, b, nil, func(context.Context, *r1sv1.Envelope) error { return nil })
	allocator := newTestEndpoint(t, filepath.Join(root, "allocator"), b, a, func(_ context.Context, envelope *r1sv1.Envelope) error { received <- envelope; return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for _, e := range []*Endpoint{client, allocator} {
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
	}
	// Destination bootstrap establishes authenticated peers even without waiting
	// for the next announce. Ordinary traffic then uses the Node's PeerID API.
	if err := client.Send(ctx, allocator.Destination(), validEnvelope()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("bootstrap envelope not received")
	}
	if err := client.SendPeer(ctx, allocator.node.Identity(), validEnvelope()); err != nil {
		t.Fatal(err)
	}
	var envelope *r1sv1.Envelope
	select {
	case envelope = <-received:
	case <-ctx.Done():
		t.Fatal("peer envelope not received")
	}
	if !bytes.Equal(envelope.Sender, client.node.Identity().Bytes()) {
		t.Fatal("direct sender authority changed")
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := allocator.PublishStatus(ctx, "r1s.allocator.capacity", AllocatorStatus{Revision: 1, Capacity: map[string]uint32{"default": 0}, Ready: true}); err != nil {
			t.Fatal(err)
		}
		client.advisoryMu.Lock()
		status, ok := client.advisory[allocator.node.Identity()]
		client.advisoryMu.Unlock()
		if ok && status.Capacity["default"] == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("capacity event did not reach authenticated subscriber")
		case <-ticker.C:
		}
	}
}
