package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	meshbus "github.com/mytecor/meshbus/core"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/transport/rns"
)

type snapshotTestEndpoint struct {
	mu               sync.Mutex
	services         []rns.Service
	changes          chan rns.Service
	bootstrapStarted chan struct{}
	releaseBootstrap chan struct{}
	peerSent         chan meshbus.PeerID
}

func (e *snapshotTestEndpoint) Name() string                    { return "local" }
func (e *snapshotTestEndpoint) Close() error                    { return nil }
func (e *snapshotTestEndpoint) Start(context.Context) error     { return nil }
func (e *snapshotTestEndpoint) Discoveries() <-chan rns.Service { return e.changes }
func (e *snapshotTestEndpoint) Services() []rns.Service {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]rns.Service(nil), e.services...)
}
func (e *snapshotTestEndpoint) Send(ctx context.Context, _ string, _ *r1sv1.Envelope) error {
	close(e.bootstrapStarted)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.releaseBootstrap:
		return nil
	}
}
func (e *snapshotTestEndpoint) SendPeer(_ context.Context, peer meshbus.PeerID, _ *r1sv1.Envelope) error {
	e.peerSent <- peer
	return nil
}

func TestBrokerSnapshotsRecoverWithoutNotificationsAndBootstrapDoesNotBlockPeerSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	active := &snapshotTestEndpoint{changes: make(chan rns.Service, 1), bootstrapStarted: make(chan struct{}), releaseBootstrap: make(chan struct{}), peerSent: make(chan meshbus.PeerID, 1)}
	server := &Server{newEndpoint: func([]byte, time.Duration, func(context.Context, *r1sv1.Envelope) error) (endpoint, error) {
		return active, nil
	}}
	serverConn, clientConn := net.Pipe()
	go func() {
		defer serverConn.Close()
		server.serveEndpoint(ctx, serverConn, frame{NetworkWait: int64(time.Second)})
	}()
	if _, err := readFrame(clientConn); err != nil {
		t.Fatal(err)
	}
	client := &Endpoint{connection: clientConn, pending: make(map[uint64]chan frame), discoveries: make(chan rns.Service, 1), done: make(chan struct{}), handler: func(context.Context, *r1sv1.Envelope) error { return nil }}
	go client.readLoop()
	defer client.Close()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// No discovery notification is sent, and the snapshot is larger than the
	// former 32-entry channel. Both initial and subsequent states must survive.
	active.mu.Lock()
	for i := range 96 {
		active.services = append(active.services, rns.Service{Identity: fmt.Sprintf("peer-%d", i), Descriptor: rns.Descriptor{Capacity: map[string]uint32{"default": 1}}})
	}
	active.mu.Unlock()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for len(client.Services()) != 96 {
		select {
		case <-ctx.Done():
			t.Fatal("broker lost snapshot")
		case <-ticker.C:
		}
	}
	copy := client.Services()
	copy[0].Descriptor.Capacity["default"] = 999
	if client.Services()[0].Descriptor.Capacity["default"] != 1 {
		t.Fatal("mutable broker snapshot")
	}
	bootstrapDone := make(chan error, 1)
	go func() { bootstrapDone <- client.Send(ctx, "unreachable", &r1sv1.Envelope{MessageId: "bootstrap"}) }()
	select {
	case <-active.bootstrapStarted:
	case <-ctx.Done():
		t.Fatal("bootstrap did not start")
	}
	peer, _ := meshbus.NewPeerID([]byte("allocator"))
	sendCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	if err := client.SendPeer(sendCtx, peer, &r1sv1.Envelope{MessageId: "ordinary"}); err != nil {
		t.Fatalf("bootstrap blocked peer send: %v", err)
	}
	select {
	case got := <-active.peerSent:
		if got != peer {
			t.Fatal("broker changed target peer")
		}
	case <-ctx.Done():
		t.Fatal("peer not delivered")
	}
	close(active.releaseBootstrap)
	if err := <-bootstrapDone; err != nil {
		t.Fatal(err)
	}
}
