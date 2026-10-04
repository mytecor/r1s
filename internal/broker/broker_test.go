//go:build !windows

package broker

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
)

func TestBrokerRetainsCredentialAndCreatesRunEndpoint(t *testing.T) {
	address := filepath.Join(os.TempDir(), fmt.Sprintf("r1s-broker-%d.sock", os.Getpid()))
	removeAddress(address)
	t.Cleanup(func() { removeAddress(address) })
	key := bytes.Repeat([]byte{0x41}, cluster.KeySize)
	clusterHash, err := cluster.ID(key)
	if err != nil {
		t.Fatal(err)
	}
	clusterID := hex.EncodeToString(clusterHash)
	identity := hex.EncodeToString(bytes.Repeat([]byte{0x22}, 16))
	bootstrap := hex.EncodeToString(bytes.Repeat([]byte{0x33}, cluster.DestinationSize))
	ready := make(chan struct{})
	created := make(chan *fakeEndpoint, 1)
	server := &Server{
		ClusterID: clusterID, ClusterKey: key,
		BootstrapDestinations: []string{bootstrap},
		Ready:                 func() { close(ready) },
		newEndpoint: func(received []byte, _ time.Duration, handler func(context.Context, *r1sv1.Envelope) error) (endpoint, error) {
			if !bytes.Equal(received, key) {
				t.Fatalf("broker endpoint key = %x", received)
			}
			fake := &fakeEndpoint{name: identity, handler: handler, discoveries: make(chan rns.Service, 1)}
			created <- fake
			return fake, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, address) }()
	select {
	case <-ready:
	case err := <-served:
		t.Fatalf("Serve() before ready: %v", err)
	}

	if id, err := Status(address); err != nil || id != clusterID {
		t.Fatalf("Status() = %q, %v", id, err)
	}
	receivedEnvelope := make(chan *r1sv1.Envelope, 1)
	clientEndpoint, err := OpenEndpoint(address, time.Second, func(_ context.Context, envelope *r1sv1.Envelope) error {
		receivedEnvelope <- envelope
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientEndpoint.Close()
	if clientEndpoint.Name() != identity || clientEndpoint.ClusterID() != clusterID {
		t.Fatalf("opened identity=%q cluster=%q", clientEndpoint.Name(), clientEndpoint.ClusterID())
	}
	if got := clientEndpoint.BootstrapDestinations(); len(got) != 1 || got[0] != bootstrap {
		t.Fatalf("bootstrap destinations = %v", got)
	}
	fake := <-created
	if err := clientEndpoint.Start(ctx); err != nil {
		t.Fatal(err)
	}
	fake.discoveries <- rns.Service{Identity: "allocator", Destination: "destination", Descriptor: rns.Descriptor{Capacity: map[string]uint32{"default": 1}}}
	select {
	case discovery := <-clientEndpoint.Discoveries():
		if discovery.Destination != "destination" {
			t.Fatalf("discovery = %+v", discovery)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not forward discovery")
	}
	outbound := &r1sv1.Envelope{MessageId: "outbound"}
	if err := clientEndpoint.Send(ctx, "destination", outbound); err != nil {
		t.Fatal(err)
	}
	if fake.sent.GetMessageId() != "outbound" {
		t.Fatalf("sent envelope = %v", fake.sent)
	}
	inbound := &r1sv1.Envelope{MessageId: "inbound", Sender: []byte("allocator")}
	if err := fake.handler(ctx, inbound); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-receivedEnvelope:
		if got.GetMessageId() != "inbound" {
			t.Fatalf("received envelope = %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not forward envelope")
	}
	if err := Shutdown(address); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not stop")
	}
}

func TestBrokerCreatesFreshIdentityPerConnection(t *testing.T) {
	address := filepath.Join(os.TempDir(), fmt.Sprintf("r1s-broker-identities-%d.sock", os.Getpid()))
	removeAddress(address)
	t.Cleanup(func() { removeAddress(address) })
	key := bytes.Repeat([]byte{0x52}, cluster.KeySize)
	clusterHash, err := cluster.ID(key)
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{ClusterID: hex.EncodeToString(clusterHash), ClusterKey: key, Ready: func() { close(ready) }}
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, address) }()
	select {
	case <-ready:
	case err := <-served:
		t.Fatalf("Serve() before ready: %v", err)
	}
	first, err := OpenEndpoint(address, time.Second, func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenEndpoint(address, time.Second, func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.Name() == second.Name() {
		t.Fatalf("broker reused identity %s", first.Name())
	}
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not stop")
	}
}

type fakeEndpoint struct {
	name        string
	handler     func(context.Context, *r1sv1.Envelope) error
	discoveries chan rns.Service
	sent        *r1sv1.Envelope
}

func (f *fakeEndpoint) Start(context.Context) error                  { return nil }
func (f *fakeEndpoint) Close() error                                 { return nil }
func (f *fakeEndpoint) Name() string                                 { return f.name }
func (f *fakeEndpoint) Discoveries() <-chan rns.Service              { return f.discoveries }
func (f *fakeEndpoint) DestinationForIdentity(string) (string, bool) { return "destination", true }
func (f *fakeEndpoint) Send(_ context.Context, _ string, envelope *r1sv1.Envelope) error {
	f.sent = envelope
	return nil
}
