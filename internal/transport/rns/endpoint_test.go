package rns

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/mytecor/meshbus"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEnvelopeHandlerReplacesForgedSenderBeforeValidation(t *testing.T) {
	authenticated := bytes.Repeat([]byte{0x42}, 16)
	received := make(chan *r1sv1.Envelope, 1)
	handler := envelopeHandler(func(_ context.Context, envelope *r1sv1.Envelope) error {
		received <- envelope
		return nil
	})
	envelope := validEnvelope()
	envelope.Sender = []byte("forged")
	data, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	message, err := meshbus.NewReceivedMessage(authenticated, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-received:
		if !bytes.Equal(delivered.GetSender(), authenticated) {
			t.Fatalf("sender = %x, want %x", delivered.GetSender(), authenticated)
		}
	case <-time.After(time.Second):
		t.Fatal("validated envelope was not delivered")
	}
}

func TestEnvelopeHandlerRejectsMalformedAndInvalidEnvelopes(t *testing.T) {
	called := make(chan struct{}, 1)
	handler := envelopeHandler(func(context.Context, *r1sv1.Envelope) error {
		called <- struct{}{}
		return nil
	})
	for _, payload := range [][]byte{[]byte("not protobuf"), {0x78, 0x01}} {
		message, err := meshbus.NewReceivedMessage(bytes.Repeat([]byte{1}, 16), payload)
		if err != nil {
			continue
		}
		if err := handler(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-called:
		t.Fatal("handler called for malformed or invalid data")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestEphemeralEndpointsReceiveFreshInMemoryIdentities(t *testing.T) {
	config := Config{EphemeralIdentity: true, ClusterKey: testClusterKey()}
	first, err := New(config, func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(config, func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.Name() == second.Name() {
		t.Fatalf("fresh ephemeral endpoints reused identity %s", first.Name())
	}
	if _, err := New(Config{IdentitySource: "identity", EphemeralIdentity: true, ClusterKey: testClusterKey()}, func(context.Context, *r1sv1.Envelope) error { return nil }); err == nil {
		t.Fatal("identity source and ephemeral identity were accepted together")
	}
}

func TestEndpointsExchangeAuthenticatedEnvelopeOverPublicMeshbusAdapter(t *testing.T) {
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	root := t.TempDir()
	received := make(chan *r1sv1.Envelope, 1)
	client := newTestEndpointWithCapacity(t, filepath.Join(root, "client"), portA, portB, nil, func(context.Context, *r1sv1.Envelope) error { return nil })
	allocator := newTestEndpoint(t, filepath.Join(root, "allocator"), portB, portA, func(_ context.Context, envelope *r1sv1.Envelope) error {
		received <- envelope
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, endpoint := range []*Endpoint{client, allocator} {
		if err := endpoint.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
	}

	select {
	case service := <-client.Discoveries():
		if service.Destination != allocator.Destination() || service.Identity != allocator.Name() || service.Descriptor.Capacity["default"] != 1 {
			t.Fatalf("service = %+v", service)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("allocator announce was not discovered")
	}

	envelope := validEnvelope()
	envelope.Sender = []byte("forged-payload-sender")
	sendContext, stopSend := context.WithTimeout(context.Background(), 8*time.Second)
	defer stopSend()
	if err := client.Send(sendContext, allocator.Destination(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-received:
		want, _ := hex.DecodeString(client.Name())
		if !bytes.Equal(delivered.GetSender(), want) {
			t.Fatalf("sender = %x, want authenticated identity %x", delivered.GetSender(), want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("envelope was not delivered")
	}
}

func TestMismatchedClusterIsNotDiscoveredOrAuthorized(t *testing.T) {
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	root := t.TempDir()
	client := newTestEndpointWithCluster(t, filepath.Join(root, "client"), portA, portB, nil, bytes.Repeat([]byte{1}, 32), func(context.Context, *r1sv1.Envelope) error {
		t.Fatal("unauthorized envelope reached client handler")
		return nil
	})
	allocator := newTestEndpointWithCluster(t, filepath.Join(root, "allocator"), portB, portA, map[string]uint32{"default": 1}, bytes.Repeat([]byte{2}, 32), func(context.Context, *r1sv1.Envelope) error {
		t.Fatal("unauthorized envelope reached allocator handler")
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, endpoint := range []*Endpoint{client, allocator} {
		if err := endpoint.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
	}
	select {
	case service := <-client.Discoveries():
		t.Fatalf("foreign cluster discovered: %+v", service)
	case <-time.After(100 * time.Millisecond):
	}
	sendContext, stop := context.WithTimeout(context.Background(), 8*time.Second)
	defer stop()
	if err := client.Send(sendContext, allocator.Destination(), validEnvelope()); !errors.Is(err, ErrClusterAuthentication) {
		t.Fatalf("Send() error = %v, want cluster authentication failure", err)
	}
}

func newTestEndpoint(t *testing.T, storage string, listenPort, targetPort int, handler func(context.Context, *r1sv1.Envelope) error) *Endpoint {
	return newTestEndpointWithCapacity(t, storage, listenPort, targetPort, map[string]uint32{"default": 1}, handler)
}

func newTestEndpointWithCapacity(t *testing.T, storage string, listenPort, targetPort int, capacity map[string]uint32, handler func(context.Context, *r1sv1.Envelope) error) *Endpoint {
	return newTestEndpointWithCluster(t, storage, listenPort, targetPort, capacity, testClusterKey(), handler)
}

func newTestEndpointWithCluster(t *testing.T, storage string, listenPort, targetPort int, capacity map[string]uint32, key []byte, handler func(context.Context, *r1sv1.Envelope) error) *Endpoint {
	t.Helper()
	config := common.DefaultConfig()
	config.EnableTransport = false
	config.ConfigPath = storage
	config.Interfaces = map[string]*common.InterfaceConfig{
		"test": {Type: "UDPInterface", Enabled: true, Address: fmt.Sprintf("127.0.0.1:%d", listenPort), TargetHost: fmt.Sprintf("127.0.0.1:%d", targetPort)},
	}
	endpoint, err := New(Config{
		Reticulum: config, IdentitySource: filepath.Join(storage, "r1sd.identity"), ClusterKey: key,
		Capacity: capacity, NetworkWait: 8 * time.Second,
	}, handler)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func testClusterKey() []byte { return bytes.Repeat([]byte{0x51}, 32) }

func freeUDPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.LocalAddr().(*net.UDPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func validEnvelope() *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: "message", Sender: []byte("payload-sender"), SentAt: timestamppb.Now(),
		Payload: &r1sv1.Envelope_ExecutionCancel{ExecutionCancel: &r1sv1.ExecutionCancel{ExecutionId: "execution"}},
	}
}
