package rns

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"quad4/reticulum-go/pkg/common"
)

func TestDeliverReplacesForgedSenderBeforeValidation(t *testing.T) {
	authenticated := bytes.Repeat([]byte{0x42}, 16)
	received := make(chan *r1sv1.Envelope, 1)
	endpoint := &Endpoint{handler: func(_ context.Context, envelope *r1sv1.Envelope) error {
		received <- envelope
		return nil
	}}
	envelope := validEnvelope()
	envelope.Sender = []byte("forged")
	data, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.deliver(&session{sender: authenticated}, data)

	select {
	case delivered := <-received:
		if !bytes.Equal(delivered.GetSender(), authenticated) {
			t.Fatalf("sender = %x, want %x", delivered.GetSender(), authenticated)
		}
	case <-time.After(time.Second):
		t.Fatal("validated envelope was not delivered")
	}
}

func TestDeliverRejectsUnauthenticatedAndInvalidEnvelopes(t *testing.T) {
	called := make(chan struct{}, 1)
	endpoint := &Endpoint{handler: func(context.Context, *r1sv1.Envelope) error {
		called <- struct{}{}
		return nil
	}}
	validData, err := proto.Marshal(validEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	endpoint.deliver(&session{}, validData)
	endpoint.deliver(&session{sender: bytes.Repeat([]byte{1}, 16)}, []byte("not protobuf"))
	select {
	case <-called:
		t.Fatal("handler called for unauthenticated or invalid data")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestParseDestination(t *testing.T) {
	want := bytes.Repeat([]byte{0xab}, 16)
	got, key, err := parseDestination("  ABABABABABABABABABABABABABABABAB  ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) || key != "abababababababababababababababab" {
		t.Fatalf("destination = %x, key = %q", got, key)
	}
	if _, _, err := parseDestination("short"); err == nil {
		t.Fatal("invalid destination accepted")
	}
}

func TestEndpointsExchangeAuthenticatedEnvelopeOverUDP(t *testing.T) {
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	root := t.TempDir()
	received := make(chan *r1sv1.Envelope, 1)
	replies := make(chan *r1sv1.Envelope, 1)
	endpointA := newTestEndpointWithCapacity(t, filepath.Join(root, "a"), portA, portB, nil, func(_ context.Context, envelope *r1sv1.Envelope) error {
		replies <- envelope
		return nil
	})
	if endpointA.advertises {
		t.Fatal("passive client endpoint advertises allocator capacity")
	}
	endpointB := newTestEndpoint(t, filepath.Join(root, "b"), portB, portA, func(_ context.Context, envelope *r1sv1.Envelope) error {
		received <- envelope
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := endpointA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpointA.Close() })
	if err := endpointB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpointB.Close() })

	select {
	case service := <-endpointA.Discoveries():
		if service.Destination != endpointB.Destination() || service.Descriptor.Capacity["default"] != 1 {
			t.Fatalf("service = %+v", service)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("allocator announce was not discovered")
	}

	envelope := validEnvelope()
	envelope.Sender = []byte("forged-payload-sender")
	sendContext, stopSend := context.WithTimeout(context.Background(), 8*time.Second)
	defer stopSend()
	if err := endpointA.Send(sendContext, endpointB.Destination(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-received:
		want, err := hex.DecodeString(endpointA.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(delivered.GetSender(), want) {
			t.Fatalf("sender = %x, want authenticated identity %x", delivered.GetSender(), want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("envelope was not delivered")
	}

	endpointA.mu.Lock()
	active := endpointA.sessions[endpointB.Destination()]
	endpointA.mu.Unlock()
	if active == nil {
		t.Fatal("outbound session was not cached")
	}
	active.link.Teardown()
	destinationHash, _, err := parseDestination(endpointB.Destination())
	if err != nil {
		t.Fatal(err)
	}
	endpointA.stack.transport.ExpirePath(destinationHash)

	reconnected := validEnvelope()
	reconnected.MessageId = "after-reconnect"
	if err := endpointA.Send(sendContext, endpointB.Destination(), reconnected); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-received:
		if delivered.GetMessageId() != "after-reconnect" {
			t.Fatalf("message after reconnect = %q", delivered.GetMessageId())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("envelope was not delivered after path and link reconnect")
	}

	release := validEnvelope()
	release.MessageId = "release"
	release.Payload = &r1sv1.Envelope_ExecutionOfferRelease{ExecutionOfferRelease: &r1sv1.ExecutionOfferRelease{RequestId: "request", OfferId: "offer"}}
	if err := endpointA.Send(sendContext, endpointB.Destination(), release); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-received:
		if !proto.Equal(delivered.GetExecutionOfferRelease(), release.GetExecutionOfferRelease()) {
			t.Fatalf("release payload=%v", delivered)
		}
		wantSender, _ := hex.DecodeString(endpointA.Name())
		if !bytes.Equal(delivered.GetSender(), wantSender) {
			t.Fatal("release sender is not authenticated")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("release not delivered")
	}
	ack := validEnvelope()
	ack.MessageId, ack.CorrelationId = "release-ack", "release"
	ack.Payload = &r1sv1.Envelope_ExecutionOfferReleaseAck{ExecutionOfferReleaseAck: &r1sv1.ExecutionOfferReleaseAck{RequestId: "request", OfferId: "offer", Outcome: r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_RELEASED}}
	if err := endpointB.Send(sendContext, endpointA.Name(), ack); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-replies:
		wantSender, _ := hex.DecodeString(endpointB.Name())
		if !proto.Equal(delivered.GetExecutionOfferReleaseAck(), ack.GetExecutionOfferReleaseAck()) || delivered.GetCorrelationId() != "release" || !bytes.Equal(delivered.GetSender(), wantSender) {
			t.Fatalf("release acknowledgement=%v", delivered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("release acknowledgement not delivered")
	}
}

func newTestEndpoint(t *testing.T, storage string, listenPort, targetPort int, handler func(context.Context, *r1sv1.Envelope) error) *Endpoint {
	return newTestEndpointWithCapacity(t, storage, listenPort, targetPort, map[string]uint32{"default": 1}, handler)
}

func newTestEndpointWithCapacity(t *testing.T, storage string, listenPort, targetPort int, capacity map[string]uint32, handler func(context.Context, *r1sv1.Envelope) error) *Endpoint {
	t.Helper()
	config := common.DefaultConfig()
	config.EnableTransport = false
	config.ConfigPath = storage
	config.Interfaces = map[string]*common.InterfaceConfig{
		"test": {
			Type:       "UDPInterface",
			Enabled:    true,
			Address:    fmt.Sprintf("127.0.0.1:%d", listenPort),
			TargetHost: fmt.Sprintf("127.0.0.1:%d", targetPort),
		},
	}
	endpoint, err := New(Config{
		Reticulum:    config,
		IdentityPath: filepath.Join(storage, "r1sd.identity"),
		Capacity:     capacity,
		NetworkWait:  8 * time.Second,
	}, handler)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

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
		MessageId: "message",
		Sender:    []byte("payload-sender"),
		SentAt:    timestamppb.Now(),
		Payload: &r1sv1.Envelope_ExecutionCancel{
			ExecutionCancel: &r1sv1.ExecutionCancel{ExecutionId: "execution"},
		},
	}
}
