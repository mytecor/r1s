package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	coreclient "github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewCreatesFreshEphemeralAuthority(t *testing.T) {
	config := Config{ClusterKey: bytes.Repeat([]byte{0x42}, cluster.KeySize)}
	first, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if len(first.Identity()) == 0 || len(second.Identity()) == 0 {
		t.Fatal("client created an empty RNS identity")
	}
	if bytes.Equal(first.Identity(), second.Identity()) {
		t.Fatalf("independent clients reused authority %x", first.Identity())
	}
}

func TestRunRequiresStartedClient(t *testing.T) {
	client, err := New(Config{ClusterKey: bytes.Repeat([]byte{0x43}, cluster.KeySize)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Run(context.Background(), &r1sv1.ExecutionRequest{
		Workload:      &r1sv1.Workload{Image: "example.test/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		ResourceClass: "default",
	}, RunOptions{})
	if !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Run error = %v, want ErrNotStarted", err)
	}
}

func TestWaiterDispatchIsCorrelationAddressed(t *testing.T) {
	client := &Client{waiters: make(map[string][]chan *r1sv1.Envelope)}
	a, cancelA := client.registerWaiter("a")
	defer cancelA()
	b, cancelB := client.registerWaiter("b")
	defer cancelB()
	client.dispatchEnvelope(&r1sv1.Envelope{CorrelationId: "b", MessageId: "message-b"})
	select {
	case <-a:
		t.Fatal("correlation a received b response")
	default:
	}
	select {
	case envelope := <-b:
		if envelope.GetMessageId() != "message-b" {
			t.Fatalf("message ID = %q", envelope.GetMessageId())
		}
	default:
		t.Fatal("correlation b did not receive its response")
	}
}

func TestClientAuthorityIsSingleRun(t *testing.T) {
	client := &Client{started: true}
	if err := client.beginRun(); err != nil {
		t.Fatal(err)
	}
	client.endRun()
	if err := client.beginRun(); !errors.Is(err, ErrRunActive) {
		t.Fatalf("second beginRun error = %v, want ErrRunActive", err)
	}
}

func TestRunControllerCompletesThroughTransportContract(t *testing.T) {
	core, err := coreclient.New(coreclient.Config{Identity: []byte("client")})
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{
		core: core, identity: []byte("client"), waiters: make(map[string][]chan *r1sv1.Envelope),
		stateChanged: make(chan struct{}, 1),
	}
	endpoint := &fakeControllerEndpoint{client: c, discoveries: make(chan rns.Service, 1)}
	c.endpoint = endpoint
	endpoint.discoveries <- rns.Service{
		Destination: "allocator-destination",
		Identity:    "616c6c6f6361746f72",
		Descriptor:  rns.Descriptor{Capacity: map[string]uint32{"default": 1}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result, err := c.Run(ctx, &r1sv1.ExecutionRequest{
		Workload:      &r1sv1.Workload{Image: "example.test/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Policy:        &r1sv1.ExecutionPolicy{},
		ResourceClass: "default",
	}, RunOptions{OfferWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempt.RunID == "" || result.Attempt.Number != 1 || result.Attempt.ExecutionID == "" {
		t.Fatalf("attempt = %+v", result.Attempt)
	}
	if result.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED {
		t.Fatalf("phase = %s", result.State.GetPhase())
	}
}

type fakeControllerEndpoint struct {
	client      *Client
	discoveries chan rns.Service
}

func (f *fakeControllerEndpoint) Start(context.Context) error { return nil }
func (f *fakeControllerEndpoint) Close() error                { return nil }
func (f *fakeControllerEndpoint) Discoveries() <-chan rns.Service {
	return f.discoveries
}
func (f *fakeControllerEndpoint) DestinationForIdentity(identity string) (string, bool) {
	return "allocator-destination", identity == "616c6c6f6361746f72"
}
func (f *fakeControllerEndpoint) Send(ctx context.Context, _ string, envelope *r1sv1.Envelope) error {
	now := time.Now().UTC()
	respond := func(response *r1sv1.Envelope, suffix string) error {
		response.MessageId = "response-" + suffix
		response.Sender = []byte("allocator")
		response.CorrelationId = envelope.GetMessageId()
		response.SentAt = timestamppb.New(now)
		return f.client.handleEnvelope(ctx, response)
	}
	switch payload := envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionRequest:
		return respond(&r1sv1.Envelope{Payload: &r1sv1.Envelope_ExecutionOffer{ExecutionOffer: &r1sv1.ExecutionOffer{
			OfferId: "offer", RequestId: payload.ExecutionRequest.GetRequestId(),
			ResourceClass: payload.ExecutionRequest.GetResourceClass(), ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		}}}, "offer")
	case *r1sv1.Envelope_ExecutionAssign:
		executionID := payload.ExecutionAssign.GetExecutionId()
		exitCode := int32(0)
		return respond(&r1sv1.Envelope{Payload: &r1sv1.Envelope_ExecutionState{ExecutionState: &r1sv1.ExecutionState{
			ExecutionId: executionID, Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED,
			OccurredAt: timestamppb.Now(), ExitCode: &exitCode, Revision: 1,
		}}}, "state")
	case *r1sv1.Envelope_ExecutionLeaseRenew:
		return respond(&r1sv1.Envelope{Payload: &r1sv1.Envelope_ExecutionLeaseRenewAck{ExecutionLeaseRenewAck: &r1sv1.ExecutionLeaseRenewAck{
			ExecutionId: payload.ExecutionLeaseRenew.GetExecutionId(), ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		}}}, "lease")
	default:
		return fmt.Errorf("unexpected payload %T", payload)
	}
}
