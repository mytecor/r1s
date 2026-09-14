package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func releaseClient(t *testing.T) (*Client, *r1sv1.Envelope) {
	t.Helper()
	core, err := New(Config{Identity: []byte("client")})
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"winner", "loser", "late"} {
		if err := core.RegisterAllocator(Allocator{Identity: []byte(name), Destination: name, Hops: uint8(i)}); err != nil {
			t.Fatal(err)
		}
	}
	_, request, err := core.CreateRequest(&r1sv1.Workload{Image: "example/image:latest"}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"winner", "loser"} {
		observeReleaseOffer(t, core, request, name)
	}
	return core, request
}

func observeReleaseOffer(t *testing.T, core *Client, request *r1sv1.Envelope, name string) {
	t.Helper()
	now := time.Now().UTC()
	err := core.Handle(context.Background(), &r1sv1.Envelope{MessageId: "offer-" + name, Sender: []byte(name), SentAt: timestamppb.New(now), CorrelationId: request.GetMessageId(),
		Payload: &r1sv1.Envelope_ExecutionOffer{ExecutionOffer: &r1sv1.ExecutionOffer{RequestId: request.GetExecutionRequest().GetRequestId(), OfferId: "offer-" + name, ResourceClass: "default", ExpiresAt: timestamppb.New(now.Add(time.Minute))}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOfferReleaseWorkerDrainsLateOffersAndRetriesStableCommands(t *testing.T) {
	core, request := releaseClient(t)
	var mu sync.Mutex
	seen := make(map[string]string)
	counts := make(map[string]int)
	send := func(ctx context.Context, destination string, envelope *r1sv1.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		if previous := seen[destination]; previous != "" && previous != envelope.GetMessageId() {
			t.Error("retry changed message ID")
		}
		seen[destination] = envelope.GetMessageId()
		counts[destination]++
		if counts[destination] == 1 {
			return nil
		} // Simulate lost delivery or acknowledgement.
		release := envelope.GetExecutionOfferRelease()
		return core.Handle(ctx, &r1sv1.Envelope{MessageId: fmt.Sprint("ack-", destination), Sender: []byte(destination), SentAt: timestamppb.Now(), CorrelationId: envelope.GetMessageId(),
			Payload: &r1sv1.Envelope_ExecutionOfferReleaseAck{ExecutionOfferReleaseAck: &r1sv1.ExecutionOfferReleaseAck{RequestId: release.GetRequestId(), OfferId: release.GetOfferId(), Outcome: r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_RELEASED}},
		})
	}
	finish := core.StartOfferReleases(context.Background(), send)
	_, assignment, err := core.Select(request.GetExecutionRequest().GetRequestId())
	if err != nil {
		t.Fatal(err)
	}
	observeReleaseOffer(t, core, request, "late")
	pending := finish()
	if len(pending) != 0 {
		t.Fatalf("pending=%v", pending)
	}
	if counts["loser"] < 2 || counts["late"] < 2 || counts["winner"] != 0 {
		t.Fatalf("send counts=%v", counts)
	}
	if assignment.GetExecutionAssign().GetOfferId() != "offer-winner" {
		t.Fatal("release worker changed assignment")
	}
}

func TestOfferReleaseWorkerStopsWithContextAndKeepsOutbox(t *testing.T) {
	core, request := releaseClient(t)
	if _, _, err := core.Select(request.GetExecutionRequest().GetRequestId()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	finish := core.StartOfferReleases(ctx, func(ctx context.Context, _ string, _ *r1sv1.Envelope) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("release not attempted")
	}
	cancel()
	finish()
	if len(core.PendingReleases()) != 1 {
		t.Fatal("cancelled send lost durable intent")
	}
}
