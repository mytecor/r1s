package transport_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/transport"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemoryDeliversCloneWithAuthenticatedSender(t *testing.T) {
	network := transport.NewMemory()
	received := make(chan *r1sv1.Envelope, 1)
	source, err := network.Register("authenticated-owner", func(_ context.Context, envelope *r1sv1.Envelope) error {
		received <- envelope
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = network.Register("allocator", func(_ context.Context, envelope *r1sv1.Envelope) error {
		received <- envelope
		envelope.MessageId = "mutated-by-handler"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	original := testEnvelope()
	original.Sender = []byte("forged-owner")
	if err := source.Send(context.Background(), "allocator", original); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	delivered := <-received
	if got := string(delivered.GetSender()); got != "authenticated-owner" {
		t.Fatalf("sender = %q, want authenticated-owner", got)
	}
	if original.GetMessageId() != "message" || string(original.GetSender()) != "forged-owner" {
		t.Fatalf("original envelope was mutated: %v", original)
	}
	if proto.Equal(original, delivered) {
		t.Fatal("delivery did not replace sender and isolate handler mutation")
	}
}

func TestMemoryDestinationErrors(t *testing.T) {
	network := transport.NewMemory()
	source, err := network.Register("source", func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	target, err := network.Register("target", func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	if err := source.Send(context.Background(), "missing", testEnvelope()); !errors.Is(err, transport.ErrUnknownDestination) {
		t.Fatalf("unknown destination error = %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Send(context.Background(), "target", testEnvelope()); !errors.Is(err, transport.ErrDestinationClosed) {
		t.Fatalf("closed destination error = %v", err)
	}
	if err := target.Send(context.Background(), "source", testEnvelope()); !errors.Is(err, transport.ErrEndpointClosed) {
		t.Fatalf("closed source error = %v", err)
	}
	if err := network.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Send(context.Background(), "target", testEnvelope()); !errors.Is(err, transport.ErrTransportClosed) {
		t.Fatalf("closed transport error = %v", err)
	}
}

func TestMemoryConcurrentDelivery(t *testing.T) {
	network := transport.NewMemory()
	const messages = 64
	var mu sync.Mutex
	seen := make(map[string]bool, messages)
	source, err := network.Register("source", func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	_, err = network.Register("target", func(_ context.Context, envelope *r1sv1.Envelope) error {
		mu.Lock()
		seen[envelope.GetMessageId()] = true
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	for index := 0; index < messages; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			envelope := testEnvelope()
			envelope.MessageId = time.Unix(int64(index), 0).String()
			if err := source.Send(context.Background(), "target", envelope); err != nil {
				t.Errorf("Send() error = %v", err)
			}
		}(index)
	}
	wait.Wait()
	if len(seen) != messages {
		t.Fatalf("delivered %d distinct messages, want %d", len(seen), messages)
	}
}

func testEnvelope() *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: "message",
		Sender:    []byte("untrusted"),
		SentAt:    timestamppb.Now(),
		Payload: &r1sv1.Envelope_ExecutionCancel{
			ExecutionCancel: &r1sv1.ExecutionCancel{ExecutionId: "execution"},
		},
	}
}
