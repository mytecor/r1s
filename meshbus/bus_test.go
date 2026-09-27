package meshbus

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type sentMessage struct {
	destination string
	payload     []byte
}

type recordingSender struct {
	mu       sync.Mutex
	messages []sentMessage
	fail     map[string]error
}

func (s *recordingSender) SendMessage(_ context.Context, destination string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, sentMessage{destination: destination, payload: bytes.Clone(payload)})
	return s.fail[destination]
}

func (s *recordingSender) snapshot() []sentMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]sentMessage, len(s.messages))
	copy(result, s.messages)
	return result
}

func newTestBus(t *testing.T, sender Sender, peers []string, now *time.Time, update func(*BusConfig)) *Bus {
	t.Helper()
	config := BusConfig{
		Sender: sender,
		Peers:  PeerSourceFunc(func() []string { return append([]string(nil), peers...) }),
		clock:  func() time.Time { return *now },
	}
	if update != nil {
		update(&config)
	}
	bus, err := NewBus(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

func TestPublishFansOutOnceToUniquePeerSnapshot(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	sender := &recordingSender{fail: map[string]error{"peer-b": errors.New("offline")}}
	bus := newTestBus(t, sender, []string{"peer-a", " peer-b ", "peer-a", ""}, &now, func(config *BusConfig) {
		config.idSource = bytes.NewReader(bytes.Repeat([]byte{0x42}, 16))
		config.FanoutConcurrency = 2
	})

	result, err := bus.Publish(context.Background(), "git.ref.updated", []byte("payload"), PublishOptions{
		TTL: time.Minute, ContentType: "application/octet-stream",
	})
	if err == nil || result.ID.String() != "42424242424242424242424242424242" || result.Attempted != 2 || result.Delivered != 1 || len(result.Failed) != 1 {
		t.Fatalf("Publish() result=%+v error=%v", result, err)
	}
	messages := sender.snapshot()
	if len(messages) != 2 {
		t.Fatalf("sent %d messages, want 2", len(messages))
	}
	destinations := map[string]bool{}
	for _, message := range messages {
		destinations[message.destination] = true
		event, decodeErr := decodeEvent(message.payload, defaultMaxEventPayload, defaultMaxEventTTL)
		if decodeErr != nil || event.ID != result.ID || event.Topic != "git.ref.updated" || string(event.Payload) != "payload" {
			t.Fatalf("wire event=%+v error=%v", event, decodeErr)
		}
	}
	if !destinations["peer-a"] || !destinations["peer-b"] {
		t.Fatalf("destinations=%v", destinations)
	}
}

func TestHandleMessageUsesAuthenticatedSenderAndDeduplicates(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	bus := newTestBus(t, &recordingSender{}, nil, &now, nil)
	received := make(chan ReceivedEvent, 2)
	subscription, err := bus.Subscribe("task.completed", func(_ context.Context, event ReceivedEvent) error {
		received <- event
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if _, err := bus.Subscribe("other.topic", func(context.Context, ReceivedEvent) error {
		t.Fatal("unmatched subscription called")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	event := Event{
		ID: EventID{1}, Topic: "task.completed", PublishedAt: now, TTL: time.Minute,
		ContentType: "application/json", Payload: []byte(`{"payloadSender":"forged"}`),
	}
	wire, err := encodeEvent(event, defaultMaxEventPayload, defaultMaxEventTTL)
	if err != nil {
		t.Fatal(err)
	}
	message, err := NewReceivedMessage([]byte{0xaa, 0xbb}, wire)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		handled, handleErr := bus.HandleMessage(context.Background(), message)
		if !handled || handleErr != nil {
			t.Fatalf("HandleMessage() handled=%v error=%v", handled, handleErr)
		}
	}
	select {
	case got := <-received:
		if got.ID != event.ID || got.Sender.String() != "aabb" || string(got.Payload) != string(event.Payload) || got.ReceivedAt != now {
			t.Fatalf("received event=%+v sender=%s", got, got.Sender)
		}
	case <-time.After(time.Second):
		t.Fatal("event was not dispatched")
	}
	select {
	case duplicate := <-received:
		t.Fatalf("duplicate event dispatched: %+v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestExpiredEventAndNonEventAreNotDispatched(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	bus := newTestBus(t, &recordingSender{}, nil, &now, nil)
	called := make(chan struct{}, 1)
	if _, err := bus.Subscribe("task.completed", func(context.Context, ReceivedEvent) error {
		called <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	event := Event{ID: EventID{1}, Topic: "task.completed", PublishedAt: now.Add(-time.Minute), TTL: time.Second, Payload: []byte("old")}
	wire, err := encodeEvent(event, defaultMaxEventPayload, defaultMaxEventTTL)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := NewReceivedMessage([]byte{1}, wire)
	if handled, err := bus.HandleMessage(context.Background(), message); !handled || err != nil {
		t.Fatalf("expired event handled=%v error=%v", handled, err)
	}
	direct, _ := NewReceivedMessage([]byte{1}, []byte("ordinary direct message"))
	if handled, err := bus.HandleMessage(context.Background(), direct); handled || err != nil {
		t.Fatalf("direct message handled=%v error=%v", handled, err)
	}
	select {
	case <-called:
		t.Fatal("expired event was dispatched")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSubscriptionQueueAppliesBackpressure(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	bus := newTestBus(t, &recordingSender{}, nil, &now, func(config *BusConfig) { config.QueueCapacity = 1 })
	started := make(chan struct{})
	release := make(chan struct{})
	if _, err := bus.Subscribe("busy.topic", func(context.Context, ReceivedEvent) error {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	deliver := func(id byte) error {
		event := Event{ID: EventID{id}, Topic: "busy.topic", PublishedAt: now, TTL: time.Minute, Payload: []byte{id}}
		wire, err := encodeEvent(event, defaultMaxEventPayload, defaultMaxEventTTL)
		if err != nil {
			return err
		}
		message, err := NewReceivedMessage([]byte{1}, wire)
		if err != nil {
			return err
		}
		_, err = bus.HandleMessage(context.Background(), message)
		return err
	}
	if err := deliver(1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if err := deliver(2); err != nil {
		t.Fatal(err)
	}
	if err := deliver(3); !errors.Is(err, ErrEventBackpressure) {
		t.Fatalf("third delivery error=%v, want backpressure", err)
	}
	close(release)
}

func TestBusBoundsSubscriptionsFanoutAndWireInput(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	sender := &recordingSender{}
	bus := newTestBus(t, sender, []string{"a", "b"}, &now, func(config *BusConfig) {
		config.MaxSubscriptions = 1
		config.MaxFanoutPeers = 1
		config.FanoutConcurrency = 1
		config.idSource = bytes.NewReader(bytes.Repeat([]byte{1}, 32))
	})
	if _, err := bus.Subscribe("one.topic", func(context.Context, ReceivedEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Subscribe("two.topic", func(context.Context, ReceivedEvent) error { return nil }); !errors.Is(err, ErrSubscriptionLimit) {
		t.Fatalf("subscription error=%v", err)
	}
	if _, err := bus.Publish(context.Background(), "one.topic", []byte("payload"), PublishOptions{}); !errors.Is(err, ErrFanoutLimit) {
		t.Fatalf("publish error=%v", err)
	}
	if _, err := bus.Publish(context.Background(), "bad topic", []byte("payload"), PublishOptions{}); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("topic error=%v", err)
	}
	invalid, _ := NewReceivedMessage([]byte{1}, append(eventMagic[:], []byte("truncated")...))
	if handled, err := bus.HandleMessage(context.Background(), invalid); !handled || !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("invalid wire handled=%v error=%v", handled, err)
	}
}

func TestDedupCacheRemainsBounded(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	bus := newTestBus(t, &recordingSender{}, nil, &now, func(config *BusConfig) { config.DedupCapacity = 1 })
	for _, id := range []byte{1, 2} {
		event := Event{ID: EventID{id}, Topic: "bounded.topic", PublishedAt: now, TTL: time.Minute, Payload: []byte{id}}
		wire, err := encodeEvent(event, defaultMaxEventPayload, defaultMaxEventTTL)
		if err != nil {
			t.Fatal(err)
		}
		message, _ := NewReceivedMessage([]byte{1}, wire)
		if handled, handleErr := bus.HandleMessage(context.Background(), message); !handled || handleErr != nil {
			t.Fatalf("event %d handled=%v error=%v", id, handled, handleErr)
		}
	}
	bus.dedupMu.Lock()
	defer bus.dedupMu.Unlock()
	if len(bus.seen) != 1 {
		t.Fatalf("dedup entries=%d, want 1", len(bus.seen))
	}
}

func TestHandlerFallsBackForOrdinaryDirectMessages(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	bus := newTestBus(t, &recordingSender{}, nil, &now, nil)
	called := false
	handler := bus.Handler(func(_ context.Context, message ReceivedMessage) error {
		called = string(message.Payload()) == "direct"
		return nil
	})
	message, _ := NewReceivedMessage([]byte{1}, []byte("direct"))
	if err := handler(context.Background(), message); err != nil || !called {
		t.Fatalf("fallback called=%v error=%v", called, err)
	}
}

func TestSubscriptionMayCloseItselfFromHandler(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	bus := newTestBus(t, &recordingSender{}, nil, &now, nil)
	var subscription *Subscription
	handled := make(chan struct{})
	var err error
	subscription, err = bus.Subscribe("self.close", func(context.Context, ReceivedEvent) error {
		_ = subscription.Close()
		close(handled)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	event := Event{ID: EventID{1}, Topic: "self.close", PublishedAt: now, TTL: time.Minute, Payload: []byte("payload")}
	wire, _ := encodeEvent(event, defaultMaxEventPayload, defaultMaxEventTTL)
	message, _ := NewReceivedMessage([]byte{1}, wire)
	if _, err := bus.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("self-closing handler deadlocked")
	}
	select {
	case <-subscription.Done():
	case <-time.After(time.Second):
		t.Fatal("subscription worker did not stop")
	}
}
