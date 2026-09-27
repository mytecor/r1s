package meshbus

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	defaultEventTTL          = time.Minute
	defaultMaxEventTTL       = time.Hour
	defaultMaxEventPayload   = 64 * 1024
	defaultDedupCapacity     = 4096
	defaultQueueCapacity     = 32
	defaultMaxSubscriptions  = 128
	defaultMaxFanoutPeers    = 256
	defaultFanoutConcurrency = 8
)

// PeerSource returns a snapshot of authenticated, transport-specific peer
// destinations. Publish never forwards an event beyond this one-hop snapshot.
type PeerSource interface {
	Peers() []string
}

// PeerSourceFunc adapts a function to PeerSource.
type PeerSourceFunc func() []string

func (f PeerSourceFunc) Peers() []string { return f() }

// BusConfig sets finite resource bounds for one in-memory event bus.
type BusConfig struct {
	Sender            Sender
	Peers             PeerSource
	DefaultTTL        time.Duration
	MaxTTL            time.Duration
	MaxPayloadBytes   int
	DedupCapacity     int
	QueueCapacity     int
	MaxSubscriptions  int
	MaxFanoutPeers    int
	FanoutConcurrency int
	OnHandlerError    func(error)

	clock    func() time.Time
	idSource io.Reader
}

// Bus distributes best-effort events over authenticated direct messages.
// It owns no durable log, replay cursor, or consumer group state.
type Bus struct {
	sender Sender
	peers  PeerSource
	config BusConfig

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.RWMutex
	closed bool
	nextID uint64
	subs   map[uint64]*Subscription

	dedupMu sync.Mutex
	seen    map[EventID]time.Time
}

// Subscription is one exact-topic handler with a bounded private queue.
type Subscription struct {
	bus     *Bus
	id      uint64
	topic   string
	handler EventHandler
	queue   chan ReceivedEvent
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

// NewBus creates a stopped-empty but immediately usable event bus.
func NewBus(config BusConfig) (*Bus, error) {
	if config.Sender == nil || config.Peers == nil {
		return nil, fmt.Errorf("%w: sender and peer source are required", ErrInvalidEvent)
	}
	if config.DefaultTTL == 0 {
		config.DefaultTTL = defaultEventTTL
	}
	if config.MaxTTL == 0 {
		config.MaxTTL = defaultMaxEventTTL
	}
	if config.MaxPayloadBytes == 0 {
		config.MaxPayloadBytes = defaultMaxEventPayload
	}
	if config.DedupCapacity == 0 {
		config.DedupCapacity = defaultDedupCapacity
	}
	if config.QueueCapacity == 0 {
		config.QueueCapacity = defaultQueueCapacity
	}
	if config.MaxSubscriptions == 0 {
		config.MaxSubscriptions = defaultMaxSubscriptions
	}
	if config.MaxFanoutPeers == 0 {
		config.MaxFanoutPeers = defaultMaxFanoutPeers
	}
	if config.FanoutConcurrency == 0 {
		config.FanoutConcurrency = defaultFanoutConcurrency
	}
	if config.DefaultTTL < time.Millisecond || config.MaxTTL < config.DefaultTTL ||
		config.MaxTTL > time.Duration(^uint32(0))*time.Millisecond || config.MaxPayloadBytes < 1 ||
		uint64(config.MaxPayloadBytes) > uint64(^uint32(0)) ||
		config.DedupCapacity < 1 || config.QueueCapacity < 1 || config.MaxSubscriptions < 1 ||
		config.MaxFanoutPeers < 1 || config.FanoutConcurrency < 1 || config.FanoutConcurrency > config.MaxFanoutPeers {
		return nil, fmt.Errorf("%w: invalid event bus bounds", ErrInvalidEvent)
	}
	if config.clock == nil {
		config.clock = time.Now
	}
	if config.idSource == nil {
		config.idSource = rand.Reader
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Bus{
		sender: config.Sender, peers: config.Peers, config: config,
		ctx: ctx, cancel: cancel, subs: make(map[uint64]*Subscription), seen: make(map[EventID]time.Time),
	}, nil
}

// Subscribe registers one exact topic. Each subscription has one worker and a
// bounded queue, so handler concurrency and memory remain finite.
func (b *Bus) Subscribe(topic string, handler EventHandler) (*Subscription, error) {
	if err := validateTopic(topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrInvalidEvent)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBusClosed
	}
	if len(b.subs) >= b.config.MaxSubscriptions {
		return nil, ErrSubscriptionLimit
	}
	b.nextID++
	ctx, cancel := context.WithCancel(b.ctx)
	subscription := &Subscription{
		bus: b, id: b.nextID, topic: topic, handler: handler,
		queue: make(chan ReceivedEvent, b.config.QueueCapacity), ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	subscription.wg.Add(1)
	b.subs[subscription.id] = subscription
	go subscription.run()
	return subscription, nil
}

// Publish creates one event and fans it out once to the current unique peer
// snapshot. All peers are attempted; partial failures are joined in the result.
func (b *Bus) Publish(ctx context.Context, topic string, payload []byte, options PublishOptions) (PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return PublishResult{}, err
	}
	if err := validateTopic(topic); err != nil {
		return PublishResult{}, err
	}
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return PublishResult{}, ErrBusClosed
	}
	if options.TTL == 0 {
		options.TTL = b.config.DefaultTTL
	}
	var id EventID
	if _, err := io.ReadFull(b.config.idSource, id[:]); err != nil {
		return PublishResult{}, fmt.Errorf("generate event ID: %w", err)
	}
	event := Event{
		ID: id, Topic: topic, PublishedAt: b.config.clock().UTC(), TTL: options.TTL,
		ContentType: options.ContentType, Payload: payload,
	}
	wire, err := encodeEvent(event, b.config.MaxPayloadBytes, b.config.MaxTTL)
	if err != nil {
		return PublishResult{}, err
	}
	result := PublishResult{ID: id, Failed: make(map[string]error)}
	var failures []error
	if !options.localSender.IsZero() {
		receivedAt := b.config.clock().UTC()
		b.duplicate(id, event.PublishedAt.Add(event.TTL), receivedAt)
		local := ReceivedEvent{Event: cloneEvent(event), Sender: options.localSender, ReceivedAt: receivedAt}
		if localErr := b.dispatch(local); localErr != nil {
			result.Failed["local"] = localErr
			failures = append(failures, localErr)
		} else {
			result.LocalDelivered = true
		}
	}
	destinations, err := b.destinations()
	if err != nil {
		return result, err
	}
	result.Attempted = len(destinations)
	if len(destinations) == 0 {
		return result, errors.Join(failures...)
	}

	jobs := make(chan string, len(destinations))
	type sendResult struct {
		destination string
		err         error
	}
	results := make(chan sendResult, len(destinations))
	for _, destination := range destinations {
		jobs <- destination
	}
	close(jobs)
	workers := min(b.config.FanoutConcurrency, len(destinations))
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for destination := range jobs {
				if sendErr := b.sender.SendMessage(ctx, destination, bytes.Clone(wire)); sendErr != nil {
					results <- sendResult{destination: destination, err: fmt.Errorf("publish event to %q: %w", destination, sendErr)}
				} else {
					results <- sendResult{destination: destination}
				}
			}
		}()
	}
	wg.Wait()
	close(results)
	for delivery := range results {
		if delivery.err != nil {
			result.Failed[delivery.destination] = delivery.err
			failures = append(failures, delivery.err)
		} else {
			result.Delivered++
		}
	}
	return result, errors.Join(failures...)
}

// HandleMessage consumes meshbus event frames and leaves other direct messages
// to the caller. A true result means the frame belonged to pub/sub even when it
// was expired, duplicated, invalid, or backpressured.
func (b *Bus) HandleMessage(_ context.Context, message ReceivedMessage) (bool, error) {
	wire := message.Payload()
	if !isEventMessage(wire) {
		return false, nil
	}
	event, err := decodeEvent(wire, b.config.MaxPayloadBytes, b.config.MaxTTL)
	if err != nil {
		return true, err
	}
	now := b.config.clock().UTC()
	expiresAt := event.PublishedAt.Add(event.TTL)
	if !expiresAt.After(now) {
		return true, nil
	}
	receiveBound := now.Add(event.TTL)
	if receiveBound.Before(expiresAt) {
		expiresAt = receiveBound
	}
	if b.duplicate(event.ID, expiresAt, now) {
		return true, nil
	}
	received := ReceivedEvent{Event: event, Sender: message.Sender(), ReceivedAt: now}
	return true, b.dispatch(received)
}

// Handler composes pub/sub with a fallback direct-message handler.
func (b *Bus) Handler(next Handler) Handler {
	return func(ctx context.Context, message ReceivedMessage) error {
		handled, err := b.HandleMessage(ctx, message)
		if handled || err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		return next(ctx, message)
	}
}

func (b *Bus) destinations() ([]string, error) {
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, destination := range b.peers.Peers() {
		destination = strings.TrimSpace(destination)
		if destination == "" {
			continue
		}
		if _, exists := seen[destination]; exists {
			continue
		}
		seen[destination] = struct{}{}
		result = append(result, destination)
		if len(result) > b.config.MaxFanoutPeers {
			return nil, ErrFanoutLimit
		}
	}
	return result, nil
}

func (b *Bus) duplicate(id EventID, expiresAt, now time.Time) bool {
	b.dedupMu.Lock()
	defer b.dedupMu.Unlock()
	for candidate, expiry := range b.seen {
		if !expiry.After(now) {
			delete(b.seen, candidate)
		}
	}
	if _, exists := b.seen[id]; exists {
		return true
	}
	if len(b.seen) >= b.config.DedupCapacity {
		var oldest EventID
		var oldestExpiry time.Time
		for candidate, expiry := range b.seen {
			if oldestExpiry.IsZero() || expiry.Before(oldestExpiry) {
				oldest, oldestExpiry = candidate, expiry
			}
		}
		delete(b.seen, oldest)
	}
	b.seen[id] = expiresAt
	return false
}

func (b *Bus) dispatch(event ReceivedEvent) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrBusClosed
	}
	var failures []error
	for _, subscription := range b.subs {
		if subscription.topic != event.Topic {
			continue
		}
		select {
		case subscription.queue <- cloneReceivedEvent(event):
		default:
			failures = append(failures, fmt.Errorf("%w: topic %q", ErrEventBackpressure, event.Topic))
		}
	}
	return errors.Join(failures...)
}

// Close stops subscriptions and rejects future publication or registration.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.cancel()
	subscriptions := make([]*Subscription, 0, len(b.subs))
	for _, subscription := range b.subs {
		subscriptions = append(subscriptions, subscription)
		subscription.cancel()
	}
	b.subs = make(map[uint64]*Subscription)
	b.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.wg.Wait()
	}
	return nil
}

func (s *Subscription) run() {
	defer func() {
		s.wg.Done()
		close(s.done)
	}()
	for {
		select {
		case <-s.ctx.Done():
			return
		case event := <-s.queue:
			if err := s.handler(s.ctx, event); err != nil && s.bus.config.OnHandlerError != nil {
				s.bus.config.OnHandlerError(err)
			}
		}
	}
}

// Close removes the subscription and cancels its handler context.
func (s *Subscription) Close() error {
	s.once.Do(func() {
		s.bus.mu.Lock()
		delete(s.bus.subs, s.id)
		s.cancel()
		s.bus.mu.Unlock()
	})
	return nil
}

// Done closes after the subscription worker stops.
func (s *Subscription) Done() <-chan struct{} { return s.done }
