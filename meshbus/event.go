package meshbus

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const MaxTopicBytes = 128

var (
	ErrInvalidEvent      = errors.New("invalid event")
	ErrInvalidTopic      = errors.New("invalid event topic")
	ErrEventBackpressure = errors.New("event subscription queue is full")
	ErrSubscriptionLimit = errors.New("event subscription limit reached")
	ErrFanoutLimit       = errors.New("event fan-out peer limit exceeded")
	ErrBusClosed         = errors.New("event bus is closed")
)

// EventID is a publisher-generated 128-bit identifier used for bounded
// duplicate suppression. It is not an authority token.
type EventID [16]byte

func (id EventID) String() string { return hex.EncodeToString(id[:]) }

func (id EventID) isZero() bool { return id == EventID{} }

// Event is the transport-neutral event data. PublishedAt and other serialized
// fields are metadata; ReceivedEvent.Sender is the authoritative peer.
type Event struct {
	ID          EventID
	Topic       string
	PublishedAt time.Time
	TTL         time.Duration
	ContentType string
	Payload     []byte
}

// ReceivedEvent pairs an event with transport-authenticated delivery metadata.
type ReceivedEvent struct {
	Event
	Sender     PeerID
	ReceivedAt time.Time
}

// EventHandler handles one event. Duplicate delivery remains possible across
// process restarts and must be safe at the application boundary.
type EventHandler func(context.Context, ReceivedEvent) error

// PublishOptions controls bounded event lifetime and payload interpretation.
type PublishOptions struct {
	TTL         time.Duration
	ContentType string
	// RemoteOnly suppresses delivery to local subscriptions when publishing
	// through Node. Direct Bus users have no local identity and remain remote-only.
	RemoteOnly bool

	localSender PeerID
}

// PublishResult reports best-effort delivery without claiming remote
// acknowledgement or exactly-once semantics.
type PublishResult struct {
	ID             EventID
	Attempted      int
	Delivered      int
	Failed         map[PeerID]error
	LocalDelivered bool
	LocalError     error
}

func validateTopic(topic string) error {
	if len(topic) == 0 || len(topic) > MaxTopicBytes || strings.HasPrefix(topic, ".") ||
		strings.HasSuffix(topic, ".") || strings.Contains(topic, "..") {
		return fmt.Errorf("%w: expected 1-%d bytes in dot-separated segments", ErrInvalidTopic, MaxTopicBytes)
	}
	for _, character := range []byte(topic) {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' {
			continue
		}
		return fmt.Errorf("%w: unsupported byte %q", ErrInvalidTopic, character)
	}
	return nil
}

func cloneEvent(event Event) Event {
	event.Payload = bytes.Clone(event.Payload)
	return event
}

func cloneReceivedEvent(event ReceivedEvent) ReceivedEvent {
	event.Event = cloneEvent(event.Event)
	return event
}
