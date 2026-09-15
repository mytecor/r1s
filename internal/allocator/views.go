package allocator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SweepExpired releases capacity held by offers whose TTL elapsed.
func (a *Allocator) SweepExpired() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	expired := a.expireOffersLocked(a.now().UTC())
	if expired > 0 {
		_ = a.persistLocked(context.Background())
	}
	return expired
}

// Available reports currently unreserved slots for a resource class.
func (a *Allocator) Available(class string) uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(a.now().UTC())
	return a.capacity.available(class)
}

// Offer returns a cloned offer snapshot.
func (a *Allocator) Offer(id string) (OfferSnapshot, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(a.now().UTC())
	record, ok := a.offers[id]
	if !ok {
		return OfferSnapshot{}, false
	}
	return OfferSnapshot{
		Offer:       proto.Clone(record.offer).(*r1sv1.ExecutionOffer),
		Client:      bytes.Clone(record.client),
		Outstanding: record.status == offerOutstanding,
		Assigned:    record.status == offerAssigned,
		Expired:     record.status == offerExpired,
		Released:    record.status == offerReleased,
		ExecutionID: record.execution,
	}, true
}

// Execution returns a cloned execution snapshot.
func (a *Allocator) Execution(id string) (ExecutionSnapshot, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.executions[id]
	if !ok {
		return ExecutionSnapshot{}, false
	}
	return ExecutionSnapshot{
		ExecutionID:   record.id,
		OfferID:       record.offerID,
		Client:        bytes.Clone(record.client),
		ResourceClass: record.resourceClass,
		Request:       proto.Clone(record.request).(*r1sv1.ExecutionRequest),
		State:         stateFromRecord(record),
	}, true
}

func (a *Allocator) expireOffersLocked(now time.Time) int {
	expired := 0
	for _, offer := range a.offers {
		if offer.status == offerOutstanding && !offer.offer.GetExpiresAt().AsTime().After(now) {
			offer.status = offerExpired
			a.capacity.release(offer.offer.GetResourceClass())
			expired++
		}
	}
	return expired
}

func (a *Allocator) offerEnvelopeLocked(offer *r1sv1.ExecutionOffer, correlationID string, now time.Time) (*r1sv1.Envelope, error) {
	messageID := a.newID()
	if messageID == "" {
		return nil, fmt.Errorf("%w: empty message ID", ErrInvalidConfig)
	}
	return &r1sv1.Envelope{
		MessageId:     messageID,
		Sender:        bytes.Clone(a.identity),
		CorrelationId: correlationID,
		SentAt:        timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionOffer{
			ExecutionOffer: proto.Clone(offer).(*r1sv1.ExecutionOffer),
		},
	}, nil
}

func (a *Allocator) stateEnvelopeLocked(record *executionRecord, correlationID string, now time.Time) (*r1sv1.Envelope, error) {
	if record == nil {
		return nil, fmt.Errorf("%w: missing execution record", ErrExecutionNotFound)
	}
	messageID := a.newID()
	if messageID == "" {
		return nil, fmt.Errorf("%w: empty message ID", ErrInvalidConfig)
	}
	return &r1sv1.Envelope{
		MessageId:     messageID,
		Sender:        bytes.Clone(a.identity),
		CorrelationId: correlationID,
		SentAt:        timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionState{
			ExecutionState: stateFromRecord(record),
		},
	}, nil
}

func stateFromRecord(record *executionRecord) *r1sv1.ExecutionState {
	return &r1sv1.ExecutionState{
		ExecutionId: record.id,
		Phase:       record.phase,
		OccurredAt:  timestamppb.New(record.occurredAt),
		Detail:      record.detail,
		ExitCode:    cloneInt32(record.exitCode),
		Revision:    record.revision,
	}
}

func terminal(phase r1sv1.ExecutionPhase) bool {
	return phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED ||
		phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED ||
		phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
}

func authorityKey(identity []byte, id string) string {
	return string(identity) + "\x00" + id
}

func cloneInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(fmt.Sprintf("generate random ID: %v", err))
	}
	return hex.EncodeToString(value[:])
}
