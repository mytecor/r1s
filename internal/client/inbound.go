package client

import (
	"bytes"
	"context"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// Handle accepts observations from an authenticated allocator and routes each
// wire payload to the durable state transition that owns it.
func (o *Client) Handle(_ context.Context, envelope *r1sv1.Envelope) error {
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	switch payload := envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionLogsResponse:
		return o.handleLogsLocked(envelope)
	case *r1sv1.Envelope_CommandError:
		return o.handleErrorLocked(envelope)
	case *r1sv1.Envelope_ExecutionOffer:
		return o.handleOfferLocked(envelope, payload.ExecutionOffer)
	case *r1sv1.Envelope_ExecutionState:
		return o.handleStateLocked(envelope, payload.ExecutionState)
	case *r1sv1.Envelope_ExecutionOfferReleaseAck:
		return o.handleReleaseAckLocked(envelope, payload.ExecutionOfferReleaseAck)
	case *r1sv1.Envelope_ExecutionLeaseRenewAck:
		return o.handleRenewAckLocked(envelope, payload.ExecutionLeaseRenewAck)
	default:
		return ErrUnsupportedMessage
	}
}

func (o *Client) handleOfferLocked(envelope *r1sv1.Envelope, offer *r1sv1.ExecutionOffer) error {
	record := o.requests[offer.GetRequestId()]
	if record == nil {
		return fmt.Errorf("%w: %q", ErrRequestNotFound, offer.GetRequestId())
	}
	if record.messageID != envelope.GetCorrelationId() || offer.GetResourceClass() != record.request.GetResourceClass() {
		return ErrConflict
	}
	if _, ok := o.allocators.lookup(envelope.GetSender()); !ok {
		return ErrUnauthorized
	}
	if existing := record.offers[offer.GetOfferId()]; existing != nil {
		if !bytes.Equal(existing.allocatorID, envelope.GetSender()) || !proto.Equal(existing.offer, offer) {
			return ErrConflict
		}
		return nil
	}
	newOffer := &offerRecord{offer: proto.Clone(offer).(*r1sv1.ExecutionOffer), allocatorID: bytes.Clone(envelope.GetSender()), receivedAt: o.now().UTC()}
	if record.executionID != "" {
		execution := o.executions[record.executionID]
		if execution.offerID == offer.GetOfferId() && bytes.Equal(execution.allocatorID, envelope.GetSender()) {
			return ErrConflict
		}
		intent, err := o.newReleaseLocked(newOffer)
		if err != nil {
			return err
		}
		newOffer.release = intent
	}
	record.offers[offer.GetOfferId()] = newOffer
	if err := o.persistLocked(context.Background()); err != nil {
		delete(record.offers, offer.GetOfferId())
		return err
	}
	return nil
}

func (o *Client) handleStateLocked(envelope *r1sv1.Envelope, state *r1sv1.ExecutionState) error {
	record := o.executions[state.GetExecutionId()]
	if record == nil {
		return fmt.Errorf("%w: %q", ErrExecutionNotFound, state.GetExecutionId())
	}
	if !bytes.Equal(record.allocatorID, envelope.GetSender()) {
		return ErrUnauthorized
	}
	if record.state != nil {
		oldRevision, newRevision := record.state.GetRevision(), state.GetRevision()
		if oldRevision > 0 || newRevision > 0 {
			if newRevision < oldRevision {
				return nil
			}
			if newRevision == oldRevision {
				if proto.Equal(record.state, state) {
					return nil
				}
				return ErrConflict
			}
		} else {
			oldTime, newTime := record.state.GetOccurredAt().AsTime(), state.GetOccurredAt().AsTime()
			if newTime.Before(oldTime) {
				return nil
			}
			if newTime.Equal(oldTime) {
				if proto.Equal(record.state, state) {
					return nil
				}
				return ErrConflict
			}
		}
		if terminal(record.state.GetPhase()) && !proto.Equal(record.state, state) {
			return ErrConflict
		}
	}
	previous := record.state
	savedSeq := o.watchSequence
	savedJournal := o.watchJournal
	record.state = proto.Clone(state).(*r1sv1.ExecutionState)
	// A terminal eviction for lease expiry converts the intent into a
	// re-request duty; ordinary completions and cancellations do not.
	if terminal(record.state.GetPhase()) && isLostLeaseState(record.state) {
		record.leaseLost = true
		record.leaseRenewMessageID = ""
	}
	event := o.watchAppendLocked(record.id, record.state)
	if err := o.persistLocked(context.Background()); err != nil {
		record.state = previous
		o.watchSequence = savedSeq
		o.watchJournal = savedJournal
		return err
	}
	o.watchNotifyLocked(event)
	return nil
}
