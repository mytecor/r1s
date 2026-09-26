package client

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type releaseIntent struct {
	MessageID    string    `json:"message_id"`
	SentAt       time.Time `json:"sent_at"`
	Destination  string    `json:"destination"`
	Acknowledged bool      `json:"acknowledged,omitempty"`
}

// PendingRelease is a stable in-memory command for one unselected allocator.
type PendingRelease struct {
	Destination string
	Envelope    *r1sv1.Envelope
}

func (o *Client) newReleaseLocked(offer *offerRecord) (*releaseIntent, error) {
	allocator, ok := o.allocators.lookup(offer.allocatorID)
	if !ok || allocator.Destination == "" {
		return nil, ErrInvalidAllocator
	}
	messageID := o.newID()
	if messageID == "" {
		return nil, fmt.Errorf("%w: empty release message ID", ErrInvalidConfig)
	}
	return &releaseIntent{MessageID: messageID, SentAt: o.now().UTC(), Destination: allocator.Destination}, nil
}

// PendingReleases returns independent envelopes until acknowledged or expired.
// Sending is best-effort; expiry is always the allocator's cleanup fallback.
func (o *Client) PendingReleases() []PendingRelease {
	o.mu.Lock()
	defer o.mu.Unlock()
	var pending []PendingRelease
	now := o.now().UTC()
	for _, request := range o.requests {
		for _, offer := range request.offers {
			intent := offer.release
			if intent == nil || intent.Acknowledged || !offer.offer.GetExpiresAt().AsTime().After(now) {
				continue
			}
			pending = append(pending, PendingRelease{Destination: intent.Destination, Envelope: &r1sv1.Envelope{
				MessageId: intent.MessageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(intent.SentAt),
				Payload: &r1sv1.Envelope_ExecutionOfferRelease{ExecutionOfferRelease: &r1sv1.ExecutionOfferRelease{
					RequestId: request.request.GetRequestId(), OfferId: offer.offer.GetOfferId(),
				}},
			}})
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Envelope.GetMessageId() < pending[j].Envelope.GetMessageId() })
	return pending
}

func (o *Client) handleReleaseAckLocked(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionOfferReleaseAck) error {
	request := o.requests[ack.GetRequestId()]
	if request == nil {
		return ErrRequestNotFound
	}
	offer := request.offers[ack.GetOfferId()]
	if offer == nil || offer.release == nil {
		return ErrConflict
	}
	if !bytes.Equal(offer.allocatorID, envelope.GetSender()) {
		return ErrUnauthorized
	}
	if offer.release.MessageID != envelope.GetCorrelationId() {
		return ErrConflict
	}
	if ack.GetOutcome() == r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_ASSIGNED {
		// The client never releases its selection; an assigned loser is inconsistent.
		return ErrConflict
	}
	if offer.release.Acknowledged {
		return nil
	}
	offer.release.Acknowledged = true
	return nil
}
