package allocator

import (
	"bytes"
	"context"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (a *Allocator) handleOfferRelease(envelope *r1sv1.Envelope, release *r1sv1.ExecutionOfferRelease) ([]*r1sv1.Envelope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record := a.offers[release.GetOfferId()]
	if record == nil {
		if dead, ok := a.tombstones[release.GetOfferId()]; ok {
			record = &offerRecord{client: dead.Client, status: offerExpired, offer: &r1sv1.ExecutionOffer{OfferId: dead.OfferID, RequestId: dead.RequestID}}
		}
		if record == nil {
			return nil, ErrOfferNotFound
		}
	}
	if !bytes.Equal(record.client, envelope.GetSender()) {
		return nil, ErrUnauthorized
	}
	if record.offer.GetRequestId() != release.GetRequestId() {
		return nil, ErrExecutionConflict
	}
	messageID := a.newID()
	if messageID == "" {
		return nil, fmt.Errorf("%w: empty message ID", ErrInvalidConfig)
	}
	now := a.now().UTC()
	previous := record.status
	outcome := r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_RELEASED
	switch record.status {
	case offerOutstanding:
		if !record.offer.GetExpiresAt().AsTime().After(now) {
			record.status = offerExpired
		} else {
			record.status = offerReleased
		}
		a.releaseClassLocked(record.offer.GetResourceClass())
	}
	switch record.status {
	case offerExpired:
		outcome = r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_EXPIRED
	case offerAssigned:
		outcome = r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_ASSIGNED
	}
	if previous != record.status {
		if err := a.persistLocked(context.Background()); err != nil {
			record.status = previous
			a.used[record.offer.GetResourceClass()]++
			return nil, err
		}
	}
	return []*r1sv1.Envelope{{
		MessageId: messageID, Sender: bytes.Clone(a.identity), CorrelationId: envelope.GetMessageId(), SentAt: timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionOfferReleaseAck{ExecutionOfferReleaseAck: &r1sv1.ExecutionOfferReleaseAck{
			RequestId: release.GetRequestId(), OfferId: release.GetOfferId(), Outcome: outcome,
		}},
	}}, nil
}
