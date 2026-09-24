package allocator

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func (a *Allocator) handleAssign(ctx context.Context, envelope *r1sv1.Envelope, assign *r1sv1.ExecutionAssign) ([]*r1sv1.Envelope, error) {
	now := a.now().UTC()
	a.mu.Lock()
	a.expireOffersLocked(now)
	offer, ok := a.offers[assign.GetOfferId()]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrOfferNotFound, assign.GetOfferId())
	}
	if !bytes.Equal(offer.client, envelope.GetSender()) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	if offer.offer.GetRequestId() != assign.GetRequestId() {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: assignment request does not match offer", ErrExecutionConflict)
	}
	switch offer.status {
	case offerReleased:
		a.mu.Unlock()
		return nil, ErrOfferReleased
	case offerExpired:
		a.mu.Unlock()
		return nil, ErrOfferExpired
	case offerAssigned:
		if offer.execution != assign.GetExecutionId() {
			a.mu.Unlock()
			return nil, ErrOfferAlreadyAssigned
		}
		response, err := a.stateEnvelopeLocked(a.executions[offer.execution], envelope.GetMessageId(), now)
		a.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return []*r1sv1.Envelope{response}, nil
	}
	if existing, exists := a.executions[assign.GetExecutionId()]; exists {
		a.mu.Unlock()
		if existing.offerID == offer.offer.GetOfferId() && bytes.Equal(existing.client, envelope.GetSender()) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrExecutionConflict, assign.GetExecutionId())
	}

	// Re-validate placement against current local capabilities at assignment.
	// The node may have changed since the offer was minted (advertisement
	// updates, policy edits, restart under a different configuration). An
	// incompatible assignment is an explicit rejection, never an invalid start:
	// the runtime must not attempt to run a workload its node no longer matches.
	if !protocol.PlacementMatches(offer.request.GetConstraints(), a.node) {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrIncompatible, protocol.PlacementConflictDetails(offer.request.GetConstraints(), a.node))
	}

	if err := a.admitLocked(envelope.GetSender(), true); err != nil {
		a.mu.Unlock()
		return nil, err
	}
	record := &executionRecord{
		id:            assign.GetExecutionId(),
		offerID:       offer.offer.GetOfferId(),
		client:        bytes.Clone(offer.client),
		resourceClass: offer.offer.GetResourceClass(),
		request:       proto.Clone(offer.request).(*r1sv1.ExecutionRequest),
		phase:         r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING,
		occurredAt:    now,
		revision:      1,
		resources:     offer.resources,
		startedAt:     now,
		leaseUntil:    now.Add(a.leaseTTL),
	}
	offer.status = offerAssigned
	offer.execution = record.id
	a.executions[record.id] = record
	if err := a.persistLocked(context.Background()); err != nil {
		delete(a.executions, record.id)
		offer.status = offerOutstanding
		offer.execution = ""
		a.mu.Unlock()
		return nil, err
	}
	startRequest := a.startRequestLocked(record)
	a.mu.Unlock()

	startErr := a.runtime.Start(ctx, startRequest, a.completionReporter(record.id))

	a.mu.Lock()
	current := a.executions[record.id]
	if startErr != nil && current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING {
		a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, startErr.Error(), nil, a.now().UTC())
	} else if startErr == nil && current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING {
		current.phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING
		current.occurredAt = a.now().UTC()
		current.revision++
	}
	response, responseErr := a.stateEnvelopeLocked(current, envelope.GetMessageId(), a.now().UTC())
	persistErr := a.persistLocked(context.Background())
	a.mu.Unlock()
	if responseErr != nil {
		return nil, responseErr
	}
	if persistErr != nil {
		return []*r1sv1.Envelope{response}, errors.Join(startErr, persistErr)
	}
	if startErr != nil {
		return []*r1sv1.Envelope{response}, errors.Join(ErrRuntimeStart, startErr)
	}
	return []*r1sv1.Envelope{response}, nil
}
