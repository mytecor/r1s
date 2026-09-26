package allocator

import (
	"bytes"
	"context"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (a *Allocator) handleInspect(envelope *r1sv1.Envelope, inspect *r1sv1.ExecutionInspect) ([]*r1sv1.Envelope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.executions[inspect.GetExecutionId()]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, inspect.GetExecutionId())
	}
	if !bytes.Equal(record.client, envelope.GetSender()) {
		return nil, ErrUnauthorized
	}
	response, err := a.stateEnvelopeLocked(record, envelope.GetMessageId(), a.now().UTC())
	if err != nil {
		return nil, err
	}
	return []*r1sv1.Envelope{response}, nil
}

func (a *Allocator) handleRequest(envelope *r1sv1.Envelope, request *r1sv1.ExecutionRequest) ([]*r1sv1.Envelope, error) {
	now := a.now().UTC()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(now)

	requestKey := authorityKey(envelope.GetSender(), request.GetRequestId())
	if offerID, ok := a.requests[requestKey]; ok {
		record := a.offers[offerID]
		if record != nil {
			if record.status == offerExpired {
				return nil, ErrOfferExpired
			}
			if !proto.Equal(record.request, request) {
				return nil, fmt.Errorf("%w: request ID %q was reused with different content", ErrExecutionConflict, request.GetRequestId())
			}
			if record.status == offerReleased {
				return nil, ErrOfferReleased
			}
			response, err := a.offerEnvelopeLocked(record.offer, envelope.GetMessageId(), now)
			if err != nil {
				return nil, err
			}
			return []*r1sv1.Envelope{response}, nil
		}
	}

	for _, dead := range a.tombstones {
		if bytes.Equal(dead.Client, envelope.GetSender()) && dead.RequestID == request.GetRequestId() {
			return nil, ErrResultExpired
		}
	}
	if len(a.offers)*2+len(a.tombstones)+2 > a.maxRecords {
		return nil, ErrCapacityExhausted
	}
	// Retention is allocator operator configuration (F22-07), never workload
	// input: a workload cannot choose bookkeeping retention, and every
	// configured horizon is bounded by CommandHorizon at construction.
	if err := a.admitLocked(envelope.GetSender(), false); err != nil {
		return nil, err
	}
	class := request.GetResourceClass()
	if a.capacity.available(class) == 0 {
		return nil, fmt.Errorf("%w: resource class %q", ErrCapacityExhausted, class)
	}
	// Placement is applied before any capacity is taken or an offer is issued:
	// an incompatible node neither reserves capacity nor returns an offer. The
	// resource class must also be one of the declared profiles when the node
	// advertises them (the class-indexed capacity ledger is the authority for
	// slots, but an advertised node that omits the class would mislead the
	// client about placement).
	if !protocol.PlacementMatches(request.GetConstraints(), a.node) {
		return nil, fmt.Errorf("%w: %s", ErrIncompatible, protocol.PlacementConflictDetails(request.GetConstraints(), a.node))
	}
	if a.node != nil && !protocol.Contains(a.node.GetResourceProfiles(), class) {
		return nil, fmt.Errorf("%w: resource class %q", ErrAdmission, class)
	}
	offerID := a.newID()
	if offerID == "" {
		return nil, fmt.Errorf("%w: empty offer ID", ErrInvalidConfig)
	}
	if _, exists := a.offers[offerID]; exists {
		return nil, fmt.Errorf("%w: offer %q", ErrIDCollision, offerID)
	}
	offer := &r1sv1.ExecutionOffer{
		OfferId:       offerID,
		RequestId:     request.GetRequestId(),
		ResourceClass: class,
		ExpiresAt:     timestamppb.New(now.Add(a.offerTTL)),
		Node:          proto.Clone(a.node).(*r1sv1.NodeCapabilities),
	}
	response, err := a.offerEnvelopeLocked(offer, envelope.GetMessageId(), now)
	if err != nil {
		return nil, err
	}
	a.offers[offerID] = &offerRecord{
		offer:     proto.Clone(offer).(*r1sv1.ExecutionOffer),
		request:   proto.Clone(request).(*r1sv1.ExecutionRequest),
		client:    bytes.Clone(envelope.GetSender()),
		status:    offerOutstanding,
		resources: a.admission.Profiles[class],
	}
	a.requests[requestKey] = offerID
	if err := a.capacity.reserve(class); err != nil {
		delete(a.requests, requestKey)
		delete(a.offers, offerID)
		return nil, err
	}
	if err := a.persistLocked(context.Background()); err != nil {
		delete(a.requests, requestKey)
		delete(a.offers, offerID)
		a.capacity.release(class)
		return nil, err
	}
	return []*r1sv1.Envelope{response}, nil
}
