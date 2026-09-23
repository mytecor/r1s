package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

// loadLocked restores the durable client snapshot. The persisted schema itself
// lives in state_schema.go; this file maps it onto the live Client state.
func (o *Client) loadLocked(ctx context.Context) error {
	if o.store == nil {
		return nil
	}
	data, err := o.store.Load(ctx)
	if err != nil {
		return errors.Join(ErrStore, err)
	}
	if len(data) == 0 {
		return nil
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("%w: decode client state: %v", ErrStore, err)
	}
	if state.Version != stateVersion {
		return fmt.Errorf("%w: unsupported client state version %d", ErrStore, state.Version)
	}
	if !bytesEqual(state.Identity, o.identity) {
		return fmt.Errorf("%w: state belongs to a different client identity", ErrStore)
	}
	o.watchSequence = state.WatchSeq
	o.loadWatchJournalLocked(state.WatchJournal)
	for _, saved := range state.Allocators {
		if len(saved.Identity) == 0 || saved.Destination == "" {
			return fmt.Errorf("%w: invalid durable allocator", ErrStore)
		}
		candidate := Allocator{Identity: saved.Identity, Destination: saved.Destination, Hops: saved.Hops, Capacity: saved.Capacity}
		candidate.TunnelHost = saved.TunnelHost
		candidate.TunnelPort = saved.TunnelPort
		candidate.TunnelDestination = saved.TunnelDestination
		if len(saved.Node) > 0 {
			candidate.Node = new(r1sv1.NodeCapabilities)
			if err := proto.Unmarshal(saved.Node, candidate.Node); err != nil {
				return fmt.Errorf("%w: decode allocator node: %v", ErrStore, err)
			}
		}
		o.allocators.put(candidate)
	}
	for _, saved := range state.Requests {
		request := new(r1sv1.ExecutionRequest)
		if err := proto.Unmarshal(saved.Request, request); err != nil {
			return fmt.Errorf("%w: decode request: %v", ErrStore, err)
		}
		if request.GetRequestId() == "" || saved.MessageID == "" || saved.CreatedAt.IsZero() || o.requests[request.GetRequestId()] != nil {
			return fmt.Errorf("%w: invalid durable request", ErrStore)
		}
		record := &requestRecord{request: request, messageID: saved.MessageID, createdAt: saved.CreatedAt, executionID: saved.ExecutionID, offers: make(map[string]*offerRecord)}
		for _, savedOffer := range saved.Offers {
			offer := new(r1sv1.ExecutionOffer)
			if err := proto.Unmarshal(savedOffer.Offer, offer); err != nil {
				return fmt.Errorf("%w: decode offer: %v", ErrStore, err)
			}
			if offer.GetOfferId() == "" || offer.GetRequestId() != request.GetRequestId() || record.offers[offer.GetOfferId()] != nil {
				return fmt.Errorf("%w: invalid durable offer", ErrStore)
			}
			if intent := savedOffer.Release; intent != nil && (intent.MessageID == "" || intent.SentAt.IsZero() || intent.Destination == "" || saved.ExecutionID == "") {
				return fmt.Errorf("%w: invalid durable offer release", ErrStore)
			}
			record.offers[offer.GetOfferId()] = &offerRecord{offer: offer, allocatorID: append([]byte(nil), savedOffer.AllocatorID...), receivedAt: savedOffer.ReceivedAt, release: savedOffer.Release}
		}
		o.requests[request.GetRequestId()] = record
	}
	for _, saved := range state.Executions {
		if saved.ID == "" || saved.RequestID == "" || saved.OfferID == "" || saved.AssignmentMessageID == "" || saved.AssignmentSentAt.IsZero() || len(saved.AllocatorID) == 0 || saved.Destination == "" || o.executions[saved.ID] != nil {
			return fmt.Errorf("%w: invalid durable execution", ErrStore)
		}
		request := o.requests[saved.RequestID]
		if request == nil || request.executionID != saved.ID {
			return fmt.Errorf("%w: execution has no matching request", ErrStore)
		}
		if offer := request.offers[saved.OfferID]; offer != nil && offer.release != nil && bytesEqual(offer.allocatorID, saved.AllocatorID) {
			return fmt.Errorf("%w: selected offer has release intent", ErrStore)
		}
		record := &executionRecord{
			id: saved.ID, requestID: saved.RequestID, offerID: saved.OfferID, allocatorID: append([]byte(nil), saved.AllocatorID...),
			destination: saved.Destination, assignmentMessageID: saved.AssignmentMessageID, assignmentSentAt: saved.AssignmentSentAt,
			inspectMessageID: saved.InspectMessageID, inspectSentAt: saved.InspectSentAt,
			cancelMessageID: saved.CancelMessageID, cancelSentAt: saved.CancelSentAt, cancelReason: saved.CancelReason,
			leaseDuration: time.Duration(saved.LeaseDurationNanos), leaseAllocators: saved.LeaseAllocators, leaseLost: saved.LeaseLost,
			leaseRenewedAt: saved.LeaseRenewedAt, leaseRenewMessageID: saved.LeaseRenewMessageID, leaseExpiresAt: saved.LeaseExpiresAt,
		}
		if len(saved.State) > 0 {
			record.state = new(r1sv1.ExecutionState)
			if err := proto.Unmarshal(saved.State, record.state); err != nil || record.state.GetExecutionId() != saved.ID {
				return fmt.Errorf("%w: invalid durable execution state", ErrStore)
			}
		}
		o.executions[saved.ID] = record
	}
	return nil
}

func (o *Client) persistLocked(ctx context.Context) error {
	if o.store == nil {
		return nil
	}
	state := persistedState{Version: stateVersion, Identity: append([]byte(nil), o.identity...), WatchSeq: o.watchSequence}
	for _, allocator := range o.allocators.all() {
		saved := persistedAllocator{Identity: append([]byte(nil), allocator.Identity...), Destination: allocator.Destination, Hops: allocator.Hops, Capacity: allocator.Capacity}
		saved.TunnelHost = allocator.TunnelHost
		saved.TunnelPort = allocator.TunnelPort
		saved.TunnelDestination = allocator.TunnelDestination
		if allocator.Node != nil {
			nodeData, err := proto.Marshal(allocator.Node)
			if err != nil {
				return errors.Join(ErrStore, err)
			}
			saved.Node = nodeData
		}
		state.Allocators = append(state.Allocators, saved)
	}
	for _, record := range o.requests {
		requestData, err := proto.Marshal(record.request)
		if err != nil {
			return errors.Join(ErrStore, err)
		}
		saved := persistedRequest{Request: requestData, MessageID: record.messageID, CreatedAt: record.createdAt, ExecutionID: record.executionID}
		for _, offer := range record.offers {
			offerData, err := proto.Marshal(offer.offer)
			if err != nil {
				return errors.Join(ErrStore, err)
			}
			saved.Offers = append(saved.Offers, persistedOffer{Offer: offerData, AllocatorID: append([]byte(nil), offer.allocatorID...), ReceivedAt: offer.receivedAt, Release: offer.release})
		}
		state.Requests = append(state.Requests, saved)
	}
	for _, record := range o.executions {
		saved := persistedExecution{
			ID: record.id, RequestID: record.requestID, OfferID: record.offerID, AllocatorID: append([]byte(nil), record.allocatorID...),
			Destination: record.destination, AssignmentMessageID: record.assignmentMessageID, AssignmentSentAt: record.assignmentSentAt,
			InspectMessageID: record.inspectMessageID, InspectSentAt: record.inspectSentAt,
			CancelMessageID: record.cancelMessageID, CancelSentAt: record.cancelSentAt, CancelReason: record.cancelReason,
			LeaseDurationNanos: int64(record.leaseDuration), LeaseAllocators: record.leaseAllocators, LeaseLost: record.leaseLost,
			LeaseRenewedAt: record.leaseRenewedAt, LeaseRenewMessageID: record.leaseRenewMessageID, LeaseExpiresAt: record.leaseExpiresAt,
		}
		if record.state != nil {
			encoded, err := proto.Marshal(record.state)
			if err != nil {
				return errors.Join(ErrStore, err)
			}
			saved.State = encoded
		}
		state.Executions = append(state.Executions, saved)
	}
	state.WatchJournal = o.watchJournalLocked()
	data, err := json.Marshal(state)
	if err != nil {
		return errors.Join(ErrStore, err)
	}
	if err := o.store.Save(ctx, data); err != nil {
		return errors.Join(ErrStore, err)
	}
	return nil
}

// loadWatchJournalLocked restores the retained journal after a restart. Sequences
// are authoritative and monotonic; watchers resuming from a durable position
// observe the same revisions in the same order.
func (o *Client) loadWatchJournalLocked(events []persistedWatchEvent) {
	if len(events) == 0 {
		return
	}
	firstSeq := events[0].Sequence
	o.watchJournal = watchJournal{firstSeq: firstSeq, events: make([]watchEvent, 0, len(events))}
	eventSeq := firstSeq
	for _, saved := range events {
		decoded := new(r1sv1.ExecutionState)
		if err := proto.Unmarshal(saved.State, decoded); err != nil {
			continue
		}
		o.watchJournal.events = append(o.watchJournal.events, watchEvent{seq: eventSeq, executionID: saved.ExecutionID, state: decoded})
		eventSeq++
	}
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
