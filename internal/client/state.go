package client

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

const stateVersion = 1

type persistedState struct {
	Version    int                  `json:"version"`
	Identity   []byte               `json:"identity"`
	Allocators []persistedAllocator `json:"allocators,omitempty"`
	Requests   []persistedRequest   `json:"requests,omitempty"`
	Executions []persistedExecution `json:"executions,omitempty"`
}

type persistedAllocator struct {
	Identity    []byte            `json:"identity"`
	Destination string            `json:"destination"`
	Hops        uint8             `json:"hops"`
	Capacity    map[string]uint32 `json:"capacity,omitempty"`
}

type persistedRequest struct {
	Request     []byte           `json:"request"`
	MessageID   string           `json:"message_id"`
	CreatedAt   time.Time        `json:"created_at"`
	Offers      []persistedOffer `json:"offers,omitempty"`
	ExecutionID string           `json:"execution_id,omitempty"`
}

type persistedOffer struct {
	Offer       []byte    `json:"offer"`
	AllocatorID []byte    `json:"allocator_id"`
	ReceivedAt  time.Time `json:"received_at"`
}

type persistedExecution struct {
	ID                  string    `json:"id"`
	RequestID           string    `json:"request_id"`
	OfferID             string    `json:"offer_id"`
	AllocatorID         []byte    `json:"allocator_id"`
	Destination         string    `json:"destination"`
	AssignmentMessageID string    `json:"assignment_message_id"`
	AssignmentSentAt    time.Time `json:"assignment_sent_at"`
	InspectMessageID    string    `json:"inspect_message_id,omitempty"`
	InspectSentAt       time.Time `json:"inspect_sent_at,omitempty"`
	CancelMessageID     string    `json:"cancel_message_id,omitempty"`
	CancelSentAt        time.Time `json:"cancel_sent_at,omitempty"`
	CancelReason        string    `json:"cancel_reason,omitempty"`
	State               []byte    `json:"state,omitempty"`
}

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
	for _, saved := range state.Allocators {
		if len(saved.Identity) == 0 || saved.Destination == "" {
			return fmt.Errorf("%w: invalid durable allocator", ErrStore)
		}
		candidate := Allocator{Identity: saved.Identity, Destination: saved.Destination, Hops: saved.Hops, Capacity: saved.Capacity}
		o.allocators[hex.EncodeToString(saved.Identity)] = cloneAllocator(candidate)
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
			record.offers[offer.GetOfferId()] = &offerRecord{offer: offer, allocatorID: append([]byte(nil), savedOffer.AllocatorID...), receivedAt: savedOffer.ReceivedAt}
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
		record := &executionRecord{
			id: saved.ID, requestID: saved.RequestID, offerID: saved.OfferID, allocatorID: append([]byte(nil), saved.AllocatorID...),
			destination: saved.Destination, assignmentMessageID: saved.AssignmentMessageID, assignmentSentAt: saved.AssignmentSentAt,
			inspectMessageID: saved.InspectMessageID, inspectSentAt: saved.InspectSentAt,
			cancelMessageID: saved.CancelMessageID, cancelSentAt: saved.CancelSentAt, cancelReason: saved.CancelReason,
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
	state := persistedState{Version: stateVersion, Identity: append([]byte(nil), o.identity...)}
	for _, allocator := range o.allocators {
		state.Allocators = append(state.Allocators, persistedAllocator{Identity: append([]byte(nil), allocator.Identity...), Destination: allocator.Destination, Hops: allocator.Hops, Capacity: allocator.Capacity})
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
			saved.Offers = append(saved.Offers, persistedOffer{Offer: offerData, AllocatorID: append([]byte(nil), offer.allocatorID...), ReceivedAt: offer.receivedAt})
		}
		state.Requests = append(state.Requests, saved)
	}
	for _, record := range o.executions {
		saved := persistedExecution{
			ID: record.id, RequestID: record.requestID, OfferID: record.offerID, AllocatorID: append([]byte(nil), record.allocatorID...),
			Destination: record.destination, AssignmentMessageID: record.assignmentMessageID, AssignmentSentAt: record.assignmentSentAt,
			InspectMessageID: record.inspectMessageID, InspectSentAt: record.inspectSentAt,
			CancelMessageID: record.cancelMessageID, CancelSentAt: record.cancelSentAt, CancelReason: record.cancelReason,
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
	data, err := json.Marshal(state)
	if err != nil {
		return errors.Join(ErrStore, err)
	}
	if err := o.store.Save(ctx, data); err != nil {
		return errors.Join(ErrStore, err)
	}
	return nil
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
