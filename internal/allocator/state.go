package allocator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// StateStore atomically loads and saves one opaque allocator snapshot.
// Implementations must not retain or mutate the supplied byte slices.
// The durable schema itself lives in state_schema.go.
type StateStore interface {
	Load(context.Context) ([]byte, error)
	Save(context.Context, []byte) error
}

func (a *Allocator) loadLocked(ctx context.Context) error {
	if a.store == nil {
		return nil
	}
	data, err := a.store.Load(ctx)
	if err != nil {
		return errors.Join(ErrStore, err)
	}
	if len(data) == 0 {
		return nil
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("%w: decode allocator state: %v", ErrStore, err)
	}
	if state.Version != 1 && state.Version != stateVersion {
		return fmt.Errorf("%w: unsupported allocator state version %d", ErrStore, state.Version)
	}
	if !bytesEqual(state.Identity, a.identity) {
		return fmt.Errorf("%w: state belongs to a different allocator identity", ErrStore)
	}

	a.highWater = state.HighWater
	if state.Tombstones != nil {
		a.tombstones = state.Tombstones
	}
	for _, saved := range state.Offers {
		offer := new(r1sv1.ExecutionOffer)
		request := new(r1sv1.ExecutionRequest)
		if err := proto.Unmarshal(saved.Offer, offer); err != nil {
			return fmt.Errorf("%w: decode offer: %v", ErrStore, err)
		}
		if err := proto.Unmarshal(saved.Request, request); err != nil {
			return fmt.Errorf("%w: decode offer request: %v", ErrStore, err)
		}
		if offer.GetOfferId() == "" || request.GetRequestId() == "" || offer.GetRequestId() != request.GetRequestId() {
			return fmt.Errorf("%w: invalid durable offer", ErrStore)
		}
		if _, exists := a.offers[offer.GetOfferId()]; exists {
			return fmt.Errorf("%w: duplicate durable offer %q", ErrStore, offer.GetOfferId())
		}
		record := &offerRecord{offer: offer, request: request, client: cloneBytes(firstBytes(saved.Client, saved.LegacySender)), status: saved.Status, execution: saved.Execution, resources: saved.Resources}
		if record.status < offerOutstanding || record.status > offerReleased {
			return fmt.Errorf("%w: invalid durable offer status", ErrStore)
		}
		requestKey := authorityKey(record.client, request.GetRequestId())
		if _, exists := a.requests[requestKey]; exists {
			return fmt.Errorf("%w: duplicate durable request %q", ErrStore, request.GetRequestId())
		}
		a.offers[offer.GetOfferId()] = record
		a.requests[requestKey] = offer.GetOfferId()
	}

	for _, saved := range state.Executions {
		request := new(r1sv1.ExecutionRequest)
		if err := proto.Unmarshal(saved.Request, request); err != nil {
			return fmt.Errorf("%w: decode execution request: %v", ErrStore, err)
		}
		if saved.ID == "" || saved.OfferID == "" || saved.ResourceClass == "" || request.GetRequestId() == "" {
			return fmt.Errorf("%w: invalid durable execution", ErrStore)
		}
		if _, exists := a.executions[saved.ID]; exists {
			return fmt.Errorf("%w: duplicate durable execution %q", ErrStore, saved.ID)
		}
		if saved.Phase < r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING || saved.Phase > r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED {
			return fmt.Errorf("%w: invalid durable execution phase", ErrStore)
		}
		if saved.StartedAt.IsZero() || saved.OccurredAt.IsZero() || protocol.Terminal(saved.Phase) != saved.Released {
			return fmt.Errorf("%w: inconsistent durable execution %q", ErrStore, saved.ID)
		}
		a.executions[saved.ID] = &executionRecord{
			id: saved.ID, offerID: saved.OfferID, client: cloneBytes(firstBytes(saved.Client, saved.LegacySender)), resourceClass: saved.ResourceClass,
			request: request, phase: saved.Phase, detail: saved.Detail, exitCode: cloneInt32(saved.ExitCode),
			occurredAt: saved.OccurredAt, startedAt: saved.StartedAt, released: saved.Released, revision: max(1, saved.Revision), resources: saved.Resources, retainUntil: saved.RetainUntil,
			leaseUntil: saved.LeaseUntil,
		}
		if protocol.Terminal(saved.Phase) && saved.RetainUntil.IsZero() {
			a.executions[saved.ID].retainUntil = saved.OccurredAt.Add(a.retention)
		}
		// A pre-lease snapshot grants running executions one fresh lease so an
		// upgrade never evicts work that was alive before the restart.
		if !protocol.Terminal(saved.Phase) && saved.LeaseUntil.IsZero() {
			a.executions[saved.ID].leaseUntil = a.now().UTC().Add(a.leaseTTL)
		}
	}

	for _, saved := range state.Replay {
		if !saved.Complete || saved.Key == "" {
			continue
		}
		envelope := new(r1sv1.Envelope)
		if err := proto.Unmarshal(saved.Envelope, envelope); err != nil {
			return fmt.Errorf("%w: decode replay envelope: %v", ErrStore, err)
		}
		entry := &replayEntry{seenAt: saved.SeenAt, done: make(chan struct{}), envelope: envelope}
		for _, encoded := range saved.Responses {
			response := new(r1sv1.Envelope)
			if err := proto.Unmarshal(encoded, response); err != nil {
				return fmt.Errorf("%w: decode replay response: %v", ErrStore, err)
			}
			entry.responses = append(entry.responses, response)
		}
		if saved.Error != "" {
			entry.err = restoreError(saved.ErrorCode, saved.Error)
		}
		close(entry.done)
		a.replay.entries[saved.Key] = entry
	}

	a.expireOffersLocked(a.now().UTC())
	for _, offer := range a.offers {
		if offer.status == offerAssigned {
			execution := a.executions[offer.execution]
			if execution == nil || execution.offerID != offer.offer.GetOfferId() || execution.resourceClass != offer.offer.GetResourceClass() {
				return fmt.Errorf("%w: assigned offer %q has no matching execution", ErrStore, offer.offer.GetOfferId())
			}
		}
		if offer.status == offerOutstanding || offer.status == offerAssigned && !a.executions[offer.execution].released {
			if err := a.capacity.reserve(offer.offer.GetResourceClass()); err != nil {
				return fmt.Errorf("%w: durable state exceeds configured capacity for class %q", ErrInvalidConfig, offer.offer.GetResourceClass())
			}
		}
	}
	return nil
}

func (a *Allocator) persistLocked(ctx context.Context) error {
	if a.store == nil {
		return nil
	}
	state := persistedState{Version: stateVersion, Identity: cloneBytes(a.identity), HighWater: a.highWater, Tombstones: a.tombstones}
	for _, record := range a.offers {
		offer, err := proto.Marshal(record.offer)
		if err != nil {
			return errors.Join(ErrStore, err)
		}
		request, err := proto.Marshal(record.request)
		if err != nil {
			return errors.Join(ErrStore, err)
		}
		state.Offers = append(state.Offers, persistedOffer{Offer: offer, Request: request, Client: cloneBytes(record.client), Status: record.status, Execution: record.execution, Resources: record.resources})
	}
	for _, record := range a.executions {
		request, err := proto.Marshal(record.request)
		if err != nil {
			return errors.Join(ErrStore, err)
		}
		state.Executions = append(state.Executions, persistedExecution{
			ID: record.id, OfferID: record.offerID, Client: cloneBytes(record.client), ResourceClass: record.resourceClass,
			Request: request, Phase: record.phase, Detail: record.detail, ExitCode: cloneInt32(record.exitCode),
			OccurredAt: record.occurredAt, StartedAt: record.startedAt, Released: record.released, Revision: record.revision, Resources: record.resources, RetainUntil: record.retainUntil,
			LeaseUntil: record.leaseUntil,
		})
	}
	for key, entry := range a.replay.entries {
		envelope, err := proto.Marshal(entry.envelope)
		if err != nil {
			return errors.Join(ErrStore, err)
		}
		saved := persistedReplay{Key: key, SeenAt: entry.seenAt, Envelope: envelope, Complete: replayDone(entry)}
		if saved.Complete {
			for _, response := range entry.responses {
				encoded, err := proto.Marshal(response)
				if err != nil {
					return errors.Join(ErrStore, err)
				}
				saved.Responses = append(saved.Responses, encoded)
			}
			if entry.err != nil {
				saved.Error = entry.err.Error()
				saved.ErrorCode = classifyError(entry.err)
			}
		}
		state.Replay = append(state.Replay, saved)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return errors.Join(ErrStore, err)
	}
	if err := a.store.Save(ctx, data); err != nil {
		return errors.Join(ErrStore, err)
	}
	return nil
}
