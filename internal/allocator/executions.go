package allocator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
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

func (a *Allocator) handleCancel(ctx context.Context, envelope *r1sv1.Envelope, cancel *r1sv1.ExecutionCancel) ([]*r1sv1.Envelope, error) {
	now := a.now().UTC()
	a.mu.Lock()
	record, ok := a.executions[cancel.GetExecutionId()]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, cancel.GetExecutionId())
	}
	if !bytes.Equal(record.client, envelope.GetSender()) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	if terminal(record.phase) || record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
		response, err := a.stateEnvelopeLocked(record, envelope.GetMessageId(), now)
		a.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return []*r1sv1.Envelope{response}, nil
	}
	previousPhase := record.phase
	previousDetail := record.detail
	previousTime := record.occurredAt
	previousRevision := record.revision
	record.phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING
	record.detail = cancel.GetReason()
	record.occurredAt = now
	record.revision++
	if err := a.persistLocked(context.Background()); err != nil {
		record.phase = previousPhase
		record.detail = previousDetail
		record.occurredAt = previousTime
		record.revision = previousRevision
		a.mu.Unlock()
		return nil, err
	}
	a.mu.Unlock()

	stopErr := a.runtime.Stop(ctx, record.id)

	a.mu.Lock()
	current := a.executions[record.id]
	if stopErr != nil {
		if current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
			current.phase = previousPhase
			current.detail = previousDetail
			current.occurredAt = a.now().UTC()
			current.revision++
		}
		persistErr := a.persistLocked(context.Background())
		a.mu.Unlock()
		return nil, errors.Join(ErrRuntimeStop, stopErr, persistErr)
	}
	if !terminal(current.phase) {
		a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED, cancel.GetReason(), nil, a.now().UTC())
	}
	response, responseErr := a.stateEnvelopeLocked(current, envelope.GetMessageId(), a.now().UTC())
	persistErr := a.persistLocked(context.Background())
	a.mu.Unlock()
	if responseErr != nil {
		return nil, responseErr
	}
	if persistErr != nil {
		return []*r1sv1.Envelope{response}, persistErr
	}
	return []*r1sv1.Envelope{response}, nil
}

// RuntimeCompleted applies an asynchronous completion or failure callback.
func (a *Allocator) RuntimeCompleted(completion r1sruntime.Completion) error {
	if completion.ExecutionID == "" {
		return fmt.Errorf("%w: execution ID is required", ErrInvalidTransition)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.executions[completion.ExecutionID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrExecutionNotFound, completion.ExecutionID)
	}
	phase := r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED
	detail := completion.Detail
	if completion.Err != nil {
		phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
		if detail == "" {
			detail = completion.Err.Error()
		}
	}
	if terminal(record.phase) {
		if record.phase == phase {
			return nil
		}
		return fmt.Errorf("%w: execution %q is already %s", ErrInvalidTransition, record.id, record.phase)
	}
	previous := *record
	used := a.capacity.snapshot()
	a.finishLocked(record, phase, detail, completion.ExitCode, a.now().UTC())
	if err := a.persistLocked(context.Background()); err != nil {
		*record = previous
		a.capacity.restore(used)
		return err
	}
	return nil
}

func (a *Allocator) startRequestLocked(record *executionRecord) r1sruntime.StartRequest {
	return r1sruntime.StartRequest{
		ExecutionID: record.id,
		Client:      bytes.Clone(record.client),
		Workload:    proto.Clone(record.request.GetWorkload()).(*r1sv1.Workload),
		Policy:      proto.Clone(record.request.GetPolicy()).(*r1sv1.ExecutionPolicy),
		StartedAt:   record.startedAt,
		Resources:   record.resources,
	}
}

func (a *Allocator) completionReporter(executionID string) r1sruntime.Reporter {
	return func(completion r1sruntime.Completion) error {
		if completion.ExecutionID == "" {
			completion.ExecutionID = executionID
		}
		if completion.ExecutionID != executionID {
			return fmt.Errorf("%w: runtime reported %q for %q", ErrExecutionConflict, completion.ExecutionID, executionID)
		}
		return a.RuntimeCompleted(completion)
	}
}

func (a *Allocator) finishLocked(record *executionRecord, phase r1sv1.ExecutionPhase, detail string, exitCode *int32, now time.Time) {
	record.phase = phase
	record.detail = detail
	record.exitCode = cloneInt32(exitCode)
	record.occurredAt = now
	record.revision++
	end := now
	if a.highWater.After(end) {
		end = a.highWater
	}
	record.retainUntil = end.Add(retention(record.request.GetPolicy()))
	if !record.released {
		a.capacity.release(record.resourceClass)
		record.released = true
	}
	// A terminal execution closes its live tunnel sessions and invalidates its
	// grants in the same local transition that commits the terminal state. The
	// in-memory registry is not persisted, so a restart already invalidates any
	// outstanding grants; this couples the tunnel lifecycle to the execution so
	// terminal state ends the session promptly without a second lease.
	a.tunnels.Invalidate(record.id)
}
