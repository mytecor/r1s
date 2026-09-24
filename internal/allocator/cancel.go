package allocator

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

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
	if protocol.Terminal(record.phase) || record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
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
	if !protocol.Terminal(current.phase) {
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
