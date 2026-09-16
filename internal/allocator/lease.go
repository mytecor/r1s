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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleLeaseRenew extends the durable client-held lease of one execution.
// Renewal is authenticated against the execution owner with the same authority
// rule as cancellation, is idempotent under replay (the envelope-level replay
// cache serves the recorded ack for a repeated message ID), and returns only
// the new expiry. A renewal never mutates execution phase or revision and
// never attaches logs, results, or other payload.
func (a *Allocator) handleLeaseRenew(envelope *r1sv1.Envelope, renew *r1sv1.ExecutionLeaseRenew) ([]*r1sv1.Envelope, error) {
	now := a.now().UTC()
	a.mu.Lock()
	record, ok := a.executions[renew.GetExecutionId()]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, renew.GetExecutionId())
	}
	if !bytes.Equal(record.client, envelope.GetSender()) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	if terminal(record.phase) {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: execution %q is %s", ErrInvalidTransition, record.id, record.phase)
	}
	if record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: execution %q is cancelling", ErrInvalidTransition, record.id)
	}
	duration := renew.GetLeaseDuration().AsDuration()
	if duration > maxLeaseTTL {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: lease duration exceeds %s", ErrLeaseTooLong, maxLeaseTTL)
	}
	previous := record.leaseUntil
	record.leaseUntil = now.Add(duration)
	if err := a.persistLocked(context.Background()); err != nil {
		record.leaseUntil = previous
		a.mu.Unlock()
		return nil, err
	}
	messageID := a.newID()
	if messageID == "" {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: empty message ID", ErrInvalidConfig)
	}
	response := &r1sv1.Envelope{
		MessageId:     messageID,
		Sender:        bytes.Clone(a.identity),
		CorrelationId: envelope.GetMessageId(),
		SentAt:        timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionLeaseRenewAck{ExecutionLeaseRenewAck: &r1sv1.ExecutionLeaseRenewAck{
			ExecutionId: record.id, ExpiresAt: timestamppb.New(record.leaseUntil),
		}},
	}
	a.mu.Unlock()
	return []*r1sv1.Envelope{response}, nil
}

// EvictExpiredLeases stops and terminally fails every non-terminal execution
// whose lease expired without renewal. Eviction follows the existing runtime
// Stop boundary exactly like cancellation: the CANCELLING transition commits
// first, the runtime stop is idempotent, and the terminal state commits with a
// lease-expiry reason that is distinguishable from client cancellation.
// Terminal metadata stays retrievable for the result_retention horizon, and
// capacity returns on eviction.
func (a *Allocator) EvictExpiredLeases(ctx context.Context) error {
	var allErr error
	for {
		a.mu.Lock()
		now := a.effectiveNowLocked()
		expiredID, expiredPhase := a.nextExpiredLeaseLocked(now)
		if expiredID == "" {
			a.mu.Unlock()
			return allErr
		}
		var err error
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		} else {
			err = a.beginEvictionLocked(expiredID, now)
		}
		a.mu.Unlock()
		if err != nil {
			return errors.Join(allErr, err)
		}
		allErr = errors.Join(allErr, a.completeEviction(ctx, expiredID, expiredPhase))
	}
}

// nextExpiredLeaseLocked returns one non-terminal execution whose lease
// expired without renewal. Callers must hold a.mu.
func (a *Allocator) nextExpiredLeaseLocked(now time.Time) (string, r1sv1.ExecutionPhase) {
	for id, record := range a.executions {
		if !terminal(record.phase) && record.phase != r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING && !record.leaseUntil.IsZero() && !record.leaseUntil.After(now) {
			return id, record.phase
		}
	}
	return "", 0
}

// beginEvictionLocked commits the CANCELLING transition for one lease-expired
// execution so concurrent commands observe the eviction in progress. Callers
// must hold a.mu.
func (a *Allocator) beginEvictionLocked(id string, now time.Time) error {
	record := a.executions[id]
	previousPhase := record.phase
	previousDetail := record.detail
	previousTime := record.occurredAt
	previousRevision := record.revision
	record.phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING
	record.detail = protocol.LeaseExpiredDetail
	record.occurredAt = now
	record.revision++
	if err := a.persistLocked(context.Background()); err != nil {
		record.phase = previousPhase
		record.detail = previousDetail
		record.occurredAt = previousTime
		record.revision = previousRevision
		return err
	}
	return nil
}

// completeEviction stops the runtime task through the same idempotent boundary
// cancellation uses, then commits the terminal evicted state. A failed stop
// restores the pre-eviction phase so a later sweep can retry the eviction.
func (a *Allocator) completeEviction(ctx context.Context, id string, previousPhase r1sv1.ExecutionPhase) error {
	stopErr := a.runtime.Stop(ctx, id)

	a.mu.Lock()
	defer a.mu.Unlock()
	record := a.executions[id]
	if record == nil {
		return nil
	}
	now := a.now().UTC()
	if stopErr != nil && !errors.Is(stopErr, r1sruntime.ErrExecutionMissing) {
		if record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
			record.phase = previousPhase
			record.detail = ""
			record.occurredAt = now
			record.revision++
		}
		return errors.Join(ErrRuntimeStop, stopErr, a.persistLocked(context.Background()))
	}
	if !terminal(record.phase) {
		a.finishLocked(record, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, protocol.LeaseExpiredDetail, nil, now)
	}
	return a.persistLocked(context.Background())
}
