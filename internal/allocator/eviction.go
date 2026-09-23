package allocator

import (
	"context"
	"errors"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// Lease-eviction sweep: the allocator's background collector that stops and
// terminally fails every execution whose durable client-held lease expired
// without renewal. The protocol side — one authenticated ExecutionLeaseRenew
// command — lives in lease.go. Both share the same leaseUntil field on
// executionRecord, but eviction is local policy triggered by Sweep, never a
// direct reaction to a network message.

// EvictExpiredLeases stops and terminally fails every non-terminal execution
// whose lease expired without renewal. Eviction follows the existing runtime
// Stop boundary exactly like cancellation: the CANCELLING transition commits
// first, the runtime stop is idempotent, and the terminal state commits with a
// lease-expiry reason that is distinguishable from client cancellation.
// A failed eviction restores the pre-eviction state and returns; the next
// sweep retries it instead of this call tight-looping over the same record.
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
		var previousDetail string
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		} else {
			previousDetail, err = a.beginEvictionLocked(expiredID, now)
		}
		a.mu.Unlock()
		if err != nil {
			return errors.Join(allErr, err)
		}
		evictErr := a.completeEviction(ctx, expiredID, expiredPhase, previousDetail)
		allErr = errors.Join(allErr, evictErr)
		if evictErr != nil {
			// A failed eviction restores the pre-eviction phase; retrying belongs
			// to the next sweep, not to a tight loop over the same record.
			return allErr
		}
	}
}

// nextExpiredLeaseLocked returns one non-terminal execution whose lease
// expired without renewal. Callers must hold a.mu.
func (a *Allocator) nextExpiredLeaseLocked(now time.Time) (string, r1sv1.ExecutionPhase) {
	for id, record := range a.executions {
		if !protocol.Terminal(record.phase) && record.phase != r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING && !record.leaseUntil.IsZero() && !record.leaseUntil.After(now) {
			return id, record.phase
		}
	}
	return "", 0
}

// beginEvictionLocked commits the CANCELLING transition for one lease-expired
// execution so concurrent commands observe the eviction in progress, and
// returns the pre-eviction detail so a failed eviction restores it. Callers
// must hold a.mu.
func (a *Allocator) beginEvictionLocked(id string, now time.Time) (string, error) {
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
		return previousDetail, err
	}
	return previousDetail, nil
}

// completeEviction stops the runtime task through the same idempotent boundary
// cancellation uses, then commits the terminal evicted state. A failed stop
// restores the pre-eviction phase and detail so a later sweep can retry the
// eviction.
func (a *Allocator) completeEviction(ctx context.Context, id string, previousPhase r1sv1.ExecutionPhase, previousDetail string) error {
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
			record.detail = previousDetail
			record.occurredAt = now
			record.revision++
		}
		return errors.Join(ErrRuntimeStop, stopErr, a.persistLocked(context.Background()))
	}
	if !protocol.Terminal(record.phase) {
		a.finishLocked(record, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, protocol.LeaseExpiredDetail, nil, now)
	}
	return a.persistLocked(context.Background())
}
