package allocator

import (
	"context"
	"errors"
	"sort"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// Recover reconciles durable non-terminal executions with the runtime. It
// never creates a missing workload: missing or conflicting runtime metadata is
// recorded as a terminal failure, while running tasks regain completion
// monitoring. An execution whose client-held lease already expired is evicted
// instead of being revived; one with a valid lease keeps running.
func (a *Allocator) Recover(ctx context.Context) error {
	recoverer, ok := a.runtime.(r1sruntime.Recoverer)
	if !ok {
		a.mu.Lock()
		hasActive := false
		for _, record := range a.executions {
			hasActive = hasActive || !terminal(record.phase)
		}
		a.mu.Unlock()
		if hasActive {
			return ErrRecoveryUnsupported
		}
		return nil
	}

	a.mu.Lock()
	ids := make([]string, 0, len(a.executions))
	for id, record := range a.executions {
		if !terminal(record.phase) {
			ids = append(ids, id)
		}
	}
	a.mu.Unlock()
	sort.Strings(ids)

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		a.mu.Lock()
		record := a.executions[id]
		if record == nil || terminal(record.phase) {
			a.mu.Unlock()
			continue
		}
		if record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
			reason := record.detail
			a.mu.Unlock()
			if err := a.runtime.Stop(ctx, id); err != nil && !errors.Is(err, r1sruntime.ErrExecutionMissing) {
				return errors.Join(ErrRuntimeStop, err)
			}
			a.mu.Lock()
			if current := a.executions[id]; current != nil && !terminal(current.phase) {
				phase := r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED
				if reason == protocol.LeaseExpiredDetail {
					// An interrupted lease eviction resumes to its distinct
					// terminal state instead of looking like a cancellation.
					phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
				}
				a.finishLocked(current, phase, reason, nil, a.now().UTC())
			}
			err := a.persistLocked(context.Background())
			a.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}
		// A lease that expired while the allocator was down evicts the
		// execution locally without reviving its runtime task.
		if !record.leaseUntil.IsZero() && !record.leaseUntil.After(a.now().UTC()) {
			a.mu.Unlock()
			if err := a.runtime.Stop(ctx, id); err != nil && !errors.Is(err, r1sruntime.ErrExecutionMissing) {
				return errors.Join(ErrRuntimeStop, err)
			}
			a.mu.Lock()
			if current := a.executions[id]; current != nil && !terminal(current.phase) {
				a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, protocol.LeaseExpiredDetail, nil, a.now().UTC())
			}
			err := a.persistLocked(context.Background())
			a.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}

		request := a.startRequestLocked(record)
		a.mu.Unlock()
		recoverErr := recoverer.Recover(ctx, request, a.completionReporter(id))

		a.mu.Lock()
		current := a.executions[id]
		if recoverErr == nil {
			if current != nil && current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING {
				current.phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING
				current.occurredAt = a.now().UTC()
				current.revision++
			}
		} else if errors.Is(recoverErr, r1sruntime.ErrExecutionMissing) || errors.Is(recoverErr, r1sruntime.ErrExecutionConflict) {
			if current != nil && !terminal(current.phase) {
				a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, recoverErr.Error(), nil, a.now().UTC())
			}
		} else {
			a.mu.Unlock()
			return errors.Join(ErrRuntimeStart, recoverErr)
		}
		persistErr := a.persistLocked(context.Background())
		a.mu.Unlock()
		if persistErr != nil {
			return persistErr
		}
	}
	return nil
}
