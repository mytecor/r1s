package allocator

import (
	"bytes"
	"context"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/proto"
)

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
	if protocol.Terminal(record.phase) {
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
		RunID:       record.request.GetRunId(),
		Attempt:     record.request.GetAttempt(),
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
