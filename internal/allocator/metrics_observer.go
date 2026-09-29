package allocator

import (
	"errors"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

const unconfiguredResourceClass = "__unconfigured__"

func (a *Allocator) metricResourceClass(class string) string {
	if _, ok := a.capacity.limits[class]; ok {
		return class
	}
	return unconfiguredResourceClass
}

func (a *Allocator) syncMetricsLocked() {
	if a.metrics == nil {
		return
	}
	classSeen := make(map[string]bool)
	classActive := make(map[string]int)
	for _, rec := range a.executions {
		class := a.metricResourceClass(rec.resourceClass)
		classSeen[class] = true
		if !protocol.Terminal(rec.phase) {
			classActive[class]++
		}
	}
	// Reset the active gauge for every class with retained records, including
	// classes a restarted allocator no longer configures capacity for, so a
	// surviving terminal record cannot leave a stale non-zero gauge behind.
	for class := range classSeen {
		a.metrics.SetActiveExecutions(class, classActive[class])
	}
	for class, limit := range a.capacity.limits {
		a.metrics.UpdateCapacity(class, limit, a.capacity.available(class))
		a.metrics.SetActiveExecutions(class, classActive[class])
	}
}

func (a *Allocator) observeRequestMetric(class string) {
	if a.metrics != nil {
		a.metrics.ObserveRequest(a.metricResourceClass(class))
	}
}

func (a *Allocator) observeOfferMetric(class string) {
	if a.metrics != nil {
		a.metrics.ObserveOffer(class)
	}
}

func (a *Allocator) observeRejectionMetric(err error) {
	if a.metrics == nil || err == nil {
		return
	}
	code := "INTERNAL"
	switch {
	case errors.Is(err, ErrAdmission):
		code = "ADMISSION"
	case errors.Is(err, ErrStore):
		code = "OUTCOME_UNKNOWN"
	case errors.Is(err, ErrUnauthorized):
		code = "UNAUTHORIZED"
	case errors.Is(err, ErrOfferNotFound), errors.Is(err, ErrExecutionNotFound):
		code = "NOT_FOUND"
	case errors.Is(err, ErrCapacityExhausted), errors.Is(err, ErrReplayCapacity):
		code = "CAPACITY"
	case errors.Is(err, ErrIncompatible):
		code = "INCOMPATIBLE"
	case errors.Is(err, ErrOfferExpired), errors.Is(err, ErrOfferReleased), errors.Is(err, ErrResultExpired), errors.Is(err, ErrCommandExpired):
		code = "EXPIRED"
	case errors.Is(err, ErrExecutionConflict), errors.Is(err, ErrReplayConflict), errors.Is(err, ErrOfferAlreadyAssigned), errors.Is(err, ErrInvalidTransition):
		code = "CONFLICT"
	case errors.Is(err, ErrTunnelDisabled), errors.Is(err, ErrTunnelNoEndpoint):
		code = "TUNNEL"
	case errors.Is(err, protocol.ErrInvalidEnvelope), errors.Is(err, ErrUnsupportedMessage), errors.Is(err, ErrLeaseTooLong):
		code = "INVALID_REQUEST"
	case errors.Is(err, ErrRuntimeStart), errors.Is(err, ErrRuntimeStop):
		code = "UNAVAILABLE"
	}
	a.metrics.ObserveRejection(code)
}

func (a *Allocator) observeLeaseEvictionMetric() {
	if a.metrics != nil {
		a.metrics.ObserveLeaseEviction()
	}
}

func (a *Allocator) observeExecutionTerminalMetric(class string, phase r1sv1.ExecutionPhase, duration time.Duration) {
	if a.metrics != nil {
		phaseStr := "unknown"
		switch phase {
		case r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED:
			phaseStr = "completed"
		case r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED:
			phaseStr = "failed"
		case r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED:
			phaseStr = "cancelled"
		}
		a.metrics.ObserveExecutionTerminal(a.metricResourceClass(class), phaseStr, duration)
	}
}

func (a *Allocator) observeFinishedMetricsLocked(record *executionRecord) {
	duration := time.Duration(0)
	if !record.startedAt.IsZero() && !record.occurredAt.Before(record.startedAt) {
		duration = record.occurredAt.Sub(record.startedAt)
	}
	a.observeExecutionTerminalMetric(record.resourceClass, record.phase, duration)
	if record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED && record.detail == protocol.LeaseExpiredDetail {
		a.observeLeaseEvictionMetric()
	}
}

func (a *Allocator) observeDispatchLatency(command string, duration time.Duration) {
	if a.metrics != nil {
		a.metrics.ObserveDispatchLatency(command, duration)
	}
}
