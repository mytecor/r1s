package client

import (
	"bytes"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

// This file is the protocol side of the client lease: the reactions to
// allocator replies about a renewal. The durable lease-holding intent policy —
// how a lease is recorded, probed, and rebound — lives in lease_intent.go.

// handleRenewAckLocked applies an allocator lease-renewal ack. Callers must
// hold o.mu.
func (o *Client) handleRenewAckLocked(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionLeaseRenewAck) error {
	record := o.executions[ack.GetExecutionId()]
	if record == nil {
		return ErrExecutionNotFound
	}
	// An empty pending renewal never matches: an ack without a correlation ID
	// cannot authorize itself even when it comes from the right allocator.
	if record.leaseRenewMessageID == "" || !bytes.Equal(record.allocatorID, envelope.GetSender()) || record.leaseRenewMessageID != envelope.GetCorrelationId() {
		return ErrUnauthorized
	}
	if record.leaseLost {
		return ErrConflict
	}
	record.leaseRenewMessageID = ""
	record.leaseRenewedAt = o.now().UTC()
	record.leaseExpiresAt = ack.GetExpiresAt().AsTime()
	return nil
}

// markRenewalFailureLocked reacts to a renewal command error. Only an
// authoritative loss (the execution is gone or its result expired) converts
// the intent to a re-request; transient failures keep the intent for retry.
// Callers must hold o.mu.
func (o *Client) markRenewalFailureLocked(executionID, code string) {
	switch code {
	case "NOT_FOUND", "EXPIRED":
	default:
		return
	}
	if record := o.executions[executionID]; record != nil {
		record.leaseRenewMessageID = ""
		record.leaseLost = true
	}
}

func (o *Client) markLeaseLostLocked(record *executionRecord) (string, *r1sv1.Envelope, bool, error) {
	record.leaseLost = true
	return record.destination, nil, true, nil
}

// isLostLeaseState reports whether an observed terminal state marks an
// execution evicted for lease expiry, which is distinguishable from a client
// cancellation and from an ordinary workload failure.
func isLostLeaseState(state *r1sv1.ExecutionState) bool {
	return state.GetPhase() == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED && state.GetDetail() == protocol.LeaseExpiredDetail
}
