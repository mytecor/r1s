// Package protocol validates the versioned wire contract before domain state is mutated.
package protocol

import (
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// DefaultLease is the default duration of the client-held execution lease.
// The allocator grants it at assignment, a bare keep-alive renewal re-records
// it, and the `--lease` flag defaults to it, so the three sites cannot drift
// apart.
const DefaultLease = 10 * time.Minute

// LeaseExpiredDetail is the stable terminal-state detail that marks an
// execution evicted because its client-held lease expired without renewal. It
// is deliberately distinct from any client-supplied cancellation reason so
// clients can tell lease loss from cancellation and, for example, re-request
// lost work.
const LeaseExpiredDetail = "execution lease expired"

func validateLeaseRenew(renew *r1sv1.ExecutionLeaseRenew) error {
	if renew == nil {
		return invalid("execution_lease_renew", "is required")
	}
	if strings.TrimSpace(renew.GetExecutionId()) == "" {
		return invalid("execution_lease_renew.execution_id", "is required")
	}
	if renew.GetLeaseDuration() == nil {
		return invalid("execution_lease_renew.lease_duration", "is required")
	}
	if err := renew.GetLeaseDuration().CheckValid(); err != nil {
		return invalid("execution_lease_renew.lease_duration", err.Error())
	}
	if renew.GetLeaseDuration().AsDuration() <= 0 {
		return invalid("execution_lease_renew.lease_duration", "must be positive")
	}
	return nil
}

func validateLeaseRenewAck(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionLeaseRenewAck) error {
	if ack == nil {
		return invalid("execution_lease_renew_ack", "is required")
	}
	if strings.TrimSpace(ack.GetExecutionId()) == "" {
		return invalid("execution_lease_renew_ack.execution_id", "is required")
	}
	if err := validTimestamp("execution_lease_renew_ack.expires_at", ack.GetExpiresAt()); err != nil {
		return err
	}
	return nil
}
