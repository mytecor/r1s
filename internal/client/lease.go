package client

import (
	"bytes"
	"context"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// clientDefaultLease is the lease duration recorded by a bare Maintain call
// when no explicit duration is supplied. It matches the allocator's default
// initial lease so one-shot callers renew at a sane cadence.
const clientDefaultLease = 10 * time.Minute

// Maintain durably records the lease-holding intent for one execution and
// returns a one-shot authenticated renewal command. The continuous renewal
// loop lives only in the `r1s serve` engine: it replays this durable intent on
// every tick. Maintain never spawns a background process.
//
// lost=true reports that the execution's lease already expired without
// renewal (the execution was evicted or its metadata is gone): the recorded
// workload must be re-requested with a fresh request, and the intent then
// rebinds to the replacement execution via RebindLeaseIntent.
func (o *Client) Maintain(executionID string, leaseDuration time.Duration) (destination string, envelope *r1sv1.Envelope, lost bool, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return "", nil, false, ErrExecutionNotFound
	}
	if record.leaseLost {
		return record.destination, nil, true, nil
	}
	if record.state != nil && terminal(record.state.GetPhase()) {
		if isLostLeaseState(record.state) {
			return o.markLeaseLostLocked(record)
		}
		return "", nil, false, ErrConflict
	}
	if leaseDuration > 0 {
		record.leaseDuration = leaseDuration
	}
	if record.leaseDuration <= 0 {
		record.leaseDuration = clientDefaultLease
	}
	// A fresh message ID per renewal so allocator replay caching can never
	// return a stale ack; replaying one renewal is still safe and idempotent.
	messageID := o.newID()
	if messageID == "" {
		return "", nil, false, ErrInvalidConfig
	}
	now := o.now().UTC()
	record.leaseRenewMessageID = messageID
	record.leaseRenewedAt = now
	envelope = &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    bytes.Clone(o.identity),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionLeaseRenew{ExecutionLeaseRenew: &r1sv1.ExecutionLeaseRenew{
			ExecutionId: record.id, LeaseDuration: durationpb.New(record.leaseDuration),
		}},
	}
	if err := o.persistLocked(context.Background()); err != nil {
		record.leaseRenewMessageID = ""
		record.leaseRenewedAt = time.Time{}
		return "", nil, false, err
	}
	return record.destination, envelope, false, nil
}

// DueLeaseRenewals returns execution IDs whose recorded lease intent is due
// for renewal: non-terminal, not lost, and past one third of the lease since
// the last renewal attempt. Callers send each renewal through Maintain.
func (o *Client) DueLeaseRenewals() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.now().UTC()
	var due []string
	for _, record := range o.executions {
		if record.leaseDuration <= 0 || record.leaseLost {
			continue
		}
		if record.state != nil && terminal(record.state.GetPhase()) {
			continue
		}
		if !record.leaseRenewedAt.IsZero() && now.Before(record.leaseRenewedAt.Add(record.leaseDuration/3)) {
			continue
		}
		due = append(due, record.id)
	}
	return due
}

// RecordLeaseIntent durably records the lease-holding intent for one execution
// without sending anything. The shared renewal loop (serve, or a foreground
// keep-alive request) replays it on every tick and renews when due.
func (o *Client) RecordLeaseIntent(executionID string, leaseDuration time.Duration) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return ErrExecutionNotFound
	}
	if record.leaseLost || (record.state != nil && terminal(record.state.GetPhase())) {
		return ErrConflict
	}
	if leaseDuration <= 0 {
		leaseDuration = clientDefaultLease
	}
	previous := record.leaseDuration
	record.leaseDuration = leaseDuration
	if err := o.persistLocked(context.Background()); err != nil {
		record.leaseDuration = previous
		return err
	}
	return nil
}

// LeaseDue reports whether the recorded intent for one execution is due for
// renewal: non-terminal, not lost, and past one third of the lease since the
// last renewal attempt.
func (o *Client) LeaseDue(executionID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil || record.leaseDuration <= 0 || record.leaseLost {
		return false
	}
	if record.state != nil && terminal(record.state.GetPhase()) {
		return false
	}
	return record.leaseRenewedAt.IsZero() || !o.now().UTC().Before(record.leaseRenewedAt.Add(record.leaseDuration/3))
}

// LeaseIntentLost reports whether an execution's lease expired before a
// renewal landed, so its recorded workload must be re-requested.
func (o *Client) LeaseIntentLost(executionID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	return record != nil && record.leaseLost
}

// LostLeaseIntents returns execution IDs whose lease expired before a renewal
// landed. Their recorded workload must be re-requested.
func (o *Client) LostLeaseIntents() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var lost []string
	for _, record := range o.executions {
		if record.leaseLost && record.leaseDuration > 0 {
			lost = append(lost, record.id)
		}
	}
	return lost
}

func (o *Client) RequestForExecution(executionID string) (*r1sv1.ExecutionRequest, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return nil, false
	}
	request := o.requests[record.requestID]
	if request == nil {
		return nil, false
	}
	return proto.Clone(request.request).(*r1sv1.ExecutionRequest), true
}

// RebindLeaseIntent moves a lost lease-holding intent to a replacement
// execution created by re-requesting the recorded workload.
func (o *Client) RebindLeaseIntent(oldExecutionID, newExecutionID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	previous := o.executions[oldExecutionID]
	replacement := o.executions[newExecutionID]
	if previous == nil || replacement == nil || previous == replacement {
		return ErrExecutionNotFound
	}
	if previous.leaseDuration <= 0 {
		return ErrConflict
	}
	savedOld := previous.leaseDuration
	previous.leaseDuration = 0
	previous.leaseLost = false
	replacement.leaseDuration = savedOld
	replacement.leaseLost = false
	replacement.leaseExpiresAt = time.Time{}
	if err := o.persistLocked(context.Background()); err != nil {
		previous.leaseDuration = savedOld
		replacement.leaseDuration = 0
		return err
	}
	return nil
}

// handleRenewAckLocked applies an allocator lease-renewal ack. Callers must
// hold o.mu.
func (o *Client) handleRenewAckLocked(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionLeaseRenewAck) error {
	record := o.executions[ack.GetExecutionId()]
	if record == nil {
		return ErrExecutionNotFound
	}
	if !bytes.Equal(record.allocatorID, envelope.GetSender()) || record.leaseRenewMessageID != envelope.GetCorrelationId() {
		return ErrUnauthorized
	}
	if record.leaseLost {
		return ErrConflict
	}
	previousID := record.leaseRenewMessageID
	previousRenewedAt := record.leaseRenewedAt
	record.leaseRenewMessageID = ""
	record.leaseRenewedAt = o.now().UTC()
	record.leaseExpiresAt = ack.GetExpiresAt().AsTime()
	if err := o.persistLocked(context.Background()); err != nil {
		record.leaseRenewMessageID = previousID
		record.leaseRenewedAt = previousRenewedAt
		record.leaseExpiresAt = time.Time{}
		return err
	}
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
		_ = o.persistLocked(context.Background())
	}
}

func (o *Client) markLeaseLostLocked(record *executionRecord) (string, *r1sv1.Envelope, bool, error) {
	record.leaseLost = true
	if err := o.persistLocked(context.Background()); err != nil {
		record.leaseLost = false
		return "", nil, false, err
	}
	return record.destination, nil, true, nil
}

// isLostLeaseState reports whether an observed terminal state marks an
// execution evicted for lease expiry, which is distinguishable from a client
// cancellation and from an ordinary workload failure.
func isLostLeaseState(state *r1sv1.ExecutionState) bool {
	return state.GetPhase() == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED && state.GetDetail() == protocol.LeaseExpiredDetail
}
