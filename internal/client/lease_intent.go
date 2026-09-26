package client

import (
	"bytes"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the in-memory lease-holding intent policy of the client: how a
// lease is recorded, probed, rebound, and reported as lost. The protocol side
// — how the client reacts to a renewal ack or an explicit lease loss — lives
// in lease_ack.go. The intent record exists only for the run lifetime; the
// ack handlers are the reaction to allocator replies.

// clientDefaultLease is the lease duration recorded by a bare Maintain call
// when no explicit duration is supplied. It matches the allocator's default
// initial lease so one-shot callers renew at a sane cadence.
const clientDefaultLease = protocol.DefaultLease

// Maintain records the in-memory lease-holding intent for one execution and
// returns a one-shot authenticated renewal command. The public run controller
// replays this intent on every tick. Maintain never spawns a background process.
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
	if record.state != nil && protocol.Terminal(record.state.GetPhase()) {
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
		if record.state != nil && protocol.Terminal(record.state.GetPhase()) {
			continue
		}
		if !record.leaseRenewedAt.IsZero() && now.Before(record.leaseRenewedAt.Add(record.leaseDuration/3)) {
			continue
		}
		due = append(due, record.id)
	}
	return due
}

// RecordLeaseIntent records the in-memory lease-holding intent for one execution
// without sending anything. The public run controller replays it on every tick
// and renews when due. Explicit
// allocator destinations ride with the intent so a re-request after lease
// loss preserves the original pinning.
func (o *Client) RecordLeaseIntent(executionID string, leaseDuration time.Duration, allocators []string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return ErrExecutionNotFound
	}
	if record.leaseLost || (record.state != nil && protocol.Terminal(record.state.GetPhase())) {
		return ErrConflict
	}
	if leaseDuration <= 0 {
		leaseDuration = clientDefaultLease
	}
	record.leaseDuration = leaseDuration
	record.leaseAllocators = pinnedAllocators(allocators)
	return nil
}

// pinnedAllocators returns a deduplicated copy of the explicit allocator
// destinations recorded with a lease-holding intent.
func pinnedAllocators(destinations []string) []string {
	if len(destinations) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(destinations))
	var pinned []string
	for _, destination := range destinations {
		if destination == "" || seen[destination] {
			continue
		}
		seen[destination] = true
		pinned = append(pinned, destination)
	}
	return pinned
}

// LeaseIntentAllocators returns the allocator destinations recorded with the
// lease-holding intent of one execution, so a re-request after lease loss can
// preserve the original pinning.
func (o *Client) LeaseIntentAllocators(executionID string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return nil
	}
	return append([]string(nil), record.leaseAllocators...)
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
	if record.state != nil && protocol.Terminal(record.state.GetPhase()) {
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

// RequestForExecution returns the recorded workload request backing one
// execution, so a re-request after lease loss can replay it verbatim.
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
// execution created by re-requesting the recorded workload, carrying the
// recorded allocator pinning along.
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
	replacement.leaseDuration = previous.leaseDuration
	previous.leaseDuration = 0
	previous.leaseLost = false
	replacement.leaseAllocators = previous.leaseAllocators
	previous.leaseAllocators = nil
	replacement.leaseLost = false
	replacement.leaseExpiresAt = time.Time{}
	return nil
}
