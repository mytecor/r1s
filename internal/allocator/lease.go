package allocator

import (
	"bytes"
	"context"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleLeaseRenew is the protocol side of the client-held lease: it extends
// the durable lease of one execution in response to an authenticated
// ExecutionLeaseRenew command. The background eviction sweep that terminates
// executions whose lease expired lives in eviction.go.
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
	if protocol.Terminal(record.phase) {
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
