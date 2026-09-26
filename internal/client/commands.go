package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CreateRequest durably creates a request before it is sent to any allocator.
// It creates an any-node request with no placement constraints.
func (o *Client) CreateRequest(workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string) (string, *r1sv1.Envelope, error) {
	return o.createRequest(workload, policy, resourceClass, nil, "", 1)
}

// CreateRequestWithConstraints durably creates a request with placement
// constraints. The constraints are validated and become part of the durable
// request, so every allocator receives the same narrowing even after restart
// and re-request.
func (o *Client) CreateRequestWithConstraints(workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, constraints *r1sv1.PlacementConstraints) (string, *r1sv1.Envelope, error) {
	return o.createRequest(workload, policy, resourceClass, constraints, "", 1)
}

// CreateNextAttempt creates a fresh request for the next at-least-once attempt
// of the same logical run. The request and eventual execution IDs are new;
// only the correlation ID is retained and the attempt number is advanced.
func (o *Client) CreateNextAttempt(previous *r1sv1.ExecutionRequest) (string, *r1sv1.Envelope, error) {
	if previous == nil || previous.GetAttempt() == ^uint64(0) {
		return "", nil, fmt.Errorf("%w: previous run attempt is invalid", ErrInvalidConfig)
	}
	return o.createRequest(previous.GetWorkload(), previous.GetPolicy(), previous.GetResourceClass(), previous.GetConstraints(), previous.GetRunId(), previous.GetAttempt()+1)
}

func (o *Client) createRequest(workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, constraints *r1sv1.PlacementConstraints, runID string, attempt uint64) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	requestID := o.uniqueIDLocked(o.requests)
	messageID := o.newID()
	if requestID == "" || messageID == "" {
		return "", nil, fmt.Errorf("%w: ID generator returned an empty or duplicate ID", ErrInvalidConfig)
	}
	now := o.now().UTC()
	if runID == "" {
		runID = initialRunID(requestID)
	}
	request := &r1sv1.ExecutionRequest{
		RequestId: requestID, Workload: cloneWorkload(workload), Policy: clonePolicy(policy), ResourceClass: resourceClass,
		Constraints: constraints, RunId: runID, Attempt: attempt,
	}
	envelope := o.requestEnvelopeLocked(request, messageID, now)
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return "", nil, err
	}
	o.requests[requestID] = &requestRecord{request: request, messageID: messageID, createdAt: now, offers: make(map[string]*offerRecord)}
	return requestID, proto.Clone(envelope).(*r1sv1.Envelope), nil
}

func initialRunID(requestID string) string {
	digest := sha256.Sum256([]byte(requestID))
	return hex.EncodeToString(digest[:16])
}

// RequestEnvelope returns the stable request envelope used for all allocators.
func (o *Client) RequestEnvelope(requestID string) (*r1sv1.Envelope, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.requests[requestID]
	if record == nil {
		return nil, false
	}
	return o.requestEnvelopeLocked(record.request, record.messageID, record.createdAt), true
}

// Inspect creates a fresh query so allocator replay caching cannot return stale state.
func (o *Client) Inspect(executionID string) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return "", nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, executionID)
	}
	previous := record.inspectMessageID
	previousSentAt := record.inspectSentAt
	record.inspectMessageID = o.newID()
	record.inspectSentAt = o.now().UTC()
	if record.inspectMessageID == "" {
		record.inspectMessageID = previous
		record.inspectSentAt = previousSentAt
		return "", nil, fmt.Errorf("%w: ID generator returned an empty ID", ErrInvalidConfig)
	}
	return record.destination, &r1sv1.Envelope{
		MessageId: record.inspectMessageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(record.inspectSentAt),
		Payload: &r1sv1.Envelope_ExecutionInspect{ExecutionInspect: &r1sv1.ExecutionInspect{ExecutionId: executionID}},
	}, nil
}

// Cancel records a stable cancellation before it is sent.
func (o *Client) Cancel(executionID, reason string) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return "", nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, executionID)
	}
	if record.cancelMessageID == "" {
		record.cancelMessageID = o.newID()
		record.cancelSentAt = o.now().UTC()
		record.cancelReason = reason
		if record.cancelMessageID == "" {
			return "", nil, fmt.Errorf("%w: ID generator returned an empty ID", ErrInvalidConfig)
		}
	} else if record.cancelReason != reason {
		return "", nil, ErrConflict
	}
	return record.destination, &r1sv1.Envelope{
		MessageId: record.cancelMessageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(record.cancelSentAt),
		Payload: &r1sv1.Envelope_ExecutionCancel{ExecutionCancel: &r1sv1.ExecutionCancel{ExecutionId: executionID, Reason: reason}},
	}, nil
}
