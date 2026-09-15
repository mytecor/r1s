package client

import (
	"bytes"
	"sort"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

// Requests returns stable, sorted snapshots.
func (o *Client) Requests() []RequestSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make([]RequestSnapshot, 0, len(o.requests))
	for _, record := range o.requests {
		result = append(result, RequestSnapshot{Request: proto.Clone(record.request).(*r1sv1.ExecutionRequest), CreatedAt: record.createdAt, OfferCount: len(record.offers), ExecutionID: record.executionID})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// Execution returns a cloned execution snapshot.
func (o *Client) Execution(executionID string) (ExecutionSnapshot, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return ExecutionSnapshot{}, false
	}
	return snapshotExecution(record), true
}

// Executions returns stable, sorted execution snapshots.
func (o *Client) Executions() []ExecutionSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make([]ExecutionSnapshot, 0, len(o.executions))
	for _, record := range o.executions {
		result = append(result, snapshotExecution(record))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ExecutionID < result[j].ExecutionID })
	return result
}

func snapshotExecution(record *executionRecord) ExecutionSnapshot {
	result := ExecutionSnapshot{
		ExecutionID: record.id, RequestID: record.requestID, OfferID: record.offerID,
		Allocator: bytes.Clone(record.allocatorID), Destination: record.destination,
	}
	if record.state != nil {
		result.State = proto.Clone(record.state).(*r1sv1.ExecutionState)
	}
	return result
}
