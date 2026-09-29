package client

import (
	"bytes"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

// RequestSnapshot is a read-only view of a client request.
type RequestSnapshot struct {
	Request     *r1sv1.ExecutionRequest
	CreatedAt   time.Time
	OfferCount  int
	ExecutionID string
}

// ExecutionSnapshot is a read-only view of a client execution.
type ExecutionSnapshot struct {
	ExecutionID string
	RequestID   string
	OfferID     string
	Allocator   []byte
	Destination string
	State       *r1sv1.ExecutionState
}

// Statistics contains an in-memory snapshot of this client run controller's activity.
type Statistics struct {
	Requests   []RequestSnapshot
	Executions []ExecutionSnapshot
}

// Stats returns a snapshot of in-memory requests and executions tracked by this controller.
func (c *Client) Stats() Statistics {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.core == nil {
		return Statistics{}
	}
	coreRequests := c.core.Requests()
	coreExecutions := c.core.Executions()
	requests := make([]RequestSnapshot, len(coreRequests))
	for i, r := range coreRequests {
		requests[i] = RequestSnapshot{
			Request:     proto.Clone(r.Request).(*r1sv1.ExecutionRequest),
			CreatedAt:   r.CreatedAt,
			OfferCount:  r.OfferCount,
			ExecutionID: r.ExecutionID,
		}
	}
	executions := make([]ExecutionSnapshot, len(coreExecutions))
	for i, e := range coreExecutions {
		var state *r1sv1.ExecutionState
		if e.State != nil {
			state = proto.Clone(e.State).(*r1sv1.ExecutionState)
		}
		executions[i] = ExecutionSnapshot{
			ExecutionID: e.ExecutionID,
			RequestID:   e.RequestID,
			OfferID:     e.OfferID,
			Allocator:   bytes.Clone(e.Allocator),
			Destination: e.Destination,
			State:       state,
		}
	}
	return Statistics{
		Requests:   requests,
		Executions: executions,
	}
}
