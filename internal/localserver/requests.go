package localserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Request implements the unary local request workflow.
func (s *Server) Request(ctx context.Context, in *r1sv1.LocalRequest) (*r1sv1.LocalRequestResponse, error) {
	offerWait := in.GetOfferWait().AsDuration()
	if offerWait <= 0 {
		offerWait = 10 * time.Second
	}
	if in.GetKeepAlive().AsDuration() < 0 {
		return nil, errors.New("request: keep-alive must not be negative")
	}
	requestID, executionID, allocator, err := s.backend.RunRequest(ctx, in.GetWorkload(), in.GetPolicy(), in.GetResourceClass(), offerWait, in.GetAllocators(), in.GetKeepAlive().AsDuration(), in.GetConstraints())
	if err != nil {
		return nil, err
	}
	return &r1sv1.LocalRequestResponse{RequestId: requestID, ExecutionId: executionID, Allocator: allocator}, nil
}

// List returns the client's saved requests.
func (s *Server) List(ctx context.Context, _ *r1sv1.LocalListRequest) (*r1sv1.LocalListResponse, error) {
	snapshots := s.backend.Requests(ctx)
	views := make([]*r1sv1.LocalRequestView, 0, len(snapshots))
	for _, snapshot := range snapshots {
		view := &r1sv1.LocalRequestView{
			RequestId: snapshot.Request.GetRequestId(), ResourceClass: snapshot.Request.GetResourceClass(),
			OfferCount: uint32(snapshot.OfferCount), ExecutionId: snapshot.ExecutionID,
		}
		views = append(views, view)
	}
	return &r1sv1.LocalListResponse{Requests: views}, nil
}

// Inspect returns the latest durable state from the allocator.
func (s *Server) Inspect(ctx context.Context, in *r1sv1.LocalInspectRequest) (*r1sv1.LocalStateResponse, error) {
	state, err := s.backend.Inspect(ctx, in.GetExecutionId(), waitDuration(in.GetWait()))
	if err != nil {
		return nil, err
	}
	return &r1sv1.LocalStateResponse{State: state}, nil
}

// Result returns the terminal state, refusing non-terminal results.
func (s *Server) Result(ctx context.Context, in *r1sv1.LocalResultRequest) (*r1sv1.LocalStateResponse, error) {
	state, err := s.backend.Inspect(ctx, in.GetExecutionId(), waitDuration(in.GetWait()))
	if err != nil {
		return nil, err
	}
	if !protocol.Terminal(state.GetPhase()) {
		return nil, fmt.Errorf("result is not terminal: phase=%s", state.GetPhase())
	}
	return &r1sv1.LocalStateResponse{State: state}, nil
}

// Cancel cancels an execution and returns its resulting state.
func (s *Server) Cancel(ctx context.Context, in *r1sv1.LocalCancelRequest) (*r1sv1.LocalStateResponse, error) {
	reason := in.GetReason()
	if reason == "" {
		reason = "cancelled by client"
	}
	state, err := s.backend.Cancel(ctx, in.GetExecutionId(), reason, waitDuration(in.GetWait()))
	if err != nil {
		return nil, err
	}
	return &r1sv1.LocalStateResponse{State: state}, nil
}

// Logs retrieves one bounded explicit log read.
func (s *Server) Logs(ctx context.Context, in *r1sv1.LocalLogsRequest) (*r1sv1.LocalLogsResponse, error) {
	response, err := s.backend.Logs(ctx, in.GetExecutionId(), in.GetStream(), in.GetOffset(), in.GetMaxBytes(), waitDuration(in.GetWait()))
	if err != nil {
		return nil, err
	}
	return &r1sv1.LocalLogsResponse{
		Data: response.GetData(), NextOffset: response.GetNextOffset(),
		Eof: response.GetEof(), Truncated: response.GetTruncated(),
	}, nil
}

// waitDuration normalizes a protobuf duration to a positive timeout, defaulting
// to 30 seconds when absent or non-positive.
func waitDuration(value *durationpb.Duration) time.Duration {
	if value == nil {
		return 30 * time.Second
	}
	wait := value.AsDuration()
	if wait <= 0 {
		return 30 * time.Second
	}
	return wait
}
