// Package localserver exposes one persistent local r1s client over a versioned
// gRPC API on a Unix socket. It is a local frontend for a single client
// identity: it owns no allocator state, performs no global scheduling, and is
// never a remotely reachable cluster-wide control endpoint by default.
package localserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Backend is the durable client engine that the local API fronts. The r1s CLI
// application implements it; tests use an in-memory fake. It deliberately has
// no Reticulum, identity, or state-store vocabulary so the API stays a thin
// transport-agnostic frontend.
type Backend interface {
	// RunRequest runs the full request -> offer collection -> selection ->
	// assignment workflow and returns after the assignment is sent. A positive
	// keepAlive durably records a lease-holding intent for the assigned
	// execution; the service renewal loop keeps it alive.
	RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration) (requestID, executionID string, allocator []byte, err error)
	// Inspect returns the allocator's latest durable state, waiting up to wait.
	Inspect(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error)
	// Cancel cancels an execution and waits up to wait for its state.
	Cancel(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error)
	// Logs retrieves one bounded explicit log read.
	Logs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error)
	// Requests returns sorted client request views.
	Requests(ctx context.Context) []client.RequestSnapshot
	// State returns the client's locally retained execution state, if any.
	State(ctx context.Context, executionID string) (*r1sv1.ExecutionState, bool, error)
	// WatchSeq returns the highest durable watch sequence.
	WatchSeq(ctx context.Context) uint64
	// WatchAfter returns transitions after a durable position.
	WatchAfter(ctx context.Context, after uint64) ([]client.WatchEvent, bool)
	// SubscribeWatch registers a live observer returning its cancel.
	SubscribeWatch(ctx context.Context, observer func(client.WatchEvent)) (cancel func())

	// Tunnel opens a live F14 direct-access tunnel session to a running
	// execution and returns the connected, authenticated byte pipe plus the
	// minted grant ID. The implementation mints the access grant over the
	// control plane and dials the allocator edge; the returned connection
	// carries arbitrary raw bytes with per-direction half-close. The grant ID
	// is surfaced so the caller (or a preamble-aware Conn) can write the
	// one-time routing preamble before any payload is relayed.
	Tunnel(ctx context.Context, executionID string) (tunnel.Conn, string, error)
}

// Server serves the LocalClient gRPC service over a Unix socket.
type Server struct {
	r1sv1.UnimplementedLocalClientServer
	backend  Backend
	grpc     *grpc.Server
	listener net.Listener
}

// New returns a server for one local client backend.
func New(backend Backend) *Server {
	return &Server{backend: backend}
}

// SocketPermission is the Unix socket file mode applied when the local API
// socket is created. Callers may relax it explicitly; the default is 0600.
type SocketPermission os.FileMode

const (
	// DefaultSocketPermission restricts the local API to the owning user.
	DefaultSocketPermission SocketPermission = 0o600
)

// Listen binds the Unix socket with the given permission and serves until ctx is
// cancelled. An existing stale socket file is replaced only if it is not alive.
// Blocks until the context is cancelled or an unrecoverable error occurs.
func (s *Server) ListenAndServe(ctx context.Context, socketPath string, permission SocketPermission) error {
	if existing, err := net.Dial("unix", socketPath); err == nil {
		_ = existing.Close()
		return fmt.Errorf("local API socket %s is already in use", socketPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		// A dead socket file from a previous run is removed below.
		var opErr *net.OpError
		if !errors.As(err, &opErr) {
			return fmt.Errorf("probe local API socket %s: %w", socketPath, err)
		}
	}
	if err := os.RemoveAll(socketPath); err != nil {
		return fmt.Errorf("remove stale local API socket %s: %w", socketPath, err)
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return fmt.Errorf("create local API socket directory: %w", err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on local API socket %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, os.FileMode(permission)); err != nil {
		_ = listener.Close()
		return fmt.Errorf("chmod local API socket %s: %w", socketPath, err)
	}
	s.listener = listener
	s.grpc = grpc.NewServer()
	r1sv1.RegisterLocalClientServer(s.grpc, s)
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.grpc.Serve(listener) }()

	select {
	case <-ctx.Done():
		s.grpc.GracefulStop()
		<-serveDone
		_ = os.Remove(socketPath)
		return nil
	case err := <-serveDone:
		if err != nil {
			_ = os.Remove(socketPath)
			return err
		}
		return nil
	}
}

// Request implements the unary local request workflow.
func (s *Server) Request(ctx context.Context, in *r1sv1.LocalRequest) (*r1sv1.LocalRequestResponse, error) {
	offerWait := in.GetOfferWait().AsDuration()
	if offerWait <= 0 {
		offerWait = 10 * time.Second
	}
	if in.GetKeepAlive().AsDuration() < 0 {
		return nil, errors.New("request: keep-alive must not be negative")
	}
	requestID, executionID, allocator, err := s.backend.RunRequest(ctx, in.GetWorkload(), in.GetPolicy(), in.GetResourceClass(), offerWait, in.GetAllocators(), in.GetKeepAlive().AsDuration())
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
	if !terminal(state.GetPhase()) {
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

// Watch streams durable execution-state transitions in revision order. It
// replays every retained transition after the requested position (the durable
// journal), then follows new transitions through the journal as they commit.
// A slow consumer that falls behind the retained journal is told to
// re-synchronize from a fresh position instead of silently missing revisions.
func (s *Server) Watch(in *r1sv1.LocalWatchRequest, stream r1sv1.LocalClient_WatchServer) error {
	after := in.GetAfter()
	ctx := stream.Context()

	// The journal is authoritative: never deliver an event that did not durably
	// commit. The live observer only wakes this loop; the journal is re-read
	// from the durable position so nothing is dropped for slow consumers.
	wake := make(chan struct{}, 1)
	cancel := s.backend.SubscribeWatch(ctx, func(client.WatchEvent) {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	defer cancel()

	for {
		events, contiguous := s.backend.WatchAfter(ctx, after)
		if !contiguous {
			// The watcher's durable position fell off the retained journal.
			// Tell it to re-synchronize from the current position.
			after = s.backend.WatchSeq(ctx)
			if err := stream.Send(&r1sv1.LocalWatchEvent{Sequence: after, Resync: true}); err != nil {
				return err
			}
			// Replay from the fresh position so the watcher rebuilds state.
			if events, contiguous = s.backend.WatchAfter(ctx, after); !contiguous {
				return errors.New("watch lost its durable position without a recoverable one")
			}
		}
		for _, event := range events {
			if err := stream.Send(&r1sv1.LocalWatchEvent{Sequence: event.Sequence, ExecutionId: event.ExecutionID, State: event.State}); err != nil {
				return err
			}
			after = event.Sequence
		}
		if len(events) == 0 && !contiguous {
			return errors.New("watch lost its durable position without a recoverable one")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

func terminal(phase r1sv1.ExecutionPhase) bool {
	return phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
}

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
