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
)

// Server serves the LocalClient gRPC service over a Unix socket.
type Server struct {
	r1sv1.UnimplementedLocalClientServer
	backend  Backend
	grpc     *grpc.Server
	listener net.Listener
}

// Backend is the durable client engine that the local API fronts. The r1s CLI
// application implements it; tests use an in-memory fake. It deliberately has
// no Reticulum, identity, or state-store vocabulary so the API stays a thin
// transport-agnostic frontend.
type Backend interface {
	// RunRequest runs the full request -> offer collection -> selection ->
	// assignment workflow and returns after the assignment is sent. A positive
	// keepAlive durably records a lease-holding intent for the assigned
	// execution; the service renewal loop keeps it alive.
	RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (requestID, executionID string, allocator []byte, err error)
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

	// Tunnel opens a live F14/F19 direct-access tunnel session to a running
	// execution and returns the connected, authenticated byte pipe plus the
	// minted grant ID. The implementation mints the access grant over the
	// control plane and dials the allocator edge; the returned connection
	// carries arbitrary raw bytes with per-direction half-close. The grant ID
	// is surfaced so the caller (or a preamble-aware Conn) can write the
	// one-time routing preamble before any payload is relayed.
	//
	// targets is the client-owned destination container port list sent in the
	// tunnel grant; the allocator binds it into the minted grant and only
	// proxies/splices to grant-carried destinations. At least one target is
	// required. targetPort selects the container port the stream is spliced to
	// from the client-supplied list; it is only a port reference — the
	// allocator resolves it against the client-supplied list, never the serve
	// process or the allocator's own configuration.
	Tunnel(ctx context.Context, executionID string, targets []tunnel.Target, targetPort uint16) (tunnel.Conn, string, error)
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

// ListenAndServe binds the Unix socket with the given permission and serves until ctx is
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
