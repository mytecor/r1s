// Package localapi is the client side of the r1s local client API. It lets a
// CLI or application reuse a persistent `r1s serve` client identity over a
// Unix socket instead of rebuilding RNS, identity, and durable client state for
// every command.
package localapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ErrUnavailable reports that no local API server is reachable at the requested
// socket. Callers surface it without falling back to creating a new identity.
var ErrUnavailable = errors.New("local r1s service is not running")

// Client talks to one local r1s service over a Unix socket.
type Client struct {
	conn       *grpc.ClientConn
	local      r1sv1.LocalClientClient
	socketPath string
}

// Dial connects to the local API service at socketPath.
func Dial(socketPath string) (*Client, error) {
	if socketPath == "" {
		return nil, errors.New("local API socket path is required")
	}
	conn, err := grpc.NewClient(
		"passthrough:///r1s-local",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return net.Dial("unix", socketPath)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to local r1s service: %w", err)
	}
	return &Client{conn: conn, local: r1sv1.NewLocalClientClient(conn), socketPath: socketPath}, nil
}

// Close releases the gRPC connection.
func (c *Client) Close() error { return c.conn.Close() }

// Ping verifies a live r1s service is reachable at the socket.
func (c *Client) Ping(ctx context.Context) error {
	if !c.socketAlive() {
		return ErrUnavailable
	}
	// Confirm the socket speaks the r1s local API and reach it quickly.
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := c.local.List(probe, &r1sv1.LocalListRequest{})
	if err != nil {
		// The Unix socket is up, but the peer is not our service.
		return ErrUnavailable
	}
	return nil
}

func (c *Client) socketAlive() bool {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Request runs the full local request workflow.
// Request runs the full local request workflow. A positive keepAlive durably
// records a lease-holding intent for the assigned execution; the service
// renewal loop then keeps it alive and re-requests the workload if the lease
// is ever lost.
func (c *Client) Request(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (requestID, executionID string, allocator []byte, err error) {
	response, err := c.local.Request(ctx, &r1sv1.LocalRequest{
		Workload: workload, Policy: policy, ResourceClass: resourceClass,
		OfferWait: durationpb.New(offerWait), Allocators: allocators, KeepAlive: durationpb.New(keepAlive), Constraints: constraints,
	})
	if err != nil {
		return "", "", nil, err
	}
	return response.GetRequestId(), response.GetExecutionId(), response.GetAllocator(), nil
}

// Inspect returns the allocator state for an execution.
func (c *Client) Inspect(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	response, err := c.local.Inspect(ctx, &r1sv1.LocalInspectRequest{ExecutionId: executionID, Wait: durationpb.New(wait)})
	if err != nil {
		return nil, err
	}
	return response.GetState(), nil
}

// Result returns the terminal state, refusing non-terminal results.
func (c *Client) Result(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	response, err := c.local.Result(ctx, &r1sv1.LocalResultRequest{ExecutionId: executionID, Wait: durationpb.New(wait)})
	if err != nil {
		return nil, err
	}
	return response.GetState(), nil
}

// Cancel cancels an execution and returns its state.
func (c *Client) Cancel(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	response, err := c.local.Cancel(ctx, &r1sv1.LocalCancelRequest{ExecutionId: executionID, Reason: reason, Wait: durationpb.New(wait)})
	if err != nil {
		return nil, err
	}
	return response.GetState(), nil
}

// Logs retrieves one bounded explicit log read.
func (c *Client) Logs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.LocalLogsResponse, error) {
	return c.local.Logs(ctx, &r1sv1.LocalLogsRequest{
		ExecutionId: executionID, Stream: stream, Offset: offset, MaxBytes: maxBytes, Wait: durationpb.New(wait),
	})
}

// List returns saved client requests.
func (c *Client) List(ctx context.Context) (*r1sv1.LocalListResponse, error) {
	return c.local.List(ctx, &r1sv1.LocalListRequest{})
}

// Watch opens a streaming watch from a durable position.
func (c *Client) Watch(ctx context.Context, after uint64) (r1sv1.LocalClient_WatchClient, error) {
	return c.local.Watch(ctx, &r1sv1.LocalWatchRequest{After: after})
}

// Tunnel opens a bidirectional raw-byte tunnel to a running execution through
// the local r1s serve bridge. The stream's first message is the open naming the
// execution; after that it carries raw payload bytes (data) and close
// half-close/full-close signals in both directions. Setup failures surface as a
// stream error before any payload is relayed.
//
// targets is the client-owned destination slot list sent in the tunnel grant
// request; at least one destination is required. targetSlot selects the slot
// the stream is spliced to from the supplied list; empty selects the unnamed
// default slot (the interactive pipe). It is a slot reference only, resolved by
// the allocator against the client-supplied list, never a raw (host, port).
func (c *Client) Tunnel(ctx context.Context, executionID string, targets []tunnel.Target, targetSlot string) (r1sv1.LocalClient_TunnelClient, error) {
	stream, err := c.local.Tunnel(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Open{Open: &r1sv1.LocalTunnelOpen{
		ExecutionId: executionID, TargetSlot: targetSlot, Targets: protocol.TargetsToProto(targets),
	}}}); err != nil {
		_ = stream.CloseSend()
		return nil, err
	}
	return stream, nil
}
