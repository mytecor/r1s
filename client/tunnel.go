package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	coreclient "github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/tunnel"
)

// TunnelTarget is a container port authorized for a run-owned tunnel session.
type TunnelTarget struct {
	Port uint16
}

// TunnelEndpoint is the allocator tunnel edge returned by an authenticated
// owner-open request.
type TunnelEndpoint struct {
	Address []byte
	PubKey  []byte
}

// OpenTunnel explicitly asks the selected allocator to authorize peerKey for
// the given container ports. It performs only the authenticated control-plane
// operation; the caller chooses and owns the application-data transport.
func (c *Client) OpenTunnel(ctx context.Context, executionID string, peerKey []byte, targets []TunnelTarget) (TunnelEndpoint, error) {
	if ctx == nil {
		return TunnelEndpoint{}, errors.New("client: context is required")
	}
	internalTargets := make([]tunnel.Target, 0, len(targets))
	for _, target := range targets {
		internalTargets = append(internalTargets, tunnel.Target{Port: target.Port})
	}
	destination, envelope, err := c.core.TunnelOpen(executionID, peerKey, internalTargets)
	if err != nil {
		return TunnelEndpoint{}, err
	}
	waiter, cancel := c.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := c.send(ctx, destination, envelope); err != nil {
		return TunnelEndpoint{}, fmt.Errorf("tunnel: request open: %w", err)
	}
	timer := time.NewTimer(defaultOperationWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return TunnelEndpoint{}, ctx.Err()
		case <-timer.C:
			return TunnelEndpoint{}, fmt.Errorf("tunnel: timed out waiting for allocator to accept execution %s", executionID)
		case response := <-waiter:
			if err := coreclient.RemoteFailure(response); err != nil {
				return TunnelEndpoint{}, fmt.Errorf("tunnel: open rejected: %w", err)
			}
			if ack := response.GetExecutionTunnelOpenAck(); ack != nil && ack.GetExecutionId() == executionID {
				if len(ack.GetAllocatorEndpoint()) == 0 || len(ack.GetAllocatorEndpointPubkey()) == 0 {
					return TunnelEndpoint{}, errors.New("tunnel: allocator returned no tunnel endpoint")
				}
				return TunnelEndpoint{Address: bytes.Clone(ack.GetAllocatorEndpoint()), PubKey: bytes.Clone(ack.GetAllocatorEndpointPubkey())}, nil
			}
		}
	}
}
