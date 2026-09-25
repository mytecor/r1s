package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/tunnel"
	"github.com/mytecor/r1s/internal/tunnel/yggdrasil"
)

// Client-side F19/F22 tunnel session management for the run-oriented client:
// the authenticated owner open over the control plane, the edge dial with its
// routing preamble, and the per-execution cache of authenticated mesh pairs.
// This is client application wiring, not the yggdrasil adapter
// (internal/tunnel/yggdrasil) and not the core tunnel contract
// (internal/tunnel); it stays in cmd/r1s because it depends on the yggdrasil
// adapter to derive the client's edge node key from the identity.

// tunnelPair is the cached authenticated mesh pair for one execution. It holds
// the connected pair (also a tunnel.Conn) and the container ports authorized
// by the owner open that established it.
type tunnelPair struct {
	executionID string
	ports       map[uint16]bool
	conn        tunnel.Conn
}

// targetPortSet computes the set of authorized container ports from a target
// list.
func targetPortSet(targets []tunnel.Target) map[uint16]bool {
	set := make(map[uint16]bool, len(targets))
	for _, t := range targets {
		set[t.Port] = true
	}
	return set
}

// portsSuperset reports whether set contains every port in want.
func portsSuperset(set, want map[uint16]bool) bool {
	for p := range want {
		if !set[p] {
			return false
		}
	}
	return true
}

// openTunnelStream targets a live execution's container port and returns a byte
// pipe that relays the run's tunnel stream to it. It is the run-owned path for
// `r1s run -p`: the run process builds its own client edge and holds the
// execution lease, so a direct-mode run is valid here (unlike the retired
// serve-backed `r1s tunnel`).
func (a *application) openTunnelStream(ctx context.Context, executionID string, targets []tunnel.Target, targetPort uint16) (tunnel.Conn, error) {
	if a.tunnelDialer == nil {
		return nil, errors.New("tunnel: client edge is not running")
	}
	if targetPort == 0 {
		return nil, errors.New("tunnel: a target container port is required")
	}
	pair, err := a.tunnelSession(ctx, executionID, targets)
	if err != nil {
		return nil, err
	}
	so, ok := pair.(tunnel.StreamOpener)
	if !ok {
		_ = pair.Close()
		return nil, errors.New("tunnel: the transport cannot open streams")
	}
	stream, err := so.OpenStream(targetPort)
	if err != nil {
		return nil, fmt.Errorf("tunnel: open target port %d: %w", targetPort, err)
	}
	return stream, nil
}

// tunnelSession returns the established, preamble-written mesh pair for the
// execution, reusing the cached pair when it already authorizes the requested
// container ports and re-establishing it (owner open, dial edge, write
// preamble) otherwise. It serializes establishment so concurrent tunnel
// requests never race to declare two opens for one execution.
func (a *application) tunnelSession(ctx context.Context, executionID string, targets []tunnel.Target) (tunnel.Conn, error) {
	a.tunnelSessionsMu.Lock()
	defer a.tunnelSessionsMu.Unlock()
	if a.tunnelSessions == nil {
		a.tunnelSessions = make(map[string]*tunnelPair)
	}
	desired := targetPortSet(targets)
	if tp := a.tunnelSessions[executionID]; tp != nil {
		// Reuse only when the cached pair already authorizes every requested
		// port; otherwise close it and re-establish with the current list.
		if portsSuperset(tp.ports, desired) {
			return tp.conn, nil
		}
		_ = tp.conn.Close()
		delete(a.tunnelSessions, executionID)
	}
	endpoint, err := a.tunnelEstablish(ctx, executionID, targets)
	if err != nil {
		return nil, err
	}
	a.tunnelSessions[executionID] = &tunnelPair{executionID: executionID, ports: desired, conn: endpoint}
	return endpoint, nil
}

// tunnelEstablish performs the owner open over the control plane and dials the
// allocator edge with the routing preamble. It is the F22-06 replacement for
// the retired mint/grant round trip: the authenticated sender is the owner, and
// the allocator binds the declared edge key and target list to the execution
// for its lifetime (no grant ID, no TTL).
func (a *application) tunnelEstablish(ctx context.Context, executionID string, targets []tunnel.Target) (tunnel.Conn, error) {
	peerKey, err := yggdrasil.NodePubKey(a.identity, yggdrasil.ClientNodeKeyContext)
	if err != nil {
		return nil, fmt.Errorf("tunnel: derive client edge key: %w", err)
	}
	destination, envelope, err := a.client.TunnelOpen(executionID, peerKey, targets)
	if err != nil {
		return nil, err
	}
	ch, cancel := a.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := a.send(destination, envelope); err != nil {
		return nil, fmt.Errorf("tunnel: request open: %w", err)
	}
	ack, err := a.awaitTunnelOpenAck(ctx, executionID, ch)
	if err != nil {
		return nil, err
	}
	ep := tunnel.Endpoint{Address: ack.GetAllocatorEndpoint(), PubKey: ack.GetAllocatorEndpointPubkey()}
	if len(ep.Address) == 0 || len(ep.PubKey) == 0 {
		return nil, errors.New("tunnel: allocator advertised no tunnel endpoint")
	}
	conn, err := a.tunnelDialer.Dial(ctx, ep)
	if err != nil {
		return nil, fmt.Errorf("tunnel: dial allocator edge: %w", err)
	}
	if pw, ok := conn.(tunnel.PreambleWriter); ok {
		if err := pw.WritePreamble(tunnel.Preamble{ExecutionID: executionID}); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("tunnel: write routing preamble: %w", err)
		}
	}
	return conn, nil
}

// awaitTunnelOpenAck waits for the allocator's open reply on the waiter
// channel registered for the open request's correlation ID.
func (a *application) awaitTunnelOpenAck(ctx context.Context, executionID string, ch chan *r1sv1.Envelope) (*r1sv1.ExecutionTunnelOpenAck, error) {
	timer := time.NewTimer(openAckTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("tunnel: timed out waiting for the allocator to accept the open for execution %s", executionID)
		case envelope := <-ch:
			if err := client.RemoteFailure(envelope); err != nil {
				return nil, fmt.Errorf("tunnel: open rejected: %w", err)
			}
			if ack := envelope.GetExecutionTunnelOpenAck(); ack != nil && ack.GetExecutionId() == executionID {
				return ack, nil
			}
		}
	}
}
