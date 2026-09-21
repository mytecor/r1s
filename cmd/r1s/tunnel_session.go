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

// Client-side F19 tunnel session management: the per-execution cache of
// authenticated mesh pairs, the grant mint over the control plane, and the
// edge dial with its routing preamble. This is client application wiring, not
// the yggdrasil adapter (internal/tunnel/yggdrasil) and not the core tunnel
// contract (internal/tunnel); it stays in cmd/r1s because it depends on the
// yggdrasil adapter to derive the client's edge node key from the identity.

// openTunnelStream targets a live execution's container port and returns a
// byte pipe that relays the client's tunnel stream to it. It is the
// service-backed path for `r1s tunnel`: only a serve process holds the F17
// keep-alive intent that keeps the execution alive for the session, so a
// direct-mode client is rejected before this point.
func (a *application) openTunnelStream(ctx context.Context, executionID string, targets []tunnel.Target, targetPort uint16) (tunnel.Conn, string, error) {
	if a.tunnelDialer == nil {
		return nil, "", errors.New("tunnel: client edge is not configured; start 'r1s serve' with the tunnel edge enabled")
	}
	if targetPort == 0 {
		return nil, "", errors.New("tunnel: a target container port is required")
	}
	pair, grantID, err := a.tunnelSession(ctx, executionID, targets)
	if err != nil {
		return nil, "", err
	}
	so, ok := pair.(tunnel.StreamOpener)
	if !ok {
		_ = pair.Close()
		return nil, "", errors.New("tunnel: the transport cannot open streams")
	}
	stream, err := so.OpenStream(targetPort)
	if err != nil {
		return nil, "", fmt.Errorf("tunnel: open target port %d: %w", targetPort, err)
	}
	return stream, grantID, nil
}

// tunnelPair is the cached authenticated mesh pair for one execution. It holds
// the connected pair (also a tunnel.Conn) and the container ports
// authorized by the grant that established it.
type tunnelPair struct {
	executionID string
	ports       map[uint16]bool
	conn        tunnel.Conn
	grantID     string
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

// tunnelSession returns the established, preamble-written mesh pair for the
// execution, reusing the cached pair when it already authorizes the requested
// container ports and re-establishing it (mint grant, dial edge, write
// preamble) otherwise. It serializes establishment so concurrent tunnel requests
// never race to mint two grants for one execution (F19 Model A: one live session
// per execution, many streams). The returned grant ID is the one that
// established the pair.
func (a *application) tunnelSession(ctx context.Context, executionID string, targets []tunnel.Target) (tunnel.Conn, string, error) {
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
			return tp.conn, tp.grantID, nil
		}
		_ = tp.conn.Close()
		delete(a.tunnelSessions, executionID)
	}
	peerKey, err := yggdrasil.NodePubKey(a.identity, yggdrasil.ClientNodeKeyContext)
	if err != nil {
		return nil, "", fmt.Errorf("tunnel: derive client edge key: %w", err)
	}
	destination, envelope, err := a.client.TunnelGrant(executionID, peerKey, targets)
	if err != nil {
		return nil, "", err
	}
	ch, cancel := a.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := a.send(destination, envelope); err != nil {
		return nil, "", fmt.Errorf("tunnel: request grant: %w", err)
	}
	ack, err := a.awaitTunnelGrantAck(ctx, executionID, ch)
	if err != nil {
		return nil, "", err
	}
	endpoint := tunnel.Endpoint{Address: ack.GetAllocatorEndpoint(), PubKey: ack.GetAllocatorEndpointPubkey()}
	if len(endpoint.Address) == 0 || len(endpoint.PubKey) == 0 {
		return nil, "", errors.New("tunnel: allocator advertised no tunnel endpoint")
	}
	conn, err := a.tunnelDialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, "", fmt.Errorf("tunnel: dial allocator edge: %w", err)
	}
	if pw, ok := conn.(tunnel.PreambleWriter); ok {
		if err := pw.WritePreamble(tunnel.Preamble{ExecutionID: executionID, GrantID: ack.GetGrantId()}); err != nil {
			_ = conn.Close()
			return nil, "", fmt.Errorf("tunnel: write routing preamble: %w", err)
		}
	}
	a.tunnelSessions[executionID] = &tunnelPair{executionID: executionID, ports: desired, conn: conn, grantID: ack.GetGrantId()}
	return conn, ack.GetGrantId(), nil
}

// awaitTunnelGrantAck waits for the allocator's mint reply on the waiter
// channel registered for the grant request's correlation ID.
func (a *application) awaitTunnelGrantAck(ctx context.Context, executionID string, ch chan *r1sv1.Envelope) (*r1sv1.ExecutionTunnelGrantAck, error) {
	timer := time.NewTimer(grantAckTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("tunnel: timed out waiting for a grant for execution %s", executionID)
		case envelope := <-ch:
			if err := client.RemoteFailure(envelope); err != nil {
				return nil, fmt.Errorf("tunnel: grant rejected: %w", err)
			}
			if ack := envelope.GetExecutionTunnelGrantAck(); ack != nil && ack.GetExecutionId() == executionID {
				return ack, nil
			}
		}
	}
}
