package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mytecor/r1s/internal/allocator"
	"github.com/mytecor/r1s/internal/tunnel"
	"github.com/mytecor/r1s/internal/tunnel/yggdrasil"
)

// tunnelEdge is the allocator-side tunnel edge inside r1sd: the embedded
// Yggdrasil node, the overlay listener, and the accept loop that validates
// routing preambles against the allocator core and splices validated sessions
// to their granted target. It exists only under --tunnel-enabled.
type tunnelEdge struct {
	node     *yggdrasil.Node
	listener *yggdrasil.Listener
	// identitySeed is the persistent private identity material the node key
	// is HKDF-derived from; it stays in memory only.
	identitySeed []byte
}

// startTunnelEdge starts the embedded Yggdrasil node and the allocator-side
// overlay listener. The node key is HKDF-derived from the daemon's persistent
// identity (no key files); the listener's endpoint advertisement is what
// minted grants return. The accept loop runs on the daemon's serve goroutine
// budget: runAcceptLoop is called from serve.
func startTunnelEdge(identitySeed []byte, options commandLine) (*tunnelEdge, error) {
	node, err := yggdrasil.NewNode(identitySeed, yggdrasil.AllocatorNodeKeyContext, yggdrasil.NodeOptions{Peers: options.tunnelPeers})
	if err != nil {
		return nil, err
	}
	listener, err := yggdrasil.NewListener(node)
	if err != nil {
		_ = node.Close()
		return nil, err
	}
	return &tunnelEdge{node: node, listener: listener, identitySeed: identitySeed}, nil
}

// runAcceptLoop accepts inbound tunnel mesh connections until the edge closes.
// It is started after the allocator core exists, because validation calls into
// the allocator core. Each accepted pair is validated once (execution + grant +
// peer key) and promoted; the pair then yields authorized streams, each spliced
// independently to its allocator-resolved target slot (F19-01).
func (e *tunnelEdge) runAcceptLoop(ctx context.Context, core *allocator.Allocator) {
	for {
		incoming, err := e.listener.Accept()
		if err != nil {
			// Accept only errors when the edge is closing (daemon shutdown);
			// a malformed preamble from one peer is drained inside Accept and
			// the loop keeps running, so a single garbage peer cannot take
			// the tunnel edge down.
			return
		}
		session, err := core.AcceptTunnel(incoming.Preamble().ExecutionID, incoming.Preamble().GrantID, incoming.PeerKey())
		if err != nil {
			_ = incoming.Reject(classifyRejection(err), err.Error())
			continue
		}
		if err := incoming.Promote(session); err != nil {
			continue
		}
		// Splice every authorized stream this pair opens. A single pair may
		// carry several concurrent streams (SSH + HTTP + ...); each resolves
		// its own target slot and fails independently.
		go e.spliceSessionStreams(ctx, incoming)
	}
}

// spliceSessionStreams drains a promoted pair's authorized streams and splices
// each to its target slot until the pair closes.
func (e *tunnelEdge) spliceSessionStreams(ctx context.Context, incoming *yggdrasil.IncomingSession) {
	for {
		stream, err := incoming.AcceptStream()
		if err != nil {
			return
		}
		go spliceToTarget(stream.Conn, stream.Target)
	}
}

// classifyRejection maps an allocator-core accept rejection to a classified
// teardown reason. The mapping is deliberately coarse: the wire detail carries
// the exact message, and the reason only distinguishes authorization
// outcomes from lifecycle ones.
func classifyRejection(err error) tunnel.Reason {
	switch {
	case errors.Is(err, allocator.ErrUnauthorized), errors.Is(err, tunnel.ErrPeerKeyMismatch), errors.Is(err, tunnel.ErrUnknownTargetSlot):
		return tunnel.ReasonUnauthorized
	case errors.Is(err, tunnel.ErrGrantExpired):
		return tunnel.ReasonGrantExpired
	case errors.Is(err, tunnel.ErrGrantNotFound), errors.Is(err, tunnel.ErrGrantReused):
		return tunnel.ReasonGrantRejected
	case errors.Is(err, tunnel.ErrSessionBusy):
		return tunnel.ReasonGrantRejected
	case errors.Is(err, allocator.ErrExecutionNotFound), errors.Is(err, allocator.ErrInvalidTransition):
		return tunnel.ReasonExecutionEnded
	default:
		return tunnel.ReasonSessionFailed
	}
}

// spliceToTarget relays bytes between the tunnel session and the granted
// allocator-local target until either side closes. The target was resolved at
// grant time from allocator-local configuration and validated at accept; the
// relay never interprets the payload.
func spliceToTarget(conn tunnel.Conn, target tunnel.Target) {
	defer conn.Close()
	targetConn, err := net.DialTimeout("tcp", net.JoinHostPort(target.Host, fmt.Sprint(target.Port)), 10*time.Second)
	if err != nil {
		_ = conn.(tunnel.ReasonCloser).CloseWithReason(tunnel.ReasonSessionFailed, "dial target: "+err.Error())
		return
	}
	defer targetConn.Close()
	relay(conn, targetConn)
}

// relay moves bytes both directions between the tunnel and the target until
// both directions end. Half-close propagation: a read EOF on one side
// half-closes the write side of the other, so interactive protocols behave;
// a hard failure (read error other than clean close) tears both down.
func relay(client tunnel.Conn, target net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		_, _ = io.Copy(target, client)
		// The tunnel's client->allocator direction ended (stdin EOF or session
		// end): half-close the target's write side so a half-duplex protocol
		// still sees a clean EOF.
		if tcp, ok := target.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()
	go func() {
		_, _ = io.Copy(client, target)
		_ = client.CloseWrite()
		done <- struct{}{}
	}()
	<-done
	<-done
}
