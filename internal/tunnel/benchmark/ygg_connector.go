package benchmark

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/mytecor/r1s/internal/tunnel"
	"github.com/mytecor/r1s/internal/tunnel/yggdrasil"
)

// yggConnector is the OLD embedded-Ygg tunnel transport (F19/F20): two
// embedded nodes peered over a local TCP link, one authenticated mesh pair per
// session carrying N multiplexed streams (N logical connections) with the
// routing preamble and accept frame. This is the transport F21-06 removes.
//
// The benchmark maps N logical connections to N streams on one established
// pair—the old transport's real concurrency model (one authenticated pair, many
// streams), exactly as the production edge and the F19-01 live-mesh acceptance
// exercise it. The pair is established once at construction (warm-up proves the
// handshake) and each Open opens one fresh stream on it.
type yggConnector struct {
	clientNode    *yggdrasil.Node
	allocatorNode *yggdrasil.Node
	listener      *yggdrasil.Listener
	dialer        *yggdrasil.Dialer
	endpoint      tunnel.Endpoint
	// linkListener is the TCP peer link between the two nodes. It must stay
	// alive for the mesh to keep both nodes in the same tree; it is cancelled
	// on Close.
	linkListener func()

	// mu serializes stream-open/accept pairing on the shared pair. Each Open
	// opens one client stream and accepts one allocator stream in order, so a
	// mutex keeps the two sides of the same stream matched.
	mu sync.Mutex
	// clientPair is the established, preamble-accepted client-side pair (a
	// StreamOpener); allocatorSession is the promoted allocator-side session
	// (AcceptStream).
	clientPair    tunnel.Conn
	allocatorPair *yggdrasil.IncomingSession
}

// newYggConnector builds the old-transport edges: two nodes peered over a
// local TCP link (mirroring the F19-01 live-mesh acceptance setup), one
// established/authorized pair, and a single stream opened on it for warm-up so
// the mesh path is converged before any measured Open.
func newYggConnector() (*yggConnector, error) {
	// Distinct, fixed seeds so both nodes derive stable, distinct overlay
	// identities and the mesh converges deterministically. These are benchmark
	// seeds only.
	clientSeed := []byte("bench-ygg-client-seed-00000000000000000000000000000")
	allocatorSeed := []byte("bench-ygg-allocator-seed-0000000000000000000000000")

	clientNode, err := yggdrasil.NewNode(clientSeed, yggdrasil.ClientNodeKeyContext, yggdrasil.NodeOptions{})
	if err != nil {
		return nil, fmt.Errorf("ygg client node: %w", err)
	}
	allocatorNode, err := yggdrasil.NewNode(allocatorSeed, yggdrasil.AllocatorNodeKeyContext, yggdrasil.NodeOptions{})
	if err != nil {
		_ = clientNode.Close()
		return nil, fmt.Errorf("ygg allocator node: %w", err)
	}

	linkListener, err := allocatorNode.Core().Listen(&url.URL{Scheme: "tcp", Host: "localhost:0"}, "")
	if err != nil {
		_ = clientNode.Close()
		_ = allocatorNode.Close()
		return nil, fmt.Errorf("ygg allocator link listen: %w", err)
	}
	if err := clientNode.Core().CallPeer(&url.URL{Scheme: "tcp", Host: linkListener.Addr().String()}, ""); err != nil {
		_ = clientNode.Close()
		_ = allocatorNode.Close()
		return nil, fmt.Errorf("ygg client call peer: %w", err)
	}
	if err := waitForYggMesh(clientNode, allocatorNode); err != nil {
		_ = clientNode.Close()
		_ = allocatorNode.Close()
		return nil, err
	}

	listener, err := yggdrasil.NewListener(allocatorNode)
	if err != nil {
		_ = clientNode.Close()
		_ = allocatorNode.Close()
		return nil, err
	}
	dialer, err := yggdrasil.NewDialer(clientNode)
	if err != nil {
		_ = listener.Close()
		_ = clientNode.Close()
		_ = allocatorNode.Close()
		return nil, err
	}

	result := &yggConnector{
		clientNode:    clientNode,
		allocatorNode: allocatorNode,
		listener:      listener,
		dialer:        dialer,
		endpoint:      listener.Endpoint(),
		linkListener:  linkListener.Cancel,
	}
	// Establish and authorize the single shared pair now (preamble + accept
	// frame, the same one-time handshake the production edge does on each
	// session), then open one throwaway stream so lazy mesh convergence is
	// finished before any measured Open.
	if err := result.establishPair(); err != nil {
		_ = result.Close()
		return nil, err
	}
	return result, nil
}

// establishPair runs the optional second half of the old transport's
// authenticated handshake: it dials the allocator, writes the routing preamble
// and waits for the allocator's accept frame, swapping the ordered AllowClient
// goroutine so the allocator side Accepts and Promotes concurrently.
// After it returns, result.clientPair and result.allocatorPair are set and both
// sides agree on one authorized session.
func (c *yggConnector) establishPair() error {
	type pairReady struct {
		incoming *yggdrasil.IncomingSession
		err      error
	}
	ready := make(chan pairReady, 1)
	go func() {
		incoming, err := c.listener.Accept()
		if err != nil {
			ready <- pairReady{err: err}
			return
		}
		// Promote sends the accept frame; without it the client's WritePreamble
		// would block forever waiting for authorization. It must run here, in
		// the accept goroutine, so the client's preamble returns.
		session := &tunnel.Session{
			ExecutionID: "bench-exec",
			PeerKey:     incoming.PeerKey(),
			Targets:     []tunnel.Target{{Port: 1}},
		}
		if err := incoming.Promote(session); err != nil {
			ready <- pairReady{err: err}
			return
		}
		ready <- pairReady{incoming: incoming}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := c.dialer.Dial(ctx, c.endpoint)
	if err != nil {
		return fmt.Errorf("ygg dial: %w", err)
	}
	pw, ok := conn.(tunnel.PreambleWriter)
	if !ok {
		_ = conn.Close()
		return errors.New("ygg conn must implement PreambleWriter")
	}
	if err := pw.WritePreamble(tunnel.Preamble{ExecutionID: "bench-exec"}); err != nil {
		_ = conn.Close()
		return fmt.Errorf("ygg write preamble: %w", err)
	}
	select {
	case r := <-ready:
		if r.err != nil {
			_ = conn.Close()
			return fmt.Errorf("ygg accept pair: %w", r.err)
		}
		c.clientPair = conn
		c.allocatorPair = r.incoming
	case <-ctx.Done():
		_ = conn.Close()
		return ctx.Err()
	}
	return nil
}

// Name implements Connector.
func (c *yggConnector) Name() string { return "Ygg" }

// Open opens one fresh logical stream on the established pair and returns its
// client-side and allocator-side ends. Under the old transport N logical
// connections are N multiplexed streams on one authenticated pair.
func (c *yggConnector) Open(ctx context.Context) (client, allocator Conn, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientPair == nil || c.allocatorPair == nil {
		return nil, nil, errors.New("ygg pair not established")
	}
	so, ok := c.clientPair.(tunnel.StreamOpener)
	if !ok {
		return nil, nil, errors.New("ygg conn must implement StreamOpener")
	}
	clientStream, err := so.OpenStream(1)
	if err != nil {
		return nil, nil, fmt.Errorf("ygg open stream: %w", err)
	}
	allocStream, err := c.allocatorPair.AcceptStream()
	if err != nil {
		_ = clientStream.Close()
		return nil, nil, fmt.Errorf("ygg accept stream: %w", err)
	}
	return clientStream, allocStream.Conn, nil
}

// Close implements Connector.
func (c *yggConnector) Close() error {
	var first error
	if c.linkListener != nil {
		c.linkListener()
	}
	for _, closer := range []interface{ Close() error }{
		c.dialer, c.listener, c.clientNode, c.allocatorNode,
	} {
		if err := closer.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// waitForYggMesh blocks until both nodes have each other in their tree, or
// times out, mirroring the F19-01 live-mesh acceptance helper.
func waitForYggMesh(client, allocator *yggdrasil.Node) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(client.Core().GetTree()) > 1 && len(allocator.Core().GetTree()) > 1 {
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("ygg mesh did not converge within 15s")
}
