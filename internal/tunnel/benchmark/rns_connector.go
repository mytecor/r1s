package benchmark

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/mytecor/r1s/internal/tunnel/rns"
)

// rnsClusterKey is a fixed 256-bit cluster secret for benchmark runs. It is
// deliberately a constant so both edges derive the same IFAC passphrase; it
// carries no secrecy in a benchmark.
var rnsClusterKey = []byte("r1s-benchmark-cluster-key-00000000000000000000")[:32]

// rnsConnector is the F21-01 private tunnel RNS transport: each logical
// connection is one Backbone/TCP-provisioned RNS Link (one Link = one stream),
// established by the client Dialer and accepted by the allocator Listener.
type rnsConnector struct {
	listener *rns.Listener
	dialer   *rns.Dialer
	ctx      context.Context
	cancel   context.CancelFunc
}

// newRNSConnector builds and starts an allocator Listener and a client Dialer
// for the RNS tunnel data plane.
func newRNSConnector() (*rnsConnector, error) {
	root, err := os.MkdirTemp("", "r1s-bench-rns")
	if err != nil {
		return nil, err
	}
	port, err := freeTCPPort()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	listener, err := rns.NewListener(rns.ListenerConfig{
		IdentitySource: filepath.Join(root, "allocator.identity"),
		ClusterKey:     rnsClusterKey,
		BindAddress:    "127.0.0.1",
		ListenPort:     port,
		ConfigPath:     filepath.Join(root, "allocator-reticulum"),
		LogLevel:       0,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	if err := listener.Start(ctx); err != nil {
		_ = listener.Close()
		cancel()
		return nil, err
	}
	dialer, err := rns.NewDialer(rns.DialerConfig{
		IdentitySource: filepath.Join(root, "client.identity"),
		ClusterKey:     rnsClusterKey,
		ConfigPath:     filepath.Join(root, "client-reticulum"),
		LogLevel:       0,
	})
	if err != nil {
		_ = listener.Close()
		cancel()
		return nil, err
	}
	if err := dialer.Start(ctx); err != nil {
		_ = dialer.Close()
		_ = listener.Close()
		cancel()
		return nil, err
	}
	return &rnsConnector{listener: listener, dialer: dialer, ctx: ctx, cancel: cancel}, nil
}

// Name implements Connector.
func (c *rnsConnector) Name() string { return "RNS" }

// Open establishes one tunnel Link from the dialer to the listener and returns
// both ends of the single stream. Each call is an independent Link, matching
// the one-connection-per-Link model.
func (c *rnsConnector) Open(ctx context.Context) (client, allocator Conn, err error) {
	endpoint := c.listener.Endpoint()
	acceptCtx, cancelAccept := context.WithCancel(ctx)
	defer cancelAccept()
	accepted := make(chan *rnsSessionPair, 1)
	acceptErr := make(chan error, 1)
	go func() {
		aConn, err := c.listener.Accept(acceptCtx)
		if err != nil {
			acceptErr <- err
			return
		}
		// The allocator side of the RNS Link is the Conn returned by Accept.
		accepted <- &rnsSessionPair{allocator: aConn}
	}()

	clientConn, err := c.dialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("rns dial: %w", err)
	}
	select {
	case pair := <-accepted:
		return clientConn, pair.allocator, nil
	case err := <-acceptErr:
		_ = clientConn.Close()
		return nil, nil, fmt.Errorf("rns accept: %w", err)
	case <-ctx.Done():
		_ = clientConn.Close()
		return nil, nil, ctx.Err()
	}
}

// Close implements Connector.
func (c *rnsConnector) Close() error {
	c.cancel()
	var first error
	if err := c.dialer.Close(); err != nil && first == nil {
		first = err
	}
	if err := c.listener.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// rnsSessionPair carries the allocator-side Conn from an Accept (client side is
// returned directly by Open).
type rnsSessionPair struct {
	allocator Conn
}

// freeTCPPort returns an available TCP port on loopback.
func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port, nil
}
