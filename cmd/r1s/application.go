package main

import (
	"context"
	"fmt"
	"io"

	r1sclient "github.com/mytecor/r1s/client"
	"github.com/mytecor/r1s/internal/tunnel"
	"github.com/mytecor/r1s/internal/tunnel/yggdrasil"
)

// application is the F22 run-oriented client process. Its RNS identity and
// client engine exist only in memory for this process; allocator state is
// still durable and authoritative. There is no persistent client database,
// local gRPC service, Unix command socket, or durable lease intent (F22-07):
// a restart of the client never resumes a run, and the allocator retains the
// execution database.
type application struct {
	ctx        context.Context
	stdout     io.Writer
	controller *r1sclient.Client
	identity   []byte

	tunnelDialer tunnel.Dialer

	// clusterSelector is the resolved cluster ID/prefix from the command line,
	// retained so the detached parent can re-execute the run child against the
	// same cluster (the child has its own ephemeral identity).
	clusterSelector string
}

// openRunApplication constructs the F22 run-oriented client. Its RNS identity
// and client engine exist only in memory for this process; allocator state is
// still durable and authoritative. There is no persistent client store, local
// API socket, or durable identity: each run is a fresh ephemeral identity.
func openRunApplication(ctx context.Context, options commandLine, stdout io.Writer) (*application, error) {
	app := &application{ctx: ctx, stdout: stdout, clusterSelector: options.clusterSelector}
	var err error
	app.controller, err = r1sclient.Open(options.clusterSelector, r1sclient.Config{NetworkWait: options.networkWait})
	if err != nil {
		return nil, fmt.Errorf("open run controller (run 'r1s cluster list'): %w", err)
	}
	app.identity = app.controller.Identity()
	return app, nil
}

// start launches the ephemeral transport edge and the offer-release worker.
// Foreground runs call it; a detached parent never calls it (the child process
// starts its own).
func (a *application) start() error {
	return a.controller.Start(a.ctx)
}

func (a *application) stop(diagnostics io.Writer) {
	// Release the client edge's overlay node before anything else. No-op in
	// direct mode (no edge is built).
	if a.tunnelDialer != nil {
		_ = a.tunnelDialer.Close()
		a.tunnelDialer = nil
	}
	if a.controller != nil {
		for _, release := range a.controller.Close() {
			fmt.Fprintf(diagnostics, "offer release pending allocator=%s offer=%s; retained for retry, lease expiry remains the fallback\n", release.Destination, release.OfferID)
		}
	}
}

func (a *application) close() {
	if a.controller != nil {
		a.controller.Close()
	}
}

// ensureRunTunnelEdge builds the run process's client-side Yggdrasil edge (node
// + dialer) on demand, deriving its overlay node key from the run's ephemeral
// identity seed. It is built only when `r1s run -p` publishes ports, so a run
// without published ports never starts an overlay node. The node key is stable
// for the run's lifetime (the identity seed persists in-memory), so published
// listeners stay bound and re-establish across attempts against the same key.
func (a *application) ensureRunTunnelEdge() error {
	if a.tunnelDialer != nil {
		return nil
	}
	node, err := yggdrasil.NewNode(a.identity, yggdrasil.ClientNodeKeyContext, yggdrasil.NodeOptions{})
	if err != nil {
		return fmt.Errorf("tunnel edge: %w", err)
	}
	dialer, err := yggdrasil.NewDialer(node)
	if err != nil {
		_ = node.Close()
		return fmt.Errorf("tunnel edge: %w", err)
	}
	a.tunnelDialer = dialer
	return nil
}
