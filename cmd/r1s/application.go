package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
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
	ctx      context.Context
	stdout   io.Writer
	endpoint *rns.Endpoint
	client   *client.Client
	identity []byte

	tunnelDialer tunnel.Dialer

	// clusterSelector is the resolved cluster ID/prefix from the command line,
	// retained so the detached parent can re-execute the run child against the
	// same cluster (the child has its own ephemeral identity).
	clusterSelector string

	// tunnelSessions caches the single authenticated mesh pair per execution
	// (F19 Model A: one live session per execution, many streams). A port-forward
	// opens one tunnel stream per inbound TCP connection; all of them share the
	// same pair, so concurrent browser connections need not each mint their own
	// grant (which the allocator would reject as a busy session). A record is
	// replaced when a request names ports the cached pair does not authorize.
	tunnelSessionsMu sync.Mutex
	tunnelSessions   map[string]*tunnelPair

	// waiterRN is the correlation-addressable registry that routes inbound
	// control-plane envelopes to the awaiting workflows (offer collection,
	// inspection, lease ack, logs, tunnel open) by correlation ID. A single
	// envelope maps to the one message ID that requested it; routing by key
	// means several concurrent awaiters never steal or drop each other's
	// replies the way a shared consume-and-drop channel would.
	waitMu  sync.Mutex
	waiters map[string][]chan *r1sv1.Envelope

	// offerReleases is the stop callback of the background offer-release
	// worker (see start). nil until start has run.
	offerReleases func() []client.PendingRelease
}

// openRunApplication constructs the F22 run-oriented client. Its RNS identity
// and client engine exist only in memory for this process; allocator state is
// still durable and authoritative. There is no persistent client store, local
// API socket, or durable identity: each run is a fresh ephemeral identity.
func openRunApplication(ctx context.Context, options commandLine, stdout io.Writer) (*application, error) {
	clusterDirectory, err := cluster.DefaultDirectory()
	if err != nil {
		return nil, err
	}
	clusterKey, _, err := cluster.Resolve(clusterDirectory, options.clusterSelector)
	if err != nil {
		return nil, fmt.Errorf("select cluster (run 'r1s cluster list'): %w", err)
	}

	app := &application{ctx: ctx, stdout: stdout, clusterSelector: options.clusterSelector}
	app.endpoint, err = rns.New(rns.Config{
		EphemeralIdentity: true,
		ClusterKey:        clusterKey,
		NetworkWait:       options.networkWait,
	}, app.handleEnvelope)
	if err != nil {
		return nil, err
	}
	identityHash, err := hex.DecodeString(app.endpoint.Name())
	if err != nil {
		app.close()
		return nil, fmt.Errorf("decode ephemeral identity: %w", err)
	}
	app.identity = identityHash
	app.client, err = client.New(client.Config{Identity: identityHash})
	if err != nil {
		app.close()
		return nil, err
	}
	return app, nil
}

// start launches the ephemeral transport edge and the offer-release worker.
// Foreground runs call it; a detached parent never calls it (the child process
// starts its own).
func (a *application) start() error {
	if err := a.endpoint.Start(a.ctx); err != nil {
		return err
	}
	a.offerReleases = a.client.StartOfferReleases(a.ctx, a.endpoint.Send)
	return nil
}

func (a *application) stop(diagnostics io.Writer) {
	if a.offerReleases != nil {
		for _, release := range a.offerReleases() {
			fmt.Fprintf(diagnostics, "offer release pending allocator=%s offer=%s; retained for retry, lease expiry remains the fallback\n", release.Destination, release.Envelope.GetExecutionOfferRelease().GetOfferId())
		}
	}
	// Release the client edge's overlay node before anything else. No-op in
	// direct mode (no edge is built).
	if a.tunnelDialer != nil {
		_ = a.tunnelDialer.Close()
		a.tunnelDialer = nil
	}
	// Stop incoming callbacks before anything backed by this process is gone.
	_ = a.endpoint.Close()
}

func (a *application) close() {
	if a.endpoint != nil {
		_ = a.endpoint.Close()
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
