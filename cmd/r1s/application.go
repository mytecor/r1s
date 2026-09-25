package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/cluster"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/transport/rns"
	"github.com/mytecor/r1s/internal/tunnel"
)

type application struct {
	ctx          context.Context
	stdout       io.Writer
	endpoint     *rns.Endpoint
	client       *client.Client
	store        *statebolt.Store
	finish       func() []client.PendingRelease
	identity     []byte
	tunnelDialer tunnel.Dialer

	// tunnelSessions caches the single authenticated mesh pair per execution
	// (F19 Model A: one live session per execution, many streams). A port-forward
	// opens one tunnel stream per inbound TCP connection; all of them share the
	// same pair, so concurrent browser connections need not each mint their own
	// grant (which the allocator would reject as a busy session). A record is
	// replaced when a request names ports the cached pair does not authorize.
	tunnelSessionsMu sync.Mutex
	tunnelSessions   map[string]*tunnelPair

	// waitMu guards waiters, the registry that routes inbound control-plane
	// envelopes to the awaiting workflows (request, inspect, cancel, lease
	// renewal, logs, tunnel grant) by correlation ID. A single envelope maps to
	// the one message ID that requested it; routing by key means several
	// concurrent awaiters never steal or drop each other's replies the way a
	// shared consume-and-drop channel would.
	waitMu  sync.Mutex
	waiters map[string][]chan *r1sv1.Envelope
}

func openApplication(ctx context.Context, options commandLine, stdout io.Writer) (*application, error) {
	clusterSource, err := clientClusterSource(options.clusterSource)
	if err != nil {
		return nil, err
	}
	clusterKey, err := cluster.LoadSource(clusterSource)
	if err != nil {
		return nil, fmt.Errorf("load cluster membership from %s (run 'r1s cluster init', 'r1s cluster join <token>', or pass '--cluster r1s1:<secret>'): %w", cluster.SourceLabel(clusterSource), err)
	}
	identityDirectory, err := identityDataDirectory(options.identitySource)
	if err != nil {
		return nil, err
	}

	app := &application{ctx: ctx, stdout: stdout}
	app.endpoint, err = rns.New(rns.Config{
		IdentitySource: options.identitySource,
		ClusterKey:     clusterKey,
		NetworkWait:    options.networkWait,
	}, app.handleEnvelope)
	if err != nil {
		return nil, err
	}
	identityHash, err := hex.DecodeString(app.endpoint.Name())
	if err != nil {
		app.close()
		return nil, fmt.Errorf("decode local identity: %w", err)
	}
	app.identity = identityHash
	statePath := options.statePath
	if strings.TrimSpace(statePath) == "" {
		if rns.IsInlineIdentitySource(options.identitySource) {
			statePath = filepath.Join(identityDirectory, app.endpoint.Name()+".client.db")
		} else {
			statePath = options.identitySource + ".client.db"
		}
	}
	app.store, err = statebolt.Open(statePath)
	if err != nil {
		app.close()
		return nil, err
	}
	app.client, err = client.New(client.Config{Identity: identityHash, Store: app.store})
	if err != nil {
		app.close()
		return nil, err
	}
	return app, nil
}

func (a *application) start() error {
	if err := a.endpoint.Start(a.ctx); err != nil {
		return err
	}
	a.finish = a.client.StartOfferReleases(a.ctx, a.endpoint.Send)
	return nil
}

func (a *application) stop(diagnostics io.Writer) {
	if a.finish != nil {
		for _, release := range a.finish() {
			fmt.Fprintf(diagnostics, "offer release pending allocator=%s offer=%s; retained for retry, lease expiry remains the fallback\n", release.Destination, release.Envelope.GetExecutionOfferRelease().GetOfferId())
		}
	}
	// Release the client edge's overlay node before the durable client store is
	// closed. No-op in direct mode (no edge is built).
	if a.tunnelDialer != nil {
		_ = a.tunnelDialer.Close()
		a.tunnelDialer = nil
	}
	// Stop incoming callbacks before the durable client store is closed.
	_ = a.endpoint.Close()
}

func (a *application) close() {
	if a.endpoint != nil {
		_ = a.endpoint.Close()
	}
	if a.store != nil {
		_ = a.store.Close()
	}
}
