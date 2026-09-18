package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/reticulumconfig"
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

	// waitMu guards waiters, the registry that routes inbound control-plane
	// envelopes to the awaiting workflows (request, inspect, cancel, lease
	// renewal, logs, tunnel grant) by correlation ID. A single envelope maps to
	// the one message ID that requested it; routing by key means several
	// concurrent awaiters never steal or drop each other's replies the way a
	// shared consume-and-drop channel would.
	waitMu  sync.Mutex
	waiters map[string][]chan *r1sv1.Envelope
}

// registerWaiter creates a one-shot delivery channel for the envelope whose
// correlation ID matches correlationID. The caller must register BEFORE sending
// the request so a reply that arrives promptly is still routed (dispatch retains
// the envelope only for already-registered waiters). The returned cancel unregisters
// the channel so a timed-out or errored await does not leak it.
func (a *application) registerWaiter(correlationID string) (chan *r1sv1.Envelope, func()) {
	ch := make(chan *r1sv1.Envelope, 1)
	a.waitMu.Lock()
	if a.waiters == nil {
		a.waiters = make(map[string][]chan *r1sv1.Envelope)
	}
	a.waiters[correlationID] = append(a.waiters[correlationID], ch)
	a.waitMu.Unlock()

	var cancelled bool
	cancel := func() {
		a.waitMu.Lock()
		defer a.waitMu.Unlock()
		if cancelled {
			return
		}
		cancelled = true
		chans := a.waiters[correlationID]
		for i, c := range chans {
			if c == ch {
				a.waiters[correlationID] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
		if len(a.waiters[correlationID]) == 0 {
			delete(a.waiters, correlationID)
		}
	}
	return ch, cancel
}

// dispatchEnvelope routes an inbound envelope to every waiter registered for
// its correlation ID. It never blocks: a waiter that raced cancellation simply
// does not receive it. Envelopes with no correlation ID (or no registered
// waiter) are dropped — their effects were already applied to the durable
// client store by handleEnvelope, so nothing is lost for a wait that has not
// started.
func (a *application) dispatchEnvelope(envelope *r1sv1.Envelope) {
	corr := envelope.GetCorrelationId()
	if corr == "" {
		return
	}
	a.waitMu.Lock()
	chans := a.waiters[corr]
	delete(a.waiters, corr)
	a.waitMu.Unlock()
	for _, ch := range chans {
		select {
		case ch <- envelope:
		default:
		}
	}
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
	reticulumConfig, err := reticulumconfig.LoadConfig(options.configPath)
	if err != nil {
		return nil, fmt.Errorf("load Reticulum config: %w", err)
	}
	reticulumConfig.EnableTransport = false
	identityDirectory, err := identityDataDirectory(options.identitySource)
	if err != nil {
		return nil, err
	}
	reticulumConfig.ConfigPath = filepath.Join(identityDirectory, "reticulum-client")

	app := &application{ctx: ctx, stdout: stdout}
	app.endpoint, err = rns.New(rns.Config{
		Reticulum:      reticulumConfig,
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

func (a *application) handleEnvelope(ctx context.Context, envelope *r1sv1.Envelope) error {
	identityKey := hex.EncodeToString(envelope.GetSender())
	if destination, ok := a.endpoint.DestinationForIdentity(identityKey); ok {
		if err := a.client.RegisterAllocator(client.Allocator{Identity: envelope.GetSender(), Destination: destination}); err != nil {
			return err
		}
	}
	if err := a.client.Handle(ctx, envelope); err != nil {
		return err
	}
	a.dispatchEnvelope(envelope)
	return nil
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

func (a *application) send(destination string, envelope *r1sv1.Envelope) error {
	ctx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
	defer cancel()
	return a.endpoint.Send(ctx, destination, envelope)
}

func (a *application) awaitState(executionID string, ch chan *r1sv1.Envelope, timeout time.Duration) (client.ExecutionSnapshot, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return client.ExecutionSnapshot{}, a.ctx.Err()
		case <-timer.C:
			return client.ExecutionSnapshot{}, fmt.Errorf("timed out waiting for execution %s state", executionID)
		case envelope := <-ch:
			if err := client.RemoteFailure(envelope); err != nil {
				return client.ExecutionSnapshot{}, err
			}
			if envelope.GetExecutionState().GetExecutionId() == executionID {
				snapshot, _ := a.client.Execution(executionID)
				return snapshot, nil
			}
		}
	}
}

func clientClusterSource(value string) (string, error) {
	if strings.TrimSpace(value) != "" {
		return value, nil
	}
	return cluster.DefaultPath()
}

func identityDataDirectory(source string) (string, error) {
	if !rns.IsInlineIdentitySource(source) {
		return filepath.Dir(source), nil
	}
	defaultClusterPath, err := cluster.DefaultPath()
	if err != nil {
		return "", err
	}
	return filepath.Dir(defaultClusterPath), nil
}
