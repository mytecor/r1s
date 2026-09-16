package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/reticulumconfig"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/cluster"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/transport/rns"
)

type application struct {
	ctx      context.Context
	stdout   io.Writer
	endpoint *rns.Endpoint
	client   *client.Client
	events   chan *r1sv1.Envelope
	store    *statebolt.Store
	finish   func() []client.PendingRelease
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

	app := &application{ctx: ctx, stdout: stdout, events: make(chan *r1sv1.Envelope, 32)}
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
	select {
	case a.events <- envelope:
	default:
	}
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

func (a *application) awaitState(executionID, correlationID string, timeout time.Duration) (client.ExecutionSnapshot, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return client.ExecutionSnapshot{}, a.ctx.Err()
		case <-timer.C:
			return client.ExecutionSnapshot{}, fmt.Errorf("timed out waiting for execution %s state", executionID)
		case envelope := <-a.events:
			if envelope.GetCorrelationId() != correlationID {
				continue
			}
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
