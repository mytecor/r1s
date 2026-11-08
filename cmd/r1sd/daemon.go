package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/allocator"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/logstore"
	runtimecontainerd "github.com/mytecor/r1s/internal/runtime/containerd"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/telemetry"
	"github.com/mytecor/r1s/internal/transport/rns"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/proto"
)

type daemon struct {
	stdout        io.Writer
	endpoint      *rns.Endpoint
	core          *allocator.Allocator
	runtime       *runtimecontainerd.Adapter
	stateStore    *statebolt.Store
	sweepInterval time.Duration
	metrics       *telemetry.Metrics
	metricsServer *telemetry.Server
	events        *telemetry.Logger
	// tunnel is the embedded tunnel edge (node + listener + accept loop). Nil
	// unless --tunnel-enabled.
	tunnel *tunnelEdge
	// tunnelStaticEndpoint carries an explicitly configured advertisement when
	// the embedded edge is off (--tunnel-endpoint/--tunnel-endpoint-pubkey).
	tunnelStaticEndpoint tunnel.Endpoint
}

func openDaemon(ctx context.Context, options commandLine, stdout, stderr io.Writer) (*daemon, error) {
	admission, err := readAdmission(options.admissionPath, options.capacity)
	if err != nil {
		return nil, err
	}
	clusterDirectory, err := cluster.DefaultDirectory()
	if err != nil {
		return nil, err
	}
	credential, clusterID, err := cluster.ResolveCredential(clusterDirectory, options.clusterSelector)
	if err != nil {
		return nil, fmt.Errorf("select cluster (run 'r1sd cluster list'): %w", err)
	}
	identityDirectory, err := identityDataDirectory(options.identitySource)
	if err != nil {
		return nil, err
	}

	node := options.node
	if node != nil {
		clone := proto.Clone(node).(*r1sv1.NodeCapabilities)
		if clone.GetOs() == "" {
			clone.Os = runtime.GOOS
		}
		if clone.GetArch() == "" {
			clone.Arch = runtime.GOARCH
		}
		node = clone
	}

	result := &daemon{stdout: stdout, sweepInterval: options.sweepInterval, events: telemetry.NewLogger(stderr, options.logJSON)}
	if options.metricsAddress != "" {
		result.metrics = telemetry.NewMetrics()
		var err error
		result.metricsServer, err = telemetry.StartServer(options.metricsAddress, result.metrics)
		if err != nil {
			return nil, fmt.Errorf("start metrics server on %q: %w", options.metricsAddress, err)
		}
	}
	result.endpoint, err = rns.New(rns.Config{
		IdentitySource: options.identitySource, ClusterKey: credential.Key,
		Capacity: options.capacity, AnnounceInterval: options.announceInterval, Node: node,
	}, result.handleEnvelope)
	if err != nil {
		result.close()
		return nil, err
	}
	if err := cluster.RecordBootstrapDestination(filepath.Join(clusterDirectory, clusterID), result.endpoint.Destination()); err != nil {
		result.close()
		return nil, fmt.Errorf("record allocator bootstrap destination: %w", err)
	}
	identityHash, err := hex.DecodeString(result.endpoint.Name())
	if err != nil {
		result.close()
		return nil, fmt.Errorf("decode local identity: %w", err)
	}
	statePath := options.statePath
	if strings.TrimSpace(statePath) == "" {
		if rns.IsInlineIdentitySource(options.identitySource) {
			statePath = filepath.Join(identityDirectory, result.endpoint.Name()+".state.db")
		} else {
			statePath = options.identitySource + ".state.db"
		}
	}
	result.stateStore, err = statebolt.Open(statePath)
	if err != nil {
		result.close()
		return nil, err
	}
	logPath := options.logPath
	if logPath == "" {
		logPath = statePath + ".logs"
	}
	logs, err := logstore.New(logPath, options.logBytes, options.logBudget)
	if err != nil {
		result.close()
		return nil, err
	}
	logBinary, err := os.Executable()
	if err != nil {
		result.close()
		return nil, err
	}
	runtimeContext, cancelRuntime := context.WithTimeout(ctx, 30*time.Second)
	result.runtime, err = runtimecontainerd.New(runtimeContext, runtimecontainerd.Config{
		Logs: logs, LogBinary: logBinary, Address: options.containerdAddress,
		Namespace: options.containerdNamespace, Snapshotter: options.containerdSnapshotter,
	})
	cancelRuntime()
	if err != nil {
		result.close()
		return nil, err
	}
	if options.tunnelEnabled {
		identitySeed, seedErr := rns.IdentitySeed(options.identitySource)
		if seedErr != nil {
			result.close()
			return nil, fmt.Errorf("load tunnel edge identity: %w", seedErr)
		}
		result.tunnel, err = startTunnelEdge(identitySeed, options)
		if err != nil {
			result.close()
			return nil, fmt.Errorf("start tunnel edge: %w", err)
		}
	} else {
		result.tunnelStaticEndpoint = tunnel.Endpoint{Address: options.tunnelEndpoint, PubKey: options.tunnelEndpointPubKey}
	}
	result.core, err = allocator.New(allocator.Config{
		Identity: identityHash, Capacity: options.capacity, Store: result.stateStore,
		Admission: admission, Logs: logs, MaxRecords: options.maxRecords, Retention: options.retention, Node: node,
		Tunnel: allocator.TunnelConfig{
			Enabled:  options.tunnelEnabled,
			Endpoint: result.tunnelEndpointAdvertisement(),
		},
		Metrics: result.metrics,
	}, result.runtime)
	if err != nil {
		result.close()
		return nil, err
	}
	return result, nil
}

func (d *daemon) serve(ctx context.Context) error {
	recoveryContext, cancelRecovery := context.WithTimeout(ctx, 30*time.Second)
	err := d.core.Recover(recoveryContext)
	cancelRecovery()
	if err != nil {
		return fmt.Errorf("reconcile allocator state: %w", err)
	}
	transportCtx, stopTransport := context.WithCancel(context.WithoutCancel(ctx))
	defer stopTransport()
	if err := d.endpoint.Start(transportCtx); err != nil {
		return err
	}
	fmt.Fprintf(d.stdout, "r1sd ready identity=%s destination=%s\n", d.endpoint.Name(), d.endpoint.Destination())
	d.events.Info("allocator.ready", "allocator started and ready", map[string]any{
		"identity":    d.endpoint.Name(),
		"destination": d.endpoint.Destination(),
	})
	if d.metricsServer != nil {
		fmt.Fprintf(d.stdout, "r1sd metrics listening on %s\n", d.metricsServer.Address())
		d.events.Info("metrics.ready", "metrics server listening", map[string]any{
			"address": d.metricsServer.Address(),
		})
	}
	if d.tunnel != nil {
		fmt.Fprintf(d.stdout, "r1sd tunnel edge ready address=%x pubkey=%x\n", d.tunnel.node.AddressBytes(), d.tunnel.node.PublicKey())
		go d.tunnel.runAcceptLoop(ctx, d.core)
	}
	statusDone := make(chan struct{})
	go func() { defer close(statusDone); d.publishStatus(ctx) }()
	ticker := time.NewTicker(d.sweepInterval)
	defer ticker.Stop()
	for {
		if err := d.core.Sweep(ctx); err != nil {
			d.events.Error("sweep.error", "state/log cleanup failed", map[string]any{"error": err.Error()})
		}
		select {
		case <-ctx.Done():
			<-statusDone
			d.core.SetDraining(true)
			sendCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = d.endpoint.PublishStatus(sendCtx, "r1s.allocator.draining", rns.AllocatorStatus{Revision: ^uint64(0), Capacity: d.core.AvailableCapacity(), Draining: true})
			cancel()
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (d *daemon) close() {
	if d.metricsServer != nil {
		_ = d.metricsServer.Close()
	}
	if d.tunnel != nil {
		_ = d.tunnel.listener.Close()
	}
	if d.endpoint != nil {
		_ = d.endpoint.Close()
	}
	if d.runtime != nil {
		_ = d.runtime.Close()
	}
	if d.stateStore != nil {
		_ = d.stateStore.Close()
	}
}

// Aggregate capacity changes and periodically refresh for peers whose interests
// arrived after startup. Advisory delivery never blocks the execution loop.
func (d *daemon) publishStatus(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var previous map[string]uint32
	var revision uint64
	var last time.Time
	for {
		capacity := d.core.AvailableCapacity()
		if !maps.Equal(previous, capacity) || time.Since(last) >= 5*time.Second {
			revision++
			sendCtx, cancel := context.WithTimeout(ctx, time.Second)
			topic := "r1s.allocator.capacity"
			if revision == 1 {
				topic = "r1s.allocator.ready"
			}
			_ = d.endpoint.PublishStatus(sendCtx, topic, rns.AllocatorStatus{Revision: revision, Capacity: capacity, Ready: true})
			cancel()
			previous, last = capacity, time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
