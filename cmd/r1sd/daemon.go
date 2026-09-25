package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
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
	"github.com/mytecor/r1s/internal/transport/rns"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/proto"
)

type daemon struct {
	stdout        io.Writer
	logger        *log.Logger
	endpoint      *rns.Endpoint
	core          *allocator.Allocator
	runtime       *runtimecontainerd.Adapter
	stateStore    *statebolt.Store
	sweepInterval time.Duration
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
	clusterKey, _, err := cluster.Resolve(clusterDirectory, options.clusterSelector)
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

	result := &daemon{stdout: stdout, logger: log.New(stderr, "r1sd: ", log.LstdFlags|log.Lmsgprefix), sweepInterval: options.sweepInterval}
	result.endpoint, err = rns.New(rns.Config{
		IdentitySource: options.identitySource, ClusterKey: clusterKey,
		Capacity: options.capacity, AnnounceInterval: options.announceInterval, Node: node,
	}, result.handleEnvelope)
	if err != nil {
		return nil, err
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
		Admission: admission, Logs: logs, MaxRecords: options.maxRecords, Node: node,
		Tunnel: allocator.TunnelConfig{
			Enabled:  options.tunnelEnabled,
			Endpoint: result.tunnelEndpointAdvertisement(),
		},
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
	if err := d.endpoint.Start(ctx); err != nil {
		return err
	}
	fmt.Fprintf(d.stdout, "r1sd ready identity=%s destination=%s\n", d.endpoint.Name(), d.endpoint.Destination())
	if d.tunnel != nil {
		fmt.Fprintf(d.stdout, "r1sd tunnel edge ready address=%x pubkey=%x\n", d.tunnel.node.AddressBytes(), d.tunnel.node.PublicKey())
		go d.tunnel.runAcceptLoop(ctx, d.core)
	}
	ticker := time.NewTicker(d.sweepInterval)
	defer ticker.Stop()
	for {
		if err := d.core.Sweep(ctx); err != nil {
			d.logger.Printf("state/log cleanup: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (d *daemon) close() {
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
