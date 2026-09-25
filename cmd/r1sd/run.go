package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/allocator"
	"github.com/mytecor/r1s/internal/cluster"
	runtimecontainerd "github.com/mytecor/r1s/internal/runtime/containerd"
)

type commandLine struct {
	showVersion           bool
	identitySource        string
	capacity              map[string]uint32
	announceInterval      time.Duration
	containerdAddress     string
	containerdNamespace   string
	containerdSnapshotter string
	logPath               string
	logBytes              int64
	logBudget             int64
	maxRecords            int
	sweepInterval         time.Duration
	admissionPath         string
	statePath             string
	clusterSelector       string
	clusterArguments      []string
	tunnelEnabled         bool
	tunnelEndpoint        []byte
	tunnelEndpointPubKey  []byte
	tunnelPeers           []string
	node                  *r1sv1.NodeCapabilities
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	options, err := parseCommandLine(arguments, stderr)
	if err != nil {
		return err
	}
	if options.showVersion {
		fmt.Fprintln(stdout, version)
		return nil
	}
	if options.clusterArguments != nil {
		directory, err := cluster.DefaultDirectory()
		if err != nil {
			return err
		}
		return cluster.RunCommand(options.clusterArguments, directory, stdout, stderr)
	}

	daemon, err := openDaemon(ctx, options, stdout, stderr)
	if err != nil {
		return err
	}
	defer daemon.close()
	return daemon.serve(ctx)
}

func parseCommandLine(arguments []string, stderr io.Writer) (commandLine, error) {
	flags := newFlagSet("r1sd", stderr)
	showVersion := flags.Bool("version", false, "print the version and exit")
	identitySource := flags.String("identity", "", "private RNS identity (hex, Base32, Base64) or file path")
	capacityValue := flags.String("capacity", "default=1", "comma-separated resource capacities, for example default=2,gpu=1")
	announceInterval := flags.Duration("announce-interval", 5*time.Minute, "service announce refresh interval")
	containerdAddress := flags.String("containerd-address", runtimecontainerd.DefaultAddress, "path to the containerd socket")
	containerdNamespace := flags.String("containerd-namespace", runtimecontainerd.DefaultNamespace, "isolated containerd namespace")
	containerdSnapshotter := flags.String("containerd-snapshotter", "", "containerd snapshotter (daemon default when empty)")
	logPath := flags.String("logs", "", "local log directory (defaults beside state database)")
	logBytes := flags.Int64("log-bytes", 1<<20, "maximum retained bytes per stream")
	logBudget := flags.Int64("log-budget", 2<<30, "maximum reserved local log data bytes")
	maxRecords := flags.Int("max-records", allocator.DefaultMaxRecords, "maximum durable offer/execution/tombstone budget")
	sweepInterval := flags.Duration("sweep-interval", time.Minute, "bounded-history and offer-expiry cleanup interval")
	admissionPath := flags.String("admission-policy", "", "local resource profiles, allowed identities, and quotas JSON")
	statePath := flags.String("state", "", "allocator state database (defaults beside the identity file or under ~/.config/r1s)")
	tunnelEnabled := flags.Bool("tunnel-enabled", false, "enable the direct-access tunnel edge (F14); requires --tunnel-endpoint/--tunnel-endpoint-pubkey or the embedded edge")
	tunnelEndpoint := flags.String("tunnel-endpoint", "", "opaque transport-neutral allocator endpoint advertisement (hex); set automatically by the F14-02 edge")
	tunnelEndpointPubKey := flags.String("tunnel-endpoint-pubkey", "", "opaque transport-neutral allocator edge public key (hex); set automatically by the F14-02 edge")
	tunnelPeers := flags.String("tunnel-peer", "", "comma-separated bootstrap peer URIs for the tunnel edge (defaults to the public Yggdrasil overlay)")
	nodeValue := flags.String("node", "", "JSON node capabilities for placement, for example {\"labels\":{\"region\":\"eu\"}}; os/arch default to the build target")
	if err := flags.Parse(arguments); err != nil {
		return commandLine{}, err
	}
	if *showVersion {
		return commandLine{showVersion: true}, nil
	}
	if flags.NArg() > 0 {
		if flags.Arg(0) == "cluster" {
			return commandLine{clusterArguments: flags.Args()[1:]}, nil
		}
		if flags.NArg() != 1 {
			return commandLine{}, errors.New("r1sd requires exactly one cluster ID or unique prefix")
		}
	} else {
		return commandLine{}, errors.New("r1sd requires exactly one cluster ID or unique prefix")
	}
	if strings.TrimSpace(*identitySource) == "" {
		return commandLine{}, errors.New("--identity is required")
	}
	if *sweepInterval <= 0 {
		return commandLine{}, errors.New("--sweep-interval must be positive")
	}
	capacity, err := parseCapacity(*capacityValue)
	if err != nil {
		return commandLine{}, err
	}
	tunnelEndpointBytes, err := parseHexBytes("tunnel-endpoint", *tunnelEndpoint)
	if err != nil {
		return commandLine{}, err
	}
	tunnelEndpointPubKeyBytes, err := parseHexBytes("tunnel-endpoint-pubkey", *tunnelEndpointPubKey)
	if err != nil {
		return commandLine{}, err
	}
	// The embedded edge derives and advertises its own endpoint when it is
	// enabled; a manually supplied static advertisement is only meaningful in
	// the edge-off static mode. Silently preferring the derived value over a
	// configured static one would hide a misconfiguration, so reject the
	// combination.
	if *tunnelEnabled && (len(tunnelEndpointBytes) > 0 || len(tunnelEndpointPubKeyBytes) > 0) {
		return commandLine{}, errors.New("--tunnel-endpoint/--tunnel-endpoint-pubkey cannot be set together with --tunnel-enabled: the embedded edge derives its own advertisement")
	}
	node, err := parseNodeCapabilities(*nodeValue)
	if err != nil {
		return commandLine{}, err
	}
	return commandLine{
		identitySource: *identitySource, capacity: capacity,
		announceInterval: *announceInterval, containerdAddress: *containerdAddress,
		containerdNamespace: *containerdNamespace, containerdSnapshotter: *containerdSnapshotter,
		logPath: *logPath, logBytes: *logBytes, logBudget: *logBudget, maxRecords: *maxRecords,
		sweepInterval: *sweepInterval, admissionPath: *admissionPath, statePath: *statePath,
		clusterSelector: flags.Arg(0),
		tunnelEnabled:   *tunnelEnabled,
		tunnelEndpoint:  tunnelEndpointBytes, tunnelEndpointPubKey: tunnelEndpointPubKeyBytes,
		tunnelPeers: tunnelPeerList(*tunnelPeers), node: node,
	}, nil
}
