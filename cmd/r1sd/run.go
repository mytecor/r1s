package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mytecor/r1s/internal/allocator"
	"github.com/mytecor/r1s/internal/cluster"
	runtimecontainerd "github.com/mytecor/r1s/internal/runtime/containerd"
	"github.com/mytecor/r1s/internal/tunnel"
)

type commandLine struct {
	showVersion           bool
	configPath            string
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
	clusterSource         string
	clusterArguments      []string
	tunnelEnabled         bool
	tunnelGrantTTL        time.Duration
	tunnelTargets         map[string]tunnel.Target
	tunnelDefaultTarget   tunnel.Target
	tunnelEndpoint        []byte
	tunnelEndpointPubKey  []byte
	tunnelPeers           []string
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
		path, err := allocatorClusterSource(options.clusterSource)
		if err != nil {
			return err
		}
		return cluster.RunCommand(options.clusterArguments, path, stdout, stderr)
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
	configPath := flags.String("rns-config", "", "path to a Reticulum-Go configuration file")
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
	clusterSource := flags.String("cluster", "", "cluster join token or state file (defaults to ~/.config/r1s/cluster)")
	tunnelEnabled := flags.Bool("tunnel-enabled", false, "enable the direct-access tunnel edge (F14); requires the server-side target configuration")
	tunnelGrantTTL := flags.Duration("tunnel-grant-ttl", allocator.DefaultTunnelGrantTTL, "minted tunnel grant lifetime")
	tunnelTargetsValue := flags.String("tunnel-target", "", "comma-separated per-resource-class tunnel targets, for example default=127.0.0.1:9000")
	tunnelDefaultTarget := flags.String("tunnel-default-target", "", "mandatory fallback tunnel target for classes without an explicit target")
	tunnelEndpoint := flags.String("tunnel-endpoint", "", "opaque transport-neutral allocator endpoint advertisement (hex); set automatically by the F14-02 edge")
	tunnelEndpointPubKey := flags.String("tunnel-endpoint-pubkey", "", "opaque transport-neutral allocator edge public key (hex); set automatically by the F14-02 edge")
	tunnelPeers := flags.String("tunnel-peer", "", "comma-separated bootstrap peer URIs for the tunnel edge (defaults to the public Yggdrasil overlay)")
	if err := flags.Parse(arguments); err != nil {
		return commandLine{}, err
	}
	if *showVersion {
		return commandLine{showVersion: true}, nil
	}
	if flags.NArg() > 0 {
		if flags.Arg(0) != "cluster" {
			return commandLine{}, fmt.Errorf("unknown command %q: expected cluster", flags.Arg(0))
		}
		return commandLine{clusterSource: *clusterSource, clusterArguments: flags.Args()[1:]}, nil
	}
	if strings.TrimSpace(*configPath) == "" || strings.TrimSpace(*identitySource) == "" {
		return commandLine{}, errors.New("--rns-config and --identity are required")
	}
	if *sweepInterval <= 0 {
		return commandLine{}, errors.New("--sweep-interval must be positive")
	}
	capacity, err := parseCapacity(*capacityValue)
	if err != nil {
		return commandLine{}, err
	}
	tunnelTargets, err := ParseTunnelTargets(*tunnelTargetsValue)
	if err != nil {
		return commandLine{}, err
	}
	var defaultTarget tunnel.Target
	if strings.TrimSpace(*tunnelDefaultTarget) != "" {
		defaultTarget, err = ParseTunnelTarget(*tunnelDefaultTarget)
		if err != nil {
			return commandLine{}, fmt.Errorf("--tunnel-default-target: %w", err)
		}
	}
	if *tunnelGrantTTL <= 0 {
		return commandLine{}, errors.New("--tunnel-grant-ttl must be positive")
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
	return commandLine{
		configPath: *configPath, identitySource: *identitySource, capacity: capacity,
		announceInterval: *announceInterval, containerdAddress: *containerdAddress,
		containerdNamespace: *containerdNamespace, containerdSnapshotter: *containerdSnapshotter,
		logPath: *logPath, logBytes: *logBytes, logBudget: *logBudget, maxRecords: *maxRecords,
		sweepInterval: *sweepInterval, admissionPath: *admissionPath, statePath: *statePath,
		clusterSource: *clusterSource,
		tunnelEnabled: *tunnelEnabled, tunnelGrantTTL: *tunnelGrantTTL,
		tunnelTargets: tunnelTargets, tunnelDefaultTarget: defaultTarget,
		tunnelEndpoint: tunnelEndpointBytes, tunnelEndpointPubKey: tunnelEndpointPubKeyBytes,
		tunnelPeers: tunnelPeerList(*tunnelPeers),
	}, nil
}
