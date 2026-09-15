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
	return commandLine{
		configPath: *configPath, identitySource: *identitySource, capacity: capacity,
		announceInterval: *announceInterval, containerdAddress: *containerdAddress,
		containerdNamespace: *containerdNamespace, containerdSnapshotter: *containerdSnapshotter,
		logPath: *logPath, logBytes: *logBytes, logBudget: *logBudget, maxRecords: *maxRecords,
		sweepInterval: *sweepInterval, admissionPath: *admissionPath, statePath: *statePath,
		clusterSource: *clusterSource,
	}, nil
}
