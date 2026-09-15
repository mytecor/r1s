package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/allocator"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/logstore"
	runtimecontainerd "github.com/mytecor/r1s/internal/runtime/containerd"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/transport/rns"
	"quad4/reticulum-go/pkg/reticulumconfig"
)

// version identifies the build. Release binaries set it with
// -ldflags "-X main.version=<tag>"; source builds report "dev".
var version = "dev"

func main() {
	if runtimecontainerd.RunLogWriter(os.Args[1:]) {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("r1sd", stderr)
	showVersion := flags.Bool("version", false, "print the version and exit")
	configPath := flags.String("rns-config", "", "path to a Reticulum-Go configuration file")
	identityPath := flags.String("identity", "", "path to the persistent r1sd identity")
	capacityValue := flags.String("capacity", "default=1", "comma-separated resource capacities, for example default=2,gpu=1")
	announceInterval := flags.Duration("announce-interval", 5*time.Minute, "service announce refresh interval")
	containerdAddress := flags.String("containerd-address", runtimecontainerd.DefaultAddress, "path to the containerd socket")
	containerdNamespace := flags.String("containerd-namespace", runtimecontainerd.DefaultNamespace, "isolated containerd namespace")
	containerdSnapshotter := flags.String("containerd-snapshotter", "", "containerd snapshotter (daemon default when empty)")
	logPath := flags.String("logs", "", "local log directory (defaults beside state database)")
	logBytes := flags.Int64("log-bytes", 1<<20, "maximum retained bytes per stream")
	logBudget := flags.Int64("log-budget", 2<<30, "maximum reserved local log data bytes")
	maxRecords := flags.Int("max-records", allocator.DefaultMaxRecords, "maximum durable offer/execution/tombstone budget")
	admissionPath := flags.String("admission-policy", "", "local resource profiles, allowed identities, and quotas JSON")
	statePath := flags.String("state", "", "allocator state database (defaults beside the identity)")
	clusterPath := flags.String("cluster", "", "cluster state file (defaults to /var/lib/r1s/cluster)")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *showVersion {
		fmt.Fprintln(stdout, version)
		return nil
	}
	if flags.NArg() > 0 {
		if flags.Arg(0) != "cluster" {
			return fmt.Errorf("unknown command %q: expected cluster", flags.Arg(0))
		}
		return cluster.RunCommand(flags.Args()[1:], allocatorClusterPath(*clusterPath), stdout, stderr)
	}
	if strings.TrimSpace(*configPath) == "" || strings.TrimSpace(*identityPath) == "" {
		return errors.New("--rns-config and --identity are required")
	}
	capacity, err := parseCapacity(*capacityValue)
	if err != nil {
		return err
	}
	var admissionReader io.Reader
	if *admissionPath != "" {
		f, err := os.Open(*admissionPath)
		if err != nil {
			return err
		}
		defer f.Close()
		admissionReader = f
	}
	admission, err := allocator.ReadAdmission(admissionReader, capacity)
	if err != nil {
		return fmt.Errorf("admission policy: %w", err)
	}
	resolvedClusterPath := allocatorClusterPath(*clusterPath)
	clusterKey, err := cluster.Load(resolvedClusterPath)
	if err != nil {
		return fmt.Errorf("load cluster membership from %s (run 'r1sd cluster join <token>'): %w", resolvedClusterPath, err)
	}
	reticulumConfig, err := reticulumconfig.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load Reticulum config: %w", err)
	}
	reticulumConfig.EnableTransport = false
	reticulumConfig.ConfigPath = filepath.Join(filepath.Dir(*identityPath), "reticulum")

	logger := log.New(stderr, "r1sd: ", log.LstdFlags|log.Lmsgprefix)
	var endpoint *rns.Endpoint
	var core *allocator.Allocator
	handler := func(_ context.Context, envelope *r1sv1.Envelope) error {
		responses, handleErr := core.Handle(context.Background(), envelope)
		if handleErr != nil {
			logger.Printf("reject message %q from %x: %v", envelope.GetMessageId(), envelope.GetSender(), handleErr)
		}
		var responseErr error
		for _, response := range responses {
			sendContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			sendErr := endpoint.Send(sendContext, hex.EncodeToString(envelope.GetSender()), response)
			cancel()
			if sendErr != nil {
				responseErr = errors.Join(responseErr, fmt.Errorf("send response: %w", sendErr))
			}
		}
		return errors.Join(handleErr, responseErr)
	}
	endpoint, err = rns.New(rns.Config{
		Reticulum:        reticulumConfig,
		IdentityPath:     *identityPath,
		ClusterKey:       clusterKey,
		Capacity:         capacity,
		AnnounceInterval: *announceInterval,
	}, handler)
	if err != nil {
		return err
	}
	defer endpoint.Close()
	identityHash, err := hex.DecodeString(endpoint.Name())
	if err != nil {
		return fmt.Errorf("decode local identity: %w", err)
	}
	if strings.TrimSpace(*statePath) == "" {
		*statePath = *identityPath + ".state.db"
	}
	stateStore, err := statebolt.Open(*statePath)
	if err != nil {
		return err
	}
	defer stateStore.Close()
	if *logPath == "" {
		*logPath = *statePath + ".logs"
	}
	logs, err := logstore.New(*logPath, *logBytes, *logBudget)
	if err != nil {
		return err
	}
	logBinary, err := os.Executable()
	if err != nil {
		return err
	}
	runtimeContext, cancelRuntime := context.WithTimeout(ctx, 30*time.Second)
	containerRuntime, err := runtimecontainerd.New(runtimeContext, runtimecontainerd.Config{
		Logs: logs, LogBinary: logBinary,
		Address:     *containerdAddress,
		Namespace:   *containerdNamespace,
		Snapshotter: *containerdSnapshotter,
	})
	cancelRuntime()
	if err != nil {
		return err
	}
	defer containerRuntime.Close()
	core, err = allocator.New(allocator.Config{Identity: identityHash, Capacity: capacity, Store: stateStore, Admission: admission, Logs: logs, MaxRecords: *maxRecords}, containerRuntime)
	if err != nil {
		return err
	}
	recoveryContext, cancelRecovery := context.WithTimeout(ctx, 30*time.Second)
	err = core.Recover(recoveryContext)
	cancelRecovery()
	if err != nil {
		return fmt.Errorf("reconcile allocator state: %w", err)
	}
	if err := endpoint.Start(ctx); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "r1sd ready identity=%s destination=%s\n", endpoint.Name(), endpoint.Destination())
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if err := core.Sweep(ctx); err != nil {
			logger.Printf("state/log cleanup: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func allocatorClusterPath(value string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return cluster.DefaultAllocatorPath()
}

func parseCapacity(value string) (map[string]uint32, error) {
	capacity := make(map[string]uint32)
	for _, entry := range strings.Split(value, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid capacity %q: expected class=slots", entry)
		}
		slots, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 32)
		if err != nil || slots == 0 {
			return nil, fmt.Errorf("invalid capacity %q: slots must be a positive uint32", entry)
		}
		class := strings.TrimSpace(parts[0])
		if _, duplicate := capacity[class]; duplicate {
			return nil, fmt.Errorf("invalid capacity %q: duplicate class", entry)
		}
		capacity[class] = uint32(slots)
	}
	return capacity, nil
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprintf(output, "Usage of %s:\n", name)
		flags.VisitAll(func(candidate *flag.Flag) {
			fmt.Fprintf(output, "  --%s value\n    \t%s", candidate.Name, candidate.Usage)
			if candidate.DefValue != "" && candidate.DefValue != "false" && candidate.DefValue != "0" {
				fmt.Fprintf(output, " (default %s)", strconv.Quote(candidate.DefValue))
			}
			fmt.Fprintln(output)
		})
	}
	return flags
}
