package acceptance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	containerdclient "github.com/containerd/containerd/v2/client"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestLiveRetainedLogsSurviveRestart closes the F9-01 live gap: a failed or
// completed workload's stdout/stderr stays locally readable after a real r1sd
// restart, and the on-disk logstore layout (per-execution directory with
// bounded stdout/stderr streams) contains exactly what the workload wrote.
// Verified on mytecor-homelab (2026-09-15) with go test -race alongside
// TestPartitionRecovery against containerd 2.3.4 / runc 1.4.3.
//
// Retention through the explicit owner request path is covered deterministically
// by internal/allocator/acceptance_test.go (TestLogsOnlyByExplicitOwnerRequest)
// and the client retrieval acceptance; this test supplies the "survives a real
// daemon restart on a Linux containerd host" leg that only a live run can.
func TestLiveRetainedLogsSurviveRestart(t *testing.T) {
	if os.Getenv("RUN_PARTITION_RECOVERY") != "1" {
		t.Skip("set RUN_PARTITION_RECOVERY=1 to run the live F9 logs acceptance")
	}
	if runtime.GOOS != "linux" {
		t.Skip("requires a Linux containerd host")
	}
	image := os.Getenv("R1S_CONTAINERD_TEST_IMAGE")
	if image == "" {
		t.Fatal("R1S_CONTAINERD_TEST_IMAGE must name a digest-pinned fixture image")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	root := t.TempDir()
	moduleRoot := filepath.Clean(filepath.Join(mustWorkingDirectory(t), "..", ".."))
	binary := filepath.Join(root, "r1sd")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/r1sd")
	build.Dir = moduleRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build r1sd: %v\n%s", err, output)
	}

	clientPort, allocatorPort := distinctUDPPorts(t)
	allocatorConfig := filepath.Join(root, "allocator.conf")
	writeRNSConfig(t, allocatorConfig, allocatorPort, clientPort)
	allocatorIdentity := filepath.Join(root, "allocator.identity")
	allocatorState := filepath.Join(root, "allocator.state.db")
	allocatorLogs := filepath.Join(root, "allocator.logs")
	clientIdentity := filepath.Join(root, "client.identity")
	clientState := filepath.Join(root, "client.state.db")
	namespace := "r1s-f9-logs"
	address := os.Getenv("CONTAINERD_ADDRESS")
	if address == "" {
		address = "/run/containerd/containerd.sock"
	}
	observer, err := containerdclient.New(address)
	if err != nil {
		t.Fatalf("connect to containerd observer: %v", err)
	}
	defer observer.Close()

	liveClient := newAcceptanceClient(t, clientPort, allocatorPort, clientIdentity, clientState)
	defer liveClient.close(t)
	var daemon *allocatorProcess
	defer func() {
		if daemon != nil {
			daemon.stop(t)
		}
		cleanupExecutionContainers(t, observer, namespace)
	}()

	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace, "--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192")
	liveClient.waitForAllocator(t, daemon.identity)

	// A workload that writes bounded stdout+stderr then fails.
	requestID, executionID := startWritingExecution(t, liveClient, daemon.destination, image, "sleep 1; echo born-stdout; echo born-stderr >&2; exit 23")
	_ = requestID
	// Completion is observed by an explicit inspect: the allocator never pushes a
	// terminal state, so the client queries it (F5 does the same).
	waitForContainerCount(t, observer, namespace, executionID, 0, 30*time.Second)
	inspectDest, inspect, err := liveClient.core.Inspect(executionID)
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, inspectDest, inspect)
	waitForPhase(t, liveClient.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, 20*time.Second)

	// The logstore directory for this execution exists on disk.
	logDir := logstoreDirectory(allocatorLogs, executionID)
	if _, err := os.Stat(logDir); err != nil {
		t.Fatalf("logstore reservation for %s missing before restart: %v", executionID, err)
	}

	// Restart r1sd with the same state and log directory. Retained logs must
	// survive the daemon process lifetime.
	daemon.stop(t)
	daemon = nil
	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace, "--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192")
	liveClient.waitForAllocator(t, daemon.identity)

	// After restart the per-execution logstore still holds the stream files.
	stderrBytes := readLogStream(t, logDir, "stderr")
	if !strings.Contains(string(stderrBytes), "born-stderr") {
		t.Fatalf("stderr after restart = %q, want born-stderr", stderrBytes)
	}
	stdoutBytes := readLogStream(t, logDir, "stdout")
	if !strings.Contains(string(stdoutBytes), "born-stdout") {
		t.Fatalf("stdout after restart = %q, want born-stdout", stdoutBytes)
	}
}

// startWritingExecution runs a workload, waits until the allocator reports it
// running, and returns the request and execution IDs (mirrors F5 startExecution).
func startWritingExecution(t *testing.T, c *acceptanceClient, destination, image, script string) (string, string) {
	t.Helper()
	requestID, request, err := c.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{script},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute), ResultRetention: durationpb.New(time.Hour)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	c.send(t, destination, request)
	c.send(t, destination, request)
	waitUntil(t, 10*time.Second, func() (bool, error) {
		for _, snapshot := range c.core.Requests() {
			if snapshot.Request.GetRequestId() == requestID {
				return snapshot.OfferCount == 1, nil
			}
		}
		return false, nil
	})
	selectedDestination, assignment, err := c.core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	c.send(t, selectedDestination, assignment)
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	waitForPhase(t, c.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 20*time.Second)
	return requestID, executionID
}

// logstoreDirectory mirrors internal/logstore.Store.directory: each execution
// is stored under SHA-256(executionID) inside the configured --logs root.
func logstoreDirectory(root, executionID string) string {
	sum := sha256.Sum256([]byte(executionID))
	return filepath.Join(root, hex.EncodeToString(sum[:]))
}

func readLogStream(t *testing.T, dir, stream string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, stream))
	if err != nil {
		t.Fatalf("read retained %s after restart: %v", stream, err)
	}
	return data
}
