package acceptance

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	containerdclient "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// TestLiveResourceLimitsEnforced verifies F10-01 on a real containerd host:
// a workload admitted through a bounded allocator profile carries exactly the
// configured memory/CPU/pids limits in its OCI spec, and an existing running
// execution keeps its admitted limits after the daemon restarts and
// reconciles it. Pinned to a Linux containerd runner (mytecor-homelab); skips
// everywhere else. The deterministic profile plumbing (invalid profiles fail
// startup, admitted profiles survive restart in state) is covered by
// TestInvalidResourceProfileFailsStartup / TestResourceProfileSurvivesRestart;
// this test supplies the real OCI-spec leg only a live run can.
func TestLiveResourceLimitsEnforced(t *testing.T) {
	if os.Getenv("RUN_PARTITION_RECOVERY") != "1" {
		t.Skip("set RUN_PARTITION_RECOVERY=1 to run the live F10 resource acceptance")
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
	namespace := fmt.Sprintf("r1s-f10-%d", time.Now().UnixNano())
	address := os.Getenv("CONTAINERD_ADDRESS")
	if address == "" {
		address = "/run/containerd/containerd.sock"
	}
	observer, err := containerdclient.New(address)
	if err != nil {
		t.Fatalf("connect to containerd observer: %v", err)
	}
	defer observer.Close()

	// Bounded profile: 64 MiB memory, half a CPU (quota 50000/100000), 16 pids.
	const (
		memoryBytes int64 = 64 << 20
		cpuMilli    int64 = 500
		pids        int64 = 16
	)
	admissionPath := filepath.Join(root, "admission.json")
	if err := os.WriteFile(admissionPath, []byte(fmt.Sprintf(
		`{"profiles":{"default":{"memory_bytes":%d,"cpu_milli":%d,"pids":%d}},"default_quota":{"offers":4,"executions":2}}`,
		memoryBytes, cpuMilli, pids,
	)), 0o600); err != nil {
		t.Fatal(err)
	}

	liveClient := newAcceptanceClient(t, clientPort, allocatorPort, clientIdentity, clientState)
	defer liveClient.close(t)
	var daemon *allocatorProcess
	defer func() {
		if daemon != nil {
			daemon.stop(t)
		}
		cleanupExecutionContainers(t, observer, namespace)
	}()

	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace,
		"--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192", "--admission-policy", admissionPath)
	service := liveClient.waitForAllocator(t, daemon.identity)
	if service.Destination != daemon.destination {
		t.Fatalf("discovered allocator destination = %s, want %s", service.Destination, daemon.destination)
	}

	// A long-running workload so the OCI spec can be inspected while it runs.
	_, assignment := startExecution(t, liveClient, daemon.destination, image, "sleep 120; exit 0")
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	waitForRunningContainer(t, observer, namespace, executionID)

	resources := containerResources(t, observer, namespace, executionID)
	if resources.MemoryBytes != memoryBytes || resources.CPUMilli != cpuMilli || resources.Pids != pids {
		t.Fatalf("live OCI resources = %+v, want memory=%d cpu=%d pids=%d", resources, memoryBytes, cpuMilli, pids)
	}

	// Restart r1sd from the same state; the admitted limits must follow the
	// recovered execution (reconcile reattaches without re-creating the spec).
	daemon.stop(t)
	daemon = nil
	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace,
		"--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192", "--admission-policy", admissionPath)
	liveClient.waitForAllocator(t, daemon.identity)
	waitForRunningContainer(t, observer, namespace, executionID)
	resources = containerResources(t, observer, namespace, executionID)
	if resources.MemoryBytes != memoryBytes || resources.CPUMilli != cpuMilli || resources.Pids != pids {
		t.Fatalf("live OCI resources after restart = %+v, want memory=%d cpu=%d pids=%d", resources, memoryBytes, cpuMilli, pids)
	}

	// Clean up the running execution; the OCI-spec assertions above already
	// proved the working set is subject to the configured limits on a real
	// containerd host (kernel-enforced through the runc cgroup).
	cancelDest, cancellation, err := liveClient.core.Cancel(executionID, "F10 limit check complete")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, cancelDest, cancellation)
	waitForPhase(t, liveClient.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED, 10*time.Second)
}

// containerResources reads the OCI spec of the running container for
// executionID and returns the configured memory/CPU/pids limits.
func containerResources(t *testing.T, observer *containerdclient.Client, namespace, executionID string) r1sruntime.Resources {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespace), 5*time.Second)
	defer cancel()
	containers, err := matchingContainers(observer, namespace, executionID)
	if err != nil || len(containers) != 1 {
		t.Fatalf("containers for %s = %d (err %v), want exactly 1", executionID, len(containers), err)
	}
	spec, err := containers[0].Spec(ctx)
	if err != nil {
		t.Fatalf("read OCI spec for %s: %v", executionID, err)
	}
	if spec.Linux == nil || spec.Linux.Resources == nil {
		t.Fatalf("spec for %s has no Linux resources: %+v", executionID, spec.Linux)
	}
	resources := r1sruntime.Resources{}
	if memory := spec.Linux.Resources.Memory; memory != nil && memory.Limit != nil {
		resources.MemoryBytes = *memory.Limit
	}
	if cpu := spec.Linux.Resources.CPU; cpu != nil && cpu.Quota != nil && cpu.Period != nil {
		resources.CPUMilli = (*cpu.Quota * 1000) / int64(*cpu.Period)
	}
	if pids := spec.Linux.Resources.Pids; pids != nil && pids.Limit != nil {
		resources.Pids = *pids.Limit
	}
	return resources
}
