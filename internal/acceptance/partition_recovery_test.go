// Package acceptance contains gated end-to-end verification of complete r1s workflows.
package acceptance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	containerdclient "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/cluster"
	runtimecontainerd "github.com/mytecor/r1s/internal/runtime/containerd"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/transport/rns"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"quad4/reticulum-go/pkg/common"
)

const executionLabel = "io.r1s.execution-id"

var acceptanceClusterKey = bytes.Repeat([]byte{0x71}, cluster.KeySize)

func TestPartitionRecovery(t *testing.T) {
	if os.Getenv("RUN_PARTITION_RECOVERY") != "1" {
		t.Skip("set RUN_PARTITION_RECOVERY=1 to run the live RNS and containerd acceptance harness")
	}
	if runtime.GOOS != "linux" {
		t.Skip("partition recovery acceptance requires a Linux containerd host")
	}
	image := os.Getenv("R1S_CONTAINERD_TEST_IMAGE")
	if image == "" {
		t.Fatal("R1S_CONTAINERD_TEST_IMAGE must name a digest-pinned fixture image")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	clientIdentity := filepath.Join(root, "client.identity")
	clientState := filepath.Join(root, "client.state.db")
	namespace := fmt.Sprintf("r1s-f5-%d", time.Now().UnixNano())
	address := os.Getenv("CONTAINERD_ADDRESS")
	if address == "" {
		address = runtimecontainerd.DefaultAddress
	}

	observer, err := containerdclient.New(address)
	if err != nil {
		t.Fatalf("connect to containerd observer: %v", err)
	}
	defer observer.Close()

	var daemon *allocatorProcess
	var liveClient *acceptanceClient
	defer func() {
		if liveClient != nil {
			liveClient.close(t)
		}
		if daemon != nil {
			daemon.stop(t)
		}
		cleanupExecutionContainers(t, observer, namespace)
	}()

	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace)
	liveClient = newAcceptanceClient(t, clientPort, allocatorPort, clientIdentity, clientState)
	service := liveClient.waitForAllocator(t, daemon.identity)
	if service.Destination != daemon.destination {
		t.Fatalf("discovered allocator destination = %s, want %s", service.Destination, daemon.destination)
	}

	requestID, assignment := startExecution(t, liveClient, daemon.destination, image, "sleep 12; exit 23")
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	waitForRunningContainer(t, observer, namespace, executionID)

	// Message-level replay before the partition must not create another task.
	liveClient.send(t, daemon.destination, assignment)
	assertSingleRunningContainer(t, observer, namespace, executionID)

	// The one-shot client goes offline. The workload remains running independently
	// of the RNS link and the client-side process lifetime.
	liveClient.close(t)
	liveClient = nil
	time.Sleep(500 * time.Millisecond)
	assertSingleRunningContainer(t, observer, namespace, executionID)

	// Restart r1sd while the client remains offline. The old process releases its
	// containerd connection without stopping the task; the new process reconciles
	// the same durable execution through Recover rather than Start.
	daemon.stop(t)
	daemon = nil
	assertSingleRunningContainer(t, observer, namespace, executionID)
	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace)
	if daemon.destination != service.Destination || daemon.identity != service.Identity {
		t.Fatalf("allocator identity changed across restart: before=%+v after=%s/%s", service, daemon.destination, daemon.identity)
	}
	assertSingleRunningContainer(t, observer, namespace, executionID)

	// Completion and cleanup happen while the client is still offline.
	waitForContainerCount(t, observer, namespace, executionID, 0, 30*time.Second)

	// Restart the client from its durable state. Select returns the exact original
	// assignment, which is replayed to the restarted allocator before a fresh
	// inspect retrieves the retained terminal result.
	liveClient = newAcceptanceClient(t, clientPort, allocatorPort, clientIdentity, clientState)
	liveClient.waitForAllocator(t, daemon.identity)
	replayedDestination, replayedAssignment, err := liveClient.core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if replayedDestination != daemon.destination || !proto.Equal(replayedAssignment, assignment) {
		t.Fatalf("durable assignment changed across restart:\nfirst=%v\nreplayed=%v", assignment, replayedAssignment)
	}
	liveClient.send(t, replayedDestination, replayedAssignment)
	assertContainerAbsentFor(t, observer, namespace, executionID, time.Second)

	inspectDestination, inspect, err := liveClient.core.Inspect(executionID)
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, inspectDestination, inspect)
	waitForPhase(t, liveClient.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, 10*time.Second)
	result, _ := liveClient.core.Execution(executionID)
	if result.State.GetExitCode() != 23 {
		t.Fatalf("recovered result exit code = %d, want 23", result.State.GetExitCode())
	}

	// Complete the feature-level replay matrix with a real cancelled container.
	_, cancellable := startExecution(t, liveClient, daemon.destination, image, "while true; do sleep 1; done")
	cancellableID := cancellable.GetExecutionAssign().GetExecutionId()
	waitForRunningContainer(t, observer, namespace, cancellableID)
	cancelDestination, cancellation, err := liveClient.core.Cancel(cancellableID, "F5 duplicate cancellation")
	if err != nil {
		t.Fatal(err)
	}
	repeatedDestination, repeatedCancellation, err := liveClient.core.Cancel(cancellableID, "F5 duplicate cancellation")
	if err != nil {
		t.Fatal(err)
	}
	if repeatedDestination != cancelDestination || !proto.Equal(repeatedCancellation, cancellation) {
		t.Fatal("client did not retain a stable cancellation envelope")
	}
	liveClient.send(t, cancelDestination, cancellation)
	liveClient.send(t, repeatedDestination, repeatedCancellation)
	waitForPhase(t, liveClient.core, cancellableID, r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED, 10*time.Second)
	waitForContainerCount(t, observer, namespace, cancellableID, 0, 10*time.Second)
}

type acceptanceClient struct {
	endpoint *rns.Endpoint
	core     *client.Client
	store    *statebolt.Store
	cancel   context.CancelFunc
	closed   bool
}

func newAcceptanceClient(t *testing.T, listenPort, targetPort int, identityPath, statePath string) *acceptanceClient {
	t.Helper()
	configuration := common.DefaultConfig()
	configuration.EnableTransport = false
	configuration.ShareInstance = false
	configuration.ConfigPath = filepath.Join(filepath.Dir(identityPath), "reticulum-client")
	configuration.Interfaces = map[string]*common.InterfaceConfig{
		"f5-loopback": {
			Type:       "UDPInterface",
			Enabled:    true,
			Address:    fmt.Sprintf("127.0.0.1:%d", listenPort),
			TargetHost: fmt.Sprintf("127.0.0.1:%d", targetPort),
		},
	}

	result := &acceptanceClient{}
	endpoint, err := rns.New(rns.Config{
		Reticulum: configuration, IdentityPath: identityPath, ClusterKey: acceptanceClusterKey, NetworkWait: 15 * time.Second,
	}, func(handlerContext context.Context, envelope *r1sv1.Envelope) error {
		identityKey := hex.EncodeToString(envelope.GetSender())
		if destination, ok := result.endpoint.DestinationForIdentity(identityKey); ok {
			if registerErr := result.core.RegisterAllocator(client.Allocator{Identity: envelope.GetSender(), Destination: destination}); registerErr != nil {
				return registerErr
			}
		}
		return result.core.Handle(handlerContext, envelope)
	})
	if err != nil {
		t.Fatal(err)
	}
	result.endpoint = endpoint
	identity, err := hex.DecodeString(endpoint.Name())
	if err != nil {
		t.Fatal(err)
	}
	store, err := statebolt.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	result.store = store
	result.core, err = client.New(client.Config{Identity: identity, Store: store})
	if err != nil {
		_ = store.Close()
		_ = endpoint.Close()
		t.Fatal(err)
	}
	startContext, cancel := context.WithCancel(context.Background())
	result.cancel = cancel
	if err := endpoint.Start(startContext); err != nil {
		result.close(t)
		t.Fatal(err)
	}
	return result
}

func (c *acceptanceClient) close(t *testing.T) {
	t.Helper()
	if c.closed {
		return
	}
	c.closed = true
	c.cancel()
	if err := c.endpoint.Close(); err != nil {
		t.Errorf("close client endpoint: %v", err)
	}
	if err := c.store.Close(); err != nil {
		t.Errorf("close client store: %v", err)
	}
}

func (c *acceptanceClient) waitForAllocator(t *testing.T, identity string) rns.Service {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for {
		select {
		case service := <-c.endpoint.Discoveries():
			if service.Identity != identity {
				continue
			}
			decoded, err := hex.DecodeString(service.Identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.core.RegisterAllocator(client.Allocator{
				Identity: decoded, Destination: service.Destination, Hops: service.Hops, Capacity: service.Descriptor.Capacity,
			}); err != nil {
				t.Fatal(err)
			}
			return service
		case <-timer.C:
			t.Fatalf("allocator %s was not discovered", identity)
		}
	}
}

func (c *acceptanceClient) send(t *testing.T, destination string, envelope *r1sv1.Envelope) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.endpoint.Send(ctx, destination, envelope); err != nil {
		t.Fatal(err)
	}
}

func startExecution(t *testing.T, c *acceptanceClient, destination, image, script string) (string, *r1sv1.Envelope) {
	t.Helper()
	requestID, request, err := c.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{script},
	}, &r1sv1.ExecutionPolicy{
		MaxRuntime: durationpb.New(time.Minute), ResultRetention: durationpb.New(time.Hour),
	}, "default")
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
	if selectedDestination != destination {
		t.Fatalf("selected destination = %s, want %s", selectedDestination, destination)
	}
	c.send(t, selectedDestination, assignment)
	waitForPhase(t, c.core, assignment.GetExecutionAssign().GetExecutionId(), r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 20*time.Second)
	return requestID, assignment
}

type allocatorProcess struct {
	command     *exec.Cmd
	done        chan error
	stderr      bytes.Buffer
	destination string
	identity    string
	stopped     bool
}

func startAllocator(t *testing.T, ctx context.Context, binary, config, identity, state, address, namespace string, extra ...string) *allocatorProcess {
	t.Helper()
	clusterPath := identity + ".cluster"
	if err := cluster.SaveNew(clusterPath, acceptanceClusterKey); err != nil {
		t.Fatal(err)
	}
	arguments := []string{
		"--rns-config", config,
		"--identity", identity,
		"--cluster", clusterPath,
		"--state", state,
		"--capacity", "default=1",
		"--announce-interval", "500ms",
		"--containerd-address", address,
		"--containerd-namespace", namespace,
	}
	if snapshotter := os.Getenv("CONTAINERD_SNAPSHOTTER"); snapshotter != "" {
		arguments = append(arguments, "--containerd-snapshotter", snapshotter)
	}
	arguments = append(arguments, extra...)
	command := exec.CommandContext(ctx, binary, arguments...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	result := &allocatorProcess{command: command, done: make(chan error, 1)}
	command.Stderr = &result.stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "r1sd ready ") {
				select {
				case ready <- line:
				default:
				}
			}
		}
	}()
	go func() { result.done <- command.Wait() }()

	select {
	case line := <-ready:
		for _, field := range strings.Fields(line) {
			key, value, found := strings.Cut(field, "=")
			if !found {
				continue
			}
			switch key {
			case "identity":
				result.identity = value
			case "destination":
				result.destination = value
			}
		}
		if len(result.identity) != 32 || len(result.destination) != 32 {
			result.stop(t)
			t.Fatalf("invalid r1sd ready line %q", line)
		}
		return result
	case err := <-result.done:
		result.stopped = true
		t.Fatalf("r1sd exited before ready: %v\n%s", err, result.stderr.String())
	case <-time.After(30 * time.Second):
		result.stop(t)
		t.Fatalf("r1sd did not become ready\n%s", result.stderr.String())
	}
	return nil
}

func (p *allocatorProcess) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	if err := p.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("signal r1sd: %v", err)
	}
	select {
	case err := <-p.done:
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			t.Errorf("r1sd exit: %v\n%s", err, p.stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = p.command.Process.Kill()
		<-p.done
		t.Errorf("r1sd did not stop within 10s\n%s", p.stderr.String())
	}
}

func waitForPhase(t *testing.T, core *client.Client, executionID string, phase r1sv1.ExecutionPhase, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, func() (bool, error) {
		snapshot, ok := core.Execution(executionID)
		if !ok || snapshot.State == nil {
			return false, nil
		}
		if snapshot.State.GetPhase() == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED {
			return false, fmt.Errorf("execution failed: %s", snapshot.State.GetDetail())
		}
		return snapshot.State.GetPhase() == phase, nil
	})
}

func waitForRunningContainer(t *testing.T, observer *containerdclient.Client, namespace, executionID string) {
	t.Helper()
	waitUntil(t, 20*time.Second, func() (bool, error) {
		containers, err := matchingContainers(observer, namespace, executionID)
		if err != nil || len(containers) != 1 {
			return false, err
		}
		ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespace), 2*time.Second)
		defer cancel()
		task, err := containers[0].Task(ctx, nil)
		if err != nil {
			return false, nil
		}
		status, err := task.Status(ctx)
		return err == nil && status.Status == containerdclient.Running, err
	})
}

func assertSingleRunningContainer(t *testing.T, observer *containerdclient.Client, namespace, executionID string) {
	t.Helper()
	containers, err := matchingContainers(observer, namespace, executionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers) != 1 {
		t.Fatalf("containers for execution %s = %d, want exactly 1", executionID, len(containers))
	}
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespace), 2*time.Second)
	defer cancel()
	task, err := containers[0].Task(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err := task.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != containerdclient.Running {
		t.Fatalf("container task status = %s, want running", status.Status)
	}
}

func waitForContainerCount(t *testing.T, observer *containerdclient.Client, namespace, executionID string, count int, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, func() (bool, error) {
		containers, err := matchingContainers(observer, namespace, executionID)
		return len(containers) == count, err
	})
}

func assertContainerAbsentFor(t *testing.T, observer *containerdclient.Client, namespace, executionID string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		containers, err := matchingContainers(observer, namespace, executionID)
		if err != nil {
			t.Fatal(err)
		}
		if len(containers) != 0 {
			t.Fatalf("replayed assignment recreated %d containers for %s", len(containers), executionID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func matchingContainers(observer *containerdclient.Client, namespace, executionID string) ([]containerdclient.Container, error) {
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespace), 2*time.Second)
	defer cancel()
	containers, err := observer.Containers(ctx)
	if err != nil {
		return nil, err
	}
	var matches []containerdclient.Container
	for _, container := range containers {
		labels, labelErr := container.Labels(ctx)
		if labelErr != nil {
			return nil, labelErr
		}
		if labels[executionLabel] == executionID {
			matches = append(matches, container)
		}
	}
	return matches, nil
}

func cleanupExecutionContainers(t *testing.T, observer *containerdclient.Client, namespace string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespace), 15*time.Second)
	defer cancel()
	containers, err := observer.Containers(ctx)
	if err != nil {
		t.Logf("list acceptance containers during cleanup: %v", err)
		return
	}
	for _, container := range containers {
		labels, labelErr := container.Labels(ctx)
		if labelErr != nil || labels[executionLabel] == "" {
			continue
		}
		task, taskErr := container.Task(ctx, nil)
		if taskErr == nil {
			status, statusErr := task.Status(ctx)
			if statusErr == nil && status.Status != containerdclient.Stopped {
				_ = task.Kill(ctx, syscall.SIGKILL, containerdclient.WithKillAll)
				if exit, waitErr := task.Wait(ctx); waitErr == nil {
					select {
					case <-exit:
					case <-ctx.Done():
					}
				}
			}
			_, _ = task.Delete(ctx, containerdclient.WithProcessKill)
		} else if !errdefs.IsNotFound(taskErr) {
			t.Logf("load acceptance task during cleanup: %v", taskErr)
		}
		if deleteErr := container.Delete(ctx, containerdclient.WithSnapshotCleanup); deleteErr != nil && !errdefs.IsNotFound(deleteErr) {
			t.Logf("delete acceptance container during cleanup: %v", deleteErr)
		}
	}
}

func waitUntil(t *testing.T, timeout time.Duration, predicate func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ready, err := predicate()
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition was not met within %s", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func writeRNSConfig(t *testing.T, path string, listenPort, targetPort int) {
	t.Helper()
	configuration := fmt.Sprintf(`[reticulum]
enable_transport = No
share_instance = No

[interfaces]
  [[f5-loopback]]
    type = UDPInterface
    enabled = Yes
    listen_ip = 127.0.0.1
    listen_port = %d
    target_host = 127.0.0.1
    target_port = %d
`, listenPort, targetPort)
	if err := os.WriteFile(path, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
}

func distinctUDPPorts(t *testing.T) (int, int) {
	t.Helper()
	first := freeUDPPort(t)
	second := freeUDPPort(t)
	for second == first {
		second = freeUDPPort(t)
	}
	return first, second
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.LocalAddr().(*net.UDPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func mustWorkingDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return directory
}
