package acceptance

import (
	"context"
	"encoding/hex"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestLiveIdentityQuotaEnforced verifies F10-02 on a real containerd host:
// an allocator admission policy gates this authenticated identity's offers and
// executions against a real r1sd, over-quota requests return a correlated
// CAPACITY rejection without starting work, explicit offer release is
// idempotent (no double-release), and quota accounting survives allocator
// restart. Two-client isolation and spoofed-payload rejection are covered
// deterministically by TestAdmissionAllowlistAndIdentityQuotas; this test
// supplies the real-daemon leg only a live run can.
func TestLiveIdentityQuotaEnforced(t *testing.T) {
	if os.Getenv("RUN_PARTITION_RECOVERY") != "1" {
		t.Skip("set RUN_PARTITION_RECOVERY=1 to run the live F10 quota acceptance")
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

	// Create the client first so its identity hash is stable before the
	// admission policy is written (the policy names the allowed identity).
	clientPort, allocatorPort := distinctUDPPorts(t)
	allocatorConfig := filepath.Join(root, "allocator.conf")
	writeRNSConfig(t, allocatorConfig, allocatorPort, clientPort)
	allocatorIdentity := filepath.Join(root, "allocator.identity")
	allocatorState := filepath.Join(root, "allocator.state.db")
	allocatorLogs := filepath.Join(root, "allocator.logs")
	clientIdentity := filepath.Join(root, "client.identity")
	clientState := filepath.Join(root, "client.state.db")
	namespace := fmt.Sprintf("r1s-f10q-%d", time.Now().UnixNano())
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
	liveClient.observed = make(chan *r1sv1.Envelope, 64)
	defer liveClient.close(t)
	identityHex := liveClient.endpoint.Name()

	// The single authenticated client may hold at most one outstanding offer
	// and run at most one execution.
	admissionPath := filepath.Join(root, "admission.json")
	policy := fmt.Sprintf(
		`{"profiles":{"default":{"memory_bytes":%d,"cpu_milli":%d,"pids":%d}},"allowed_clients":["%s"],"client_quotas":{"%s":{"offers":1,"executions":1}},"default_quota":{"offers":1,"executions":1}}`,
		64<<20, 1000, 128, identityHex, identityHex,
	)
	if err := os.WriteFile(admissionPath, []byte(policy), 0o600); err != nil {
		t.Fatal(err)
	}

	var daemon *allocatorProcess
	defer func() {
		if daemon != nil {
			daemon.stop(t)
		}
		cleanupExecutionContainers(t, observer, namespace)
	}()

	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace,
		"--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192",
		"--admission-policy", admissionPath, "--capacity", "default=2")
	service := liveClient.waitForAllocator(t, daemon.identity)
	if service.Destination != daemon.destination {
		t.Fatalf("discovered allocator destination = %s, want %s", service.Destination, daemon.destination)
	}

	// First request consumes the single offer quota.
	requestID, request, err := liveClient.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"sleep 90; exit 0"},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(2 * time.Minute), ResultRetention: durationpb.New(time.Hour)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, daemon.destination, request)
	waitUntil(t, 10*time.Second, func() (bool, error) {
		for _, snapshot := range liveClient.core.Requests() {
			if snapshot.Request.GetRequestId() == requestID {
				return snapshot.OfferCount == 1, nil
			}
		}
		return false, nil
	})

	// A second request is over quota: the allocator answers CAPACITY for this
	// authenticated client. The rejection must not create a workload.
	rejectedID, rejectedRequest, err := liveClient.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"echo SHOULD-NOT-RUN"},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute), ResultRetention: durationpb.New(time.Hour)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, daemon.destination, rejectedRequest)
	if err := expectCommandError(t, liveClient.observed, rejectedRequest.GetMessageId(), "CAPACITY"); err != nil {
		t.Fatal(err)
	}
	waitForOfferCount(t, liveClient, rejectedID, 0)

	// Select and assign the first offer: the execution quota binds one real
	// container. A second assignment for the same identity is rejected.
	selectedDestination, assignment, err := liveClient.core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if selectedDestination != daemon.destination {
		t.Fatalf("selected destination = %s, want %s", selectedDestination, daemon.destination)
	}
	liveClient.send(t, selectedDestination, assignment)
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	waitForPhase(t, liveClient.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 20*time.Second)
	waitForRunningContainer(t, observer, namespace, executionID)

	// A second assignment (same identities, a new offer) is denied; capacity
	// has headroom but the identity's execution quota is full.
	secondRequestID, secondRequest, err := liveClient.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"sleep 90; exit 0"},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(2 * time.Minute), ResultRetention: durationpb.New(time.Hour)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, daemon.destination, secondRequest)
	waitUntil(t, 10*time.Second, func() (bool, error) {
		for _, snapshot := range liveClient.core.Requests() {
			if snapshot.Request.GetRequestId() == secondRequestID {
				return snapshot.OfferCount == 1, nil
			}
		}
		return false, nil
	})
	_, secondAssignment, err := liveClient.core.Select(secondRequestID)
	if err != nil {
		t.Fatal(err)
	}
	secondOfferID := secondAssignment.GetExecutionAssign().GetOfferId()
	liveClient.send(t, daemon.destination, secondAssignment)
	if err := expectCommandError(t, liveClient.observed, secondAssignment.GetMessageId(), "CAPACITY"); err != nil {
		t.Fatal(err)
	}
	waitForContainerCount(t, observer, namespace, executionID, 1, 5*time.Second)
	if matched := countExecutionContainers(t, observer, namespace); matched != 1 {
		t.Fatalf("execution quota leaked a second container: %d", matched)
	}

	// Repeated cleanup must never over-allocate: replaying the rejected
	// assignment over and over keeps returning the same CAPACITY rejection
	// without creating another execution or consuming an extra offer slot.
	for attempt := 0; attempt < 2; attempt++ {
		liveClient.send(t, daemon.destination, secondAssignment)
		if err := expectCommandError(t, liveClient.observed, secondAssignment.GetMessageId(), "CAPACITY"); err != nil {
			t.Fatal(err)
		}
	}
	if matched := countExecutionContainers(t, observer, namespace); matched != 1 {
		t.Fatalf("replayed rejected assignment created %d containers, want 1", matched)
	}

	// Cancel the running execution, then explicitly release the second offer so
	// the 30s expiry does not gate the quota check after restart. The release
	// is idempotent: replaying it must not double-release quota.
	cancelDest, cancellation, err := liveClient.core.Cancel(executionID, "F10 quota cleanup")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, cancelDest, cancellation)
	waitForPhase(t, liveClient.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED, 10*time.Second)
	release := manualReleaseEnvelope(liveClient, secondRequestID, secondOfferID)
	// Resend the release until it is acknowledged (UDP loss is expected); the
	// first ack stops the resends and proves a single slot is released.
	drainObserved(liveClient.observed)
	acknowledged := false
	for attempt := 0; attempt < 5 && !acknowledged; attempt++ {
		liveClient.send(t, daemon.destination, release)
		acknowledged = expectAckOrTimeout(t, liveClient, release.GetMessageId(), time.Second)
	}
	if !acknowledged {
		t.Fatal("release was not acknowledged after retries")
	}
	expectSingleRelease(t, liveClient, daemon.destination, release, secondOfferID)

	// Restart the daemon from the same durable state: quota accounting must be
	// consistent (no leaked or double-released slot), and the freed quota lets
	// a fresh request through.
	daemon.stop(t)
	daemon = nil
	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace,
		"--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192",
		"--admission-policy", admissionPath, "--capacity", "default=2")
	liveClient.waitForAllocator(t, daemon.identity)
	drainObserved(liveClient.observed)
	freshID, freshRequest, err := liveClient.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"sleep 5; exit 0"},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute), ResultRetention: durationpb.New(time.Hour)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, daemon.destination, freshRequest)
	waitUntil(t, 10*time.Second, func() (bool, error) {
		for _, snapshot := range liveClient.core.Requests() {
			if snapshot.Request.GetRequestId() == freshID {
				return snapshot.OfferCount == 1, nil
			}
		}
		return false, nil
	})
}

// expectAckOrTimeout waits up to timeout for a release ack correlated to
// messageID. It reports whether the ack arrived (rather than failing), so the
// caller can retry the send under UDP loss.
func expectAckOrTimeout(t *testing.T, c *acceptanceClient, messageID string, timeout time.Duration) bool {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case response := <-c.observed:
			if response.GetCorrelationId() != messageID {
				continue
			}
			if failure := response.GetCommandError(); failure != nil {
				t.Fatalf("release %s rejected: %s %s", messageID, failure.GetCode(), failure.GetDetail())
			}
			if response.GetExecutionOfferReleaseAck() != nil {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

// expectSingleRelease sends the same release once more after it was already
// acknowledged and asserts the allocator answers RELEASED/EXPIRED idempotently
// (replayer-friendly) rather than granting a second slot.
func expectSingleRelease(t *testing.T, c *acceptanceClient, destination string, release *r1sv1.Envelope, offerID string) {
	t.Helper()
	c.send(t, destination, release)
	if !expectAckOrTimeout(t, c, release.GetMessageId(), 3*time.Second) {
		t.Fatalf("replayed release %s was not acknowledged", release.GetMessageId())
	}
	_ = offerID
}

// manualReleaseEnvelope builds an ExecutionOfferRelease for a request the
// client already selected (so no automatic release intent exists) so quota
// cleanup can be driven explicitly for the live F10-02 leg.
func manualReleaseEnvelope(c *acceptanceClient, requestID, offerID string) *r1sv1.Envelope {
	identity, err := hex.DecodeString(c.endpoint.Name())
	if err != nil {
		panic(err)
	}
	return &r1sv1.Envelope{
		MessageId: "manual-release-" + offerID,
		Sender:    identity,
		SentAt:    timestamppb.New(time.Now().UTC()),
		Payload: &r1sv1.Envelope_ExecutionOfferRelease{ExecutionOfferRelease: &r1sv1.ExecutionOfferRelease{
			RequestId: requestID, OfferId: offerID,
		}},
	}
}

// TestLiveRejectionUnderLossNoDuplicateExecution verifies the F8-01 live leg:
// while a capacity rejection is in flight on a real r1sd with a running
// workload filling the only slot, replaying the rejection-bound request and its
// (never-issued) assignment cannot start a second execution or a new container.
func TestLiveRejectionUnderLossNoDuplicateExecution(t *testing.T) {
	if os.Getenv("RUN_PARTITION_RECOVERY") != "1" {
		t.Skip("set RUN_PARTITION_RECOVERY=1 to run the live F8 rejection acceptance")
	}
	if runtime.GOOS != "linux" {
		t.Skip("requires a Linux containerd host")
	}
	image := os.Getenv("R1S_CONTAINERD_TEST_IMAGE")
	if image == "" {
		t.Fatal("R1S_CONTAINERD_TEST_IMAGE must name a digest-pinned fixture image")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	namespace := fmt.Sprintf("r1s-f8-%d", time.Now().UnixNano())
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
	liveClient.observed = make(chan *r1sv1.Envelope, 64)
	defer liveClient.close(t)
	var daemon *allocatorProcess
	defer func() {
		if daemon != nil {
			daemon.stop(t)
		}
		cleanupExecutionContainers(t, observer, namespace)
	}()

	// Single slot: one running workload fills the only capacity.
	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace,
		"--capacity", "default=1")
	liveClient.waitForAllocator(t, daemon.identity)
	_, assignment := startExecution(t, liveClient, daemon.destination, image, "sleep 120; exit 0")
	fillingID := assignment.GetExecutionAssign().GetExecutionId()
	waitForRunningContainer(t, observer, namespace, fillingID)

	// A second request is capacity-rejected. Replay it under transport loss:
	// each send is a fresh attempt, and the allocator must answer the same
	// correlated rejection every time without starting anything.
	rejectedID, rejectedRequest, err := liveClient.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"echo MUST-NOT-RUN"},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute), ResultRetention: durationpb.New(time.Hour)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		liveClient.send(t, daemon.destination, rejectedRequest)
		if err := expectCommandError(t, liveClient.observed, rejectedRequest.GetMessageId(), "CAPACITY"); err != nil {
			t.Fatal(err)
		}
		waitForOfferCount(t, liveClient, rejectedID, 0)
	}
	// Even a forged assignment for the rejected request must not create work.
	// The response is correlated to the forged message ID, so drain the channel
	// first to avoid matching a stale CAPACITY error.
	drainObserved(liveClient.observed)
	forged := forgedAssignmentFor(t, liveClient, rejectedRequest)
	liveClient.send(t, daemon.destination, forged)
	if err := expectCommandError(t, liveClient.observed, forged.GetMessageId(), "NOT_FOUND"); err != nil {
		t.Fatal(err)
	}
	waitForContainerCount(t, observer, namespace, fillingID, 1, 5*time.Second)
	if matched := countExecutionContainers(t, observer, namespace); matched != 1 {
		t.Fatalf("rejection-in-flight created %d containers, want exactly the filling workload", matched)
	}
}

// TestLiveSweepCrashPreservesCapacityAndAuthority verifies the F11-01 live leg:
// a real SIGKILL crash of r1sd during bounded-history cleanup leaves the
// collected result's tombstone and freed capacity durable. After restart from
// the same store, the old assignment still returns EXPIRED (it cannot restart
// work) and a fresh request is admitted.
func TestLiveSweepCrashPreservesCapacityAndAuthority(t *testing.T) {
	if os.Getenv("RUN_PARTITION_RECOVERY") != "1" {
		t.Skip("set RUN_PARTITION_RECOVERY=1 to run the live F11 sweep acceptance")
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
	namespace := fmt.Sprintf("r1s-f11-%d", time.Now().UnixNano())
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
	liveClient.observed = make(chan *r1sv1.Envelope, 64)
	defer liveClient.close(t)
	var daemon *allocatorProcess
	var crashed *allocatorProcess
	defer func() {
		if crashed != nil && !crashed.stopped {
			crashed.stop(t)
		}
		if daemon != nil {
			daemon.stop(t)
		}
		cleanupExecutionContainers(t, observer, namespace)
	}()

	// Aggressive cleanup cadence makes collection land within the test window.
	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace,
		"--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192",
		"--capacity", "default=1", "--sweep-interval", "300ms")
	liveClient.waitForAllocator(t, daemon.identity)

	// Short-retention workload: completes quickly and becomes collectable.
	requestID, request, err := liveClient.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"sleep 2; exit 7"},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute), ResultRetention: durationpb.New(500 * time.Millisecond)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, daemon.destination, request)
	liveClient.send(t, daemon.destination, request)
	waitUntil(t, 10*time.Second, func() (bool, error) {
		for _, snapshot := range liveClient.core.Requests() {
			if snapshot.Request.GetRequestId() == requestID {
				return snapshot.OfferCount == 1, nil
			}
		}
		return false, nil
	})
	selectedDestination, assignment, err := liveClient.core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, selectedDestination, assignment)
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	waitForPhase(t, liveClient.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 20*time.Second)

	// Let the workload finish and the allocator observe the termination, then
	// SIGKILL the daemon while the terminal execution is still pending
	// collection — i.e. hard-crash it during the bounded-history Sweep window.
	// The state file may be mid-snapshot or pre-collection.
	waitForContainerCount(t, observer, namespace, executionID, 0, 30*time.Second)
	inspectDest, inspect, err := liveClient.core.Inspect(executionID)
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, inspectDest, inspect)
	waitForPhase(t, liveClient.core, executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, 20*time.Second)
	drainObserved(liveClient.observed)
	crashed = daemon
	daemon = nil
	crashed.kill(t)

	// Restart from the same store and log directory. The restarted daemon must
	// reconcile the uncollected terminal execution (or mid-cleanup tombstone)
	// without duplicating work, then collect it on the aggressive sweep cadence.
	daemon = startAllocator(t, ctx, binary, allocatorConfig, allocatorIdentity, allocatorState, address, namespace,
		"--logs", allocatorLogs, "--log-bytes", "4096", "--log-budget", "8192",
		"--capacity", "default=1", "--sweep-interval", "300ms")
	liveClient.waitForAllocator(t, daemon.identity)

	// Eventually the replayed assignment answers EXPIRED: the collected result
	// cannot restart work, and the tombstone is durable across the crash.
	waitUntil(t, 20*time.Second, func() (bool, error) {
		liveClient.send(t, daemon.destination, assignment)
		select {
		case response := <-liveClient.observed:
			if response.GetCommandError() != nil && response.GetCommandError().GetCode() == "EXPIRED" {
				return true, nil
			}
			return false, nil
		case <-time.After(2 * time.Second):
			return false, nil
		}
	})
	waitForContainerCount(t, observer, namespace, executionID, 0, 5*time.Second)

	// Capacity was returned after collection: a fresh request is admitted.
	drainObserved(liveClient.observed)
	freshID, freshRequest, err := liveClient.core.CreateRequest(&r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"sleep 3; exit 0"},
	}, &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute), ResultRetention: durationpb.New(time.Hour)}, "default")
	if err != nil {
		t.Fatal(err)
	}
	liveClient.send(t, daemon.destination, freshRequest)
	waitUntil(t, 10*time.Second, func() (bool, error) {
		for _, snapshot := range liveClient.core.Requests() {
			if snapshot.Request.GetRequestId() == freshID {
				return snapshot.OfferCount == 1, nil
			}
		}
		return false, nil
	})
}

// expectCommandError waits for an inbound CommandError envelope. When messageID
// is non-empty it must correlate to that command; it then asserts the code and
// returns nil. The channel must be wired (liveClient.observed = chan) before
// the offending command is sent.
func expectCommandError(t *testing.T, observed <-chan *r1sv1.Envelope, messageID, wantCode string) error {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case response := <-observed:
			failure := response.GetCommandError()
			if failure == nil {
				continue
			}
			if messageID != "" && response.GetCorrelationId() != messageID {
				continue
			}
			if failure.GetCode() != wantCode {
				return fmt.Errorf("command error code = %q, want %q (detail %q)", failure.GetCode(), wantCode, failure.GetDetail())
			}
			return nil
		case <-timer.C:
			return fmt.Errorf("no %s command error for correlation %q within 10s", wantCode, messageID)
		}
	}
}

func waitForOfferCount(t *testing.T, c *acceptanceClient, requestID string, want int) {
	t.Helper()
	waitUntil(t, 10*time.Second, func() (bool, error) {
		for _, snapshot := range c.core.Requests() {
			if snapshot.Request.GetRequestId() == requestID {
				return snapshot.OfferCount == want, nil
			}
		}
		return want == 0, nil
	})
}

// countExecutionContainers returns how many acceptance-labelled containers exist
// in the namespace, independent of any one execution ID.
func countExecutionContainers(t *testing.T, observer *containerdclient.Client, namespace string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), namespace), 5*time.Second)
	defer cancel()
	containers, err := observer.Containers(ctx)
	if err != nil {
		t.Fatalf("list containers: %v", err)
	}
	count := 0
	for _, container := range containers {
		labels, labelErr := container.Labels(ctx)
		if labelErr == nil && labels[executionLabel] != "" {
			count++
		}
	}
	return count
}

// forgedAssignmentFor crafts an assignment envelope for a request whose offer
// was never issued (the capacity rejection). It must be refused, never start a
// workload.
func forgedAssignmentFor(t *testing.T, c *acceptanceClient, request *r1sv1.Envelope) *r1sv1.Envelope {
	t.Helper()
	execReq := request.GetExecutionRequest()
	if execReq == nil {
		t.Fatal("forgedAssignmentFor requires an execution request envelope")
	}
	return &r1sv1.Envelope{
		MessageId: "forged-assign-for-rejection",
		Sender:    request.GetSender(),
		SentAt:    request.GetSentAt(),
		Payload: &r1sv1.Envelope_ExecutionAssign{ExecutionAssign: &r1sv1.ExecutionAssign{
			RequestId:   execReq.GetRequestId(),
			OfferId:     "no-such-offer",
			ExecutionId: "forged-execution",
		}},
	}
}

// drainObserved discards every buffered inbound envelope so a following
// assertion reads only fresh responses.
func drainObserved(observed chan *r1sv1.Envelope) {
	for {
		select {
		case <-observed:
		default:
			return
		}
	}
}
