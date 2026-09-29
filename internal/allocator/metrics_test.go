package allocator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"github.com/mytecor/r1s/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAllocatorMetricsTelemetry(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	m := telemetry.NewMetrics()

	cfg := Config{
		Identity: []byte("test-allocator"),
		Capacity: map[string]uint32{"default": 2},
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Metrics:  m,
	}

	alloc, err := New(cfg, runtime)
	if err != nil {
		t.Fatalf("allocator.New: %v", err)
	}

	// 1. Request -> Offer
	req := requestEnvelope(clock.Now(), "msg-1", "client-1", "req-1")
	offerResp := mustHandle(t, alloc, req)
	offer := offerResp.GetExecutionOffer()
	if offer == nil {
		t.Fatal("expected offer response")
	}

	// Verify requests and offers recorded
	if count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_requests_total"); count != 1 {
		t.Errorf("requests_total count = %d, want 1", count)
	}
	if count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_offers_total"); count != 1 {
		t.Errorf("offers_total count = %d, want 1", count)
	}
	if count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_dispatch_latency_seconds"); count < 1 {
		t.Errorf("dispatch_latency_seconds count = %d, want at least 1", count)
	}

	// 2. Assign -> Running
	assign := assignEnvelope(clock.Now(), "msg-2", "client-1", "req-1", offer.GetOfferId(), "exec-1")
	stateResp := mustHandle(t, alloc, assign)
	if stateResp.GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("phase = %v, want RUNNING", stateResp.GetExecutionState().GetPhase())
	}

	// 3. Rejection metric on unauthorized cancel
	badCancel := cancelEnvelope(clock.Now(), "bad-cancel", "other-client", "exec-1")
	if _, err := alloc.Handle(context.Background(), badCancel); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}

	// Verify rejection recorded with UNAUTHORIZED reason
	if count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_rejections_total"); count != 1 {
		t.Errorf("rejections_total count = %d, want 1", count)
	}

	// 4. Complete execution -> ObserveExecutionTerminal
	clock.Advance(3 * time.Second)
	exitCode := int32(0)
	if err := runtime.complete("exec-1", r1sruntime.Completion{ExecutionID: "exec-1", ExitCode: &exitCode}); err != nil {
		t.Fatalf("RuntimeCompleted: %v", err)
	}

	if count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_executions_total"); count != 1 {
		t.Errorf("executions_total count = %d, want 1", count)
	}
	if count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_execution_duration_seconds"); count != 1 {
		t.Errorf("execution_duration_seconds count = %d, want 1", count)
	}

	// 5. Test Lease Eviction metric
	clock.Advance(1 * time.Second)
	req2 := requestEnvelope(clock.Now(), "msg-3", "client-2", "req-2")
	offerResp2 := mustHandle(t, alloc, req2)
	assign2 := assignEnvelope(clock.Now(), "msg-4", "client-2", "req-2", offerResp2.GetExecutionOffer().GetOfferId(), "exec-2")
	mustHandle(t, alloc, assign2)

	// Expire the lease
	clock.Advance(30 * time.Minute)
	if err := alloc.EvictExpiredLeases(context.Background()); err != nil {
		t.Fatalf("EvictExpiredLeases: %v", err)
	}

	if count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_lease_evictions_total"); count != 1 {
		t.Errorf("lease_evictions_total count = %d, want 1", count)
	}
}

// dispatchLatencySeries returns the set of command labels observed in the
// dispatch_latency_seconds histogram.
func dispatchLatencySeries(t *testing.T, m *telemetry.Metrics) map[string]bool {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	series := make(map[string]bool)
	for _, f := range families {
		if f.GetName() != "r1s_allocator_dispatch_latency_seconds" {
			continue
		}
		for _, metric := range f.GetMetric() {
			command := ""
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "command" {
					command = lp.GetValue()
				}
			}
			series[command] = true
		}
	}
	return series
}

// TestDispatchLatencyLabelsCommand checks that the dispatch-latency metric is
// attributed to the actual command for exit paths that do not reach the payload
// switch: the replay-duplicate ack must be labeled "request", and an unknown
// payload must not be silently labeled as the previous command.
func TestDispatchLatencyLabelsCommand(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	m := telemetry.NewMetrics()

	cfg := Config{
		Identity: []byte("test-allocator"),
		Capacity: map[string]uint32{"default": 2},
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Metrics:  m,
	}
	alloc, err := New(cfg, runtime)
	if err != nil {
		t.Fatalf("allocator.New: %v", err)
	}

	req := requestEnvelope(clock.Now(), "msg-1", "client", "req-1")
	if _, err := alloc.Handle(context.Background(), req); err != nil {
		t.Fatalf("first request: %v", err)
	}
	// A redelivered message with the same ID is answered from durable state
	// as a replay duplicate; it must still count toward the request command.
	if _, err := alloc.Handle(context.Background(), req); err != nil {
		t.Fatalf("duplicate request: %v", err)
	}
	// An envelope with a payload type outside the dispatch table still records
	// a latency observation, but it must not borrow a neighbor's label: it is
	// allowed to appear as "unknown" only because it is genuinely unknown.
	if _, err := alloc.Handle(context.Background(), &r1sv1.Envelope{MessageId: "msg-2", Sender: []byte("client"), SentAt: timestamppb.New(clock.Now())}); err == nil {
		t.Fatalf("expected error for payload-less envelope")
	}

	series := dispatchLatencySeries(t, m)
	if !series["request"] {
		t.Errorf("dispatch_latency missing command=\"request\" series, got %v", series)
	}
	if len(series) > 2 {
		t.Errorf("dispatch_latency has unexpected extra command series: %v", series)
	}
}

// gaugeValue returns the numeric value of a gauge series matched by one label.
func gaugeValue(t *testing.T, m *telemetry.Metrics, familyName, labelName, labelValue string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != familyName {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

func metricLabelValues(t *testing.T, m *telemetry.Metrics, familyName, labelName string) map[string]bool {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	values := make(map[string]bool)
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelName {
					values[label.GetValue()] = true
				}
			}
		}
	}
	return values
}

func counterValue(t *testing.T, m *telemetry.Metrics, familyName string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var total float64
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
	}
	return total
}

func TestRequestMetricsBoundUnconfiguredResourceClasses(t *testing.T) {
	clock := newFakeClock()
	m := telemetry.NewMetrics()
	alloc, err := New(Config{
		Identity: []byte("test-allocator"),
		Capacity: map[string]uint32{"default": 1},
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Metrics:  m,
	}, newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}

	for i := range 32 {
		request := requestEnvelope(clock.Now(), fmt.Sprintf("message-%d", i), "client", fmt.Sprintf("request-%d", i))
		request.GetExecutionRequest().ResourceClass = fmt.Sprintf("untrusted-%d", i)
		if _, err := alloc.Handle(context.Background(), request); !errors.Is(err, ErrCapacityExhausted) {
			t.Fatalf("request %d error = %v, want ErrCapacityExhausted", i, err)
		}
	}

	labels := metricLabelValues(t, m, "r1s_allocator_requests_total", "resource_class")
	if len(labels) != 1 || !labels[unconfiguredResourceClass] {
		t.Fatalf("request resource_class labels = %v, want only %q", labels, unconfiguredResourceClass)
	}
}

func TestCompletionMetricsWaitForDurableCommit(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	store := &releaseFailStore{memoryStateStore: newMemoryStateStore()}
	m := telemetry.NewMetrics()
	alloc, err := New(Config{
		Identity: []byte("test-allocator"),
		Capacity: map[string]uint32{"default": 1},
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Store:    store,
		Metrics:  m,
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	offer := mustHandle(t, alloc, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	mustHandle(t, alloc, assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))

	store.failAt = store.saves + 1
	exitCode := int32(0)
	if err := runtime.complete("execution", r1sruntime.Completion{ExitCode: &exitCode}); !errors.Is(err, ErrStore) {
		t.Fatalf("completion error = %v, want ErrStore", err)
	}
	if got := counterValue(t, m, "r1s_allocator_executions_total"); got != 0 {
		t.Fatalf("executions_total after failed commit = %v, want 0", got)
	}
	if got := gaugeValue(t, m, "r1s_allocator_active_executions", "resource_class", "default"); got != 1 {
		t.Fatalf("active_executions after rollback = %v, want 1", got)
	}
	if got := gaugeValue(t, m, "r1s_allocator_available_slots", "resource_class", "default"); got != 0 {
		t.Fatalf("available_slots after rollback = %v, want 0", got)
	}

	if err := runtime.complete("execution", r1sruntime.Completion{ExitCode: &exitCode}); err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, m, "r1s_allocator_executions_total"); got != 1 {
		t.Fatalf("executions_total after successful retry = %v, want 1", got)
	}
}

// TestSyncMetricsResetsLegacyClassActiveGauge checks that a retained terminal
// record whose class is not in the current capacity limits still resets the
// active-executions gauge to zero instead of leaving a stale non-zero value.
// The scenario mirrors an allocator restarted with a reduced capacity config
// while a terminal record for a removed class is still retained.
func TestSyncMetricsResetsLegacyClassActiveGauge(t *testing.T) {
	clock := newFakeClock()
	m := telemetry.NewMetrics()

	alloc, err := New(Config{
		Identity: []byte("test-allocator"),
		Capacity: map[string]uint32{"default": 1},
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Metrics:  m,
	}, newFakeRuntime())
	if err != nil {
		t.Fatalf("allocator.New: %v", err)
	}

	// Inject a non-terminal legacy-class record directly; the allocator state
	// map is package-private, and this is the exact state a restart with a
	// changed capacity config can leave behind.
	alloc.mu.Lock()
	alloc.executions["exec-1"] = &executionRecord{
		id:            "exec-1",
		resourceClass: "legacy",
		phase:         r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING,
		occurredAt:    clock.Now().UTC(),
		revision:      1,
	}
	alloc.syncMetricsLocked()
	alloc.mu.Unlock()

	if got := gaugeValue(t, m, "r1s_allocator_active_executions", "resource_class", unconfiguredResourceClass); got != 1 {
		t.Errorf("active_executions[%s] = %v, want 1 while running", unconfiguredResourceClass, got)
	}

	// The record goes terminal; the next sync must reset the gauge to zero even
	// though "legacy" is not among the configured capacity classes.
	alloc.mu.Lock()
	alloc.executions["exec-1"].phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED
	alloc.syncMetricsLocked()
	alloc.mu.Unlock()

	if got := gaugeValue(t, m, "r1s_allocator_active_executions", "resource_class", unconfiguredResourceClass); got != 0 {
		t.Errorf("active_executions[%s] = %v after completion, want 0", unconfiguredResourceClass, got)
	}
}
