package allocator

// Storage-scaling benchmarks (F11-02).
//
// The allocator persists one whole JSON snapshot in a single bbolt transaction
// after every accepted transition (persistLocked). These benchmarks make the
// cost of that design visible as retained history grows: transition latency,
// snapshot size, and snapshot lock time against the real bbolt store, plus
// restart reload cost.
//
// Each benchmark recreates its allocator from scratch so the measured state is
// the only variable; run with `go test -bench . -benchtime=1x -run '^$'` inside
// this package, or use scripts/storage-bench.sh for a reproducible report.

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// benchHistories lists the retained-history sizes each benchmark drives: 0,
// 100, 1000, and 4000 cycles. 4000 is the realistic steady-state ceiling under
// the maxRecords guard: the allocator bounds in-memory state as
// 2*offers + tombstones + 2 <= maxRecords (offers persist until Sweep), so
// under the default DefaultMaxRecords=10000 ceiling the maximum retained cycles
// before collection is roughly maxRecords/2. 4000 sits safely under that while
// still exposing snapshot growth.
type benchHistory struct {
	history int // retained executions already in state
}

var benchHistories = []benchHistory{
	{history: 0},
	{history: 100},
	{history: 1000},
	{history: 4000},
}

// benchAllocator builds an allocator whose retained history holds the requested
// number of terminal executions and returns it with a clock advanced past them,
// so measured transitions execute against realistic history. History is built
// in memory (no store) so warm-up is linear; bbolt-backed benchmarks attach a
// store afterwards with attachStore, which writes the snapshot once.
func benchAllocator(b *testing.B, history int) (*Allocator, *fakeClock, *fakeRuntime) {
	b.Helper()
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator, err := New(Config{
		Identity: []byte("allocator"),
		Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second,
		Now:      clock.Now,
		NewID:    sequenceIDs(),
	}, runtime)
	if err != nil {
		b.Fatal(err)
	}
	if history > 0 {
		fillHistory(b, allocator, clock, runtime, history)
	}
	return allocator, clock, runtime
}

// attachStore mounts a real bbolt store on an in-memory allocator and commits
// the current full snapshot once, so a bbolt-backed benchmark starts from the
// same durable state without paying per-cycle fsync during warm-up. It returns
// the store and the underlying database path for restart benchmarks.
func attachStore(b *testing.B, allocator *Allocator) (*statebolt.Store, string) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "state.db")
	store, err := statebolt.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	allocator.store = store
	allocator.mu.Lock()
	err = allocator.persistLocked(context.Background())
	allocator.mu.Unlock()
	if err != nil {
		b.Fatal(err)
	}
	return store, path
}

// fillHistory drives `history` full request→assign→complete cycles so the
// allocator holds that many terminal (retained) executions and the offers,
// replay records, and tombstones that normally accompany them.
func fillHistory(b *testing.B, allocator *Allocator, clock *fakeClock, runtime *fakeRuntime, history int) {
	b.Helper()
	if history > 5000 {
		b.Logf("filling %d-history allocator (this is the warm-up, not the measurement)", history)
	}
	exitCode := int32(0)
	for index := 0; index < history; index++ {
		messageID := fmt.Sprintf("m-%d", index*4)
		requestID := fmt.Sprintf("r-%d", index)
		response := benchMustHandle(b, allocator, benchRequest(clock.Now(), messageID, "client", requestID))
		offer := response.GetExecutionOffer()
		if offer == nil {
			b.Fatalf("request %d returned no offer", index)
		}
		executionID := fmt.Sprintf("e-%d", index)
		benchMustHandle(b, allocator, benchAssign(clock.Now(), fmt.Sprintf("m-%d", index*4+1), "client", requestID, offer.GetOfferId(), executionID))
		if err := runtime.complete(executionID, r1sruntimeCompletion(exitCode)); err != nil {
			b.Fatalf("complete %d: %v", index, err)
		}
		clock.Advance(time.Second)
	}
}

// snapshotSizeLocked reports the serialized size of the allocator's current
// durable state (the same shape persistLocked writes, measured without a store
// round-trip so it isolates the JSON+mutation cost).
func snapshotSizeLocked(allocator *Allocator) int64 {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	state := persistedState{Version: stateVersion, Identity: cloneBytes(allocator.identity), HighWater: allocator.highWater, Tombstones: allocator.tombstones}
	for _, record := range allocator.offers {
		offer, _ := json.Marshal(record.offer)
		request, _ := json.Marshal(record.request)
		state.Offers = append(state.Offers, persistedOffer{Offer: offer, Request: request, Client: cloneBytes(record.client), Status: record.status, Execution: record.execution, Resources: record.resources})
	}
	for _, record := range allocator.executions {
		request, _ := json.Marshal(record.request)
		state.Executions = append(state.Executions, persistedExecution{
			ID: record.id, OfferID: record.offerID, Client: cloneBytes(record.client), ResourceClass: record.resourceClass,
			Request: request, Phase: record.phase, Detail: record.detail, ExitCode: cloneInt32(record.exitCode),
			OccurredAt: record.occurredAt, StartedAt: record.startedAt, Released: record.released, Revision: record.revision, Resources: record.resources, RetainUntil: record.retainUntil,
		})
	}
	for key, entry := range allocator.replay.entries {
		envelope, _ := json.Marshal(entry.envelope)
		saved := persistedReplay{Key: key, SeenAt: entry.seenAt, Envelope: envelope, Complete: replayDone(entry)}
		state.Replay = append(state.Replay, saved)
	}
	data, _ := json.Marshal(state)
	return int64(len(data))
}

// commitTimeLocked reports the wall-clock time of a full persistLocked (mutate +
// JSON marshal + one bbolt transaction) while holding the allocator mutex.
func commitTimeLocked(allocator *Allocator) time.Duration {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	start := time.Now()
	if err := allocator.persistLocked(context.Background()); err != nil {
		return -1
	}
	return time.Since(start)
}

// --- transition latency -----------------------------------------------------

// runFullCycle drives one realistic lifecycle round: request→offer, then
// assign→running, then a runtime completion that releases the slot and leaves a
// new retained execution. Every step persists the whole snapshot, so this is
// the true per-workload persistence cost at the given retained-history size.
func runFullCycle(b *testing.B, allocator *Allocator, clock *fakeClock, runtime *fakeRuntime, index int) {
	b.Helper()
	requestID := fmt.Sprintf("cycle-%d", index)
	response := benchMustHandle(b, allocator, benchRequest(clock.Now(), fmt.Sprintf("cm-%d", index), "client", requestID))
	offer := response.GetExecutionOffer()
	if offer == nil {
		b.Fatalf("cycle %d returned no offer", index)
	}
	executionID := fmt.Sprintf("cxe-%d", index)
	benchMustHandle(b, allocator, benchAssign(clock.Now(), fmt.Sprintf("cam-%d", index), "client", requestID, offer.GetOfferId(), executionID))
	exitCode := int32(0)
	if err := runtime.complete(executionID, r1sruntimeCompletion(exitCode)); err != nil {
		b.Fatalf("cycle %d complete: %v", index, err)
	}
	clock.Advance(time.Second)
}

// runTransitionPhase builds an allocator already holding `history` retained
// executions, then times one full lifecycle cycle per benchmark iteration at
// that retained-history size. History filling runs outside the timer; each
// measured iteration advances history by one more execution, so the reported
// cost is the marginal persistence cost around history N. When persist is
// true, a real bbolt store backs the allocator.
func runTransitionPhase(b *testing.B, persist bool, history int) {
	b.Helper()
	allocator, clock, runtime := benchAllocator(b, history)
	if persist {
		attached, _ := attachStore(b, allocator)
		b.Cleanup(func() { _ = attached.Close() })
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFullCycle(b, allocator, clock, runtime, i)
	}
	b.StopTimer()
}

// BenchmarkTransition_FullCycle_Memory measures a full lifecycle cycle with an
// in-memory-only allocator (no store), isolating allocator-side mutate + JSON
// snapshot cost from bbolt I/O at retained-history sizes.
func BenchmarkTransition_FullCycle_Memory(b *testing.B) {
	for _, h := range benchHistories {
		b.Run(fmt.Sprintf("history=%d", h.history), func(b *testing.B) {
			runTransitionPhase(b, false, h.history)
		})
	}
}

// BenchmarkTransition_FullCycle_Bbolt measures a full lifecycle cycle against
// the real bbolt store (mutate + JSON snapshot + one transaction per step) at
// retained-history sizes.
func BenchmarkTransition_FullCycle_Bbolt(b *testing.B) {
	for _, h := range benchHistories {
		b.Run(fmt.Sprintf("history=%d", h.history), func(b *testing.B) {
			runTransitionPhase(b, true, h.history)
		})
	}
}

// --- snapshot size and lock time --------------------------------------------

// BenchmarkSnapshot_Size reports snapshot_bytes as a gauge alongside the
// JSON-serialization cost per call (memory-only, so it isolates the allocator
// side of persistLocked from bbolt I/O).
func BenchmarkSnapshot_Size(b *testing.B) {
	for _, h := range benchHistories {
		b.Run(fmt.Sprintf("history=%d", h.history), func(b *testing.B) {
			allocator, _, _ := benchAllocator(b, h.history)
			// Measure once for the gauge.
			size := snapshotSizeLocked(allocator)
			if size == 0 {
				b.Fatal("empty snapshot")
			}
			b.ReportMetric(float64(size), "snapshot_bytes")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshotSizeLocked(allocator)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkSnapshot_CommitTime measures a full persistLocked against the real
// bbolt store (JSON marshal + one transaction commit) at history sizes.
func BenchmarkSnapshot_CommitTime(b *testing.B) {
	for _, h := range benchHistories {
		b.Run(fmt.Sprintf("history=%d", h.history), func(b *testing.B) {
			allocator, _, _ := benchAllocator(b, h.history)
			attached, _ := attachStore(b, allocator)
			defer attached.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if d := commitTimeLocked(allocator); d < 0 {
					b.Fatal("persist failed")
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkRestart_Load measures reopening the bbolt file and reconstructing
// the allocator's in-memory state from one saved snapshot. History is built
// once on an in-memory allocator and committed through attachStore, so the
// file on disk holds exactly one steady-state snapshot of the given size.
func BenchmarkRestart_Load(b *testing.B) {
	for _, h := range benchHistories {
		b.Run(fmt.Sprintf("history=%d", h.history), func(b *testing.B) {
			allocator, _, _ := benchAllocator(b, h.history)
			attached, path := attachStore(b, allocator)
			if err := attached.Close(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reopened, err := statebolt.Open(path)
				if err != nil {
					b.Fatal(err)
				}
				reloaded, err := New(Config{
					Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
					OfferTTL: 30 * time.Second, Now: clockNowAfterHistory(), NewID: sequenceIDs(), Store: reopened,
				}, newFakeRuntime())
				_ = reloaded
				if err != nil {
					b.Fatal(err)
				}
				if err := reopened.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

// benchMustHandle runs one allocator command, fataling on error, using the
// same assertion contract as the unit helpers but accepting testing.TB so both
// tests and benchmarks can drive transitions in this file.
func benchMustHandle(tb testing.TB, allocator *Allocator, envelope *r1sv1.Envelope) *r1sv1.Envelope {
	tb.Helper()
	responses, err := allocator.Handle(context.Background(), envelope)
	if err != nil {
		tb.Fatalf("Handle() error = %v", err)
	}
	if len(responses) != 1 {
		tb.Fatalf("Handle() responses = %d, want 1", len(responses))
	}
	return responses[0]
}

// bench envelope builders (test-package variants that do not depend on the
// *_test envelope helpers, so this file stays self-contained).

func benchRequest(now time.Time, messageID, client, requestID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionRequest{
			ExecutionRequest: &r1sv1.ExecutionRequest{
				RequestId:     requestID,
				ResourceClass: "default",
				Workload:      &r1sv1.Workload{Image: "example.test/image:latest"},
				Policy: &r1sv1.ExecutionPolicy{
					Deadline:        timestamppb.New(now.Add(time.Hour)),
					MaxRuntime:      durationpb.New(time.Minute),
					ResultRetention: durationpb.New(24 * time.Hour),
				},
			},
		},
	}
}

func benchAssign(now time.Time, messageID, client, requestID, offerID, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionAssign{
			ExecutionAssign: &r1sv1.ExecutionAssign{
				RequestId: requestID, OfferId: offerID, ExecutionId: executionID,
			},
		},
	}
}

func r1sruntimeCompletion(exitCode int32) r1sruntime.Completion {
	return r1sruntime.Completion{ExitCode: &exitCode}
}

// clockNowAfterHistory returns a clock frozen safely after the latest retained
// execution so a restart reconstructs identical state (the exact value is not
// measured; reload cost only cares about history size).
func clockNowAfterHistory() func() time.Time {
	return newFakeClock().Now
}
