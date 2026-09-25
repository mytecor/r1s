package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

// fakeLogSource simulates allocator-local logs: a per-(execution, stream) byte
// store that can be temporarily partitioned and later grows. Reads are by
// offset exactly like the real allocator, so the tail must drain gaps with no
// duplicates or losses.
type fakeLogSource struct {
	mu          sync.Mutex
	data        map[string]map[string][]byte
	partitioned bool
}

func newFakeLogSource() *fakeLogSource {
	return &fakeLogSource{data: make(map[string]map[string][]byte)}
}

func (f *fakeLogSource) append(executionID, stream string, data string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.data[executionID] == nil {
		f.data[executionID] = make(map[string][]byte)
	}
	f.data[executionID][stream] = append(f.data[executionID][stream], data...)
}

func (f *fakeLogSource) setPartitioned(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitioned = v
}

func (f *fakeLogSource) tailChunk(ctx context.Context, executionID, stream string, offset uint64, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.partitioned {
		// A partitioned allocator times out; never evidence of loss.
		return nil, errLogTimeout(offset)
	}
	data := f.data[executionID][stream]
	if offset > uint64(len(data)) {
		return nil, errLogTimeout(offset)
	}
	end := offset + MaxLogChunkBytes
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return &r1sv1.ExecutionLogsResponse{
		ExecutionId: executionID, Stream: stream, Offset: offset, Data: data[offset:end],
		NextOffset: end, Eof: end == uint64(len(data)),
	}, nil
}

func drainUntil(t *testing.T, ctx context.Context, tail *runTail, want string, targets ...*bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		tail.poll(ctx)
		total := ""
		for _, target := range targets {
			total += target.String()
		}
		if strings.Contains(total, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tail did not deliver %q; got %q", want, total)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRunTailSeparatesStdoutAndStderr(t *testing.T) {
	source := newFakeLogSource()
	var out, errW bytes.Buffer
	tail := newForegroundTail(source, &out, &errW)
	tail.SetActive("exec-1", "run-1", 1)

	source.append("exec-1", "stdout", "normal output\n")
	source.append("exec-1", "stderr", "warning\n")
	drainUntil(t, context.Background(), tail, "normal output\n", &out, &errW)
	drainUntil(t, context.Background(), tail, "warning\n", &out, &errW)

	if !strings.Contains(out.String(), "normal output\n") || strings.Contains(out.String(), "warning") {
		t.Fatalf("stdout = %q", out.String())
	}
	if !strings.Contains(errW.String(), "warning\n") || strings.Contains(errW.String(), "normal output") {
		t.Fatalf("stderr = %q", errW.String())
	}
	// The tail must never write to stdout the stream it read from stderr.
	if strings.Contains(out.String(), "warning") {
		t.Fatalf("stdout stole stderr bytes: %q", out.String())
	}
}

func TestRunTailRecoversPartitionGapByOffsetWithNoDuplicates(t *testing.T) {
	source := newFakeLogSource()
	var got bytes.Buffer
	tail := newForegroundTail(source, &got, &got)
	tail.SetActive("exec-1", "run-1", 1)

	// Phase 1: drain the pre-partition bytes.
	source.append("exec-1", "stdout", "hello ")
	drainUntil(t, context.Background(), tail, "hello ", &got)
	before := len(got.String())

	// Phase 2: partition, then the allocator produces bytes the tail cannot see.
	source.setPartitioned(true)
	source.append("exec-1", "stdout", "world and then a lot more follow")
	tail.poll(context.Background())
	if got.Len() != before {
		t.Fatalf("partition leaked bytes: %q", got.String()[before:])
	}

	// Phase 3: reconnect. The tail resumes at its recorded offset, so it drains
	// exactly the bytes produced during the partition — none skipped, none
	// duplicated — and the message is byte-identical to the concatenation.
	source.setPartitioned(false)
	drainUntil(t, context.Background(), tail, "hello world and then a lot more follow", &got)
	want := "hello " + "world and then a lot more follow"
	if got.String() != want {
		t.Fatalf("reconnected bytes = %q, want %q (no dup/loss)", got.String(), want)
	}
}

func TestRunTailBoundedChunkRequest(t *testing.T) {
	if MaxLogChunkBytes < 16<<10 || MaxLogChunkBytes > 64<<10 {
		t.Fatalf("MaxLogChunkBytes = %d, want within 16..64 KiB", MaxLogChunkBytes)
	}
	if MaxLogChunkBytes > protocol.MaxLogBytes {
		t.Fatalf("MaxLogChunkBytes %d exceeds protocol cap %d", MaxLogChunkBytes, protocol.MaxLogBytes)
	}
}

func TestRunTailRescheduleEmitsMarkerAndResetsOffsets(t *testing.T) {
	dir := t.TempDir()
	file, err := os.OpenFile(filepath.Join(dir, "output.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	source := newFakeLogSource()

	tail := newDetachedTail(source, file)
	tail.SetActive("exec-1", "run-1", 1)
	source.append("exec-1", "stdout", "attempt one")
	tail.poll(context.Background())
	if off := tail.Offsets()["stdout"]; off != 11 {
		t.Fatalf("initial offset = %d, want 11", off)
	}

	// A reschedule to a new execution appends a bounded service marker and the
	// offsets reset to the new execution's start.
	tail.SetActive("exec-2", "run-1", 2)
	if off := tail.Offsets()["stdout"]; off != 0 {
		t.Fatalf("offset not reset after reschedule: %d", off)
	}
	source.append("exec-2", "stdout", "attempt two")
	tail.poll(context.Background())

	content, err := os.ReadFile(filepath.Join(dir, "output.log"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	if !strings.Contains(text, "[r1s] run=run-1 attempt=1\n") {
		t.Fatalf("missing start marker in %q", text)
	}
	if !strings.Contains(text, "[r1s] rescheduled attempt=2\n") {
		t.Fatalf("missing reschedule marker in %q", text)
	}
	// Both attempts land in the same file, in order, without clobbering the
	// marker or each other.
	suf := "attempt one" + "[r1s] rescheduled attempt=2\n" + "attempt two"
	if !strings.HasSuffix(text, suf) {
		t.Fatalf("output did not append both attempts in order: %q", text)
	}
}

func TestRunTailDropsStaleChunkAfterReschedule(t *testing.T) {
	source := newFakeLogSource()
	var out bytes.Buffer
	tail := newForegroundTail(source, &out, &out)
	tail.SetActive("exec-1", "run-1", 1)
	source.append("exec-1", "stdout", "aaaaaaaaaaaaaaaaaa") // 18 bytes
	tail.poll(context.Background())
	if off := tail.Offsets()["stdout"]; off != 18 {
		t.Fatalf("offset = %d, want 18", off)
	}

	// Simulate a chunk fetched for exec-1 landing after a reschedule already
	// rotated to exec-2: the old stream must not misroute into exec-2's offsets.
	tail.SetActive("exec-2", "run-1", 2)
	source.append("exec-1", "stdout", "STALE")
	source.append("exec-2", "stdout", "fresh")
	tail.poll(context.Background())
	if tail.Offsets()["stdout"] != 5 {
		t.Fatalf("offset = %d, want 5 (exec-2 fresh bytes)", tail.Offsets()["stdout"])
	}
	if strings.Contains(out.String(), "STALE") {
		t.Fatalf("stale exec-1 bytes leaked into exec-2: %q", out.String())
	}
}
