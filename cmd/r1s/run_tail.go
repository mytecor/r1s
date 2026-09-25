package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// Foreground and detached log continuity (F22-05).
//
// r1s run tails allocator-local stdout/stderr by byte offset so a temporary
// RNS partition can be recovered after reconnect with no duplicate or missing
// bytes within allocator retention limits. Logs are always an explicit
// authenticated pull (ExecutionLogsRequest/ExecutionLogsResponse); nothing here
// ever attaches or auto-sends logs on completion, failure, inspection, or
// reconnect. Both streams and the reschedule markers share the retrieved line
// destination so a detached run records all attempts in one output file.

// tailChunkSource is the narrow read surface the run tail loop needs. The
// application implements it with retrieveLogs (an RNS round trip); tests provide
// a fake so offset recovery, partition gaps, and rescheduling can be exercised
// without a network.
type tailChunkSource interface {
	tailChunk(ctx context.Context, executionID, stream string, offset uint64, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error)
}

// application tailChunk adapts the existing explicit bounded log read to the
// tail loop's narrow surface.
func (a *application) tailChunk(ctx context.Context, executionID, stream string, offset uint64, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	return a.retrieveLogs(ctx, executionID, stream, offset, MaxLogChunkBytes, wait)
}

// tailInterval paces one log poll. Each poll is at most one bounded chunk per
// stream, so a short interval keeps the terminal responsive without letting a
// run hold more than a few control round trips in flight.
const tailInterval = 200 * time.Millisecond

// tailLogWait bounds one log retrieval wait inside the tail loop. A timeout is
// inconclusive (partition), never evidence of loss, and simply schedules the
// next poll.
const tailLogWait = 2 * time.Second

// MaxLogChunkBytes is the bounded chunk the run tail requests. It sits inside
// F22's 16–64 KiB implementation range and stays well under the protocol cap.
const MaxLogChunkBytes = 64 << 10

type runTail struct {
	source tailChunkSource

	// stdoutTarget and stderrTarget receive the corresponding allocator stream
	// in foreground mode. In detached mode both are nil and every stream plus
	// the service markers land in file instead.
	stdoutTarget io.Writer
	stderrTarget io.Writer
	file         *os.File // detached destination (nil in foreground)

	mu      sync.Mutex
	active  string            // current execution ID tailed
	seen    string            // last execution ID the offsets belong to
	off     map[string]uint64 // stream -> next byte offset in the active execution
	runID   string            // logical run ID for markers
	attempt uint64            // attempt of the active execution (for markers)
}

func newForegroundTail(source tailChunkSource, stdout, stderr io.Writer) *runTail {
	return &runTail{source: source, stdoutTarget: stdout, stderrTarget: stderr}
}

func newDetachedTail(source tailChunkSource, file *os.File) *runTail {
	return &runTail{source: source, file: file}
}

// SetActive records the current execution and attempt. When the execution
// changes (an at-least-once reschedule), the offsets reset to the start of the
// new execution and a bounded service marker is appended to the detached file.
// In foreground mode the rescheduling boundary is already surfaced by the run
// loop's own status lines, so no extra marker is written to the terminal.
func (t *runTail) SetActive(executionID, runID string, attempt uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = executionID
	if runID != "" {
		t.runID = runID
	}
	t.attempt = attempt
	if executionID == t.seen {
		return
	}
	t.off = map[string]uint64{"stdout": 0, "stderr": 0}
	if t.file != nil {
		if t.seen == "" {
			fmt.Fprintf(t.file, "[r1s] run=%s attempt=%d\n", t.runID, attempt)
		} else {
			fmt.Fprintf(t.file, "[r1s] rescheduled attempt=%d\n", attempt)
		}
	}
	t.seen = executionID
}

// Offsets returns the current next-byte offsets for both streams (test hook).
func (t *runTail) Offsets() map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]uint64, len(t.off))
	for stream, off := range t.off {
		out[stream] = off
	}
	return out
}

// poll fetches one bounded chunk per stream at the current offsets and writes
// it to the right destination. Failures are inconclusive: the offsets stay put,
// so a reconnect drains exactly the gap between the offsets and what the
// allocator retained, with no duplicates.
func (t *runTail) poll(ctx context.Context) {
	t.mu.Lock()
	executionID := t.active
	offsets := make(map[string]uint64, len(t.off))
	for stream, off := range t.off {
		offsets[stream] = off
	}
	t.mu.Unlock()
	if executionID == "" {
		return
	}
	for _, stream := range []string{"stdout", "stderr"} {
		offset := offsets[stream]
		chunk, err := t.source.tailChunk(ctx, executionID, stream, offset, tailLogWait)
		if err != nil {
			// Partition, timeout, expired retention, or unreachable allocator:
			// never rescheduling evidence from the tail. Keep polling at the
			// same offset so reconnect drains the gap.
			continue
		}
		data := chunk.GetData()
		if len(data) == 0 {
			continue
		}
		t.mu.Lock()
		current := t.off[stream]
		t.mu.Unlock()
		if current != offset {
			// The execution was rescheduled between the read and now. Drop the
			// stale chunk rather than misroute it into the new attempt; the
			// offsets already belong to the new execution.
			continue
		}
		t.write(stream, data)
	}
}

func (t *runTail) write(stream string, data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var destination io.Writer
	switch {
	case t.file != nil:
		destination = t.file
	case stream == "stdout":
		destination = t.stdoutTarget
	default:
		destination = t.stderrTarget
	}
	if destination == nil {
		return
	}
	_, _ = destination.Write(data)
	// Advance the offset by the exact bytes written so a later reschedule or
	// partition resume starts where we ended.
	t.off[stream] += uint64(len(data))
}

func (t *runTail) run(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(tailInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			t.poll(ctx)
		}
	}
}
