package benchmark

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// connectorFactory builds a fresh connector for one transport. The F21-05
// harness drives both transports through one shared driver; adding or removing
// a transport (Ygg is removed in F21-06) is a table change here.
type connectorFactory struct {
	name string
	new  func() (Connector, error)
	// LinuxOnly marks a transport whose edges require a real Linux host. Both
	// benchmark transports are in-process loopback, so this is always false; it
	// exists so a future live leg (real system Yggdrasil) can be marked.
	LinuxOnly bool
}

var connectorFactories = []connectorFactory{
	{name: "Ygg", new: func() (Connector, error) { return newYggConnector() }},
	{name: "RNS", new: func() (Connector, error) { return newRNSConnector() }},
}

// TestBothTransportsConnect is the two-stack e2e/unit leg of F21-05: it proves
// both transport harnesses establish a connection and round-trip a small
// incompressible payload before any benchmark measures them. This keeps
// benchmark rows from silently measuring a dead transport. It doubles as the
// "two private stacks connect over a local pair" acceptance at the harness
// level; the F21-01 package already asserts the RNS stack's identity/IFAC and
// single-Backbone-interface properties, and F21-05's live/e2e matrix is proven
// separately against the real edges (cmd/r1sd, cmd/r1s).
func TestBothTransportsConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, factory := range connectorFactories {
		t.Run(factory.name, func(t *testing.T) {
			connector, err := factory.new()
			if err != nil {
				t.Fatalf("new %s connector: %v", factory.name, err)
			}
			defer connector.Close()

			client, allocator, err := connector.Open(ctx)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer client.Close()
			defer allocator.Close()

			msg := []byte("r1s-f21-05-ping")
			if _, err := client.Write(msg); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := client.CloseWrite(); err != nil {
				t.Fatalf("CloseWrite: %v", err)
			}
			got := make([]byte, 0, len(msg))
			buf := make([]byte, 8)
			for len(got) < len(msg) {
				n, err := allocator.Read(buf)
				got = append(got, buf[:n]...)
				if err != nil {
					t.Fatalf("read after %d/%d: %v", len(got), len(msg), err)
				}
			}
			// Half-close must deliver EOF after the payload is drained.
			if _, err := allocator.Read(buf); err != io.EOF {
				t.Fatalf("after CloseWrite expected EOF, got %v", err)
			}
			if string(got) != string(msg) {
				t.Fatalf("round-trip = %q, want %q", got, msg)
			}
		})
	}
}

// TestBothTransportsConcurrent is the harness-level concurrency smoke leg:
// several connections open and transfer independently (the live acceptance
// items "concurrent local TCP connections work independently" and "closing one
// Link does not affect the others" at the transport level).
func TestBothTransportsConcurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, factory := range connectorFactories {
		t.Run(factory.name, func(t *testing.T) {
			connector, err := factory.new()
			if err != nil {
				t.Fatalf("new %s connector: %v", factory.name, err)
			}
			defer connector.Close()

			const n = 8
			var clients, allocators [n]Conn
			for i := 0; i < n; i++ {
				client, allocator, err := connector.Open(ctx)
				if err != nil {
					t.Fatalf("open %d: %v", i, err)
				}
				clients[i], allocators[i] = client, allocator
				if _, err := client.Write([]byte{byte(i)}); err != nil {
					t.Fatalf("open %d write: %v", i, err)
				}
				buf := make([]byte, 1)
				if _, err := io.ReadFull(allocator, buf); err != nil {
					t.Fatalf("open %d read: %v", i, err)
				}
				if buf[0] != byte(i) {
					t.Fatalf("open %d got %d", i, buf[0])
				}
			}
			// Close them independently (all but the survivor). This proves one
			// Link/session close does not disturb the others.
			for i := 1; i < n; i++ {
				clients[i].Close()
				allocators[i].Close()
			}
			// The survivor still transfers after the rest closed.
			if _, err := clients[0].Write([]byte("still-alive")); err != nil {
				t.Fatalf("survivor write: %v", err)
			}
			buf := make([]byte, 11)
			if _, err := io.ReadFull(allocators[0], buf); err != nil {
				t.Fatalf("survivor read: %v", err)
			}
		})
	}
}

// TestSuiteSmoke runs the full F21-05 workload suite with tiny sizes against
// both transports, proving the driver completes without deadlock or error. It
// is not a measurement; it guards the harness so a regression fails this fast
// unit test rather than a long recorded run.
func TestSuiteSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for _, factory := range connectorFactories {
		t.Run(factory.name, func(t *testing.T) {
			connector, err := factory.new()
			if err != nil {
				t.Fatalf("new %s connector: %v", factory.name, err)
			}
			defer connector.Close()
			var output strings.Builder
			if err := Run(ctx, &output, connector, SmallSuite()); err != nil {
				t.Fatalf("Run: %v\n%s", err, output.String())
			}
		})
	}
}

// TestHostRecord fills the "recorded numbers and host/environment/version"
// acceptance from the harness side: every admitted connector is named and the
// host record is non-empty, so a recorded run is reproducible.
func TestHostRecord(t *testing.T) {
	if record := HostRecord(); record == "" {
		t.Fatal("empty host record")
	}
	if len(connectorFactories) != 2 {
		t.Fatalf("expected Ygg and RNS connectors, got %d", len(connectorFactories))
	}
}

// TestRecordedBenchmark is the F21-05 acceptance gate that records the full
// benchmark rows for both transports. It is env-gated (R1S_TEST_F21_BENCHMARK=recorded)
// so it is excluded from `make check` and never runs under `go test -race`:
// benchmark rows must describe the production binary rather than race-detector
// instrumentation, and the Channel packet volume makes the raced run unsuitable
// as a performance record.
//
// Usage:
//
//	R1S_TEST_F21_BENCHMARK=recorded go test ./internal/tunnel/benchmark/ \
//	  -run TestRecordedBenchmark -count=1 > f21-05-bench.txt 2>&1
//
// The rows are then reviewed and copied into
// roadmap/f21-tunnel-rns-dataplane/f21-05-benchmark-live-acceptance.md.
func TestRecordedBenchmark(t *testing.T) {
	if os.Getenv("R1S_TEST_F21_BENCHMARK") != "recorded" {
		t.Skip("set R1S_TEST_F21_BENCHMARK=recorded to record benchmark rows")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for _, factory := range connectorFactories {
		t.Run(factory.name, func(t *testing.T) {
			connector, err := factory.new()
			if err != nil {
				t.Fatalf("new %s connector: %v", factory.name, err)
			}
			defer connector.Close()

			// Write to a temp file so the output survives stdout truncation, and
			// concurrently to os.Stdout so `go test ... > file` captures it.
			f, err := os.CreateTemp("", "r1s-f21-bench-*.txt")
			if err != nil {
				t.Fatalf("create temp: %v", err)
			}
			fName := f.Name()
			defer os.Remove(fName)
			defer f.Close()

			w := io.MultiWriter(f, os.Stdout)
			if err := Run(ctx, w, connector, DefaultSuite()); err != nil {
				t.Fatalf("Run: %v", err)
			}

			// Also emit the content through t.Log so it appears in the go test stream.
			if content, err := os.ReadFile(fName); err == nil {
				for _, line := range strings.Split(string(content), "\n") {
					t.Log(line)
				}
			}

			fmt.Fprintf(os.Stderr, "\n=== %s recorded to %s ===\n", factory.name, fName)
		})
	}
}
