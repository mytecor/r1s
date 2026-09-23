// Package benchmark is the F21-05 old-vs-new tunnel transport benchmark and
// live-acceptance driver. It measures the OLD embedded-Ygg tunnel against the
// NEW private-RNS (Link/Channel/Buffer) data plane head to head on the same
// host, so the recorded overhead of the RNS path is an explicit input to the
// F21-06 go/no-go decision that removes the Ygg transport.
//
// The package deliberately lives OUTSIDE internal/tunnel/yggdrasil and
// internal/tunnel/rns: it must drive both transports (which must not import
// each other) through one shared contract, so the two `_test` packages in the
// transports implement Connector against it. It is the only place that imports
// the Reticulum-Go debug package (to silence the RNS transport's global slog
// output so rows are measured, not drowned in link-lifecycle lines).
//
// Logical-connection model. The benchmark measures workloads, not transport
// mechanisms: a "connection" is one logical byte pipe to a container service
// (one local TCP connection). Each transport maps that to its real
// concurrency primitive, so the numbers reflect the actual design difference
// being evaluated:
//
//   - old Ygg transport: one authenticated mesh pair carries N multiplexed
//     streams, so N concurrent connections are N streams on one pair;
//   - new RNS transport: one Link carries exactly one stream, so N concurrent
//     connections are N Links.
//
// Connector.Open hides that choice; the driver never sees it.
package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/debug"
)

func init() {
	// The RNS transport logs through Reticulum's global slog handler at its
	// package-default INFO level regardless of config.LogLevel. Benchmarks that
	// open many Links/pairs would otherwise spew link-lifecycle lines into the
	// measured process; level 0 silences them (debug.SetDebugLevel is safe to
	// call before Init and is process-wide). The Ygg transport's default logger
	// already discards, so this only affects the RNS side.
	debug.SetDebugLevel(0)
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// Conn is the minimal byte pipe a benchmark measures. It is the narrowed
// tunnel-stream shape (io.ReadWriteCloser + CloseWrite for half-close), which
// both transports already satisfy.
type Conn interface {
	io.ReadWriteCloser
	CloseWrite() error
}

// Connector opens logical tunnel connections to a fresh in-process allocator
// edge on one transport and returns the connected (client-side, allocator-side)
// byte pair for each connection. Concurrent Open calls must work independently.
// Connector is created once per benchmark row.
type Connector interface {
	// Name is the transport label used in benchmark row names.
	Name() string
	// Open establishes one logical connection and returns both ends of the same
	// stream. The caller closes both ends when done.
	Open(ctx context.Context) (client, allocator Conn, err error)
	// Close tears down the underlying transport resources.
	Close() error
}

// interruptOnCancel closes active connections when ctx expires. The transport
// Conn contract has no per-operation deadline, so closing is what unblocks a
// Read or Write that is already in progress. The returned function stops the
// watcher after a successful workload.
func interruptOnCancel(ctx context.Context, conns ...Conn) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			for _, conn := range conns {
				_ = conn.Close()
			}
		case <-done:
		}
	}()
	return func() { close(done) }
}

// --- workload primitives ----------------------------------------------------

// throughput drives a bulk transfer of `size` bytes from client to allocator
// over one connection (payload larger than the RNS MTU goes through the stock
// Channel/Buffer path). It returns the elapsed time for the transfer.
func throughput(ctx context.Context, c Connector, size int) (time.Duration, error) {
	client, allocator, err := c.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	defer allocator.Close()
	stopInterrupt := interruptOnCancel(ctx, client, allocator)
	defer stopInterrupt()

	payload := make([]byte, size)
	// Use incompressible data so the workload models already-compressed or
	// encrypted tunnel traffic and cannot gain an artificial wire-size advantage
	// from content. The v1.2.0 compatibility writer sends the standard
	// uncompressed StreamDataMessage form, so payload generation is nearly free
	// relative to the transfer.
	if _, err := rand.Read(payload); err != nil {
		return 0, fmt.Errorf("random payload: %w", err)
	}

	recvDone := make(chan error, 1)
	go func() {
		got := int64(0)
		buf := make([]byte, 64*1024)
		for got < int64(size) {
			n, rerr := allocator.Read(buf)
			got += int64(n)
			if rerr != nil {
				recvDone <- rerr
				return
			}
		}
		recvDone <- nil
	}()

	start := time.Now()
	writeErr := writeAll(client, payload)
	elapsed := time.Since(start)
	if writeErr == nil {
		if err := client.CloseWrite(); err != nil {
			writeErr = err
		}
	}
	recvErr := <-recvDone
	if writeErr != nil {
		return 0, fmt.Errorf("client write: %w", writeErr)
	}
	if recvErr != nil && recvErr != io.EOF {
		return 0, fmt.Errorf("allocator read: %w", recvErr)
	}
	return elapsed, nil
}

// writeAll copies p to w until fully written or error.
func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// readAll reads up to want bytes from r, tolerating a trailing io.EOF only
// after want bytes.
func readAll(r io.Reader, want int) (int, error) {
	buf := make([]byte, 64*1024)
	got := 0
	for got < want {
		n, err := r.Read(buf)
		got += n
		if err != nil {
			if err == io.EOF && got >= want {
				return got, nil
			}
			return got, err
		}
	}
	return got, nil
}
