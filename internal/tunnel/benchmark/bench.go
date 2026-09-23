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
	"strings"
	"sync"
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

// --- benchmark rows (shared methodology) ------------------------------------

// runOpenLatency measures per-connection establishment cost: the time to open a
// fresh logical connection (one Link on RNS, one pair+preamble on Ygg) and
// exchange a single byte. This is the direct "overhead of the RNS Link path"
// the feature records.
func openLatency(ctx context.Context, c Connector, iters int) (time.Duration, error) {
	var total time.Duration
	for i := 0; i < iters; i++ {
		elapsed, err := openLatencyOnce(ctx, c)
		if err != nil {
			return 0, err
		}
		total += elapsed
	}
	return total, nil
}

func openLatencyOnce(ctx context.Context, c Connector) (time.Duration, error) {
	client, allocator, err := c.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	defer allocator.Close()
	stopInterrupt := interruptOnCancel(ctx, client, allocator)
	defer stopInterrupt()

	start := time.Now()
	// A single byte request/ack verifies both directions are live.
	if _, err := client.Write([]byte{0x01}); err != nil {
		return 0, fmt.Errorf("open ping write: %w", err)
	}
	if _, err := readAll(allocator, 1); err != nil {
		return 0, fmt.Errorf("open ping read: %w", err)
	}
	if _, err := allocator.Write([]byte{0x02}); err != nil {
		return 0, fmt.Errorf("open pong write: %w", err)
	}
	if _, err := readAll(client, 1); err != nil {
		return 0, fmt.Errorf("open pong read: %w", err)
	}
	return time.Since(start), nil
}

// runHTTPLatency measures one small request/response round-trip over an already
// established connection: the interactive-protocol latency (HTTP headers, SSH
// handshake) the relay must not inflate. Each Open is reused across iters so
// this isolates steady-state per-message latency from per-connection setup.
func runHTTPLatency(ctx context.Context, c Connector, iters int) (time.Duration, error) {
	client, allocator, err := c.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	defer allocator.Close()
	stopInterrupt := interruptOnCancel(ctx, client, allocator)
	defer stopInterrupt()

	request := make([]byte, 512)   // ~ an HTTP GET + a few headers
	response := make([]byte, 2048) // ~ a small HTTP response body
	if _, err := rand.Read(request); err != nil {
		return 0, err
	}
	if _, err := rand.Read(response); err != nil {
		return 0, err
	}

	var total time.Duration
	for i := 0; i < iters; i++ {
		start := time.Now()
		if err := writeAll(client, request); err != nil {
			return 0, fmt.Errorf("req write: %w", err)
		}
		if _, err := readAll(allocator, len(request)); err != nil {
			return 0, fmt.Errorf("req read: %w", err)
		}
		if err := writeAll(allocator, response); err != nil {
			return 0, fmt.Errorf("resp write: %w", err)
		}
		if _, err := readAll(client, len(response)); err != nil {
			return 0, fmt.Errorf("resp read: %w", err)
		}
		total += time.Since(start)
	}
	return total, nil
}

// runConcurrent measures n connections transferring concurrently. The driver
// opens all n, then times n simultaneous full transfers. This is where the
// transport maps n connections to its primitive: n streams on one Ygg pair, or
// n independent Links on RNS. A slow transport under concurrency shows here.
func runConcurrent(ctx context.Context, c Connector, n, size int) (time.Duration, error) {
	conns := make([]struct {
		client, allocator Conn
	}, n)
	for i := 0; i < n; i++ {
		client, allocator, err := c.Open(ctx)
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].client.Close()
				conns[j].allocator.Close()
			}
			return 0, fmt.Errorf("open conn %d: %w", i, err)
		}
		conns[i].client, conns[i].allocator = client, allocator
	}
	defer func() {
		for i := range conns {
			conns[i].client.Close()
			conns[i].allocator.Close()
		}
	}()
	allConns := make([]Conn, 0, 2*len(conns))
	for i := range conns {
		allConns = append(allConns, conns[i].client, conns[i].allocator)
	}
	stopInterrupt := interruptOnCancel(ctx, allConns...)
	defer stopInterrupt()

	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		return 0, err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	start := time.Now()
	for i := range conns {
		wg.Add(1)
		go func(client, allocator Conn) {
			defer wg.Done()
			recvDone := make(chan error, 1)
			go func() {
				_, err := readAll(allocator, size)
				recvDone <- err
			}()
			if err := writeAll(client, payload); err != nil {
				errCh <- err
				return
			}
			if err := client.CloseWrite(); err != nil {
				errCh <- err
				return
			}
			if err := <-recvDone; err != nil && err != io.EOF {
				errCh <- err
			}
		}(conns[i].client, conns[i].allocator)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errCh)
	for err := range errCh {
		if err != nil {
			return 0, err
		}
	}
	return elapsed, nil
}

// Report prints one measured row to w in the storage-bench style: label and
// per-op cost in the benchmark output. transport,test,spec identify the row;
// elapsed is the raw measurement and ops the number of operations it spans, so
// per-op ns/op is derived consistently across rows.
func Report(w io.Writer, transport, test string, elapsed time.Duration, ops int, bytes int64) {
	fmt.Fprintf(w, "Benchmark%s/%s-%-3d %8d  %12.2f ns/op\n",
		transport, test, runtime.GOMAXPROCS(0), 1,
		float64(elapsed.Nanoseconds())/float64(ops))
	if bytes > 0 {
		fmt.Fprintf(w, "  %10s  %12.2f MB/s\n",
			fmt.Sprintf("%d bytes", bytes),
			(float64(bytes)/float64(elapsed.Seconds()))/(1<<20))
	}
}

// HostRecord returns a standalone host/environment/version string to record
// alongside rows for reproducibility.
func HostRecord() string {
	var commit string
	if out, err := gitShortCommit(); err == nil {
		commit = out
	}
	fields := []string{
		runtime.GOOS + "/" + runtime.GOARCH,
		fmt.Sprintf("cpu=%d", runtime.NumCPU()),
		"go=" + strings.TrimPrefix(runtime.Version(), "go"),
	}
	if commit != "" {
		fields = append(fields, "commit="+commit)
	}
	return strings.Join(fields, " | ")
}

// Suite is the F21-05 workload set: the transport-level rows the acceptance
// requires (1 MiB, 10 MiB, 10 concurrent streams, and an HTTP request/response
// latency) plus a per-connection open-latency row that exposes the cost of the
// one-Link-per-stream mapping directly. It is the shared methodology: every
// transport runs the same sizes and the same repetition counts, so the recorded
// overhead of the RNS Link/Channel/Buffer path is an apples-to-apples input to
// the F21-06 go/no-go decision.
type Suite struct {
	// Bulk1MiB and Bulk10MiB are single-stream transfer sizes in bytes.
	Bulk1MiB, Bulk10MiB int
	// ConcurrentStreams is the number of simultaneous connections in the
	// concurrency row, and ConcurrentBytes the per-stream transfer size.
	ConcurrentStreams, ConcurrentBytes int
	// HTTPIters is the number of request/response round-trips in the HTTP row,
	// and OpenIters the number of fresh-connection setups in the open row.
	HTTPIters, OpenIters int
}

// DefaultSuite returns the standard F21-05 workload sizes.
func DefaultSuite() Suite {
	return Suite{
		Bulk1MiB:          1 << 20,
		Bulk10MiB:         10 << 20,
		ConcurrentStreams: 10,
		ConcurrentBytes:   1 << 20,
		HTTPIters:         200,
		OpenIters:         20,
	}
}

// SmallSuite returns tiny sizes for the fast unit/corridor leg (TestSuiteSmoke)
// that proves the driver completes without deadlock or error on both
// transports before any recorded run.
func SmallSuite() Suite {
	return Suite{
		// These are smoke sizes, not recorded benchmark rows. They remain well
		// above the RNS MDU so segmentation is exercised, while staying below the
		// race detector's multi-second slow-link/staleness corridor.
		Bulk1MiB:          32 * 1024,
		Bulk10MiB:         128 * 1024,
		ConcurrentStreams: 4,
		ConcurrentBytes:   16 * 1024,
		HTTPIters:         5,
		OpenIters:         3,
	}
}

// Run executes the workload suite against connector and writes the recorded
// rows to w in the storage-bench style. It returns the first workload error, if
// any, so a caller can fail loudly rather than record a half-measured row. The
// host record is written first so every row below it is reproducible.
func Run(ctx context.Context, w io.Writer, connector Connector, suite Suite) error {
	fmt.Fprintf(w, "r1s F21-05 tunnel transport benchmark\n")
	fmt.Fprintf(w, "transport: %s\n", connector.Name())
	fmt.Fprintf(w, "host: %s\n\n", HostRecord())

	type row struct {
		name    string
		elapsed time.Duration
		ops     int
		bytes   int64
	}
	var rows []row

	// 1 MiB and 10 MiB single-stream transfers.
	for _, spec := range []struct {
		label string
		size  int
	}{
		{"1MiB", suite.Bulk1MiB},
		{"10MiB", suite.Bulk10MiB},
	} {
		elapsed, err := throughput(ctx, connector, spec.size)
		if err != nil {
			return fmt.Errorf("%s transfer: %w", spec.label, err)
		}
		rows = append(rows, row{name: "Transfer/" + spec.label, elapsed: elapsed, ops: 1, bytes: int64(spec.size)})
	}

	// HTTP request/response latency over an established connection.
	if elapsed, err := runHTTPLatency(ctx, connector, suite.HTTPIters); err != nil {
		return fmt.Errorf("http latency: %w", err)
	} else {
		rows = append(rows, row{name: "HTTP", elapsed: elapsed, ops: suite.HTTPIters})
	}

	// N concurrent streams, each transferring ConcurrentBytes.
	if elapsed, err := runConcurrent(ctx, connector, suite.ConcurrentStreams, suite.ConcurrentBytes); err != nil {
		return fmt.Errorf("concurrent: %w", err)
	} else {
		rows = append(rows, row{name: "Concurrent", elapsed: elapsed, ops: suite.ConcurrentStreams, bytes: int64(suite.ConcurrentStreams) * int64(suite.ConcurrentBytes)})
	}

	// Per-connection open latency (one Link on RNS, pair+preamble on Ygg).
	if elapsed, err := openLatency(ctx, connector, suite.OpenIters); err != nil {
		return fmt.Errorf("open latency: %w", err)
	} else {
		rows = append(rows, row{name: "Open", elapsed: elapsed, ops: suite.OpenIters})
	}

	for _, r := range rows {
		Report(w, connector.Name(), r.name, r.elapsed, r.ops, r.bytes)
	}
	return nil
}
