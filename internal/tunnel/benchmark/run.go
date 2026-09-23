package benchmark

import (
	"context"
	"fmt"
	"io"
	"time"
)

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
