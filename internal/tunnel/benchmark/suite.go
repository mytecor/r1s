package benchmark

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
