package benchmark

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"
)

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
