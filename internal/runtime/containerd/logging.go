package containerd

import (
	"context"
	"flag"
	"io"
	"strconv"

	"github.com/containerd/containerd/v2/core/runtime/v2/logging"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/mytecor/r1s/internal/logstore"
)

// RunLogWriter handles the private shim entry point, independent of r1sd lifetime.
func RunLogWriter(args []string) bool {
	active := false
	for _, arg := range args {
		active = active || arg == "--log-writer"
	}
	if !active {
		return false
	}
	flags := flag.NewFlagSet("r1sd log writer", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dir := flags.String("log-writer", "", "directory")
	limit := flags.Int64("log-limit", 0, "byte limit")
	parseErr := flags.Parse(args)
	logging.Run(func(_ context.Context, config *logging.Config, ready func() error) error {
		if parseErr != nil {
			return parseErr
		}
		return logstore.Capture(*dir, *limit, config.Stdout, config.Stderr, ready)
	})
	return true
}

func (b *clientBackend) taskIO(id string) (cio.Creator, error) {
	if b.logs == nil {
		return cio.NullIO, nil
	}
	dir, limit, err := b.logs.Reserve(id)
	if err != nil {
		return nil, err
	}
	return cio.BinaryIO(b.logBinary, map[string]string{"--log-writer": dir, "--log-limit": formatLimit(limit)}), nil
}

// Kept separate to avoid exposing shim-specific arguments outside the adapter.
func formatLimit(limit int64) string { return strconv.FormatInt(limit, 10) }
