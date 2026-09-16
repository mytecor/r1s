package main

import (
	"fmt"
	"io"
	"time"

	"github.com/mytecor/r1s/internal/protocol"
)

func (a *application) logs(args []string, diagnostics io.Writer) error {
	f := newFlagSet("r1s logs", diagnostics)
	stream := f.String("stream", "stderr", "stdout or stderr")
	offset := f.Uint64("offset", 0, "byte offset in retained stream")
	limit := f.Uint("bytes", protocol.MaxLogBytes, "maximum bytes to retrieve (1..128)")
	wait := f.Duration("wait", 30*time.Second, "response timeout")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 1 || *limit == 0 || *limit > protocol.MaxLogBytes || *wait <= 0 {
		return fmt.Errorf("logs requires an execution ID, 1..128 bytes, and positive wait")
	}
	chunk, err := a.retrieveLogs(a.ctx, f.Arg(0), *stream, *offset, uint32(*limit), *wait)
	if err != nil {
		return err
	}
	if _, err := a.stdout.Write(chunk.GetData()); err != nil {
		return err
	}
	fmt.Fprintf(diagnostics, "next_offset=%d eof=%t truncated=%t\n", chunk.GetNextOffset(), chunk.GetEof(), chunk.GetTruncated())
	return nil
}
