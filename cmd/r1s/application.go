package main

import (
	"context"
	"fmt"
	"io"

	r1sclient "github.com/mytecor/r1s/client"
)

// application is the run-oriented client process. Its client engine exists
// only in this process; the authority broker owns the corresponding ephemeral
// RNS endpoint and cluster credential. There is no persistent client database,
// run service, or durable lease intent: a restart never resumes a run, and the
// allocator retains the execution database.
type application struct {
	ctx        context.Context
	stdout     io.Writer
	controller *r1sclient.Client
}

// openRunApplication constructs a controller through the current cluster
// authority context. The broker creates a fresh ephemeral identity, while this
// process remains the sole owner of run state and lifecycle.
func openRunApplication(ctx context.Context, options commandLine, stdout io.Writer) (*application, error) {
	app := &application{ctx: ctx, stdout: stdout}
	var err error
	app.controller, err = r1sclient.OpenCurrent(r1sclient.Config{NetworkWait: options.networkWait})
	if err != nil {
		return nil, fmt.Errorf("open run controller: %w", err)
	}
	return app, nil
}

// start launches the ephemeral transport edge and the offer-release worker.
// Foreground runs call it; a detached parent never calls it (the child process
// starts its own).
func (a *application) start() error {
	return a.controller.Start(a.ctx)
}

func (a *application) stop(diagnostics io.Writer) {
	if a.controller != nil {
		for _, release := range a.controller.Close() {
			fmt.Fprintf(diagnostics, "offer release pending allocator=%s offer=%s; retained for retry, lease expiry remains the fallback\n", release.Destination, release.OfferID)
		}
	}
}

func (a *application) close() {
	if a.controller != nil {
		a.controller.Close()
	}
}
