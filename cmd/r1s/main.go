package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// version identifies the build. Release binaries set it with
// -ldflags "-X main.version=<tag>"; source builds report "dev".
var version = "dev"

type signalExitError struct{ signal os.Signal }

func (e *signalExitError) Error() string { return e.signal.String() }
func (e *signalExitError) Unwrap() error { return context.Canceled }
func (e *signalExitError) ExitStatus() int {
	if e.signal == syscall.SIGTERM {
		return 143
	}
	return 130
}

func main() {
	ctx, cancel := context.WithCancelCause(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() { cancel(&signalExitError{signal: <-signals}) }()
	defer cancel(nil)
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		var status interface{ ExitStatus() int }
		if errors.As(err, &status) {
			os.Exit(status.ExitStatus())
		}
		if errors.Is(err, context.Canceled) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
