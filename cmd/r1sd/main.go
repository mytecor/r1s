package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	runtimecontainerd "github.com/mytecor/r1s/internal/runtime/containerd"
)

// version identifies the build. Release binaries set it with
// -ldflags "-X main.version=<tag>"; source builds report "dev".
var version = "dev"

func main() {
	if runtimecontainerd.RunLogWriter(os.Args[1:]) {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
