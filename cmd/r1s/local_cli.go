package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/localapi"
	"github.com/mytecor/r1s/internal/protocol"
)

// localAPISocketAlive reports whether the local API service is listening on the
// given Unix socket. It deliberately checks only socket reachability, not the
// protocol, so the CLI can transparently discover a running 'r1s serve' without
// treating an absent service as an error for the default path.
func localAPISocketAlive(socketPath string) bool {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// localCLI routes a CLI workflow through a persistent local r1s service instead
// of building an RNS endpoint, identity, and state store for the command. It is
// commandHandler-compatible with direct mode so output and exit semantics are
// preserved.
type localCLI struct {
	ctx        context.Context
	stdout     io.Writer
	socketPath string
	client     *localapi.Client
}

func openLocalCLI(ctx context.Context, options commandLine, stdout io.Writer, stderr io.Writer) (*localCLI, error) {
	client, err := localapi.Dial(strings.TrimSpace(options.socketPath))
	if err != nil {
		return nil, err
	}
	handler := &localCLI{ctx: ctx, stdout: stdout, socketPath: options.socketPath, client: client}
	// Refuse to silently create a second identity or assignment: an unreachable
	// socket is an error, never a fallback.
	if err := client.Ping(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("--socket: %w at %s; start the service with 'r1s serve' first", err, options.socketPath)
	}
	return handler, nil
}

func (l *localCLI) Close() error {
	if l.client == nil {
		return nil
	}
	return l.client.Close()
}

// serve is only meaningful in direct mode.
func (l *localCLI) serve(args []string, stderr io.Writer) error {
	return errors.New("serve: start the local service in direct mode, then use --socket for workflows; 'r1s serve --socket <path>' requires --rns-config and --identity")
}

func (l *localCLI) request(args []string, stderr io.Writer) error {
	flags := newFlagSet("r1s request [options] '<ExecutionRequest JSON>'", stderr)
	offerWait := flags.Duration("offer-wait", 10*time.Second, "time to discover allocators and collect offers")
	var allocators stringValues
	flags.Var(&allocators, "allocator", "allocator destination hash; repeat or omit for announce discovery")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("request: exactly one JSON argument is required")
	}
	if *offerWait <= 0 {
		return errors.New("request: --offer-wait must be positive")
	}
	request, err := decodeRequestJSON(flags.Arg(0))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(l.ctx, *offerWait+30*time.Second)
	defer cancel()
	requestID, executionID, allocator, err := l.client.Request(ctx, request.GetWorkload(), request.GetPolicy(), request.GetResourceClass(), *offerWait, allocators)
	if err != nil {
		return err
	}
	fmt.Fprintf(l.stdout, "request=%s execution=%s allocator=%x status=assignment-sent\n", requestID, executionID, allocator)
	return nil
}

func (l *localCLI) list(args []string, stderr io.Writer) error {
	flags := newFlagSet("r1s list", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("list: unexpected arguments")
	}
	ctx, cancel := context.WithTimeout(l.ctx, 10*time.Second)
	defer cancel()
	response, err := l.client.List(ctx)
	if err != nil {
		return err
	}
	for _, view := range response.GetRequests() {
		fmt.Fprintf(l.stdout, "request=%s class=%s offers=%d execution=%s\n", view.GetRequestId(), view.GetResourceClass(), view.GetOfferCount(), view.GetExecutionId())
	}
	return nil
}

func (l *localCLI) inspect(args []string, stderr io.Writer, resultOnly bool) error {
	name := "inspect"
	if resultOnly {
		name = "result"
	}
	flags := newFlagSet("r1s "+name, stderr)
	wait := flags.Duration("wait", 30*time.Second, "time to wait for allocator state")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("%s: execution ID is required", name)
	}
	if *wait <= 0 {
		return fmt.Errorf("%s: --wait must be positive", name)
	}
	executionID := flags.Arg(0)
	ctx, cancel := context.WithTimeout(l.ctx, *wait+10*time.Second)
	defer cancel()
	var state *r1sv1.ExecutionState
	var err error
	if resultOnly {
		state, err = l.client.Result(ctx, executionID, *wait)
	} else {
		state, err = l.client.Inspect(ctx, executionID, *wait)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(l.stdout, "execution=%s %s\n", executionID, stateLine(state))
	return nil
}

func (l *localCLI) cancel(args []string, stderr io.Writer) error {
	flags := newFlagSet("r1s cancel", stderr)
	reason := flags.String("reason", "cancelled by client", "cancellation reason")
	wait := flags.Duration("wait", 30*time.Second, "time to wait for allocator state")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("cancel: execution ID is required")
	}
	if *wait <= 0 {
		return errors.New("cancel: --wait must be positive")
	}
	executionID := flags.Arg(0)
	ctx, cancel := context.WithTimeout(l.ctx, *wait+10*time.Second)
	defer cancel()
	state, err := l.client.Cancel(ctx, executionID, *reason, *wait)
	if err != nil {
		return err
	}
	fmt.Fprintf(l.stdout, "execution=%s %s\n", executionID, stateLine(state))
	return nil
}

func (l *localCLI) logs(args []string, diagnostics io.Writer) error {
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
	ctx, cancel := context.WithTimeout(l.ctx, *wait+10*time.Second)
	defer cancel()
	response, err := l.client.Logs(ctx, f.Arg(0), *stream, *offset, uint32(*limit), *wait)
	if err != nil {
		return err
	}
	if _, err := l.stdout.Write(response.GetData()); err != nil {
		return err
	}
	fmt.Fprintf(diagnostics, "next_offset=%d eof=%t truncated=%t\n", response.GetNextOffset(), response.GetEof(), response.GetTruncated())
	return nil
}

// stateLine renders an execution state without the execution ID, which the
// caller already printed.
func stateLine(state *r1sv1.ExecutionState) string {
	if state == nil {
		return "phase=unknown"
	}
	exit := ""
	if state.ExitCode != nil {
		exit = fmt.Sprintf(" exit_code=%d", state.GetExitCode())
	}
	return fmt.Sprintf("phase=%s occurred_at=%s%s detail=%q", phaseName(state.GetPhase()), state.GetOccurredAt().AsTime().Format(time.RFC3339), exit, state.GetDetail())
}
