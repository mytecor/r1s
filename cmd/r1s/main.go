package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/transport/rns"
	"google.golang.org/protobuf/encoding/protojson"
	"quad4/reticulum-go/pkg/reticulumconfig"
)

// version identifies the build. Release binaries set it with
// -ldflags "-X main.version=<tag>"; source builds report "dev".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type application struct {
	ctx      context.Context
	stdout   io.Writer
	endpoint *rns.Endpoint
	client   *client.Client
	events   chan *r1sv1.Envelope
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("r1s", stderr)
	showVersion := flags.Bool("version", false, "print the version and exit")
	configPath := flags.String("rns-config", "", "path to a Reticulum-Go configuration file")
	identityPath := flags.String("identity", "", "path to the persistent client identity")
	statePath := flags.String("state", "", "client state database (defaults beside the identity)")
	networkWait := flags.Duration("network-timeout", 30*time.Second, "RNS path, link, and response timeout")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	commandArguments := flags.Args()
	if *showVersion {
		if len(commandArguments) != 0 {
			return errors.New("--version does not take a command")
		}
		fmt.Fprintln(stdout, version)
		return nil
	}
	if len(commandArguments) == 0 {
		return errors.New("command is required: request, list, inspect, cancel, result, or logs")
	}
	command := commandArguments[0]
	args := commandArguments[1:]
	if !knownCommand(command) {
		return fmt.Errorf("unknown command %q: expected request, list, inspect, cancel, result, or logs", command)
	}
	if containsHelp(args) {
		return dispatch(&application{}, command, args, stderr)
	}
	if strings.TrimSpace(*configPath) == "" || strings.TrimSpace(*identityPath) == "" {
		return errors.New("--rns-config and --identity are required")
	}
	reticulumConfig, err := reticulumconfig.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load Reticulum config: %w", err)
	}
	reticulumConfig.EnableTransport = false
	reticulumConfig.ConfigPath = filepath.Join(filepath.Dir(*identityPath), "reticulum-client")
	events := make(chan *r1sv1.Envelope, 32)
	var endpoint *rns.Endpoint
	var clientCore *client.Client
	handler := func(handlerContext context.Context, envelope *r1sv1.Envelope) error {
		identityKey := hex.EncodeToString(envelope.GetSender())
		if destination, ok := endpoint.DestinationForIdentity(identityKey); ok {
			if err := clientCore.RegisterAllocator(client.Allocator{Identity: envelope.GetSender(), Destination: destination}); err != nil {
				return err
			}
		}
		if err := clientCore.Handle(handlerContext, envelope); err != nil {
			return err
		}
		select {
		case events <- envelope:
		default:
		}
		return nil
	}
	endpoint, err = rns.New(rns.Config{Reticulum: reticulumConfig, IdentityPath: *identityPath, NetworkWait: *networkWait}, handler)
	if err != nil {
		return err
	}
	defer endpoint.Close()
	identityHash, err := hex.DecodeString(endpoint.Name())
	if err != nil {
		return fmt.Errorf("decode local identity: %w", err)
	}
	if strings.TrimSpace(*statePath) == "" {
		*statePath = *identityPath + ".client.db"
	}
	store, err := statebolt.Open(*statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	clientCore, err = client.New(client.Config{Identity: identityHash, Store: store})
	if err != nil {
		return err
	}
	app := &application{ctx: ctx, stdout: stdout, endpoint: endpoint, client: clientCore, events: events}
	if command != "list" {
		if err := endpoint.Start(ctx); err != nil {
			return err
		}
		finishReleases := clientCore.StartOfferReleases(ctx, endpoint.Send)
		defer func() {
			for _, release := range finishReleases() {
				fmt.Fprintf(stderr, "offer release pending allocator=%s offer=%s; retained for retry, lease expiry remains the fallback\n", release.Destination, release.Envelope.GetExecutionOfferRelease().GetOfferId())
			}
			// Stop incoming callbacks before the durable client store is closed.
			_ = endpoint.Close()
		}()
	}
	return dispatch(app, command, args, stderr)
}

func dispatch(app *application, command string, args []string, stderr io.Writer) error {
	switch command {
	case "logs":
		return app.logs(args, stderr)
	case "request":
		return app.request(args, stderr)
	case "list":
		return app.list(args, stderr)
	case "inspect":
		return app.inspect(args, stderr, false)
	case "cancel":
		return app.cancel(args, stderr)
	case "result":
		return app.inspect(args, stderr, true)
	default:
		return fmt.Errorf("unknown command %q: expected request, list, inspect, cancel, result, or logs", command)
	}
}

func knownCommand(command string) bool {
	return command == "logs" || command == "request" || command == "list" || command == "inspect" || command == "cancel" || command == "result"
}

func containsHelp(arguments []string) bool {
	for _, argument := range arguments {
		if argument == "-h" || argument == "--help" {
			return true
		}
	}
	return false
}

func (a *application) request(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s request [options] '<ExecutionRequest JSON>'", stderr)
	offerWait := flags.Duration("offer-wait", 10*time.Second, "time to discover allocators and collect offers")
	var allocators stringValues
	flags.Var(&allocators, "allocator", "allocator destination hash; repeat or omit for announce discovery")
	if err := flags.Parse(arguments); err != nil {
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
	requestID, envelope, err := a.client.CreateRequest(request.GetWorkload(), request.GetPolicy(), request.GetResourceClass())
	if err != nil {
		return err
	}
	sent := make(map[string]bool)
	var rejections []error
	for _, destination := range allocators {
		if err := a.send(destination, envelope); err != nil {
			return fmt.Errorf("send request to %s: %w", destination, err)
		}
		sent[strings.ToLower(destination)] = true
	}
	timer := time.NewTimer(*offerWait)
	defer timer.Stop()
collect:
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case <-timer.C:
			break collect
		case response := <-a.events:
			if response.GetCorrelationId() == envelope.GetMessageId() {
				if err := client.RemoteFailure(response); err != nil {
					rejections = append(rejections, err)
					fmt.Fprintln(stderr, err)
				}
			}
		case service := <-a.endpoint.Discoveries():
			if service.Descriptor.Capacity[request.GetResourceClass()] == 0 {
				continue
			}
			identity, decodeErr := hex.DecodeString(service.Identity)
			if decodeErr != nil {
				continue
			}
			if err := a.client.RegisterAllocator(client.Allocator{Identity: identity, Destination: service.Destination, Hops: service.Hops, Capacity: service.Descriptor.Capacity}); err != nil {
				return err
			}
			if !sent[service.Destination] {
				if err := a.send(service.Destination, envelope); err != nil {
					continue
				}
				sent[service.Destination] = true
			}
		}
	}
	destination, assignment, err := a.client.Select(requestID)
	if err != nil {
		return fmt.Errorf("request %s: %w", requestID, errors.Join(append(rejections, err)...))
	}
	if err := a.send(destination, assignment); err != nil {
		return fmt.Errorf("send assignment: %w", err)
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	snapshot, _ := a.client.Execution(executionID)
	fmt.Fprintf(a.stdout, "request=%s execution=%s allocator=%x status=assignment-sent\n", requestID, executionID, snapshot.Allocator)
	return nil
}

func (a *application) list(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s list", stderr)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("list: unexpected arguments")
	}
	requests := a.client.Requests()
	for _, request := range requests {
		fmt.Fprintf(a.stdout, "request=%s class=%s offers=%d execution=%s\n", request.Request.GetRequestId(), request.Request.GetResourceClass(), request.OfferCount, request.ExecutionID)
	}
	return nil
}

func (a *application) inspect(arguments []string, stderr io.Writer, resultOnly bool) error {
	name := "inspect"
	if resultOnly {
		name = "result"
	}
	flags := newFlagSet("r1s "+name, stderr)
	wait := flags.Duration("wait", 30*time.Second, "time to wait for allocator state")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("%s: execution ID is required", name)
	}
	if *wait <= 0 {
		return fmt.Errorf("%s: --wait must be positive", name)
	}
	executionID := flags.Arg(0)
	destination, envelope, err := a.client.Inspect(executionID)
	if err != nil {
		return err
	}
	if err := a.send(destination, envelope); err != nil {
		return err
	}
	if err := a.waitForState(executionID, envelope.GetMessageId(), *wait); err != nil {
		return err
	}
	snapshot, _ := a.client.Execution(executionID)
	if resultOnly && !terminal(snapshot.State.GetPhase()) {
		return fmt.Errorf("result is not terminal: phase=%s", phaseName(snapshot.State.GetPhase()))
	}
	printState(a.stdout, snapshot)
	return nil
}

func (a *application) cancel(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s cancel", stderr)
	reason := flags.String("reason", "cancelled by client", "cancellation reason")
	wait := flags.Duration("wait", 30*time.Second, "time to wait for allocator state")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("cancel: execution ID is required")
	}
	if *wait <= 0 {
		return errors.New("cancel: --wait must be positive")
	}
	executionID := flags.Arg(0)
	destination, envelope, err := a.client.Cancel(executionID, *reason)
	if err != nil {
		return err
	}
	if err := a.send(destination, envelope); err != nil {
		return err
	}
	if err := a.waitForState(executionID, envelope.GetMessageId(), *wait); err != nil {
		return err
	}
	snapshot, _ := a.client.Execution(executionID)
	printState(a.stdout, snapshot)
	return nil
}

func (a *application) send(destination string, envelope *r1sv1.Envelope) error {
	ctx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
	defer cancel()
	return a.endpoint.Send(ctx, destination, envelope)
}

func (a *application) waitForState(executionID, correlationID string, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting for execution %s state", executionID)
		case envelope := <-a.events:
			if envelope.GetCorrelationId() != correlationID {
				continue
			}
			if err := client.RemoteFailure(envelope); err != nil {
				return err
			}
			if envelope.GetExecutionState().GetExecutionId() == executionID {
				return nil
			}
		}
	}
}

func printState(writer io.Writer, snapshot client.ExecutionSnapshot) {
	state := snapshot.State
	if state == nil {
		fmt.Fprintf(writer, "execution=%s phase=unknown\n", snapshot.ExecutionID)
		return
	}
	exit := ""
	if state.ExitCode != nil {
		exit = fmt.Sprintf(" exit_code=%d", state.GetExitCode())
	}
	fmt.Fprintf(writer, "execution=%s phase=%s occurred_at=%s%s detail=%q\n", snapshot.ExecutionID, phaseName(state.GetPhase()), state.GetOccurredAt().AsTime().Format(time.RFC3339), exit, state.GetDetail())
}

func phaseName(phase r1sv1.ExecutionPhase) string {
	return strings.ToLower(strings.TrimPrefix(phase.String(), "EXECUTION_PHASE_"))
}

func terminal(phase r1sv1.ExecutionPhase) bool {
	return phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
}

type stringValues []string

func (v *stringValues) String() string { return strings.Join(*v, ",") }
func (v *stringValues) Set(value string) error {
	*v = append(*v, value)
	return nil
}

const maxRequestJSONBytes = 1 << 20

func decodeRequestJSON(value string) (*r1sv1.ExecutionRequest, error) {
	if len(value) > maxRequestJSONBytes {
		return nil, fmt.Errorf("request: JSON exceeds %d bytes", maxRequestJSONBytes)
	}
	request := new(r1sv1.ExecutionRequest)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(value), request); err != nil {
		return nil, fmt.Errorf("request: decode JSON: %w", err)
	}
	if request.GetRequestId() != "" {
		return nil, errors.New("request: requestId must be omitted; the client generates it")
	}
	if strings.TrimSpace(request.GetResourceClass()) == "" {
		request.ResourceClass = "default"
	}
	return request, nil
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprintf(output, "Usage of %s:\n", name)
		flags.VisitAll(func(candidate *flag.Flag) {
			fmt.Fprintf(output, "  --%s value\n    \t%s", candidate.Name, candidate.Usage)
			if candidate.DefValue != "" && candidate.DefValue != "false" && candidate.DefValue != "0" {
				fmt.Fprintf(output, " (default %s)", strconv.Quote(candidate.DefValue))
			}
			fmt.Fprintln(output)
		})
	}
	return flags
}
