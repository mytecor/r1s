package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mytecor/r1s/internal/cluster"
)

type commandLine struct {
	showVersion     bool
	configPath      string
	identitySource  string
	statePath       string
	clusterSource   string
	socketPath      string
	socketCandidate string
	networkWait     time.Duration
	command         string
	arguments       []string
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	options, err := parseCommandLine(arguments, stderr)
	if err != nil {
		return err
	}
	if options.showVersion {
		fmt.Fprintln(stdout, version)
		return nil
	}
	if options.command == "cluster" {
		path, err := clientClusterSource(options.clusterSource)
		if err != nil {
			return err
		}
		return cluster.RunCommand(options.arguments, path, stdout, stderr)
	}
	if containsHelp(options.arguments) {
		return dispatch(&application{}, options.command, options.arguments, stderr)
	}

	// Service-backed mode: every workflow command runs through the local API.
	// An explicit --socket is authoritative and required to be live (an
	// unreachable explicit socket is an error, never a fallback). Without an
	// explicit --socket, a living default socket is discovered so the CLI works
	// transparently on a host where 'r1s serve' is already running; otherwise
	// the command falls back to direct mode (BACKLOG #13: socket discovery).
	if strings.TrimSpace(options.socketPath) != "" {
		handler, err := openLocalCLI(ctx, options, stdout, stderr)
		if err != nil {
			return err
		}
		defer handler.Close()
		return dispatch(handler, options.command, options.arguments, stderr)
	}
	if options.socketCandidate != "" {
		candidate, err := defaultSocketPath()
		if err == nil && candidate == options.socketCandidate && localAPISocketAlive(candidate) {
			options.socketPath = candidate
			handler, err := openLocalCLI(ctx, options, stdout, stderr)
			if err == nil {
				defer handler.Close()
				return dispatch(handler, options.command, options.arguments, stderr)
			}
		}
		// No live default service: direct mode needs an identity and cluster.
		if strings.TrimSpace(options.configPath) == "" || strings.TrimSpace(options.identitySource) == "" {
			return fmt.Errorf("no local r1s service is running at %s, and --rns-config and --identity are required for direct mode; start 'r1s serve' or pass them explicitly", options.socketCandidate)
		}
	}

	app, err := openApplication(ctx, options, stdout)
	if err != nil {
		return err
	}
	defer app.close()
	if options.command != "list" && options.command != "serve" {
		if err := app.start(); err != nil {
			return err
		}
		defer app.stop(stderr)
	}
	return dispatch(app, options.command, options.arguments, stderr)
}

func parseCommandLine(arguments []string, stderr io.Writer) (commandLine, error) {
	flags := newFlagSet("r1s", stderr)
	showVersion := flags.Bool("version", false, "print the version and exit")
	configPath := flags.String("rns-config", "", "path to a Reticulum-Go configuration file")
	identitySource := flags.String("identity", "", "private RNS identity (hex, Base32, Base64) or file path")
	statePath := flags.String("state", "", "client state database (defaults beside the identity file or under ~/.config/r1s)")
	clusterSource := flags.String("cluster", "", "cluster join token or state file (defaults to ~/.config/r1s/cluster)")
	socketPath := flags.String("socket", "", "local API socket; when set, workflows run through a persistent r1s serve service instead of direct mode")
	socketCandidate := ""
	networkWait := flags.Duration("network-timeout", 30*time.Second, "RNS path, link, and response timeout")
	if err := flags.Parse(arguments); err != nil {
		return commandLine{}, err
	}
	if strings.TrimSpace(*socketPath) == "" {
		// Socket discovery: when no explicit socket is given, remember the
		// default path so run can transparently use a live local service
		// without forcing the user to type --socket.
		if discovered, err := defaultSocketPath(); err == nil {
			socketCandidate = discovered
		}
	}
	commandArguments := flags.Args()
	if *showVersion {
		if len(commandArguments) != 0 {
			return commandLine{}, errors.New("--version does not take a command")
		}
		return commandLine{showVersion: true}, nil
	}
	if len(commandArguments) == 0 {
		return commandLine{}, errors.New("command is required: cluster, serve, request, list, inspect, cancel, result, or logs")
	}
	command := commandArguments[0]
	if command != "cluster" && !knownCommand(command) {
		return commandLine{}, fmt.Errorf("unknown command %q: expected cluster, serve, request, list, inspect, cancel, result, or logs", command)
	}
	if command != "cluster" && !containsHelp(commandArguments[1:]) && strings.TrimSpace(*socketPath) == "" && socketCandidate == "" && (strings.TrimSpace(*configPath) == "" || strings.TrimSpace(*identitySource) == "") {
		return commandLine{}, errors.New("--rns-config and --identity are required (or pass --socket, or start 'r1s serve' so its socket is discovered)")
	}
	return commandLine{
		configPath:      *configPath,
		identitySource:  *identitySource,
		statePath:       *statePath,
		clusterSource:   *clusterSource,
		socketPath:      *socketPath,
		socketCandidate: socketCandidate,
		networkWait:     *networkWait,
		command:         command,
		arguments:       commandArguments[1:],
	}, nil
}

// commandHandler is implemented by both direct mode (*application) and
// service-backed mode (*localCLI) so one dispatch serves both.
type commandHandler interface {
	logs(args []string, diagnostics io.Writer) error
	request(args []string, stderr io.Writer) error
	list(args []string, stderr io.Writer) error
	inspect(args []string, stderr io.Writer, resultOnly bool) error
	cancel(args []string, stderr io.Writer) error
	serve(args []string, stderr io.Writer) error
}

func dispatch(handler commandHandler, command string, args []string, stderr io.Writer) error {
	switch command {
	case "serve":
		return handler.serve(args, stderr)
	case "logs":
		return handler.logs(args, stderr)
	case "request":
		return handler.request(args, stderr)
	case "list":
		return handler.list(args, stderr)
	case "inspect":
		return handler.inspect(args, stderr, false)
	case "cancel":
		return handler.cancel(args, stderr)
	case "result":
		return handler.inspect(args, stderr, true)
	default:
		return fmt.Errorf("unknown command %q: expected serve, request, list, inspect, cancel, result, or logs", command)
	}
}

func knownCommand(command string) bool {
	return command == "serve" || command == "logs" || command == "request" || command == "list" || command == "inspect" || command == "cancel" || command == "result"
}

func containsHelp(arguments []string) bool {
	for _, argument := range arguments {
		if argument == "-h" || argument == "--help" {
			return true
		}
	}
	return false
}
