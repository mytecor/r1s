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

// commandLine holds the parsed top-level r1s options that survive past
// parseCommandLine. F22-07 removed the legacy client control plane (local
// socket, durable identity/state, serve, CRUD commands), so the only command
// surfaces are version, cluster management, and `run`. run carries the
// selector and network timeout that openRunApplication needs.
type commandLine struct {
	showVersion     bool
	clusterSelector string
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
		directory, err := cluster.DefaultDirectory()
		if err != nil {
			return err
		}
		return cluster.RunCommand(options.arguments, directory, stdout, stderr)
	}
	// `run --help` and `run <cluster> --help` print run help (flag.ErrHelp)
	// without needing a resolvable cluster or a live transport.
	if options.command == "run" && containsHelp(options.arguments) {
		return (&application{}).runExecution(options.arguments, stderr)
	}
	// The only remaining workflow is `run`. Its client identity, state, and
	// RNS transport are ephemeral and in-memory; the cluster selector is
	// positional. A `-d` parent never owns the run: it only spawns the
	// lease-holding child and observes its ownership handshake, so it never
	// builds an RNS overlay node; the child, a fresh process, starts its own
	// ephemeral transport.
	app, err := openRunApplication(ctx, options, stdout)
	if err != nil {
		return err
	}
	defer app.close()
	if !containsDetachFlag(options.arguments) {
		if err := app.start(); err != nil {
			return err
		}
		defer app.stop(stderr)
	}
	return app.runExecution(options.arguments, stderr)
}

func parseCommandLine(arguments []string, stderr io.Writer) (commandLine, error) {
	flags := newFlagSet("r1s", stderr)
	showVersion := flags.Bool("version", false, "print the version and exit")
	networkWait := flags.Duration("network-timeout", 30*time.Second, "RNS path, link, and response timeout")
	if err := flags.Parse(arguments); err != nil {
		return commandLine{}, err
	}
	commandArguments := flags.Args()
	if *showVersion {
		if len(commandArguments) != 0 {
			return commandLine{}, errors.New("--version does not take a command")
		}
		return commandLine{showVersion: true}, nil
	}
	if len(commandArguments) == 0 {
		return commandLine{}, errors.New("command is required: cluster, run")
	}
	command := commandArguments[0]
	if !knownCommand(command) {
		return commandLine{}, fmt.Errorf("unknown command %q: expected cluster or run", command)
	}
	argumentsAfterCommand := commandArguments[1:]
	if command == "cluster" {
		return commandLine{command: command, arguments: argumentsAfterCommand}, nil
	}
	// `run` takes the cluster selector positionally, except `run -h/--help`
	// which must work without selecting a cluster (the run flag set prints its
	// own help and returns flag.ErrHelp).
	if len(argumentsAfterCommand) > 0 && (argumentsAfterCommand[0] == "-h" || argumentsAfterCommand[0] == "--help") {
		return commandLine{networkWait: *networkWait, command: command, arguments: argumentsAfterCommand}, nil
	}
	if len(argumentsAfterCommand) == 0 || strings.TrimSpace(argumentsAfterCommand[0]) == "" {
		return commandLine{}, errors.New("run: cluster ID or unique prefix is required")
	}
	return commandLine{
		networkWait:     *networkWait,
		clusterSelector: argumentsAfterCommand[0],
		command:         command,
		arguments:       argumentsAfterCommand[1:],
	}, nil
}

func knownCommand(command string) bool {
	return command == "run" || command == "cluster"
}

func containsHelp(arguments []string) bool {
	for _, argument := range arguments {
		if argument == "-h" || argument == "--help" {
			return true
		}
	}
	return false
}

// containsDetachFlag reports whether a `r1s run` argument list requests
// detached mode, so the detached parent can skip building an RNS node it will
// never use.
func containsDetachFlag(arguments []string) bool {
	for _, argument := range arguments {
		if argument == "-d" || argument == "--detach" {
			return true
		}
	}
	return false
}
