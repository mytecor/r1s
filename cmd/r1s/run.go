package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mytecor/r1s/internal/cluster"
)

// commandLine holds the parsed top-level r1s options that survive past
// parseCommandLine. F22-07 removed the legacy client control plane (local
// socket, durable identity/state, serve, CRUD commands), so the only command
// surfaces are version, cluster management, and `run`. run carries the network
// timeout used by the broker-created endpoint.
type commandLine struct {
	showVersion bool
	networkWait time.Duration
	command     string
	arguments   []string
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
		if len(options.arguments) > 0 && (options.arguments[0] == "use" || options.arguments[0] == "status" || options.arguments[0] == "unset") {
			return runClusterSession(ctx, options.arguments, stdout, stderr)
		}
		directory, err := cluster.DefaultDirectory()
		if err != nil {
			return err
		}
		return cluster.RunCommand(options.arguments, directory, stdout, stderr)
	}
	if options.command == "__cluster-broker" {
		return runClusterBroker(ctx, options.arguments, stdout)
	}
	// `run --help` prints run help without needing an active broker.
	if options.command == "run" && containsHelp(options.arguments) {
		return (&application{}).runExecution(options.arguments, stderr)
	}
	// The only remaining workflow is `run`. Its client identity and RNS
	// transport are broker-created and ephemeral; run state remains in this
	// process. A `-d` parent never owns the run: it only spawns the
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
	if command == "__cluster-broker" {
		return commandLine{networkWait: *networkWait, command: command, arguments: argumentsAfterCommand}, nil
	}
	return commandLine{
		networkWait: *networkWait,
		command:     command,
		arguments:   argumentsAfterCommand,
	}, nil
}

func knownCommand(command string) bool {
	return command == "run" || command == "cluster" || command == "__cluster-broker"
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
