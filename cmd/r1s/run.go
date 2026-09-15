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
	showVersion    bool
	configPath     string
	identitySource string
	statePath      string
	clusterSource  string
	networkWait    time.Duration
	command        string
	arguments      []string
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

	app, err := openApplication(ctx, options, stdout)
	if err != nil {
		return err
	}
	defer app.close()
	if options.command != "list" {
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
		return commandLine{}, errors.New("command is required: cluster, request, list, inspect, cancel, result, or logs")
	}
	command := commandArguments[0]
	if command != "cluster" && !knownCommand(command) {
		return commandLine{}, fmt.Errorf("unknown command %q: expected cluster, request, list, inspect, cancel, result, or logs", command)
	}
	if command != "cluster" && !containsHelp(commandArguments[1:]) && (strings.TrimSpace(*configPath) == "" || strings.TrimSpace(*identitySource) == "") {
		return commandLine{}, errors.New("--rns-config and --identity are required")
	}
	return commandLine{
		configPath:     *configPath,
		identitySource: *identitySource,
		statePath:      *statePath,
		clusterSource:  *clusterSource,
		networkWait:    *networkWait,
		command:        command,
		arguments:      commandArguments[1:],
	}, nil
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
