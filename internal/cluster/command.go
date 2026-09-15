package cluster

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
)

// RunCommand implements the shared `cluster init|join|show` command surface.
func RunCommand(arguments []string, path string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		return errors.New("cluster command is required: init, join, or show")
	}
	switch arguments[0] {
	case "init":
		flags := clusterFlagSet("cluster init", stderr)
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("cluster init: unexpected arguments")
		}
		key, err := Generate()
		if err != nil {
			return err
		}
		if err := SaveNew(path, key); err != nil {
			return fmt.Errorf("cluster init: %w", err)
		}
		return printMembership(stdout, path, key, true)
	case "join":
		flags := clusterFlagSet("cluster join <join-token>", stderr)
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 1 {
			return errors.New("cluster join: exactly one join token is required")
		}
		key, err := ParseToken(flags.Arg(0))
		if err != nil {
			return err
		}
		if err := SaveNew(path, key); err != nil {
			return fmt.Errorf("cluster join: %w", err)
		}
		return printMembership(stdout, path, key, false)
	case "show":
		flags := clusterFlagSet("cluster show", stderr)
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("cluster show: unexpected arguments")
		}
		key, err := Load(path)
		if err != nil {
			return err
		}
		return printMembership(stdout, path, key, false)
	default:
		return fmt.Errorf("unknown cluster command %q: expected init, join, or show", arguments[0])
	}
}

func printMembership(output io.Writer, path string, key []byte, includeToken bool) error {
	id, err := ID(key)
	if err != nil {
		return err
	}
	fmt.Fprintf(output, "Cluster ID: %s\n", hex.EncodeToString(id))
	if includeToken {
		token, err := Token(key)
		if err != nil {
			return err
		}
		fmt.Fprintf(output, "Join token: %s\n", token)
	}
	fmt.Fprintf(output, "Cluster state: %s\n", path)
	return nil
}

func clusterFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}
