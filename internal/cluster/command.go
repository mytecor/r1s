package cluster

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
)

// RunCommand implements the shared `cluster init|join|list` command surface.
func RunCommand(arguments []string, directory string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		return errors.New("cluster command is required: init, join, or list")
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
		id, err := SaveCredential(directory, key)
		if err != nil {
			return fmt.Errorf("cluster init: %w", err)
		}
		return printMembership(stdout, directory, id, key, true)
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
		id, err := SaveCredential(directory, key)
		if err != nil {
			return fmt.Errorf("cluster join: %w", err)
		}
		return printMembership(stdout, directory, id, key, false)
	case "list":
		flags := clusterFlagSet("cluster list", stderr)
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("cluster list: unexpected arguments")
		}
		ids, err := List(directory)
		if err != nil {
			return err
		}
		for _, id := range ids {
			fmt.Fprintln(stdout, id)
		}
		return nil
	default:
		return fmt.Errorf("unknown cluster command %q: expected init, join, or list", arguments[0])
	}
}

func printMembership(output io.Writer, directory, id string, key []byte, includeToken bool) error {
	fmt.Fprintf(output, "Cluster ID: %s\n", id)
	if includeToken {
		token, err := Token(key)
		if err != nil {
			return err
		}
		fmt.Fprintf(output, "Join token: %s\n", token)
	}
	fmt.Fprintf(output, "Cluster credential: %s\n", filepath.Join(directory, id))
	return nil
}

func clusterFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}
