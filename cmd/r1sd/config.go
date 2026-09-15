package main

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
)

func allocatorClusterSource(value string) (string, error) {
	if strings.TrimSpace(value) != "" {
		return value, nil
	}
	return cluster.DefaultPath()
}

func identityDataDirectory(source string) (string, error) {
	if !rns.IsInlineIdentitySource(source) {
		return filepath.Dir(source), nil
	}
	defaultClusterPath, err := cluster.DefaultPath()
	if err != nil {
		return "", err
	}
	return filepath.Dir(defaultClusterPath), nil
}

func parseCapacity(value string) (map[string]uint32, error) {
	capacity := make(map[string]uint32)
	for _, entry := range strings.Split(value, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid capacity %q: expected class=slots", entry)
		}
		slots, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 32)
		if err != nil || slots == 0 {
			return nil, fmt.Errorf("invalid capacity %q: slots must be a positive uint32", entry)
		}
		class := strings.TrimSpace(parts[0])
		if _, duplicate := capacity[class]; duplicate {
			return nil, fmt.Errorf("invalid capacity %q: duplicate class", entry)
		}
		capacity[class] = uint32(slots)
	}
	return capacity, nil
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
