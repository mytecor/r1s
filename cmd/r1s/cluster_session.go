package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mytecor/r1s/internal/broker"
	"github.com/mytecor/r1s/internal/cluster"
)

const brokerStartTimeout = 15 * time.Second

func runClusterSession(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	address, err := broker.DefaultAddress()
	if err != nil {
		return err
	}
	switch arguments[0] {
	case "use":
		useOptions, err := parseClusterUse(arguments[1:], stderr)
		if err != nil {
			return err
		}
		directory, err := cluster.DefaultDirectory()
		if err != nil {
			return err
		}
		key, id, err := cluster.Resolve(directory, useOptions.selector)
		if err != nil {
			return fmt.Errorf("cluster use: %w", err)
		}
		detached := useOptions.detach
		if _, statusErr := broker.Status(address); statusErr == nil {
			if err := broker.Shutdown(address); err != nil {
				return fmt.Errorf("cluster use: stop current broker: %w", err)
			}
			if err := waitBrokerStopped(ctx, address); err != nil {
				return err
			}
		}
		if detached {
			if err := launchClusterBroker(ctx, id); err != nil {
				return err
			}
			return printCurrentCluster(stdout, id, address)
		}
		server := &broker.Server{ClusterID: id, ClusterKey: key, Ready: func() {
			_ = printCurrentCluster(stdout, id, address)
		}}
		if err := server.Serve(ctx, address); err != nil {
			return err
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return nil
	case "status":
		flags := newFlagSet("r1s cluster status", stderr)
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("cluster status: unexpected arguments")
		}
		id, err := broker.Status(address)
		if err != nil {
			return broker.ErrUnavailable
		}
		return printCurrentCluster(stdout, id, address)
	case "unset":
		flags := newFlagSet("r1s cluster unset", stderr)
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("cluster unset: unexpected arguments")
		}
		id, err := broker.Status(address)
		if err != nil {
			return broker.ErrUnavailable
		}
		if err := broker.Shutdown(address); err != nil {
			return fmt.Errorf("cluster unset: %w", err)
		}
		if err := waitBrokerStopped(ctx, address); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Cluster unset: %s\n", id)
		return nil
	default:
		return fmt.Errorf("unknown cluster command %q", arguments[0])
	}
}

type clusterUseOptions struct {
	selector string
	detach   bool
}

func parseClusterUse(arguments []string, stderr io.Writer) (clusterUseOptions, error) {
	flags := newFlagSet("r1s cluster use [-d] <cluster>", stderr)
	detach := flags.Bool("d", false, "run the cluster authority broker in the background")
	detachLong := flags.Bool("detach", false, "run the cluster authority broker in the background")
	if err := flags.Parse(arguments); err != nil {
		return clusterUseOptions{}, err
	}
	if flags.NArg() != 1 {
		return clusterUseOptions{}, errors.New("cluster use: exactly one cluster ID or unique prefix is required")
	}
	return clusterUseOptions{selector: flags.Arg(0), detach: *detach || *detachLong}, nil
}

func printCurrentCluster(output io.Writer, id, address string) error {
	_, err := fmt.Fprintf(output, "Current cluster: %s\nBroker socket: %s\n", id, address)
	return err
}

func launchClusterBroker(ctx context.Context, clusterID string) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cluster use: resolve executable: %w", err)
	}
	return launchDetachedProcess(ctx, executable, []string{"__cluster-broker", clusterID}, detachedProcess{
		label: "cluster use", timeout: brokerStartTimeout,
		validate: func(line string) error {
			if line != clusterID {
				return fmt.Errorf("ready cluster %q does not match %q", line, clusterID)
			}
			return nil
		},
	})
}

func runClusterBroker(ctx context.Context, arguments []string, stdout io.Writer) error {
	if len(arguments) != 1 {
		return errors.New("internal cluster broker requires one full cluster ID")
	}
	directory, err := cluster.DefaultDirectory()
	if err != nil {
		return err
	}
	key, id, err := cluster.Resolve(directory, arguments[0])
	if err != nil {
		return err
	}
	if id != arguments[0] {
		return errors.New("internal cluster broker requires a full cluster ID")
	}
	address, err := broker.DefaultAddress()
	if err != nil {
		return err
	}
	server := &broker.Server{ClusterID: id, ClusterKey: key, Ready: func() {
		fmt.Fprintln(stdout, id)
	}}
	return server.Serve(ctx, address)
}

func waitBrokerStopped(ctx context.Context, address string) error {
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := broker.Status(address); err != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("cluster broker did not stop")
		case <-ticker.C:
		}
	}
}
