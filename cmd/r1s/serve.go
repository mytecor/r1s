package main

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/localserver"
)

// serveOptions are parsed from `r1s serve`.
type serveOptions struct {
	socketPath string
	permission uint32
}

// serve runs the persistent local client behind a versioned gRPC API on a Unix
// socket. Exactly one client identity and state store stays alive for the
// service lifetime; Watch streams are replayed from the durable journal so a
// watcher reconnects without inventing or reordering revisions. The serve
// process is also the only continuous lease-renewal holder: every recorded
// lease-holding intent is replayed from durable state on every tick.
func (a *application) serve(args []string, stderr io.Writer) error {
	options, err := parseServeOptions(args, stderr)
	if err != nil {
		return err
	}
	// Serve mode keeps the RNS endpoint and offer-release loop alive for the
	// whole process, exactly as the CLI does for one workflow.
	if err := a.start(); err != nil {
		return err
	}
	defer a.stop(stderr)

	go a.runLeaseMaintainer(a.ctx, stderr)

	fmt.Fprintf(stderr, "serving local client API on %s\n", options.socketPath)
	return localserver.New(a).ListenAndServe(a.ctx, options.socketPath, localserver.SocketPermission(options.permission))
}

// parseServeOptions reads the serve subcommand flags.
func parseServeOptions(args []string, stderr io.Writer) (serveOptions, error) {
	flags := newFlagSet("r1s serve", stderr)
	socketPath := flags.String("socket", "", "Unix socket path (defaults to ~/.config/r1s/<identity>.sock)")
	permission := flags.Uint64("socket-mode", 0o600, "Unix socket permission bits")
	if err := flags.Parse(args); err != nil {
		return serveOptions{}, err
	}
	if flags.NArg() != 0 {
		return serveOptions{}, errors.New("serve: unexpected arguments")
	}
	if *permission > 0o777 {
		return serveOptions{}, fmt.Errorf("serve: --socket-mode must be 0..0777")
	}
	if *socketPath == "" {
		path, err := defaultSocketPath()
		if err != nil {
			return serveOptions{}, err
		}
		*socketPath = path
	}
	return serveOptions{
		socketPath: *socketPath,
		permission: uint32(*permission),
	}, nil
}

// tunnelPeerList parses a comma-separated bootstrap peer URI list for the
// tunnel edge. Empty entries are dropped; an empty value joins the standard
// public Yggdrasil overlay. Peering is edge configuration, never a protocol
// feature.
func tunnelPeerList(value string) []string {
	var peers []string
	for _, peer := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(peer); trimmed != "" {
			peers = append(peers, trimmed)
		}
	}
	return peers
}

// defaultSocketPath places the socket beside the client state database default
// directory, which is already identity-scoped and reliably writable.
func defaultSocketPath() (string, error) {
	path, err := cluster.DefaultDirectory()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(path), "client.sock"), nil
}
