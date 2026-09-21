package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
)

// portMapping is one Docker-style --port host:container mapping. host is the
// client-side local port a listener binds on 127.0.0.1; container is the
// container-side destination port the allocator splices the tunnel stream to.
// There are no named slots: each --port is just which host port to expose and
// which container port it reaches.
type portMapping struct {
	host      uint16
	container uint16
}

// parsePort parses a positive uint16 port.
func parsePort(raw string) (uint16, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("port is required")
	}
	value, err := strconv.ParseUint(raw, 10, 16)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("port must be a positive uint16, got %q", raw)
	}
	return uint16(value), nil
}

// parsePortMapping parses one --port value of the form host:container.
func parsePortMapping(raw string) (portMapping, error) {
	hostRaw, containerRaw, ok := strings.Cut(strings.TrimSpace(raw), ":")
	if !ok {
		return portMapping{}, errors.New("expected host:container (e.g. --port 8080:80)")
	}
	host, err := parsePort(hostRaw)
	if err != nil {
		return portMapping{}, fmt.Errorf("host: %w", err)
	}
	container, err := parsePort(containerRaw)
	if err != nil {
		return portMapping{}, fmt.Errorf("container: %w", err)
	}
	return portMapping{host: host, container: container}, nil
}

// tunnel is commandHandler-compatible: service-backed mode binds a local TCP
// listener for each --port and forwards inbound connections to the container
// port over the tunnel. Direct mode rejects the session with a clear
// CommandError because only a persistent 'r1s serve' holds the F17 keep-alive
// intent that keeps the execution alive for the duration of the forward.
func (l *localCLI) tunnel(args []string, stderr io.Writer) error {
	var portMappings []portMapping
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--port" || a == "-port":
			if i+1 >= len(args) || strings.TrimSpace(args[i+1]) == "" {
				return errors.New("tunnel: --port requires host:container")
			}
			mapping, err := parsePortMapping(args[i+1])
			if err != nil {
				return fmt.Errorf("tunnel: --port %q: %w", args[i+1], err)
			}
			portMappings = append(portMappings, mapping)
			i++
		case strings.HasPrefix(a, "--port=") || strings.HasPrefix(a, "-port="):
			raw := strings.TrimPrefix(strings.TrimPrefix(a, "--port="), "-port=")
			mapping, err := parsePortMapping(raw)
			if err != nil {
				return fmt.Errorf("tunnel: --port %q: %w", raw, err)
			}
			portMappings = append(portMappings, mapping)
		case a == "--help" || a == "-h":
			fmt.Fprintf(stderr, "Usage: r1s tunnel <execution-id> --port <host>:<container> [--port ...]")
			fmt.Fprintf(stderr, "\n\n  --port <host>:<container>  forward 127.0.0.1:<host> to the container port\n")
			fmt.Fprintf(stderr, "                             <container> (repeatable, Docker-style). Open\n")
			fmt.Fprintf(stderr, "                             127.0.0.1:<host> in a browser or client to reach\n")
			fmt.Fprintf(stderr, "                             the container service.\n")
			return nil
		case strings.HasPrefix(a, "-") && a != "-":
			return fmt.Errorf("tunnel: unknown flag: %s", a)
		default:
			positional = append(positional, a)
		}
	}
	if len(positional) != 1 {
		return errors.New("tunnel: execution ID is required")
	}
	executionID := strings.TrimSpace(positional[0])
	if executionID == "" {
		return errors.New("tunnel: execution ID is required")
	}
	if len(portMappings) == 0 {
		return errors.New("tunnel: at least one --port host:container is required")
	}

	// The grant needs the set of container ports (the destinations inside the
	// execution). Deduplicate so a single grant authorizes them all; the host
	// ports are bound locally and never sent to the allocator.
	seenPorts := make(map[uint16]bool)
	targets := make([]tunnel.Target, 0, len(portMappings))
	for _, mapping := range portMappings {
		if !seenPorts[mapping.container] {
			seenPorts[mapping.container] = true
			targets = append(targets, tunnel.Target{Port: mapping.container})
		}
	}

	ctx, cancel := context.WithCancel(l.ctx)
	defer cancel()

	// Bind a local listener for every --port mapping. A bind failure (privileged
	// port, port in use) is reported before any forwarding starts so the whole
	// command fails fast.
	type boundListener struct {
		listener net.Listener
		mapping  portMapping
	}
	var listeners []boundListener
	for _, mapping := range portMappings {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", mapping.host))
		if err != nil {
			for _, bl := range listeners {
				_ = bl.listener.Close()
			}
			return fmt.Errorf("tunnel: bind host port %d: %w", mapping.host, err)
		}
		listeners = append(listeners, boundListener{listener: ln, mapping: mapping})
		fmt.Fprintf(stderr, "tunnel: forwarding 127.0.0.1:%d -> container port %d (execution %s)\n",
			mapping.host, mapping.container, executionID)
	}

	// Accept connections on every listener; each connection gets its own tunnel
	// stream to the mapping's container port. The local listeners forward until
	// the command is interrupted.
	var wg sync.WaitGroup
	for _, bl := range listeners {
		wg.Add(1)
		go func(bl boundListener, targetPort uint16) {
			defer wg.Done()
			for {
				conn, err := bl.listener.Accept()
				if err != nil {
					return // listener closed (shutdown)
				}
				go l.relayPort(ctx, executionID, targets, targetPort, conn, stderr)
			}
		}(bl, bl.mapping.container)
	}

	// Run until interrupted (SIGINT/SIGTERM cancels l.ctx). On exit, close every
	// listener so the accept goroutines stop.
	<-ctx.Done()
	for _, bl := range listeners {
		_ = bl.listener.Close()
	}
	wg.Wait()
	return nil
}

// relayPort forwards bytes between an accepted local TCP connection and one
// tunnel stream to a container port, until either side closes. Each connection
// is independent: a browser can open many connections to one host port, and
// each is a separate multiplexed stream on the shared authenticated pair.
func (l *localCLI) relayPort(ctx context.Context, executionID string, targets []tunnel.Target, containerPort uint16, conn net.Conn, stderr io.Writer) {
	defer conn.Close()
	stream, err := l.client.Tunnel(ctx, executionID, targets, containerPort)
	if err != nil {
		fmt.Fprintf(stderr, "tunnel: open container port %d: %v\n", containerPort, err)
		return
	}
	done := make(chan struct{}, 2)

	// conn -> tunnel: read from the local TCP socket and send data frames.
	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				if serr := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Data{Data: append([]byte(nil), buf[:n]...)}}); serr != nil {
					return
				}
			}
			if rerr != nil {
				_ = stream.CloseSend()
				return
			}
		}
	}()

	// tunnel -> conn: receive data frames and write them to the local socket.
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			msg, rerr := stream.Recv()
			if rerr != nil {
				return
			}
			switch payload := msg.GetPayload().(type) {
			case *r1sv1.LocalTunnelMessage_Data:
				if _, werr := conn.Write(payload.Data); werr != nil {
					return
				}
			case *r1sv1.LocalTunnelMessage_Close:
				// A write half-close propagates as a half-close on the local
				// socket so half-duplex protocols see a clean EOF; a full close
				// ends the connection.
				if payload.Close.GetHalf() {
					if tcp, ok := conn.(*net.TCPConn); ok {
						_ = tcp.CloseWrite()
					}
					continue
				}
				return
			}
		}
	}()

	<-done
	<-done
}

// tunnel is rejected in direct mode: a direct run would let the execution's
// lease lapse (default 10 minutes) mid-forward because only a serve process
// holds the F17 keep-alive intent that renews it.
func (a *application) tunnel(args []string, stderr io.Writer) error {
	return errors.New("tunnel: direct mode is not supported; start 'r1s serve' so its socket is discovered, then run 'r1s tunnel <executionID> --port host:container'")
}
