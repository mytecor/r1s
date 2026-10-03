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
	"time"

	r1sclient "github.com/mytecor/r1s/client"
)

// Run-owned published ports (F22-06): `r1s run -p host:container` binds a local
// loopback listener for each mapping for the lifetime of the logical run and
// relays every inbound connection to the currently active execution attempt over
// one authenticated owner-open tunnel. On reschedule the listener stays bound
// while the old execution's session closes and a fresh owner open routes new
// connections to the replacement execution. Existing streams end at the old
// session; a stream is never silently re-routed to a different execution.

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

// portListValue accumulates repeatable --publish/--port host:container values.
type portListValue struct {
	mappings []portMapping
}

func (p *portListValue) String() string {
	bits := make([]string, 0, len(p.mappings))
	for _, m := range p.mappings {
		bits = append(bits, fmt.Sprintf("%d:%d", m.host, m.container))
	}
	return strings.Join(bits, ",")
}

func (p *portListValue) Set(raw string) error {
	mapping, err := parsePortMapping(raw)
	if err != nil {
		return err
	}
	p.mappings = append(p.mappings, mapping)
	return nil
}

// parsePublishFlag parses a comma-separated host:container list passed to the
// detached child via the internal --r1s-publish flag.
func parsePublishFlag(raw string) ([]portMapping, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var mappings []portMapping
	for _, item := range strings.Split(raw, ",") {
		if strings.TrimSpace(item) == "" {
			continue
		}
		mapping, err := parsePortMapping(item)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mapping)
	}
	return mappings, nil
}

// runPublisher owns the run's published loopback listeners and the single
// authenticated mesh pair for the currently active execution. A listener stays
// bound across reschedules; each inbound connection opens a stream to the
// current active execution and is spliced to its container port. Old streams
// (opened against a previous execution's pair) end when that pair is closed.
type runPublisher struct {
	ctx       context.Context
	tunnel    *r1sclient.RunTunnel
	targets   []r1sclient.TunnelTarget // container ports to authorize (deduped)
	listeners []*net.TCPListener
	// listenerPorts[i] is the container port relayed by listeners[i]. A listener
	// maps to exactly one host:container pair even when several host ports share
	// the same container port, so rebinding and multi-route relays stay correct.
	listenerPorts []uint16
	wg            sync.WaitGroup
}

// newRunPublisher binds every published listener. The client edge node is built
// lazily on first establishment. targets is the deduped container-port list; a
// bind failure (privileged port, port in use) is reported before any forwarding
// starts so the whole run fails fast.
func newRunPublisher(ctx context.Context, app *application, mappings []portMapping) (*runPublisher, error) {
	targets := make([]r1sclient.TunnelTarget, 0, len(mappings))
	seen := make(map[uint16]bool, len(mappings))
	for _, m := range mappings {
		if !seen[m.container] {
			seen[m.container] = true
			targets = append(targets, r1sclient.TunnelTarget{Port: m.container})
		}
	}
	runTunnel, err := app.controller.NewRunTunnel(targets)
	if err != nil {
		return nil, err
	}
	p := &runPublisher{ctx: ctx, tunnel: runTunnel, targets: targets}
	for _, m := range mappings {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", m.host))
		if err != nil {
			p.close()
			return nil, fmt.Errorf("publish: bind host port %d: %w", m.host, err)
		}
		p.listeners = append(p.listeners, ln.(*net.TCPListener))
		p.listenerPorts = append(p.listenerPorts, m.container)
	}
	return p, nil
}

func (p *runPublisher) start() {
	for _, ln := range p.listeners {
		p.wg.Add(1)
		go func(ln *net.TCPListener) {
			defer p.wg.Done()
			for {
				conn, err := ln.AcceptTCP()
				if err != nil {
					return // listener closed (run end)
				}
				go p.handleConn(conn)
			}
		}(ln)
	}
}

func (p *runPublisher) handleConn(conn *net.TCPConn) {
	defer conn.Close()
	p.serve(conn)
}

// SetActive points the publisher at a new execution attempt. When the execution
// changes, the old pair is closed (its streams end) and subsequent connections
// re-establish a fresh owner open against the replacement. The listeners stay
// bound across the switch.
func (p *runPublisher) SetActive(executionID string) {
	p.tunnel.SetActive(executionID)
}

// serve opens a stream to the container port for a connection and relays bytes
// until both directions end. RunTunnel binds the stream to the active execution
// at open time and never reroutes it if the run reschedules.
func (p *runPublisher) serve(conn *net.TCPConn) {
	// Resolve the container port for this connection by matching the local
	// address against the bound listeners' ports.
	containerPort, ok := p.containerFor(conn.LocalAddr())
	if !ok {
		_ = conn.Close()
		return
	}
	dialCtx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
	defer cancel()
	stream, err := p.tunnel.Dial(dialCtx, containerPort)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer stream.Close()
	p.relay(stream, conn)
}

// containerFor resolves the container port a local connection was accepted on
// (so a listener maps to exactly its own container port, even when several host
// ports share one container port).
func (p *runPublisher) containerFor(local net.Addr) (uint16, bool) {
	tcpAddr, ok := local.(*net.TCPAddr)
	if !ok {
		return 0, false
	}
	for i, ln := range p.listeners {
		if ln.Addr().(*net.TCPAddr).Port == tcpAddr.Port {
			return p.listenerPorts[i], true
		}
	}
	return 0, false
}

// relay moves bytes both directions until both directions end. A read EOF on one
// side half-closes the write side of the other so interactive protocols behave.
func (p *runPublisher) relay(client r1sclient.Stream, target *net.TCPConn) {
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		if _, err := io.Copy(target, client); err != nil {
			_ = client.Close()
			_ = target.Close()
			return
		}
		_ = target.CloseWrite()
	}()
	go func() {
		if _, err := io.Copy(client, target); err != nil {
			_ = client.Close()
			_ = target.Close()
		}
		_ = client.CloseWrite()
		done <- struct{}{}
	}()
	<-done
	<-done
}

// close stops every listener (blocked accepts release) and closes the active
// public run tunnel. Called when the run ends.
func (p *runPublisher) close() {
	for _, ln := range p.listeners {
		_ = ln.Close()
	}
	p.wg.Wait()
	_ = p.tunnel.Close()
}
