package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	coreclient "github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/tunnel"
	"github.com/mytecor/r1s/internal/tunnel/yggdrasil"
)

var (
	// ErrNoActiveExecution is returned when Dial is called before SetActive.
	ErrNoActiveExecution = errors.New("run tunnel has no active execution")
	// ErrTunnelTargetNotAllowed is returned when Dial names a container port
	// that was not declared when the RunTunnel was created.
	ErrTunnelTargetNotAllowed = errors.New("run tunnel target port is not allowed")
	// ErrRunTunnelClosed is returned after a RunTunnel has been closed.
	ErrRunTunnelClosed = errors.New("run tunnel is closed")
	// ErrRunTunnelActive is returned when a Client is asked to own a second
	// live RunTunnel for its one logical run.
	ErrRunTunnelActive = errors.New("r1s client already owns a run tunnel")
)

// TunnelTarget is a container port authorized for a run-owned tunnel session.
type TunnelTarget struct {
	Port uint16
}

// Stream is a reliable, ordered, bidirectional byte stream to one container
// port of the active execution attempt. It remains bound to that attempt and
// ends when its tunnel pair is closed; it is never rerouted after SetActive.
type Stream interface {
	io.ReadWriteCloser
	CloseRead() error
	CloseWrite() error
}

// TunnelEndpoint is the allocator tunnel edge returned by an authenticated
// owner-open request.
type TunnelEndpoint struct {
	Address []byte
	PubKey  []byte
}

type runTunnelEdgeFactory func(identity []byte) (tunnel.Dialer, []byte, error)
type tunnelOpenFunc func(context.Context, string, []byte, []TunnelTarget) (TunnelEndpoint, error)

// RunTunnel owns the data-plane side of a logical run's tunnel. The active
// execution may change across attempts, while each established stream stays
// bound to the attempt on which it was opened.
type RunTunnel struct {
	client     *Client
	targets    []TunnelTarget
	allowed    map[uint16]struct{}
	newEdge    runTunnelEdgeFactory
	openTunnel tunnelOpenFunc

	mu       sync.Mutex
	active   string
	dialer   tunnel.Dialer
	peerKey  []byte
	pair     tunnel.Conn
	pairExec string
	closed   bool
}

// NewRunTunnel creates the Client's logical-run-owned tunnel for a fixed set of
// allowed container ports. Transport resources are created lazily by the first
// Dial. Close the current RunTunnel before creating another one.
func (c *Client) NewRunTunnel(targets []TunnelTarget) (*RunTunnel, error) {
	runTunnel, err := newRunTunnel(c, targets, newYggdrasilRunTunnelEdge, c.OpenTunnel)
	if err != nil {
		return nil, err
	}
	if err := c.registerRunTunnel(runTunnel); err != nil {
		return nil, err
	}
	return runTunnel, nil
}

func newRunTunnel(c *Client, targets []TunnelTarget, newEdge runTunnelEdgeFactory, openTunnel tunnelOpenFunc) (*RunTunnel, error) {
	if c == nil {
		return nil, errors.New("client: run tunnel requires a client")
	}
	if len(targets) == 0 {
		return nil, errors.New("client: run tunnel requires at least one target")
	}
	if newEdge == nil || openTunnel == nil {
		return nil, errors.New("client: run tunnel implementation is unavailable")
	}
	allowed := make(map[uint16]struct{}, len(targets))
	normalized := make([]TunnelTarget, 0, len(targets))
	for _, target := range targets {
		if target.Port == 0 {
			return nil, errors.New("client: run tunnel target port must be positive")
		}
		if _, exists := allowed[target.Port]; exists {
			continue
		}
		allowed[target.Port] = struct{}{}
		normalized = append(normalized, target)
	}
	return &RunTunnel{
		client: c, targets: normalized, allowed: allowed,
		newEdge: newEdge, openTunnel: openTunnel,
	}, nil
}

func newYggdrasilRunTunnelEdge(identity []byte) (tunnel.Dialer, []byte, error) {
	node, err := yggdrasil.NewNode(identity, yggdrasil.ClientNodeKeyContext, yggdrasil.NodeOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("tunnel edge: %w", err)
	}
	dialer, err := yggdrasil.NewDialer(node)
	if err != nil {
		_ = node.Close()
		return nil, nil, fmt.Errorf("tunnel edge: %w", err)
	}
	return dialer, bytes.Clone(node.PublicKey()), nil
}

// SetActive switches future Dial calls to executionID. If it differs from the
// current pair's execution, the old pair is closed so all of its streams end;
// the replacement pair is not established until a later Dial.
func (t *RunTunnel) SetActive(executionID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.active = strings.TrimSpace(executionID)
	if t.pair != nil && t.pairExec != t.active {
		_ = t.pair.Close()
		t.pair = nil
		t.pairExec = ""
	}
}

// Dial opens a stream to an allowed container port on the active execution.
// Concurrent calls share one authenticated tunnel pair per execution.
func (t *RunTunnel) Dial(ctx context.Context, port uint16) (Stream, error) {
	if ctx == nil {
		return nil, errors.New("client: context is required")
	}
	if _, ok := t.allowed[port]; !ok {
		return nil, fmt.Errorf("%w: %d", ErrTunnelTargetNotAllowed, port)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrRunTunnelClosed
	}
	if t.active == "" {
		return nil, ErrNoActiveExecution
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.pair == nil || t.pairExec != t.active {
		if err := t.establishLocked(ctx); err != nil {
			return nil, err
		}
	}
	opener, ok := t.pair.(tunnel.StreamOpener)
	if !ok {
		_ = t.pair.Close()
		t.pair = nil
		t.pairExec = ""
		return nil, errors.New("tunnel: transport cannot open streams")
	}
	stream, err := opener.OpenStream(port)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (t *RunTunnel) establishLocked(ctx context.Context) error {
	if t.dialer == nil {
		dialer, peerKey, err := t.newEdge(t.client.Identity())
		if err != nil {
			return err
		}
		t.dialer = dialer
		t.peerKey = peerKey
	}
	endpoint, err := t.openTunnel(ctx, t.active, t.peerKey, t.targets)
	if err != nil {
		return err
	}
	pair, err := t.dialer.Dial(ctx, tunnel.Endpoint{Address: endpoint.Address, PubKey: endpoint.PubKey})
	if err != nil {
		return fmt.Errorf("tunnel: dial allocator edge: %w", err)
	}
	if writer, ok := pair.(tunnel.PreambleWriter); ok {
		if err := writer.WritePreamble(tunnel.Preamble{ExecutionID: t.active}); err != nil {
			_ = pair.Close()
			return fmt.Errorf("tunnel: write routing preamble: %w", err)
		}
	}
	t.pair = pair
	t.pairExec = t.active
	return nil
}

// Close closes the active pair and lazy client edge. It is safe to call more
// than once; subsequent Dial calls return ErrRunTunnelClosed.
func (t *RunTunnel) Close() error { return t.close(true) }

func (t *RunTunnel) close(unregister bool) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	pair := t.pair
	dialer := t.dialer
	t.pair = nil
	t.dialer = nil
	t.mu.Unlock()
	var err error
	if pair != nil {
		err = pair.Close()
	}
	if dialer != nil {
		err = errors.Join(err, dialer.Close())
	}
	if unregister {
		t.client.unregisterRunTunnel(t)
	}
	return err
}

// OpenTunnel explicitly asks the selected allocator to authorize peerKey for
// the given container ports. It performs only the authenticated control-plane
// operation; the caller chooses and owns the application-data transport.
func (c *Client) OpenTunnel(ctx context.Context, executionID string, peerKey []byte, targets []TunnelTarget) (TunnelEndpoint, error) {
	if ctx == nil {
		return TunnelEndpoint{}, errors.New("client: context is required")
	}
	internalTargets := make([]tunnel.Target, 0, len(targets))
	for _, target := range targets {
		internalTargets = append(internalTargets, tunnel.Target{Port: target.Port})
	}
	destination, envelope, err := c.core.TunnelOpen(executionID, peerKey, internalTargets)
	if err != nil {
		return TunnelEndpoint{}, err
	}
	waiter, cancel := c.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := c.send(ctx, destination, envelope); err != nil {
		return TunnelEndpoint{}, fmt.Errorf("tunnel: request open: %w", err)
	}
	timer := time.NewTimer(defaultOperationWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return TunnelEndpoint{}, ctx.Err()
		case <-timer.C:
			return TunnelEndpoint{}, fmt.Errorf("tunnel: timed out waiting for allocator to accept execution %s", executionID)
		case response := <-waiter:
			if err := coreclient.RemoteFailure(response); err != nil {
				return TunnelEndpoint{}, fmt.Errorf("tunnel: open rejected: %w", err)
			}
			if ack := response.GetExecutionTunnelOpenAck(); ack != nil && ack.GetExecutionId() == executionID {
				if len(ack.GetAllocatorEndpoint()) == 0 || len(ack.GetAllocatorEndpointPubkey()) == 0 {
					return TunnelEndpoint{}, errors.New("tunnel: allocator returned no tunnel endpoint")
				}
				return TunnelEndpoint{Address: bytes.Clone(ack.GetAllocatorEndpoint()), PubKey: bytes.Clone(ack.GetAllocatorEndpointPubkey())}, nil
			}
		}
	}
}
