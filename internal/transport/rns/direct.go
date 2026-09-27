package rns

import (
	"bytes"
	"context"
	"fmt"

	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	coretransport "github.com/mytecor/r1s/internal/transport"
	"github.com/mytecor/r1s/meshbus"
)

var _ meshbus.Sender = (*Endpoint)(nil)

// SendMessage sends opaque application bytes over an authenticated realm
// session. It does not inspect the payload or accept a payload-provided sender.
func (e *Endpoint) SendMessage(ctx context.Context, target string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) == 0 {
		return fmt.Errorf("%w: payload is required", meshbus.ErrInvalidMessage)
	}
	destinationHash, key, err := parseDestination(target)
	if err != nil {
		return err
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return coretransport.ErrEndpointClosed
	}
	if !e.started {
		e.mu.Unlock()
		return ErrNotStarted
	}
	e.mu.Unlock()
	destinationHash, key, active := e.connections.resolve(destinationHash, key)
	if active == nil || active.link.GetStatus() != link.StatusActive {
		active, err = e.connect(ctx, destinationHash, key)
		if err != nil {
			return err
		}
	}
	if err := e.waitAuthenticated(ctx, active); err != nil {
		return fmt.Errorf("authenticate RNS session: %w", err)
	}
	if len(payload) > active.channel.MDU() {
		return fmt.Errorf("direct message: %d bytes exceed RNS Channel MDU %d", len(payload), active.channel.MDU())
	}
	if err := e.sendChannel(ctx, active, &directMessage{data: bytes.Clone(payload)}); err != nil {
		return fmt.Errorf("send RNS Channel message: %w", err)
	}
	return nil
}
