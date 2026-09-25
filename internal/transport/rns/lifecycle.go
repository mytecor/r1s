package rns

import (
	"context"
	"fmt"

	coretransport "github.com/mytecor/r1s/internal/transport"
)

// Start starts Reticulum interfaces, publishes the service descriptor, and refreshes it periodically.
func (e *Endpoint) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return coretransport.ErrEndpointClosed
	}
	if e.started {
		e.mu.Unlock()
		return nil
	}
	e.started = true
	e.mu.Unlock()
	if err := e.stack.Start(); err != nil {
		e.mu.Lock()
		e.started = false
		e.mu.Unlock()
		return fmt.Errorf("start Reticulum stack: %w", err)
	}
	if e.advertises {
		if err := e.destination.Announce(false, nil, nil); err != nil {
			_ = e.stack.Close()
			e.mu.Lock()
			e.started = false
			e.mu.Unlock()
			return fmt.Errorf("announce r1s service: %w", err)
		}
	}
	announceContext, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	e.stopAnnounce = cancel
	e.mu.Unlock()
	if e.advertises {
		go e.announceLoop(announceContext)
	}
	return nil
}

// Close shuts down links and the Reticulum endpoint. It is idempotent.
func (e *Endpoint) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	if e.stopAnnounce != nil {
		e.stopAnnounce()
	}
	e.mu.Unlock()
	for _, active := range e.connections.allSessions() {
		active.link.Teardown()
	}
	return e.stack.Close()
}
