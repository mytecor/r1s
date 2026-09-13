// Package transport defines authenticated message delivery boundaries.
package transport

import (
	"context"
	"errors"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

var (
	ErrInvalidEndpoint    = errors.New("invalid endpoint")
	ErrDuplicateEndpoint  = errors.New("endpoint already registered")
	ErrUnknownDestination = errors.New("unknown destination")
	ErrDestinationClosed  = errors.New("destination is closed")
	ErrEndpointClosed     = errors.New("endpoint is closed")
	ErrTransportClosed    = errors.New("transport is closed")
)

// Handler receives an envelope whose sender was authenticated by the transport.
type Handler func(context.Context, *r1sv1.Envelope) error

// Sender sends envelopes to transport-specific destinations.
type Sender interface {
	Send(context.Context, string, *r1sv1.Envelope) error
}

// Endpoint is one closeable, authenticated transport identity.
type Endpoint interface {
	Sender
	Name() string
	Close() error
}
