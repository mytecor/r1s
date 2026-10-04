package broker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
	"google.golang.org/protobuf/proto"
)

var ErrUnavailable = errors.New("no active r1s cluster; run `r1s cluster use -d <cluster>` or keep `cluster use <cluster>` running")

const localHandshakeTimeout = 5 * time.Second

// Endpoint is a run-owned view of one broker-created ephemeral RNS endpoint.
// The broker retains the cluster key; this process owns all run-controller
// state and closes the remote endpoint when its run ends.
type Endpoint struct {
	connection net.Conn
	identity   string
	clusterID  string
	bootstrap  []string
	handler    func(context.Context, *r1sv1.Envelope) error

	writeMu     sync.Mutex
	mu          sync.Mutex
	nextID      uint64
	pending     map[uint64]chan frame
	routes      map[string]string
	discoveries chan rns.Service
	done        chan struct{}
	closeOnce   sync.Once
}

// OpenEndpoint connects to the selected-cluster broker and allocates a fresh
// authenticated transport identity for one run controller.
func OpenEndpoint(address string, networkWait time.Duration, handler func(context.Context, *r1sv1.Envelope) error) (*Endpoint, error) {
	if handler == nil {
		return nil, errors.New("broker: envelope handler is required")
	}
	connection, err := dial(address)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	_ = connection.SetDeadline(time.Now().Add(localHandshakeTimeout))
	if err := writeFrame(connection, frame{Version: protocolVersion, Type: "open", NetworkWait: int64(networkWait)}); err != nil {
		connection.Close()
		return nil, err
	}
	opened, err := readFrame(connection)
	if err != nil {
		connection.Close()
		return nil, err
	}
	if opened.Type != "opened" || opened.Version != protocolVersion || opened.Identity == "" {
		connection.Close()
		if opened.Error != "" {
			return nil, errors.New(opened.Error)
		}
		return nil, errors.New("broker: invalid open response")
	}
	bootstrap, err := cluster.NormalizeBootstrapDestinations(opened.Bootstrap)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("broker: invalid bootstrap destinations: %w", err)
	}
	_ = connection.SetDeadline(time.Time{})
	endpoint := &Endpoint{
		connection: connection, identity: opened.Identity, clusterID: opened.ClusterID,
		bootstrap: bootstrap, handler: handler,
		pending: make(map[uint64]chan frame), routes: make(map[string]string),
		discoveries: make(chan rns.Service, 32), done: make(chan struct{}),
	}
	go endpoint.readLoop()
	return endpoint, nil
}

func (e *Endpoint) Name() string { return e.identity }

func (e *Endpoint) ClusterID() string { return e.clusterID }

func (e *Endpoint) BootstrapDestinations() []string {
	return append([]string(nil), e.bootstrap...)
}

func (e *Endpoint) Discoveries() <-chan rns.Service { return e.discoveries }

func (e *Endpoint) DestinationForIdentity(identity string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	destination, ok := e.routes[identity]
	return destination, ok
}

func (e *Endpoint) Start(ctx context.Context) error {
	return e.request(ctx, frame{Type: "start"})
}

func (e *Endpoint) Send(ctx context.Context, target string, envelope *r1sv1.Envelope) error {
	data, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}
	return e.request(ctx, frame{Type: "send", Target: target, Envelope: data})
}

func (e *Endpoint) request(ctx context.Context, message frame) error {
	e.mu.Lock()
	e.nextID++
	message.ID = e.nextID
	response := make(chan frame, 1)
	e.pending[message.ID] = response
	e.mu.Unlock()
	if err := e.write(message); err != nil {
		e.removePending(message.ID)
		return err
	}
	select {
	case <-ctx.Done():
		e.removePending(message.ID)
		return ctx.Err()
	case <-e.done:
		e.removePending(message.ID)
		return ErrUnavailable
	case reply := <-response:
		if reply.Error != "" {
			return errors.New(reply.Error)
		}
		return nil
	}
}

func (e *Endpoint) removePending(id uint64) {
	e.mu.Lock()
	delete(e.pending, id)
	e.mu.Unlock()
}

func (e *Endpoint) write(message frame) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	return writeFrame(e.connection, message)
}

func (e *Endpoint) readLoop() {
	defer e.finish()
	for {
		message, err := readFrame(e.connection)
		if err != nil {
			return
		}
		switch message.Type {
		case "response":
			e.mu.Lock()
			response := e.pending[message.ID]
			delete(e.pending, message.ID)
			e.mu.Unlock()
			if response != nil {
				response <- message
			}
		case "discovery":
			var service rns.Service
			if json.Unmarshal(message.Service, &service) != nil {
				continue
			}
			e.mu.Lock()
			e.routes[service.Identity] = service.Destination
			e.mu.Unlock()
			select {
			case e.discoveries <- service:
			default:
			}
		case "envelope":
			var envelope r1sv1.Envelope
			if proto.Unmarshal(message.Envelope, &envelope) != nil {
				continue
			}
			if message.Destination != "" {
				e.mu.Lock()
				e.routes[hex.EncodeToString(envelope.GetSender())] = message.Destination
				e.mu.Unlock()
			}
			_ = e.handler(context.Background(), &envelope)
		}
	}
}

func (e *Endpoint) finish() {
	e.closeOnce.Do(func() {
		_ = e.connection.Close()
		close(e.done)
	})
}

func (e *Endpoint) Close() error {
	e.finish()
	return nil
}
