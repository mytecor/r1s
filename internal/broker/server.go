// Package broker provides the local cluster-authority broker used by
// `r1s cluster use`. The broker owns cluster credentials and one fresh RNS
// endpoint per connected run controller; it never owns run state, leases, or
// rescheduling.
package broker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	meshbus "github.com/mytecor/meshbus/core"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
	"google.golang.org/protobuf/proto"
)

type endpoint interface {
	Start(context.Context) error
	Send(context.Context, string, *r1sv1.Envelope) error
	SendPeer(context.Context, meshbus.PeerID, *r1sv1.Envelope) error
	Close() error
	Name() string
	Discoveries() <-chan rns.Service
	Services() []rns.Service
}

type endpointFactory func([]byte, time.Duration, func(context.Context, *r1sv1.Envelope) error) (endpoint, error)

func newRNSEndpoint(key []byte, networkWait time.Duration, handler func(context.Context, *r1sv1.Envelope) error) (endpoint, error) {
	return rns.New(rns.Config{EphemeralIdentity: true, ClusterKey: key, NetworkWait: networkWait}, handler)
}

// Server is one selected-cluster authority broker. Each accepted open request
// gets an independent ephemeral transport identity; disconnecting the socket
// closes only that endpoint and does not make transport state an execution
// lifetime signal.
type Server struct {
	ClusterID             string
	ClusterKey            []byte
	BootstrapDestinations []string
	Ready                 func()

	newEndpoint endpointFactory
	mu          sync.Mutex
	listener    net.Listener
	cancel      context.CancelFunc
}

// Serve listens until ctx is canceled or a local shutdown request is received.
func (s *Server) Serve(ctx context.Context, address string) error {
	derived, err := cluster.ID(s.ClusterKey)
	if err != nil || s.ClusterID != hex.EncodeToString(derived) {
		return errors.New("broker: cluster identity does not match its key")
	}
	bootstrap, err := cluster.NormalizeBootstrapDestinations(s.BootstrapDestinations)
	if err != nil {
		return fmt.Errorf("broker: invalid bootstrap destinations: %w", err)
	}
	s.BootstrapDestinations = bootstrap
	listener, err := listen(address)
	if err != nil {
		return fmt.Errorf("broker: listen: %w", err)
	}
	defer removeAddress(address)
	serveCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.listener, s.cancel = listener, cancel
	s.mu.Unlock()
	if s.Ready != nil {
		s.Ready()
	}
	defer func() {
		cancel()
		_ = listener.Close()
		s.mu.Lock()
		s.listener, s.cancel = nil, nil
		s.mu.Unlock()
	}()
	go func() {
		<-serveCtx.Done()
		_ = listener.Close()
	}()
	for {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			if serveCtx.Err() != nil || errors.Is(acceptErr, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("broker: accept: %w", acceptErr)
		}
		go s.serveConnection(serveCtx, connection)
	}
}

func (s *Server) serveConnection(ctx context.Context, connection net.Conn) {
	defer connection.Close()
	first, err := readFrame(connection)
	if err != nil {
		return
	}
	if first.Version != protocolVersion {
		_ = writeFrame(connection, frame{Version: protocolVersion, Type: "error", Error: "unsupported broker protocol version"})
		return
	}
	switch first.Type {
	case "status":
		_ = writeFrame(connection, frame{Version: protocolVersion, Type: "status", ClusterID: s.ClusterID})
	case "shutdown":
		_ = writeFrame(connection, frame{Version: protocolVersion, Type: "shutdown", ClusterID: s.ClusterID})
		s.mu.Lock()
		cancel := s.cancel
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	case "open":
		s.serveEndpoint(ctx, connection, first)
	default:
		_ = writeFrame(connection, frame{Type: "error", Error: "unsupported broker request"})
	}
}

func (s *Server) serveEndpoint(ctx context.Context, connection net.Conn, first frame) {
	factory := s.newEndpoint
	if factory == nil {
		factory = newRNSEndpoint
	}
	writer := &lockedWriter{writer: connection}
	var active endpoint
	handler := func(_ context.Context, envelope *r1sv1.Envelope) error {
		data, err := proto.Marshal(envelope)
		if err != nil {
			return err
		}
		return writer.write(frame{Type: "envelope", Envelope: data})
	}
	created, err := factory(s.ClusterKey, time.Duration(first.NetworkWait), handler)
	if err != nil {
		_ = writer.write(frame{Type: "error", Error: err.Error()})
		return
	}
	active = created
	defer active.Close()
	if err := writer.write(frame{
		Version: protocolVersion, Type: "opened", ClusterID: s.ClusterID,
		Identity: active.Name(), Bootstrap: s.BootstrapDestinations,
	}); err != nil {
		return
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sends := make(chan struct{}, 64)
	for {
		request, err := readFrame(connection)
		if err != nil {
			return
		}
		switch request.Type {
		case "start":
			err = active.Start(sessionCtx)
			if err == nil {
				go forwardDiscoveries(sessionCtx, active, writer)
			}
		case "send", "send-peer":
			select {
			case sends <- struct{}{}:
				go func(request frame) {
					defer func() { <-sends }()
					sendErr := sendEnvelope(sessionCtx, active, time.Duration(first.NetworkWait), request)
					response := frame{Type: "response", ID: request.ID}
					if sendErr != nil {
						response.Error = sendErr.Error()
					}
					if writer.write(response) != nil {
						_ = connection.Close()
					}
				}(request)
				continue
			default:
				err = errors.New("broker: too many pending sends")
			}

		case "close":
			_ = writer.write(frame{Type: "response", ID: request.ID})
			return
		default:
			err = fmt.Errorf("unsupported endpoint request %q", request.Type)
		}
		response := frame{Type: "response", ID: request.ID}
		if err != nil {
			response.Error = err.Error()
		}
		if writeErr := writer.write(response); writeErr != nil {
			return
		}
	}
}

func sendEnvelope(ctx context.Context, active endpoint, timeout time.Duration, request frame) error {
	var envelope r1sv1.Envelope
	if err := proto.Unmarshal(request.Envelope, &envelope); err != nil {
		return err
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if request.Type == "send" {
		return active.Send(ctx, request.Target, &envelope)
	}
	identity, err := hex.DecodeString(request.Target)
	if err != nil {
		return err
	}
	peer, err := meshbus.NewPeerID(identity)
	if err != nil {
		return err
	}
	return active.SendPeer(ctx, peer, &envelope)
}

func forwardDiscoveries(ctx context.Context, active endpoint, writer *lockedWriter) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var previous string
	for {
		data, err := json.Marshal(active.Services())
		if err != nil {
			return
		}
		if string(data) != previous {
			if writer.write(frame{Type: "services", Service: data}) != nil {
				return
			}
			previous = string(data)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-active.Discoveries():
		}
	}
}

type lockedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (w *lockedWriter) write(message frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return writeFrame(w.writer, message)
}

// Status returns the selected cluster ID of a running broker.
func Status(address string) (string, error) {
	response, err := requestControl(address, "status")
	if err != nil {
		return "", err
	}
	return response.ClusterID, nil
}

// Shutdown asks the current broker to stop accepting authority sessions.
func Shutdown(address string) error {
	_, err := requestControl(address, "shutdown")
	return err
}

func requestControl(address, requestType string) (frame, error) {
	connection, err := dial(address)
	if err != nil {
		return frame{}, err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(localHandshakeTimeout))
	if err := writeFrame(connection, frame{Version: protocolVersion, Type: requestType}); err != nil {
		return frame{}, err
	}
	response, err := readFrame(connection)
	if err != nil {
		return frame{}, err
	}
	if response.Error != "" {
		return frame{}, errors.New(response.Error)
	}
	if response.Version != protocolVersion || response.Type != requestType || response.ClusterID == "" {
		return frame{}, errors.New("invalid broker control response")
	}
	return response, nil
}
