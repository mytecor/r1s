package localserver

import (
	"errors"
	"io"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"github.com/mytecor/r1s/internal/tunnel"
)

// Tunnel implements the LocalTunnel bidirectional stream: it relays raw bytes
// between the CLI and a live execution through the allocator edge. stdout stays
// byte-clean — only tunnel payload flows back — and setup failures (unknown
// execution, missing grant target, mesh unreachable) are returned as gRPC
// errors before a single payload byte is relayed.
//
// Message contract:
//   - The first client->server message MUST be an open naming the execution.
//   - data carries raw payload bytes in either direction.
//   - A close{half:true} is a write half-close: the serve stops writing to the
//     allocator (the CLI's stdin reached EOF) but keeps reading replies.
//   - A close{reason} from server->client ends the client's read direction and
//     reports why the tunnel read side ended (execution ended, rejected,
//     expired, revoked, unauthorized, mesh unreachable, or clean close).
//   - A close{half:false} fully closes the session.
func (s *Server) Tunnel(stream r1sv1.LocalClient_TunnelServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open == nil {
		return errors.New("tunnel: the first message must be an open")
	}
	executionID := open.GetExecutionId()
	if executionID == "" {
		return errors.New("tunnel: execution ID is required")
	}
	targetPort := uint16(open.GetTargetPort())
	targets, err := protocol.ValidateTunnelTargets(open.GetTargets())
	if err != nil {
		return err
	}
	if targetPort == 0 {
		return errors.New("tunnel: a target container port is required")
	}

	// Mint the grant and dial the allocator edge. Any error here is a setup
	// failure surfaced as a gRPC status; the CLI maps it to a CommandError. The
	// backend writes the routing preamble (execution ID + grant ID) onto the
	// edge itself before relaying when the transport supports it; the serve
	// relay only moves raw bytes after that. The client-owned targets are
	// carried through to the tunnel grant request, and the backend opens the
	// stream for the requested container port.
	conn, _, err := s.backend.Tunnel(stream.Context(), executionID, targets, targetPort)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Pump the allocator edge -> serve -> CLI direction. It ends when the read
	// side of the tunnel closes (execution ended, session torn down, etc.) and
	// sends a close message carrying the reason before finishing.
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		s.pumpTunnel(stream, conn)
	}()

	// Main loop: CLI -> serve -> allocator edge. Every teardown path joins the
	// pump goroutine before returning so no stream.Send happens on a stream the
	// handler has already abandoned: conn.Close() unblocks the pump's Read with
	// ErrReadAborted (which the pump treats as a local teardown), and the walk
	// finishes before the gRPC stream is torn down.
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// The client half-closed its write side (stdin EOF): stop
				// writing to the allocator but keep relaying the reply direction
				// until it ends.
				_ = conn.CloseWrite()
				<-pumpDone
				return nil
			}
			// The client disconnected or its context was cancelled; the tunnel
			// ends. The pump wakes on ErrReadAborted and finishes by itself.
			_ = conn.Close()
			<-pumpDone
			return nil
		}
		switch payload := msg.GetPayload().(type) {
		case *r1sv1.LocalTunnelMessage_Data:
			if _, werr := conn.Write(payload.Data); werr != nil {
				_ = conn.Close()
				<-pumpDone
				return werr
			}
		case *r1sv1.LocalTunnelMessage_Close:
			if payload.Close.GetHalf() {
				// Client half-closes its write (stdin EOF). Stop writing to the
				// allocator but keep reading replies.
				if err := conn.CloseWrite(); err != nil {
					_ = conn.Close()
					<-pumpDone
					return err
				}
				continue
			}
			// Client fully closes the session.
			_ = conn.Close()
			<-pumpDone
			return nil
		}
	}
}

// pumpTunnel copies bytes from the tunnel Conn to the gRPC stream until the
// tunnel's read side ends, then sends a close message carrying the teardown
// reason. It is run in its own goroutine so reads never block the CLI->tunnel
// direction.
func (s *Server) pumpTunnel(stream r1sv1.LocalClient_TunnelServer, conn tunnel.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if serr := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Data{Data: data}}); serr != nil {
				return
			}
		}
		if err != nil {
			// End the client's read direction and report why.
			reason := tunnel.ReasonClosed
			detail := ""
			var sessionErr *tunnel.SessionError
			switch {
			case errors.As(err, &sessionErr):
				// A classified teardown carries both its reason and detail.
				reason = sessionErr.Reason
				detail = sessionErr.Detail
			case errors.Is(err, io.EOF):
				// A clean mutual close.
			case errors.Is(err, tunnel.ErrReadAborted):
				// The read side was torn down locally (the client or the relay
				// closed the tunnel, not a transport failure). The client is
				// already gone, so sending a close reason is pointless.
				return
			default:
				// A non-EOF, non-classified stream error is not a clean close and
				// is not a grant rejection: it is a generic mid-session failure.
				reason = tunnel.ReasonSessionFailed
			}
			_ = stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Close{Close: &r1sv1.LocalTunnelClose{Reason: string(reason), Detail: detail}}})
			return
		}
	}
}
