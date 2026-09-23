package main

import (
	"context"
	"fmt"
	"io"
	"net"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
)

// relayPort forwards bytes between an accepted local TCP connection and one
// tunnel stream to a container port, until either side closes. Each connection
// is independent: a browser can open many connections to one host port, and
// each is a separate multiplexed stream on the shared authenticated pair.
//
// This is the low-level F19 client-edge byte pump (the `r1s tunnel` data
// plane). It is deliberately decoupled from CLI parsing and port-mapping
// presentation (tunnel.go) so the I/O semantics — send data frames, propagate
// half-close, translate a Close frame into a local socket close — are testable
// and reusable without touching the command surface.
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
