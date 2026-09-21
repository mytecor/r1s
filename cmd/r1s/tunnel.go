package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
	"golang.org/x/term"
)

// parseTargetSlot parses one "name@host:port" or bare "host:port" destination
// slot from the CLI. An empty name makes it the unnamed default slot (the
// interactive pipe). Parsing validates the host:port shape with
// net.SplitHostPort so a malformed address is rejected before the client
// sends it to the allocator.
func parseTargetSlot(raw string) (tunnel.Target, error) {
	name := ""
	hostPort := strings.TrimSpace(raw)
	if at := strings.LastIndex(hostPort, "@"); at >= 0 {
		name = strings.TrimSpace(hostPort[:at])
		hostPort = strings.TrimSpace(hostPort[at+1:])
		// An explicit but empty name ("@host:port") is an error, not an unnamed
		// default slot: silently mapping it to the interactive pipe would hide a
		// typo the user meant as a named slot.
		if name == "" {
			return tunnel.Target{}, fmt.Errorf("expected name@host:port")
		}
	}
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return tunnel.Target{}, fmt.Errorf("expected host:port (name@host:port for a named slot)")
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return tunnel.Target{}, fmt.Errorf("host is required")
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return tunnel.Target{}, fmt.Errorf("port must be a positive uint16")
	}
	return tunnel.Target{ID: name, Host: host, Port: uint16(port)}, nil
}

// tunnel is commandHandler-compatible: service-backed mode relays stdin/stdout
// over the LocalTunnel stream; direct mode rejects the session with a clear
// CommandError because only a persistent 'r1s serve' holds the F17 keep-alive
// intent that keeps the execution alive for the duration of the session.
func (l *localCLI) tunnel(args []string, stderr io.Writer) error {
	// The tunnel command has two flag surfaces from F20-01: --target-slot
	// (repeatable, supplies the client-owned destination list) and --target
	// (the slot selector kept from F19-01). The usage shape is:
	//   r1s tunnel <id> [--target-slot <name@host:port>] [--target <name>]
	// The execution ID is the first non-flag token. The stdlib flag package
	// stops at the first positional, so borrow interspersed parsing here.
	var targetSlot string
	seenTarget := false
	var targetSlots []string
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--target" || a == "-target":
			if seenTarget || i+1 >= len(args) || strings.TrimSpace(args[i+1]) == "" {
				return errors.New("tunnel: --target requires a slot name")
			}
			targetSlot = strings.TrimSpace(args[i+1])
			seenTarget = true
			i++
		case strings.HasPrefix(a, "--target=") || strings.HasPrefix(a, "-target="):
			if seenTarget {
				return errors.New("tunnel: --target given more than once")
			}
			targetSlot = strings.TrimPrefix(strings.TrimPrefix(a, "--target="), "-target=")
			targetSlot = strings.TrimSpace(targetSlot)
			if targetSlot == "" {
				return errors.New("tunnel: --target requires a slot name")
			}
			seenTarget = true
		case a == "--target-slot" || a == "-target-slot":
			if i+1 >= len(args) || strings.TrimSpace(args[i+1]) == "" {
				return errors.New("tunnel: --target-slot requires a name@host:port or host:port")
			}
			targetSlots = append(targetSlots, strings.TrimSpace(args[i+1]))
			i++
		case strings.HasPrefix(a, "--target-slot=") || strings.HasPrefix(a, "-target-slot="):
			slot := strings.TrimPrefix(strings.TrimPrefix(a, "--target-slot="), "-target-slot=")
			targetSlots = append(targetSlots, strings.TrimSpace(slot))
		case a == "--help" || a == "-h":
			fmt.Fprintf(stderr, "Usage: r1s tunnel <execution-id> [--target-slot <name@host:port>] [--target <slot>]")
			fmt.Fprintf(stderr, "\n\n  --target-slot <addr>  add a client-owned destination slot (repeatable);")
			fmt.Fprintf(stderr, "\n                       use name@host:port for a named slot, or host:port for the default\n")
			fmt.Fprintf(stderr, "  --target <slot>       open the named slot (must be in --target-slot list);\n")
			fmt.Fprintf(stderr, "                       omit for the interactive pipe to the default slot\n")
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
	if len(targetSlots) == 0 {
		return errors.New("tunnel: at least one --target-slot is required")
	}

	// Parse the destination slots the client owns.
	targets := make([]tunnel.Target, 0, len(targetSlots))
	for _, raw := range targetSlots {
		target, err := parseTargetSlot(raw)
		if err != nil {
			return fmt.Errorf("tunnel: --target-slot %q: %w", raw, err)
		}
		targets = append(targets, target)
	}

	// A --target name must be one of the client-supplied destinations. Reject
	// an unknown name here (fail-fast) instead of after a network round-trip,
	// when the edge would refuse the stream with ReasonUnauthorized.
	if targetSlot != "" {
		found := false
		for _, t := range targets {
			if t.ID == targetSlot {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("tunnel: --target %q is not in the --target-slot list", targetSlot)
		}
	}

	ctx, cancel := context.WithCancel(l.ctx)
	defer cancel()

	// Open the bidirectional LocalTunnel stream. A setup failure surfaces here
	// as a gRPC error before any payload is relayed; stdout stays byte-clean.
	stream, err := l.client.Tunnel(ctx, executionID, targets, targetSlot)
	if err != nil {
		return fmt.Errorf("tunnel: %w", err)
	}

	// Raw mode for interactive protocols (SSH and similar): keystrokes are
	// delivered as bytes, the terminal is restored on exit, and stdout is the
	// data channel so only tunnel bytes travel through it. A failure to enter
	// raw mode is reported rather than silently ignored.
	restore, raw := rawTerminal(l.stdin, stderr)
	if raw {
		defer restore()
	}

	// stdin -> serve. On stdin EOF the write half-close is signaled via
	// CloseSend; the read direction stays live so replies keep flowing until
	// the session ends.
	//
	// sendMu serialises the send direction: a gRPC stream does not allow
	// CloseSend concurrently with SendMsg, and both the send goroutine and the
	// receive loop below issue send-direction operations, so every Send and
	// CloseSend must happen under the same lock. The goroutine itself is not
	// interrupted (a raw-mode terminal read cannot be cancelled), so its only
	// exit point is stdin EOF, a Send failure, or a latched CloseSend from the
	// receive loop; the process lifetime bounds any residual block on stdin.
	var sendMu sync.Mutex
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		buf := make([]byte, 32*1024)
		for {
			n, rerr := l.stdin.Read(buf)
			if n > 0 {
				sendMu.Lock()
				serr := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Data{Data: append([]byte(nil), buf[:n]...)}})
				sendMu.Unlock()
				if serr != nil {
					return
				}
			}
			if rerr != nil {
				sendMu.Lock()
				_ = stream.CloseSend()
				sendMu.Unlock()
				return
			}
		}
	}()
	// closeSend latches a half-close of the send direction from this (receive)
	// goroutine. More than one CloseSend is harmless but must still be locked,
	// so every caller funnels through here.
	closeSend := func() {
		sendMu.Lock()
		_ = stream.CloseSend()
		sendMu.Unlock()
	}

	// serve -> stdout until the session's read direction ends.
	reason := tunnel.ReasonClosed
	detail := ""
	var recvErr error
readLoop:
	for {
		msg, rerr := stream.Recv()
		if rerr != nil {
			recvErr = rerr
			break
		}
		switch payload := msg.GetPayload().(type) {
		case *r1sv1.LocalTunnelMessage_Data:
			if _, werr := l.stdout.Write(payload.Data); werr != nil {
				return fmt.Errorf("tunnel: write stdout: %w", werr)
			}
		case *r1sv1.LocalTunnelMessage_Close:
			reason = tunnel.Reason(payload.Close.GetReason())
			detail = payload.Close.GetDetail()
			if payload.Close.GetHalf() {
				// The serve half-closed its write-to-client direction: no more
				// replies, but the client keeps its send direction open until stdin
				// EOF, then this side finishes. Wait for the send goroutine (and
				// yield to cancellation) so everything the user typed is relayed
				// before exiting.
				select {
				case <-sendDone:
				case <-ctx.Done():
				}
				break readLoop
			}
			// A full close ends the session: stop both directions. The send
			// goroutine may be blocked on a terminal read that cannot be
			// interrupted, so it is left to end with the process; cancelling the
			// context makes any in-flight Send fail so it does not linger on the
			// stream.
			closeSend()
			cancel()
			break readLoop
		}
	}
	closeSend()
	cancel()

	if recvErr != nil && !errors.Is(recvErr, io.EOF) && !errors.Is(recvErr, context.Canceled) {
		return fmt.Errorf("tunnel: %w", recvErr)
	}
	if !reason.Normal() {
		message := "tunnel: session ended: " + string(reason)
		if detail != "" {
			message += ": " + detail
		}
		return errors.New(message)
	}
	return nil
}

// tunnel is rejected in direct mode: a direct run would let the execution's
// lease lapse (default 10 minutes) mid-session because only a serve process
// holds the F17 keep-alive intent that renews it.
func (a *application) tunnel(args []string, stderr io.Writer) error {
	return errors.New("tunnel: direct mode is not supported; start 'r1s serve' so its socket is discovered, then run 'r1s tunnel <executionID>'")
}

// rawTerminal switches stdin to raw mode for interactive sessions and returns a
// restore function plus whether raw mode is active. It is a no-op when stdin is
// not a terminal (for example piped input), so the tunnel still works for
// scripted use. A failure to enter raw mode is reported on stderr.
func rawTerminal(input *os.File, stderr io.Writer) (restore func(), raw bool) {
	if input == nil || !term.IsTerminal(int(input.Fd())) {
		return func() {}, false
	}
	state, err := term.MakeRaw(int(input.Fd()))
	if err != nil {
		fmt.Fprintf(stderr, "tunnel: failed to enter raw mode: %v\n", err)
		return func() {}, false
	}
	return func() { _ = term.Restore(int(input.Fd()), state) }, true
}
