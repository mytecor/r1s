package yggdrasil

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mytecor/r1s/internal/tunnel"
)

// Stream framing over the yggdrasil packet interface.
//
// The mesh below ironwood delivers packets for one session in order and
// reliably (per-peer ordered queue, encrypted session delivery to the read
// channel in order), but the API is packet-level: WriteTo rejects messages
// above the packet MTU and ReadFrom returns whole packets. The tunnel contract
// (tunnel.Conn) is a byte stream with per-direction half-close, so the edge
// adapts packets to a stream with typed frames:
//
//	wire format (one frame = one packet):
//	  +------+--------------------+------------------+
//	  | type | len (BE, 2 bytes)  | payload ...      |
//	  +------+--------------------+------------------+
//
//	  type:    0x01 payload, 0x02 write-EOF (empty payload),
//	           0x03 close (payload: reason byte + detail), 0x04 preamble,
//	           0x05 accept (empty payload)
//	  len:     payload length (0..maxPacketPayload)
//	  payload: raw tunnel bytes; no further framing inside
//
// One frame is exactly one packet: the writer segments on the MTU and the
// reader never sees a partial frame, so reassembly is pure concatenation and
// the handshake reads whole frames without a byte-level reassembly state
// machine.

const (
	frameTypeData     byte = 0x01
	frameTypeEOF      byte = 0x02
	frameTypeClose    byte = 0x03
	frameTypePreamble byte = 0x04
	frameTypeAccept   byte = 0x05

	// frameHeaderSize is the 1-byte type + 2-byte big-endian length.
	frameHeaderSize = 3

	// closeDetailMax bounds the teardown detail carried in a close frame.
	closeDetailMax = 512
)

// frame is one decoded wire frame.
type frame struct {
	typ     byte
	payload []byte
}

// frameError distinguishes malformed wire input from transport failure so the
// reader ends the session instead of misreporting it as a mesh failure.
type frameError struct{ reason string }

func (e *frameError) Error() string { return "yggdrasil tunnel frame: " + e.reason }

// appendFrame appends one typed frame (type + length + payload) to dst and
// returns the extended slice. One frame is exactly one packet: the caller must
// keep len(payload) <= maxPacketPayload (payload Write segments; control
// frames are small and fixed).
func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	dst = append(dst, typ)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(payload)))
	return append(dst, payload...)
}

// parseFrame consumes one complete frame from buf. It returns the frame, the
// number of bytes consumed, and whether a complete frame is present. A
// malformed header (unknown type, oversized length) yields an error; an
// incomplete frame yields ok=false without error.
func parseFrame(buf []byte) (frame, int, bool, error) {
	if len(buf) < frameHeaderSize {
		return frame{}, 0, false, nil
	}
	typ := buf[0]
	length := int(binary.BigEndian.Uint16(buf[1:3]))
	if typ < frameTypeData || typ > frameTypeAccept {
		return frame{}, 0, false, &frameError{fmt.Sprintf("unknown frame type 0x%02x", typ)}
	}
	if length > maxPacketPayload {
		return frame{}, 0, false, &frameError{"oversized frame length"}
	}
	if len(buf) < frameHeaderSize+length {
		return frame{}, 0, false, nil
	}
	payload := append([]byte(nil), buf[frameHeaderSize:frameHeaderSize+length]...)
	return frame{typ: typ, payload: payload}, frameHeaderSize + length, true, nil
}

// reasonCodes is the one-byte wire code of a teardown reason. The mapping is
// frozen at v1: unknown codes degrade to ReasonSessionFailed on an older edge
// instead of breaking it.
var reasonCodes = map[tunnel.Reason]byte{
	tunnel.ReasonClosed:          0x01,
	tunnel.ReasonExecutionEnded:  0x02,
	tunnel.ReasonGrantRejected:   0x03,
	tunnel.ReasonGrantExpired:    0x04,
	tunnel.ReasonGrantRevoked:    0x05,
	tunnel.ReasonUnauthorized:    0x06,
	tunnel.ReasonSessionFailed:   0x07,
	tunnel.ReasonMeshUnreachable: 0x08,
}

// reasonFromCode inverts reasonCodes; an unknown code degrades to
// ReasonSessionFailed so a future reason never breaks an older edge.
func reasonFromCode(code byte) tunnel.Reason {
	for reason, wire := range reasonCodes {
		if code == wire {
			return reason
		}
	}
	return tunnel.ReasonSessionFailed
}

// closePayload encodes a classified teardown reason for a close frame: one
// reason byte plus the bounded detail.
func closePayload(reason tunnel.Reason, detail string) []byte {
	if len(detail) > closeDetailMax {
		detail = detail[:closeDetailMax]
	}
	return append([]byte{reasonCode(reason)}, detail...)
}

// parseClosePayload turns a close frame payload into the *tunnel.SessionError
// the peer observes on its next Read.
func parseClosePayload(payload []byte) error {
	if len(payload) == 0 {
		return &tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: "close frame without reason"}
	}
	reason := reasonFromCode(payload[0])
	detail := ""
	if len(payload) > 1 {
		detail = string(payload[1:])
	}
	return &tunnel.SessionError{Reason: reason, Detail: detail}
}

// reasonCode returns the wire code for a reason.
func reasonCode(reason tunnel.Reason) byte {
	if code, ok := reasonCodes[reason]; ok {
		return code
	}
	return reasonCodes[tunnel.ReasonSessionFailed]
}

// preambleWire is the JSON shape of the one-time routing preamble.
type preambleWire struct {
	ExecutionID string `json:"execution_id"`
	GrantID     string `json:"grant_id"`
}

// encodePreamble serializes the routing preamble for the preamble frame. It
// is deliberately self-describing JSON: the preamble is small, sent once per
// session, never on the payload path, and the format stays debuggable.
func encodePreamble(p tunnel.Preamble) []byte {
	encoded, _ := json.Marshal(preambleWire{ExecutionID: p.ExecutionID, GrantID: p.GrantID})
	return encoded
}

// decodePreamble parses the routing preamble from a preamble frame payload.
func decodePreamble(payload []byte) (tunnel.Preamble, error) {
	var p preambleWire
	if err := json.Unmarshal(payload, &p); err != nil {
		return tunnel.Preamble{}, fmt.Errorf("tunnel preamble: %w", err)
	}
	if p.ExecutionID == "" || p.GrantID == "" {
		return tunnel.Preamble{}, errors.New("tunnel preamble: execution and grant IDs are required")
	}
	return tunnel.Preamble{ExecutionID: p.ExecutionID, GrantID: p.GrantID}, nil
}
