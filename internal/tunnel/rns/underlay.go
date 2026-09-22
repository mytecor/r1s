package rns

import (
	"sync"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/ifac"
)

// This file works around two Reticulum-Go v1.2.0 defects in its IFAC
// machinery that make the stock Backbone/TCP interface impossible to use
// directly for the private tunnel underlay:
//
//  1. handleInboundPacket applies its multi-hop PLAIN/GROUP filter to the raw
//     packet bytes before preprocessInboundPacket unmasks the IFAC, so an
//     IFAC-masked packet (whose scrambled header byte and hop count parse as a
//     bogus multi-hop PLAIN packet) is dropped before it can be unmasked.
//  2. preprocessInboundPacket re-applies ApplyIFACInbound on every inbound
//     packet; because Unmask clears the IFAC flag, an interface that already
//     unmasked (deferInboundIFAC=false) has its packet dropped as "IFAC
//     configured but flag cleared".
//
// Together they make it impossible to satisfy both the multi-hop filter (needs
// unmasked bytes) and ApplyIFACInbound (needs masked bytes with the flag) at
// the same time. The fix is to take the cluster-wire protection entirely out
// of Reticulum's IFAC pipeline: the tunnel package wraps each concrete
// Backbone interface in an underlay that reports GetIFAC() == nil to the
// transport (so ApplyIFACInbound is a passthrough and the filter always sees
// unmasked bytes), and performs cluster masking/unmasking itself at the wire
// boundary.

// wireCipher owns the cluster-shared interface cipher used to protect the
// private tunnel underlay. It is an Interface Access Code identity derived
// from the cluster secret (network name + HKDF passphrase), exactly as the
// plan specifies, but it is applied directly to whole frames instead of being
// registered with Reticulum's IFAC machinery. Both edges derive the same key,
// so only cluster members can form the underlay: a non-member cannot produce
// masked frames that verify.
//
// ifac.Identity.Mask/Unmask share an internal scratch buffer and are not
// goroutine-safe, so the cipher serialises them with a mutex (the transport
// may issue announces and link data concurrently).
type wireCipher struct {
	mu sync.Mutex
	id *ifac.Identity
}

// newWireCipher builds the cluster wire cipher from the derived passphrase.
func newWireCipher(passphrase []byte) (*wireCipher, error) {
	id, err := ifac.New(ifac.DefaultSize, ifacNetworkName, string(passphrase))
	if err != nil {
		return nil, err
	}
	return &wireCipher{id: id}, nil
}

// mask protects an outbound clear packet for the wire. It is the plain IFAC
// Mask: sets the IFAC flag, appends the authenticated signature, and XOR-masks
// the payload. The wire receiver unmasks with the same cluster key.
func (c *wireCipher) mask(clear []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id.Mask(clear)
}

// unmask authenticates and recovers the clear packet from a wire frame. The
// boolean is false when the frame does not carry the IFAC flag or does not
// verify under the cluster key (a non-member, corrupted, or forged frame); the
// caller must drop it. The explicit flag check makes the underlay strict: an
// un-attested plain frame is never accepted, only frames this cipher masked.
func (c *wireCipher) unmask(frame []byte) ([]byte, bool) {
	if len(frame) < 2 || frame[0]&0x80 == 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	clear, ok, err := c.id.Unmask(frame)
	if err != nil || !ok {
		return nil, false
	}
	return clear, true
}

// underlay adapts a concrete Reticulum Backbone interface (server listener,
// spawned per-connection client, or dialer client) to the tunnel transport.
// It embeds the concrete interface so the transport sees a real pointer (the
// abstract base types are rejected by RegisterInterface), delegates all
// lifecycle and stat accessors, and overrides the handful of methods where the
// cluster wire protection and the IFAC policy come into play:
//
//   - GetIFAC returns nil so the transport's preprocessInboundPacket is a
//     passthrough (fixing the double-unmask drop).
//   - Send (and the frame-level SendPathRequest/SendLinkPacket) mask with the
//     cluster cipher before handing bytes to the real interface.
//   - Inbound frames are unmasked by bindClientInbound, which overrides the
//     concrete client's packet callback so Reticulum's own inbound IFAC check
//     (which would drop the masked frame) is bypassed for the spawned and
//     dialer clients.
type underlay struct {
	common.NetworkInterface // embedded: lifecycle, state, and stat accessors
	cipher                  *wireCipher

	mu sync.RWMutex
	cb common.PacketCallback
}

var _ common.NetworkInterface = (*underlay)(nil)

// newUnderlay wraps a concrete backbone interface with the cluster cipher.
func newUnderlay(real common.NetworkInterface, cipher *wireCipher) *underlay {
	return &underlay{NetworkInterface: real, cipher: cipher}
}

// GetIFAC reports nil so Reticulum's transport treats every inbound packet as
// an unprotected clear packet. The cluster protection is applied by the
// underlay itself; Reticulum's IFAC machinery is never involved.
func (u *underlay) GetIFAC() common.IFAC { return nil }

// SetIFAC swallows IFAC assignments from config plumbing: the underlay carries
// its cluster cipher in u.cipher, never in Reticulum's IFACIdentity field.
func (u *underlay) SetIFAC(common.IFAC) {}

// Send masks the clear packet with the cluster cipher and transmits through
// the concrete interface. The pass-through assures the transport's
// sendOnInterface/IsEnabled/IsOnline gate still applies before reaching here.
func (u *underlay) Send(data []byte, address string) error {
	masked, err := u.cipher.mask(data)
	if err != nil {
		return err
	}
	return u.NetworkInterface.Send(masked, address)
}

// SetPacketCallback records the transport callback. It is the callback the
// transport registered for this underlay; bindClientInbound routes the
// unmasked inbound frames back to it.
func (u *underlay) SetPacketCallback(cb common.PacketCallback) {
	u.mu.Lock()
	u.cb = cb
	u.mu.Unlock()
}

// GetPacketCallback returns the transport callback recorded by
// SetPacketCallback.
func (u *underlay) GetPacketCallback() common.PacketCallback {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.cb
}

// sendPacketCallback delivers an unmasked inbound frame to the transport
// callback on this underlay. It is the terminal hop of the inbound path.
func (u *underlay) sendPacketCallback(clear []byte) {
	u.mu.RLock()
	cb := u.cb
	u.mu.RUnlock()
	if cb != nil {
		cb(clear, u)
	}
}

// bindClientInbound rewires a concrete Backbone client interface's inbound
// path so cluster frames are unmasked by the underlay instead of being dropped
// by Reticulum's own IFAC policy. The concrete client's onFrame handler calls
// ProcessIncomingFrom, which with deferInboundIFAC=true (set here) skips
// ApplyIFACInbound and hands the still-masked frame to its packet callback;
// the callback is replaced with an unmask-and-forward that passes the clear
// frame to the underlay's transport callback. It must be called after the
// underlay is registered with the transport (so its callback is installed).
func bindClientInbound(client common.NetworkInterface, u *underlay) {
	if deferrer, ok := client.(interface{ SetDeferInboundIFAC(bool) }); ok {
		deferrer.SetDeferInboundIFAC(true)
	}
	client.SetPacketCallback(func(masked []byte, _ common.NetworkInterface) {
		clear, ok := u.cipher.unmask(masked)
		if !ok {
			return
		}
		u.sendPacketCallback(clear)
	})
}
