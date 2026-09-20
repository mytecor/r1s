// Package yggdrasil holds the F14-02 tunnel edge(s) on top of the Yggdrasil
// overlay. It is the ONLY package in the tree that may import yggdrasil-go
// (and its ironwood dependency): the core, the protocol, the local API, and
// the transport adapter build and run without it.
//
// Process topology: each process runs an embedded yggdrasil-go node; there is
// no host-level Yggdrasil daemon and no separate r1s-tunneld binary. The
// allocator edge (a Listener) and the client edge (a Dialer) both live in this
// package, sharing the generic stream contract from internal/tunnel. The mesh
// below ironwood delivers packets ordered and reliable; the stream adapter
// (stream.go) turns packets into the tunnel byte stream.
package yggdrasil

import (
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/sha256"
	"fmt"

	"github.com/yggdrasil-network/yggdrasil-go/src/core"
)

// NodeKeyContext selects the HKDF derivation domain for a node key. Client and
// allocator edges never share a key even when they share a seed, because the
// info string differs; a stable seed means a stable overlay identity, so
// grants and sessions survive reconnects within their TTL and re-requesting
// after a restart reuses the same key (F14-01). No key files are created,
// backed up, or rotated.
type NodeKeyContext string

const (
	// ClientNodeKeyContext derives the client bridge's overlay node key.
	ClientNodeKeyContext NodeKeyContext = "r1s-tunnel-node-key-v1:client-edge"
	// AllocatorNodeKeyContext derives the allocator edge's overlay node key.
	AllocatorNodeKeyContext NodeKeyContext = "r1s-tunnel-node-key-v1:allocator-edge"
)

// NodePublicKeySize is the size of a derived node public key. Yggdrasil node
// public keys are Curve25519/ed25519-sized (32 bytes); the field bound in the
// transport-neutral grant contract is internal/tunnel.MaxPeerKeySize.
const NodePublicKeySize = ed25519.PublicKeySize

// NodeKeyFromSeed derives the ed25519.PrivateKey for a node key context from a
// persistent identity seed. The seed is fed through HKDF with a domain-separated
// info string, and the 32-byte output is the ed25519 seed. Deterministic: the
// same seed and context always yield the same node private key, public key, and
// overlay address.
func NodeKeyFromSeed(identitySeed []byte, context NodeKeyContext) (ed25519.PrivateKey, error) {
	if len(identitySeed) == 0 {
		return nil, fmt.Errorf("node key derivation: identity seed is required")
	}
	seed, err := hkdf.Key(sha256.New, identitySeed, nil, string(context), ed25519.SeedSize)
	if err != nil {
		return nil, fmt.Errorf("node key derivation: %w", err)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

// NodePubKey returns the derived node public key for the given context.
func NodePubKey(identitySeed []byte, context NodeKeyContext) (ed25519.PublicKey, error) {
	key, err := NodeKeyFromSeed(identitySeed, context)
	if err != nil {
		return nil, err
	}
	return key.Public().(ed25519.PublicKey), nil
}

// Node is the process's embedded yggdrasil node. The key is HKDF-derived from
// the persistent identity seed (no key files to create, back up, or rotate), a
// stable key means a stable overlay address, and the Core is used as a
// net.PacketConn: the mesh supplies an encrypted, ordered, reliable packet
// path, and the stream adapter (stream.go) turns packets into the tunnel byte
// stream. A host-level Yggdrasil daemon may run independently on the same
// machine; the r1s node is a separate overlay identity and never touches a
// host tun device.
type Node struct {
	context NodeKeyContext
	key     ed25519.PrivateKey
	core    *core.Core
}

// NodeOptions tunes the embedded node at construction.
type NodeOptions struct {
	// Peers are the bootstrap peer URIs ("tcp://host:port"). Empty joins the
	// standard public Yggdrasil overlay; private meshes set their own set.
	// Peering is edge configuration, never a protocol feature.
	Peers []string
	// LogSink receives node diagnostics; nil discards them.
	LogSink Logger
}

// NewNode derives a node for a context from a persistent identity seed and
// starts the embedded mesh node. The derived key is fed to the Core as an
// ed25519 TLS certificate (yggdrasil-go's node identity), so the overlay
// address is a pure function of the persistent identity.
func NewNode(identitySeed []byte, context NodeKeyContext, options NodeOptions) (*Node, error) {
	key, err := NodeKeyFromSeed(identitySeed, context)
	if err != nil {
		return nil, err
	}
	nodeCore, err := startCore(key, options)
	if err != nil {
		return nil, err
	}
	return &Node{context: context, key: key, core: nodeCore}, nil
}

// PrivateKey returns the derived node private key.
func (n *Node) PrivateKey() ed25519.PrivateKey { return n.key }

// PublicKey returns the derived node public key.
func (n *Node) PublicKey() ed25519.PublicKey { return n.key.Public().(ed25519.PublicKey) }

// AddressBytes returns the node's overlay address as opaque bytes: ironwood's
// packet-layer peer address is the full ed25519 public key, and the
// transport-neutral Endpoint.Address carries the same bytes everywhere, so the
// client dials the same key the allocator accepts at.
func (n *Node) AddressBytes() []byte {
	if n.core == nil {
		return nodeAddress(n.key.Public().(ed25519.PublicKey))
	}
	return append([]byte(nil), n.core.PublicKey()...)
}

// PacketIO exposes the node's packet interface for the edge machinery. The
// Core is the net.PacketConn the mux and the stream adapter run over.
func (n *Node) PacketIO() packetIO { return n.core }

// Core returns the embedded node's core for peering (edge configuration and
// tests only: peers join the node, never sessions).
func (n *Node) Core() *core.Core { return n.core }

// Close stops the embedded node.
func (n *Node) Close() error {
	if n.core != nil {
		n.core.Stop()
		n.core = nil
	}
	return nil
}

// nodeAddress returns the node's overlay address used at the yggdrasil packet
// layer. Yggdrasil's ironwood encryption layer uses the full ed25519 public
// key (32 bytes) as the peer address in ReadFrom/WriteTo; the 16-byte ygg
// IPv6 address is a higher-level application-layer address derived from the
// same key. The transport-neutral Endpoint.Address field carries the same
// bytes everywhere, so the client dials the same key the allocator accepts at.
func nodeAddress(publicKey ed25519.PublicKey) []byte {
	return append([]byte(nil), publicKey...)
}
