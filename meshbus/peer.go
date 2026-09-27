// Generic, transport-independent realm peer discovery.
//
// The entity here is a PeerDirectory, not a Cluster: the realm is a security
// boundary, whereas the directory is only the observable state of the network.
// A discovered peer carries an authenticated public PeerID plus an opaque
// transport route and bounded advisory metadata. Discovery is advisory only —
// presence never grants application authorization, and a route learned through
// discovery is never an authenticated sender identity.
package meshbus

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// defaultDirectoryMaxPeers bounds how many peers one directory remembers.
	defaultDirectoryMaxPeers = 1024
	// defaultMetadataBytes bounds total application metadata carried on peer
	// records, so a peer cannot grow memory without limit.
	defaultMetadataBytes = 4096
)

var (
	// ErrInvalidPeer is returned when a peer record fails validation.
	ErrInvalidPeer = errors.New("invalid discovered peer")
	// ErrPeerLimit is returned when the directory is at capacity.
	ErrPeerLimit = errors.New("peer directory capacity reached")
	// ErrMetadataLimit is returned when application metadata exceeds bounds.
	ErrMetadataLimit = errors.New("peer metadata exceeds bound")
)

// Peer describes one authenticated realm peer learned through discovery. It
// carries only enough information to identify and route to the peer; allocator
// capacity, runtime, tunnel or execution state never belongs here.
type Peer struct {
	// ID is the authenticated public identity established by the transport.
	// It never comes from application metadata on the wire.
	ID PeerID
	// Route is the opaque transport route/address used to reach the peer. A
	// route learned through discovery is not an authenticated sender identity.
	Route string
	// Metadata holds optional bounded application metadata (for example
	// advisory os/arch hints). Presence and metadata are advisory only.
	Metadata map[string]string
	// Hops is an optional advisory path metric such as hop count.
	Hops uint8
	// LastSeen is the local time the peer was last (re)discovered.
	LastSeen time.Time
}

// isValid reports whether the peer carries a valid authenticated identity.
func (p Peer) isValid() bool { return !p.ID.IsZero() }

// metadataBytes returns the total UTF-8 byte size of the metadata map.
func metadataBytes(metadata map[string]string) int {
	total := 0
	for key, value := range metadata {
		total += len(key) + len(value)
	}
	return total
}

// DirectoryConfig sets the finite resource bounds for one peer directory.
type DirectoryConfig struct {
	// MaxPeers bounds the number of remembered peers.
	MaxPeers int
	// MaxMetadataBytes bounds the total application metadata carried on each
	// peer record. Zero uses the default bound.
	MaxMetadataBytes int

	now func() time.Time
}

func (c DirectoryConfig) apply() DirectoryConfig {
	if c.MaxPeers < 1 {
		c.MaxPeers = defaultDirectoryMaxPeers
	}
	if c.MaxMetadataBytes < 1 {
		c.MaxMetadataBytes = defaultMetadataBytes
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c
}

// PeerDirectory is a bounded, observable record of realm peers learned through
// discovery. It remembers peers keyed by authenticated PeerID, updates an
// existing peer, resolves a PeerID to its current transport route, returns a
// bounded immutable snapshot for pub/sub fan-out, and removes or expires stale
// entries. It is safe for concurrent use.
type PeerDirectory struct {
	config DirectoryConfig

	mu    sync.RWMutex
	peers map[PeerID]Peer
}

// NewPeerDirectory creates an empty directory with the given resource bounds.
func NewPeerDirectory(config DirectoryConfig) *PeerDirectory {
	config = config.apply()
	return &PeerDirectory{
		config: config,
		peers:  make(map[PeerID]Peer),
	}
}

// Remember adds or updates one discovered peer. An existing peer with the same
// authenticated PeerID is updated in place instead of creating a duplicate.
// The route may change while the identity stays stable. Returns ErrPeerLimit
// when the directory is at capacity and the identity is new, and
// ErrMetadataLimit when application metadata exceeds the configured bound.
func (d *PeerDirectory) Remember(peer Peer) error {
	if !peer.isValid() {
		return fmt.Errorf("%w: identity is required and must be authenticated", ErrInvalidPeer)
	}
	if peer.Metadata == nil {
		peer.Metadata = map[string]string{}
	}
	if n := metadataBytes(peer.Metadata); n > d.config.MaxMetadataBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrMetadataLimit, n, d.config.MaxMetadataBytes)
	}
	// Snapshot the metadata so the caller cannot mutate the stored record.
	peer.Metadata = cloneMetadata(peer.Metadata)
	peer.LastSeen = d.config.now()

	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.peers[peer.ID]; !exists && len(d.peers) >= d.config.MaxPeers {
		return ErrPeerLimit
	}
	d.peers[peer.ID] = peer
	return nil
}

// Resolve returns the current transport route for an authenticated PeerID. A
// route learned through discovery is not an authenticated sender identity.
func (d *PeerDirectory) Resolve(id PeerID) (route string, ok bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	peer, exists := d.peers[id]
	if !exists {
		return "", false
	}
	return peer.Route, true
}

// Get returns an immutable copy of one discovered peer.
func (d *PeerDirectory) Get(id PeerID) (Peer, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	peer, exists := d.peers[id]
	if !exists {
		return Peer{}, false
	}
	return clonePeer(peer), true
}

// Peers returns an immutable, copy-safe snapshot of the known realm peers,
// ordered by identity for deterministic fan-out. The result is capped at the
// configured MaxPeers. Discovery is advisory only.
func (d *PeerDirectory) Peers() []Peer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make([]Peer, 0, len(d.peers))
	for _, peer := range d.peers {
		result = append(result, clonePeer(peer))
	}
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].ID.Bytes(), result[j].ID.Bytes()) < 0
	})
	return result
}

// Routes returns the bounded, copy-safe snapshot of transport routes used for
// pub/sub fan-out, ordered by identity. This is the PeerSource the Bus consumes.
func (d *PeerDirectory) Routes() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make([]string, 0, len(d.peers))
	for _, peer := range d.peers {
		result = append(result, peer.Route)
	}
	sort.Strings(result)
	return result
}

// Remove forgets one peer. It is a no-op when the peer is unknown.
func (d *PeerDirectory) Remove(id PeerID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.peers, id)
}

// ExpireStale removes peers whose LastSeen is older than the given age. With a
// zero age only zero-valued LastSeen records age out. It updates LastSeen on
// every Remember, so idle peers are what age out.
func (d *PeerDirectory) ExpireStale(olderThan time.Duration) int {
	cutoff := d.config.now().Add(-olderThan)
	d.mu.Lock()
	defer d.mu.Unlock()
	removed := 0
	for id, peer := range d.peers {
		if peer.LastSeen.Before(cutoff) {
			delete(d.peers, id)
			removed++
		}
	}
	return removed
}

// Len returns the number of remembered peers.
func (d *PeerDirectory) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.peers)
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	clone := make(map[string]string, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

func clonePeer(peer Peer) Peer {
	peer.Metadata = cloneMetadata(peer.Metadata)
	return peer
}

// Ensure the directory provides the bounded route snapshot the Bus consumes.
var _ = PeerSourceFunc((*PeerDirectory)(nil).Routes)
