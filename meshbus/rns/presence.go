package rns

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Presence wire constants. The generic presence descriptor is versioned
// independently from any application protocol (F24-05): it carries only what
// realm discovery needs, never events, subscriptions or application logic.
const (
	genericProtocolVersion = "meshbus.v1"
	maxPresenceBytes       = 256
	// maxPresenceKeys bounds the advisory metadata a peer may carry in one
	// announce so a hostile peer cannot grow memory without bound.
	maxPresenceKeys = 16
)

var (
	// ErrInvalidPresence is returned when announce app_data is malformed,
	// oversized, or describes a foreign realm.
	ErrInvalidPresence = errors.New("invalid meshbus presence descriptor")
	// ErrRealmMismatch reports an announce that names a different realm.
	ErrRealmMismatch = errors.New("announce names a different realm")
)

// Presence is the small bounded announcement descriptor used by new meshbus
// applications. It names the announcing peer's realm (the discovery security
// boundary) and carries bounded advisory metadata. It never carries events,
// subscriptions, or bulk data.
type Presence struct {
	// Protocol is the wire version marker ("meshbus.v1").
	Protocol string `json:"p"`
	// Realm is the hex-encoded realm identifier the announcing peer belongs to.
	Realm string `json:"realm"`
	// Metadata is an optional bounded advisory map (for example os/arch).
	Metadata map[string]string `json:"meta,omitempty"`
}

// marshal encodes the presence descriptor within the announce app-data budget.
func (p Presence) marshal() ([]byte, error) {
	if strings.TrimSpace(p.Protocol) == "" {
		p.Protocol = genericProtocolVersion
	}
	if p.Metadata == nil {
		p.Metadata = map[string]string{}
	}
	if len(p.Metadata) > maxPresenceKeys {
		return nil, fmt.Errorf("%w: %d metadata keys exceed %d", ErrInvalidPresence, len(p.Metadata), maxPresenceKeys)
	}
	data, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPresence, err)
	}
	if len(data) > maxPresenceBytes {
		return nil, fmt.Errorf("%w: encoded size %d exceeds %d bytes", ErrInvalidPresence, len(data), maxPresenceBytes)
	}
	return data, nil
}

// parsePresence decodes and validates an inbound generic presence descriptor.
// It returns the presence together with its decoded realm id.
func parsePresence(data []byte) (Presence, []byte, error) {
	if len(data) == 0 || len(data) > maxPresenceBytes {
		return Presence{}, nil, fmt.Errorf("%w: encoded size must be between 1 and %d bytes", ErrInvalidPresence, maxPresenceBytes)
	}
	var presence Presence
	if err := json.Unmarshal(data, &presence); err != nil {
		return Presence{}, nil, fmt.Errorf("%w: %v", ErrInvalidPresence, err)
	}
	if presence.Protocol != genericProtocolVersion {
		return Presence{}, nil, fmt.Errorf("%w: unsupported protocol %q", ErrInvalidPresence, presence.Protocol)
	}
	if len(presence.Metadata) > maxPresenceKeys {
		return Presence{}, nil, fmt.Errorf("%w: %d metadata keys exceed %d", ErrInvalidPresence, len(presence.Metadata), maxPresenceKeys)
	}
	if presence.Metadata == nil {
		presence.Metadata = map[string]string{}
	}
	realmID, err := hex.DecodeString(presence.Realm)
	if err != nil {
		return Presence{}, nil, fmt.Errorf("%w: realm must be hexadecimal", ErrInvalidPresence)
	}
	return presence, realmID, nil
}

// PresenceCodec owns the announce wire format for one application family. New
// meshbus applications use GenericPresenceCodec; an existing application (for
// example r1s) contributes a codec that preserves its established descriptor
// format so discovery stays wire-compatible. The adapter itself never
// interprets application descriptor bytes: it only verifies the announcing
// peer's realm, then hands validated app_data to the codec.
type PresenceCodec interface {
	// Build returns the app_data this endpoint announces, or an error to refuse
	// advertising. realmID is the endpoint's own realm identifier and
	// identityHash its local identity. The result must stay within the announce
	// app-data budget.
	Build(realmID, identityHash []byte) ([]byte, error)
	// Parse validates inbound announce app_data against the expected realm
	// identifier (the adapter's own realm) and returns the advisory presence
	// metadata. An error ignores the announce — for example an unknown
	// descriptor or a descriptor naming a foreign realm. The adapter passes its
	// real realm identifier, so a forged or mismatched announce cannot enter
	// the peer directory.
	Parse(appData []byte, expectedRealmID []byte) (map[string]string, error)
}

// GenericPresenceCodec speaks the meshbus.v1 presence wire format. It is the
// default for new meshbus applications.
type GenericPresenceCodec struct{}

// Build encodes a generic presence descriptor announcing realmID with the
// given advisory metadata.
func (GenericPresenceCodec) Build(realmID, identityHash []byte) ([]byte, error) {
	presence := Presence{
		Protocol: genericProtocolVersion,
		Realm:    hex.EncodeToString(realmID),
	}
	return presence.marshal()
}

// Parse decodes a generic presence descriptor and verifies it names the same
// realm the adapter is bound to. A mismatch is rejected before any delivery.
func (GenericPresenceCodec) Parse(appData []byte, expectedRealmID []byte) (map[string]string, error) {
	presence, realmID, err := parsePresence(appData)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(realmID, expectedRealmID) {
		return nil, fmt.Errorf("%w: announced %s", ErrRealmMismatch, presence.Realm)
	}
	return presence.Metadata, nil
}
