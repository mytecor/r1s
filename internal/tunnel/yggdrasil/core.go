package yggdrasil

import (
	"crypto/ed25519"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/gologme/log"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
)

// Logger is the node diagnostic sink type yggdrasil-go's Core expects.
type Logger = core.Logger

// DefaultPeers is the bootstrap peer URI set joined when no custom peers are
// configured.
var DefaultPeers = []string{
	"tcp://127.0.0.1:9002",
	"tls://ygg5.mk16.de:1338",
	"tls://supanadit.com:15021",
	"tls://ins.8px.sk:4321",
	"quic://asia.deinfra.org:15015",
	"tls://ygg-msk-1.averyan.ru:8362",
}

func defaultPeers() []string {
	if env := strings.TrimSpace(os.Getenv("R1S_TUNNEL_PEERS")); env != "" {
		var peers []string
		for _, p := range strings.Split(env, ",") {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				peers = append(peers, trimmed)
			}
		}
		if len(peers) > 0 {
			return peers
		}
	}
	return append([]string(nil), DefaultPeers...)
}

// startCore starts the embedded yggdrasil node over the derived private key.
// The TLS certificate is generated in memory from the same key (yggdrasil-go
// requires an ed25519 tls.Certificate for its node identity); nothing is
// persisted. Peers are joined after startup so a private peer set stays edge
// configuration.
func startCore(key ed25519.PrivateKey, options NodeOptions) (*core.Core, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("yggdrasil node: derived key has wrong size %d", len(key))
	}
	nodeConfig := config.GenerateConfig()
	nodeConfig.PrivateKey = config.KeyBytes(key)
	if err := nodeConfig.GenerateSelfSignedCertificate(); err != nil {
		return nil, fmt.Errorf("yggdrasil node: build certificate: %w", err)
	}
	logger := options.LogSink
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	nodeCore, err := core.New(nodeConfig.Certificate, logger)
	if err != nil {
		return nil, fmt.Errorf("yggdrasil node: %w", err)
	}
	peers := options.Peers
	if peers == nil {
		peers = defaultPeers()
	}
	if err := applyPeers(nodeCore, peers); err != nil {
		nodeCore.Stop()
		return nil, err
	}
	return nodeCore, nil
}

// applyPeers joins the configured bootstrap peers. A malformed peer URI is a
// configuration error; a peer that cannot be reached yet is not: the mesh
// reconnects on its own schedule and a session dial fails fast while no path
// exists.
func applyPeers(nodeCore *core.Core, peers []string) error {
	for _, uri := range peers {
		parsed, err := url.Parse(uri)
		if err != nil {
			return fmt.Errorf("yggdrasil peer %q: %w", uri, err)
		}
		if err := nodeCore.AddPeer(parsed, ""); err != nil {
			return fmt.Errorf("yggdrasil peer %q: %w", uri, err)
		}
	}
	return nil
}
