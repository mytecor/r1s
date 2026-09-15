// Package cluster defines the shared-secret membership primitive used by r1s transports.
package cluster

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	KeySize        = 32
	tokenPrefix    = "r1s1:"
	stateVersion   = 1
	idDomain       = "r1s-cluster-id-v1"
	DefaultRelPath = ".config/r1s/cluster"
)

var (
	ErrInvalidKey   = errors.New("invalid cluster key")
	ErrInvalidToken = errors.New("invalid cluster join token")
	ErrInvalidState = errors.New("invalid cluster state")
	ErrStateExists  = errors.New("cluster state already exists")
)

type State struct {
	Version int    `json:"version"`
	Key     string `json:"key"`
}

func Generate() ([]byte, error) {
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("generate cluster key: %w", err)
	}
	return key, nil
}

func ID(key []byte) ([]byte, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: expected %d bytes", ErrInvalidKey, KeySize)
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(idDomain))
	_, _ = digest.Write(key)
	return digest.Sum(nil), nil
}

func Token(key []byte) (string, error) {
	if len(key) != KeySize {
		return "", fmt.Errorf("%w: expected %d bytes", ErrInvalidKey, KeySize)
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(key), nil
}

func ParseToken(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, tokenPrefix) {
		return nil, fmt.Errorf("%w: expected %q prefix", ErrInvalidToken, tokenPrefix)
	}
	key, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, tokenPrefix))
	if err != nil || len(key) != KeySize {
		return nil, fmt.Errorf("%w: expected a base64url-encoded %d-byte key", ErrInvalidToken, KeySize)
	}
	return key, nil
}

// LoadSource loads cluster membership from an inline join token or a state
// file path. It first tries the value as a file; only a missing file falls
// back to token parsing. Inline tokens are not persisted.
func LoadSource(value string) ([]byte, error) {
	key, err := Load(value)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return ParseToken(value)
}

// SourceLabel returns a non-secret description suitable for diagnostics.
func SourceLabel(value string) string {
	if _, err := os.Stat(value); errors.Is(err, os.ErrNotExist) {
		return "inline join token"
	}
	return value
}

func Load(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster state: %w", err)
	}
	var state State
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing data", ErrInvalidState)
	}
	if state.Version != stateVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidState, state.Version)
	}
	key, err := ParseToken(state.Key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	return key, nil
}

// SaveNew creates state without replacing an existing membership. Rejoining the
// same cluster is idempotent; changing membership requires an explicit future
// rotation operation.
func SaveNew(path string, key []byte) error {
	token, err := Token(key)
	if err != nil {
		return err
	}
	if existing, loadErr := Load(path); loadErr == nil {
		if bytes.Equal(existing, key) {
			return nil
		}
		return ErrStateExists
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return loadErr
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create cluster state directory: %w", err)
	}
	data, err := json.Marshal(State{Version: stateVersion, Key: token})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrStateExists
		}
		return fmt.Errorf("create cluster state: %w", err)
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write cluster state: %w", err)
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close cluster state: %w", closeErr)
	}
	return nil
}

// DefaultPath returns the per-user cluster membership path shared by both
// clients and allocators.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, DefaultRelPath), nil
}
