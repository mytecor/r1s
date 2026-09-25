// Package cluster defines the shared-secret membership primitive used by r1s transports.
package cluster

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	KeySize        = 32
	tokenPrefix    = "r1s1:"
	stateVersion   = 1
	idDomain       = "r1s-cluster-id-v1"
	DefaultRelPath = ".config/r1s/clusters"
)

var (
	ErrInvalidKey   = errors.New("invalid cluster key")
	ErrInvalidToken = errors.New("invalid cluster join token")
	ErrInvalidState = errors.New("invalid cluster state")
	ErrStateExists  = errors.New("cluster state already exists")
	ErrNotFound     = errors.New("cluster credential not found")
	ErrAmbiguous    = errors.New("ambiguous cluster identifier")
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

// SaveCredential atomically stores key under its full derived public ID. An
// existing credential for the same cluster is an idempotent success.
func SaveCredential(directory string, key []byte) (string, error) {
	id, err := ID(key)
	if err != nil {
		return "", err
	}
	idText := fmt.Sprintf("%x", id)
	path := filepath.Join(directory, idText)
	if existing, loadErr := Load(path); loadErr == nil {
		if bytes.Equal(existing, key) {
			return idText, nil
		}
		return "", ErrStateExists
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return "", loadErr
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create cluster credential directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", fmt.Errorf("protect cluster credential directory: %w", err)
	}
	token, err := Token(key)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(State{Version: stateVersion, Key: token})
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	temporary, err := os.CreateTemp(directory, ".credential-*")
	if err != nil {
		return "", fmt.Errorf("create temporary cluster credential: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return "", fmt.Errorf("protect temporary cluster credential: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return "", fmt.Errorf("write cluster credential: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return "", fmt.Errorf("sync cluster credential: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close cluster credential: %w", err)
	}
	// Hard-linking a complete temporary file commits without replacing a
	// credential that another process may have created concurrently.
	if err := os.Link(temporaryPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, loadErr := Load(path)
			if loadErr == nil && bytes.Equal(existing, key) {
				return idText, nil
			}
			return "", ErrStateExists
		}
		return "", fmt.Errorf("commit cluster credential: %w", err)
	}
	committed = true
	_ = os.Remove(temporaryPath)
	if directoryHandle, err := os.Open(directory); err == nil {
		_ = directoryHandle.Sync()
		_ = directoryHandle.Close()
	}
	return idText, nil
}

// List returns valid credential IDs in lexical order. Unrelated files and the
// legacy single-file store outside directory are ignored.
func List(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read cluster credential directory: %w", err)
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || len(name) != sha256.Size*2 || strings.ToLower(name) != name {
			continue
		}
		if _, decodeErr := hex.DecodeString(name); decodeErr != nil {
			continue
		}
		key, loadErr := Load(filepath.Join(directory, name))
		if loadErr != nil {
			return nil, fmt.Errorf("load cluster credential %s: %w", name, loadErr)
		}
		id, idErr := ID(key)
		if idErr != nil || fmt.Sprintf("%x", id) != name {
			return nil, fmt.Errorf("%w: credential filename %s does not match its key", ErrInvalidState, name)
		}
		ids = append(ids, name)
	}
	sort.Strings(ids)
	return ids, nil
}

// Resolve loads the credential selected by a unique public-ID prefix.
func Resolve(directory, selector string) ([]byte, string, error) {
	selector = strings.TrimSpace(strings.ToLower(selector))
	if selector == "" {
		return nil, "", fmt.Errorf("%w: empty cluster identifier", ErrNotFound)
	}
	if len(selector) > sha256.Size*2 {
		return nil, "", fmt.Errorf("%w: %q", ErrNotFound, selector)
	}
	for _, character := range selector {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return nil, "", fmt.Errorf("%w: cluster identifier must be a hexadecimal prefix", ErrNotFound)
		}
	}
	ids, err := List(directory)
	if err != nil {
		return nil, "", err
	}
	var match string
	for _, id := range ids {
		if strings.HasPrefix(id, selector) {
			if match != "" {
				return nil, "", fmt.Errorf("%w %q (matches %s and %s)", ErrAmbiguous, selector, match, id)
			}
			match = id
		}
	}
	if match == "" {
		return nil, "", fmt.Errorf("%w %q", ErrNotFound, selector)
	}
	key, err := Load(filepath.Join(directory, match))
	if err != nil {
		return nil, "", err
	}
	return key, match, nil
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

// DefaultDirectory returns the per-user credential directory shared by both
// clients and allocators.
func DefaultDirectory() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, DefaultRelPath), nil
}
