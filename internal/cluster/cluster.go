// Package cluster defines the shared-secret membership primitive used by r1s transports.
package cluster

import (
	"bytes"
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

	"github.com/mytecor/meshbus/security/realm"
)

const (
	KeySize                  = realm.KeySize
	tokenPrefix              = "r1s1:"
	tokenVersion             = 1
	stateVersion             = 1
	DestinationSize          = 16
	MaxBootstrapDestinations = 32
	DefaultRelPath           = ".config/r1s/realms"
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
	Version               int      `json:"version"`
	Key                   string   `json:"key"`
	BootstrapDestinations []string `json:"bootstrapDestinations,omitempty"`
}

// Credential keeps the realm membership secret separate from public RNS
// routing hints. Bootstrap destinations grant no authority; every connection
// still has to prove possession of Key before an r1s envelope is delivered.
type Credential struct {
	Key                   []byte
	BootstrapDestinations []string
}

type membershipToken struct {
	Version               int      `json:"version"`
	Key                   string   `json:"key"`
	BootstrapDestinations []string `json:"bootstrapDestinations"`
}

func Generate() ([]byte, error) {
	key, err := realm.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("generate cluster key: %w", err)
	}
	return key, nil
}

func ID(key []byte) ([]byte, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: expected %d bytes", ErrInvalidKey, KeySize)
	}
	opened, err := OpenRealm(key)
	if err != nil {
		return nil, err
	}
	return opened.ID(), nil
}

// OpenRealm uses the standard meshbus realm profile. r1s keeps cluster as its
// product-facing name, but no longer maintains separate ID or authentication
// domains below that API.
func OpenRealm(key []byte) (*realm.Realm, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("%w: expected %d bytes", ErrInvalidKey, KeySize)
	}
	opened, err := realm.Open(realm.Config{Key: key})
	if err != nil {
		return nil, fmt.Errorf("open cluster realm: %w", err)
	}
	return opened, nil
}

func Token(key []byte) (string, error) {
	if len(key) != KeySize {
		return "", fmt.Errorf("%w: expected %d bytes", ErrInvalidKey, KeySize)
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(key), nil
}

func ParseToken(value string) ([]byte, error) {
	credential, err := ParseCredentialToken(value)
	if err != nil {
		return nil, err
	}
	return credential.Key, nil
}

// CredentialToken serializes a membership for transfer. Key-only credentials
// encode directly; credentials with routing hints serialize a versioned
// payload under the same r1s1 prefix. The hints remain public, non-authoritative data.
func CredentialToken(credential Credential) (string, error) {
	keyToken, err := Token(credential.Key)
	if err != nil {
		return "", err
	}
	destinations, err := NormalizeBootstrapDestinations(credential.BootstrapDestinations)
	if err != nil {
		return "", err
	}
	if len(destinations) == 0 {
		return keyToken, nil
	}
	payload, err := json.Marshal(membershipToken{
		Version: tokenVersion, Key: keyToken, BootstrapDestinations: destinations,
	})
	if err != nil {
		return "", err
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(payload), nil
}

// ParseCredentialToken parses a membership token carrying a realm key and optional
// bounded public allocator destinations.
func ParseCredentialToken(value string) (Credential, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, tokenPrefix) {
		return Credential{}, fmt.Errorf("%w: expected %q prefix", ErrInvalidToken, tokenPrefix)
	}
	raw := strings.TrimPrefix(value, tokenPrefix)
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: invalid payload", ErrInvalidToken)
	}
	if len(payload) == KeySize {
		return Credential{Key: payload}, nil
	}
	var token membershipToken
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&token); err != nil {
		return Credential{}, fmt.Errorf("%w: invalid payload: %v", ErrInvalidToken, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || token.Version != tokenVersion {
		return Credential{}, fmt.Errorf("%w: unsupported payload", ErrInvalidToken)
	}
	key, err := parseKeyToken(token.Key)
	if err != nil {
		return Credential{}, err
	}
	destinations, err := NormalizeBootstrapDestinations(token.BootstrapDestinations)
	if err != nil || len(destinations) == 0 {
		return Credential{}, fmt.Errorf("%w: invalid bootstrap destinations", ErrInvalidToken)
	}
	return Credential{Key: key, BootstrapDestinations: destinations}, nil
}

func parseKeyToken(value string) ([]byte, error) {
	if !strings.HasPrefix(value, tokenPrefix) {
		return nil, fmt.Errorf("%w: expected %q prefix", ErrInvalidToken, tokenPrefix)
	}
	key, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, tokenPrefix))
	if err != nil || len(key) != KeySize {
		return nil, fmt.Errorf("%w: expected a base64url-encoded %d-byte key", ErrInvalidToken, KeySize)
	}
	return key, nil
}

// SaveMembership persists a transferred credential and merges its validated
// public routing hints with any hints already learned locally.
func SaveMembership(directory string, credential Credential) (string, error) {
	id, err := SaveCredential(directory, credential.Key)
	if err != nil {
		return "", err
	}
	for _, destination := range credential.BootstrapDestinations {
		if err := RecordBootstrapDestination(filepath.Join(directory, id), destination); err != nil {
			return "", err
		}
	}
	return id, nil
}

func Load(path string) ([]byte, error) {
	credential, err := LoadCredential(path)
	if err != nil {
		return nil, err
	}
	return credential.Key, nil
}

// LoadCredential reads the cluster state and its bounded public bootstrap destinations.
func LoadCredential(path string) (Credential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Credential{}, fmt.Errorf("read cluster state: %w", err)
	}
	var state State
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return Credential{}, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Credential{}, fmt.Errorf("%w: trailing data", ErrInvalidState)
	}
	if state.Version != stateVersion {
		return Credential{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidState, state.Version)
	}
	key, err := parseKeyToken(state.Key)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	destinations, err := NormalizeBootstrapDestinations(state.BootstrapDestinations)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	return Credential{Key: key, BootstrapDestinations: destinations}, nil
}

// NormalizeBootstrapDestinations validates, lower-cases, deduplicates, sorts,
// and bounds 16-byte RNS destination hashes.
func NormalizeBootstrapDestinations(destinations []string) ([]string, error) {
	unique := make(map[string]struct{}, len(destinations))
	for _, destination := range destinations {
		destination = strings.ToLower(strings.TrimSpace(destination))
		decoded, err := hex.DecodeString(destination)
		if err != nil || len(decoded) != DestinationSize {
			return nil, fmt.Errorf("invalid bootstrap destination %q: expected a %d-byte hexadecimal hash", destination, DestinationSize)
		}
		unique[destination] = struct{}{}
	}
	if len(unique) > MaxBootstrapDestinations {
		return nil, fmt.Errorf("too many bootstrap destinations: %d exceeds %d", len(unique), MaxBootstrapDestinations)
	}
	result := make([]string, 0, len(unique))
	for destination := range unique {
		result = append(result, destination)
	}
	sort.Strings(result)
	return result, nil
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
	credential, id, err := ResolveCredential(directory, selector)
	if err != nil {
		return nil, "", err
	}
	return credential.Key, id, nil
}

// ResolveCredential loads the selected membership and its durable public
// bootstrap hints.
func ResolveCredential(directory, selector string) (Credential, string, error) {
	selector = strings.TrimSpace(strings.ToLower(selector))
	if selector == "" {
		return Credential{}, "", fmt.Errorf("%w: empty cluster identifier", ErrNotFound)
	}
	if len(selector) > sha256.Size*2 {
		return Credential{}, "", fmt.Errorf("%w: %q", ErrNotFound, selector)
	}
	for _, character := range selector {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return Credential{}, "", fmt.Errorf("%w: cluster identifier must be a hexadecimal prefix", ErrNotFound)
		}
	}
	ids, err := List(directory)
	if err != nil {
		return Credential{}, "", err
	}
	var match string
	for _, id := range ids {
		if strings.HasPrefix(id, selector) {
			if match != "" {
				return Credential{}, "", fmt.Errorf("%w %q (matches %s and %s)", ErrAmbiguous, selector, match, id)
			}
			match = id
		}
	}
	if match == "" {
		return Credential{}, "", fmt.Errorf("%w %q", ErrNotFound, selector)
	}
	credential, err := LoadCredential(filepath.Join(directory, match))
	if err != nil {
		return Credential{}, "", err
	}
	return credential, match, nil
}

// RecordBootstrapDestination durably adds one public allocator destination to
// an existing credential without changing its realm key.
func RecordBootstrapDestination(path, destination string) error {
	credential, err := LoadCredential(path)
	if err != nil {
		return err
	}
	validated, err := NormalizeBootstrapDestinations([]string{destination})
	if err != nil {
		return err
	}
	destination = validated[0]
	for _, existing := range credential.BootstrapDestinations {
		if existing == destination {
			return nil
		}
	}
	destinations := append([]string(nil), credential.BootstrapDestinations...)
	if len(destinations) == MaxBootstrapDestinations {
		destinations = destinations[1:]
	}
	destinations, err = NormalizeBootstrapDestinations(append(destinations, destination))
	if err != nil {
		return err
	}
	token, err := Token(credential.Key)
	if err != nil {
		return err
	}
	data, err := json.Marshal(State{Version: stateVersion, Key: token, BootstrapDestinations: destinations})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".credential-update-*")
	if err != nil {
		return fmt.Errorf("create temporary cluster credential: %w", err)
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
		return fmt.Errorf("protect temporary cluster credential: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write cluster credential: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync cluster credential: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close cluster credential: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace cluster credential: %w", err)
	}
	committed = true
	if directoryHandle, err := os.Open(directory); err == nil {
		_ = directoryHandle.Sync()
		_ = directoryHandle.Close()
	}
	return nil
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
