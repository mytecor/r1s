package containerd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	containerdclient "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/containerd/errdefs"
	"github.com/mytecor/r1s/internal/logstore"
)

const (
	executionLabel = "io.r1s.execution-id"
	specLabel      = "io.r1s.spec-digest"
)

type clientBackend struct {
	address     string
	logs        *logstore.Store
	logBinary   string
	client      *containerdclient.Client
	namespace   string
	snapshotter string
}

func newClientBackend(ctx context.Context, config Config) (*clientBackend, error) {
	client, err := containerdclient.New(config.Address, containerdclient.WithDefaultNamespace(config.Namespace))
	if err != nil {
		return nil, fmt.Errorf("connect to containerd: %w", err)
	}
	implementation := &clientBackend{address: config.Address, client: client, namespace: config.Namespace, snapshotter: config.Snapshotter, logs: config.Logs, logBinary: config.LogBinary}
	if _, err := client.Version(implementation.context(ctx)); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("query containerd version: %w", err)
	}
	return implementation, nil
}

func (b *clientBackend) Close() error { return b.client.Close() }

func (b *clientBackend) context(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, b.namespace)
}

func verifyLabels(ctx context.Context, container containerdclient.Container, executionID, fingerprint string) error {
	labels, err := container.Labels(ctx)
	if err != nil {
		return fmt.Errorf("read container %q labels: %w", container.ID(), err)
	}
	if labels[executionLabel] != executionID || (fingerprint != "" && labels[specLabel] != fingerprint) {
		return fmt.Errorf("%w: %q", ErrExecutionConflict, executionID)
	}
	return nil
}

func pinnedDigest(image string) (string, error) {
	specification, err := reference.Parse(image)
	if err != nil {
		return "", fmt.Errorf("parse image reference %q: %w", image, err)
	}
	digest := specification.Digest()
	if digest == "" {
		return "", fmt.Errorf("image reference %q must be pinned by digest", image)
	}
	if err := digest.Validate(); err != nil {
		return "", fmt.Errorf("image reference %q has invalid digest: %w", image, err)
	}
	return digest.String(), nil
}

func containerID(executionID string) string {
	digest := sha256.Sum256([]byte(executionID))
	return "r1s-" + hex.EncodeToString(digest[:])
}

func ignoreNotFound(err error) error {
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}
