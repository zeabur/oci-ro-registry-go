package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/zeabur/stratus/pkg/registry"
	"github.com/zeabur/stratus/pkg/storage"
)

// OCIManifestUpdater encapsulates manifest fetch/merge logic for OCI indexes.
type OCIManifestUpdater struct {
	storage storage.Storage
	bucket  string
}

// NewOCIManifestUpdater returns a configured manifest updater.
func NewOCIManifestUpdater(s storage.Storage, bucket string) *OCIManifestUpdater {
	return &OCIManifestUpdater{storage: s, bucket: bucket}
}

// GetRemoteIndex fetches the existing index from the configured storage.
func (u *OCIManifestUpdater) GetRemoteIndex(ctx context.Context, repo string) (*registry.OciManifestIndex, error) {
	if u.storage == nil {
		return nil, errors.New("storage client is not configured")
	}

	result, _, err := u.storage.GetObject(ctx, u.bucket, registry.IndexPath(repo))
	if errors.Is(err, storage.ErrObjectNotFound) {
		return nil, storage.ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	defer func() {
		_ = result.Close()
	}()

	body, err := io.ReadAll(result)
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	var index registry.OciManifestIndex
	if err := json.Unmarshal(body, &index); err != nil {
		return nil, fmt.Errorf("unmarshal index: %w", err)
	}

	return &index, nil
}

// MergeIndex merges existing and incoming OCI indexes, avoiding duplicates.
// Incoming manifests take precedence for each ref.name.
func (u *OCIManifestUpdater) MergeIndex(existing, incoming *registry.OciManifestIndex) *registry.OciManifestIndex {
	if existing == nil {
		return incoming
	}
	if incoming == nil {
		return existing
	}

	merged := &registry.OciManifestIndex{SchemaVersion: 2}
	seen := make(map[string]bool)
	allManifests := append(incoming.Manifests, existing.Manifests...)

	for _, m := range allManifests {
		refName := m.Annotations["org.opencontainers.image.ref.name"]
		if refName == "" {
			merged.Manifests = append(merged.Manifests, m)
			continue
		}
		if seen[refName] {
			continue
		}
		seen[refName] = true
		merged.Manifests = append(merged.Manifests, m)
	}

	return merged
}
