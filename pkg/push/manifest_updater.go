package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/zeabur/stratus/internal/storage"
)

// OCIManifestUpdater encapsulates manifest fetch/merge logic for OCI indexes.
type OCIManifestUpdater struct {
	storage storage.Storage
	bucket  string
}

// OciIndexManifest represents a manifest entry in an OCI index
type OciIndexManifest struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations"`
}

// OciIndex represents an OCI image index
type OciIndex struct {
	SchemaVersion int                `json:"schemaVersion"`
	Manifests     []OciIndexManifest `json:"manifests"`
}

// NewOCIManifestUpdater returns a configured manifest updater.
func NewOCIManifestUpdater(s storage.Storage, bucket string) *OCIManifestUpdater {
	return &OCIManifestUpdater{storage: s, bucket: bucket}
}

// GetRemoteIndex fetches the existing index from the configured storage.
func (u *OCIManifestUpdater) GetRemoteIndex(ctx context.Context, repo string) (*OciIndex, error) {
	key := s3IndexPath(repo)

	if u.storage == nil {
		return nil, errors.New("storage client is not configured")
	}

	result, _, err := u.storage.GetObject(ctx, u.bucket, key)
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

	var index OciIndex
	err = json.Unmarshal(body, &index)
	if err != nil {
		return nil, fmt.Errorf("unmarshal index: %w", err)
	}

	return &index, nil
}

// MergeIndex merges existing and incoming OCI indexes, avoiding duplicates.
func (u *OCIManifestUpdater) MergeIndex(existing, incoming *OciIndex) *OciIndex {
	if existing == nil {
		return incoming
	}
	if incoming == nil {
		return existing
	}

	merged := &OciIndex{
		SchemaVersion: 2,
		Manifests:     nil,
	}

	seen := make(map[string]bool)
	// Prefer incoming manifests for each ref.name, then fall back to existing.
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
