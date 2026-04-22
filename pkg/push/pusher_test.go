package push

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/zeabur/stratus/v2/pkg/registry"
)

func TestPushOciLayoutUploadsToProvidedStorage(t *testing.T) {
	t.Parallel()

	const repo = "demo/repo"
	const tag = "v1"
	newDigest := strings.Repeat("a", 64)
	existingDigest := strings.Repeat("b", 64)
	blobData := []byte("layer-data")

	localIndex := registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType:   "application/vnd.oci.image.manifest.v1+json",
				Digest:      "sha256:" + newDigest,
				Size:        int64(len(blobData)),
				Annotations: map[string]string{},
			},
		},
	}
	localIndexBytes, err := json.Marshal(localIndex)
	if err != nil {
		t.Fatalf("marshal local index: %v", err)
	}

	ociLayoutData := []byte(`{"imageLayoutVersion":"1.0.0"}`)
	fsys := fstest.MapFS{
		"index.json":                            {Data: localIndexBytes},
		"oci-layout":                            {Data: ociLayoutData},
		path.Join("blobs", "sha256", newDigest): {Data: blobData},
	}

	existingIndex := registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType:   "application/vnd.oci.image.manifest.v1+json",
				Digest:      "sha256:" + existingDigest,
				Size:        123,
				Annotations: map[string]string{"org.opencontainers.image.ref.name": "previous"},
			},
		},
	}
	existingIndexBytes, err := json.Marshal(existingIndex)
	if err != nil {
		t.Fatalf("marshal existing index: %v", err)
	}

	mock := &mockStorage{
		objects: map[string][]byte{
			registry.IndexPath(repo): existingIndexBytes,
		},
	}

	if err := PushOciLayout(context.Background(), mock, "test-bucket", fsys, repo, tag); err != nil {
		t.Fatalf("PushOciLayout: %v", err)
	}

	blobKey := registry.BlobPath(repo, newDigest)
	if stored := mock.StoredObject(blobKey); stored == nil || !bytes.Equal(stored, blobData) {
		t.Fatalf("blob %s not uploaded correctly", blobKey)
	}

	layoutKey := registry.OciLayoutPath(repo)
	if stored := mock.StoredObject(layoutKey); stored == nil || !bytes.Equal(stored, ociLayoutData) {
		t.Fatalf("oci-layout not uploaded correctly")
	}

	indexKey := registry.IndexPath(repo)
	indexBytes := mock.StoredObject(indexKey)
	if indexBytes == nil {
		t.Fatalf("index.json was not uploaded")
	}

	var merged registry.OciManifestIndex
	if err := json.Unmarshal(indexBytes, &merged); err != nil {
		t.Fatalf("unmarshal merged index: %v", err)
	}
	if len(merged.Manifests) != 2 {
		t.Fatalf("expected 2 manifests, got %d", len(merged.Manifests))
	}

	var foundNew, foundExisting bool
	for _, m := range merged.Manifests {
		switch m.Digest {
		case "sha256:" + newDigest:
			foundNew = m.Annotations["org.opencontainers.image.ref.name"] == tag
		case "sha256:" + existingDigest:
			foundExisting = true
		}
	}
	if !foundNew {
		t.Fatalf("new manifest missing or annotation mismatch")
	}
	if !foundExisting {
		t.Fatalf("existing manifest missing from merged index")
	}
}

func TestPushOciLayoutRequiresStorage(t *testing.T) {
	t.Parallel()

	digest := strings.Repeat("c", 64)
	index := registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType:   "application/vnd.oci.image.manifest.v1+json",
				Digest:      "sha256:" + digest,
				Size:        1,
				Annotations: map[string]string{},
			},
		},
	}
	indexBytes, err := json.Marshal(index)
	if err != nil {
		t.Fatalf("marshal index: %v", err)
	}

	fsys := fstest.MapFS{
		"index.json":                         {Data: indexBytes},
		"oci-layout":                         {Data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		path.Join("blobs", "sha256", digest): {Data: []byte("x")},
	}

	err = PushOciLayout(context.Background(), nil, "test-bucket", fsys, "repo", "tag")
	if err == nil {
		t.Fatalf("expected error when storage is nil")
	}
}

func TestPushOciLayout_WithLogOutput(t *testing.T) {
	t.Parallel()

	const repo = "log/repo"
	const tag = "v1"
	digest := strings.Repeat("d", 64)
	blobData := []byte("log-test")

	localIndex := registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType:   "application/vnd.oci.image.manifest.v1+json",
				Digest:      "sha256:" + digest,
				Size:        int64(len(blobData)),
				Annotations: map[string]string{},
			},
		},
	}
	localIndexBytes, _ := json.Marshal(localIndex)

	fsys := fstest.MapFS{
		"index.json":                         {Data: localIndexBytes},
		"oci-layout":                         {Data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		path.Join("blobs", "sha256", digest): {Data: blobData},
	}

	mock := &mockStorage{}
	var buf bytes.Buffer

	if err := PushOciLayout(context.Background(), mock, "test-bucket", fsys, repo, tag, WithLogOutput(&buf)); err != nil {
		t.Fatalf("PushOciLayout: %v", err)
	}

	if buf.Len() == 0 {
		t.Error("expected log output, got nothing")
	}
}

// Kept for compatibility: io is used via io.Discard in integration tests
var _ = io.Discard
