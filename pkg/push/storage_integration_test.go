//go:build integration

package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/zeabur/stratus/v2/pkg/config"
	"github.com/zeabur/stratus/v2/pkg/registry"
	"github.com/zeabur/stratus/v2/pkg/storage"
)

// buildIntegrationCase reads the environment variables matching the config,
// and returns a ready store. Skips the test if required vars are absent.
func buildIntegrationCase(t *testing.T) (store *storage.MinioStorage, bucket string) {
	t.Helper()

	cfg := config.Load()
	store, err := storage.MinioStorageFromConfig(cfg)
	if errors.Is(err, storage.ErrMissingConfig) {
		t.Skip("Skipping integration test due to missing configuration")
	}
	if err != nil {
		t.Fatalf("MinioStorageFromConfig: %v", err)
	}

	return store, cfg.BucketName
}

func withIsolatedPrefix(t *testing.T, rawClient *minio.Client, bucket string) string {
	t.Helper()
	prefix := fmt.Sprintf("integration-test/%d-%016x/", time.Now().UnixNano(), rand.Int63())

	t.Cleanup(func() {
		ctx := context.Background()
		objsCh := rawClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		})

		toDelete := make(chan minio.ObjectInfo, 4)
		go func() {
			defer close(toDelete)
			for obj := range objsCh {
				if obj.Err != nil {
					t.Logf("cleanup list error (prefix=%s): %v", prefix, obj.Err)
					return
				}
				toDelete <- obj
			}
		}()

		for rerr := range rawClient.RemoveObjects(ctx, bucket, toDelete, minio.RemoveObjectsOptions{}) {
			t.Logf("cleanup remove error (prefix=%s key=%s): %v", prefix, rerr.ObjectName, rerr.Err)
		}
	})

	return prefix
}

// ---- tests ----

func TestIntegration_PushOciLayout(t *testing.T) {
	store, bucket := buildIntegrationCase(t)
	raw := store.GetClient()

	ctx := context.Background()
	prefix := withIsolatedPrefix(t, raw, bucket)

	const tag = "integration-v1"
	newDigest := strings.Repeat("a", 64)
	existingDigest := strings.Repeat("b", 64)
	blobData := []byte("integration test layer data")

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

	fsys := fstest.MapFS{
		"index.json":                            {Data: localIndexBytes},
		"oci-layout":                            {Data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		path.Join("blobs", "sha256", newDigest): {Data: blobData},
	}

	// Pre-create an existing remote index to verify merging.
	existingIndex := registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:" + existingDigest,
				Size:      123,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "previous",
				},
			},
		},
	}
	existingIndexBytes, _ := json.Marshal(existingIndex)

	repo := prefix + "push-test/oci-layout-repo"
	indexKey := registry.IndexPath(repo)

	if _, err := raw.PutObject(ctx, bucket, indexKey,
		bytes.NewReader(existingIndexBytes), int64(len(existingIndexBytes)),
		minio.PutObjectOptions{ContentType: "application/vnd.oci.image.index.v1+json"},
	); err != nil {
		t.Fatalf("pre-create index via raw client: %v", err)
	}

	if err := PushOciLayout(ctx, store, bucket, fsys, repo, tag, WithLogOutput(io.Discard)); err != nil {
		t.Fatalf("PushOciLayout: %v", err)
	}

	// Verify index.json content type.
	idxInfo, err := raw.StatObject(ctx, bucket, indexKey, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("StatObject index.json: %v", err)
	}
	if idxInfo.ContentType != "application/vnd.oci.image.index.v1+json" {
		t.Errorf("index ContentType: got %q want %q", idxInfo.ContentType, "application/vnd.oci.image.index.v1+json")
	}

	// Verify oci-layout exists.
	if _, err := raw.StatObject(ctx, bucket, registry.OciLayoutPath(repo), minio.StatObjectOptions{}); err != nil {
		t.Fatalf("StatObject oci-layout: %v", err)
	}

	// Verify blob cache-control.
	blobKey := registry.BlobPath(repo, newDigest)
	blobInfo, err := raw.StatObject(ctx, bucket, blobKey, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("StatObject blob: %v", err)
	}
	if cc := blobInfo.Metadata.Get("Cache-Control"); cc != CacheControlPublicImmutable {
		t.Errorf("blob Cache-Control: got %q want %q", cc, CacheControlPublicImmutable)
	}

	// Verify merged index has 2 manifests with correct ref.names.
	idxObj, err := raw.GetObject(ctx, bucket, indexKey, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("GetObject index: %v", err)
	}
	var merged registry.OciManifestIndex
	decodeErr := json.NewDecoder(idxObj).Decode(&merged)
	_ = idxObj.Close()
	if decodeErr != nil {
		t.Fatalf("decode merged index: %v", decodeErr)
	}
	if len(merged.Manifests) != 2 {
		t.Fatalf("merged manifest count: got %d want 2", len(merged.Manifests))
	}

	refNames := make(map[string]string, 2)
	for _, m := range merged.Manifests {
		refNames[m.Digest] = m.Annotations["org.opencontainers.image.ref.name"]
	}
	if refNames["sha256:"+newDigest] != tag {
		t.Errorf("new manifest ref.name: got %q want %q", refNames["sha256:"+newDigest], tag)
	}
	if _, ok := refNames["sha256:"+existingDigest]; !ok {
		t.Error("existing manifest missing from merged index")
	}
}
