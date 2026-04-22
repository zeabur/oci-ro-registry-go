package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/zeabur/stratus/internal/config"
	"github.com/zeabur/stratus/internal/storage"
)

// buildTestCase reads the environment variables matching the config,
// and returns a ready store. Skips the test if required vars are absent.
func buildTestCase(t *testing.T) (store *storage.MinioStorage, bucket string) {
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

func withIsolatedPrefix(t *testing.T, raw *minio.Client, bucket string) string {
	t.Helper()
	prefix := fmt.Sprintf("integration-test/%d-%016x/", time.Now().UnixNano(), rand.Int63())

	t.Cleanup(func() {
		ctx := context.Background()
		objsCh := raw.ListObjects(ctx, bucket, minio.ListObjectsOptions{
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

		for rerr := range raw.RemoveObjects(ctx, bucket, toDelete, minio.RemoveObjectsOptions{}) {
			t.Logf("cleanup remove error (prefix=%s key=%s): %v", prefix, rerr.ObjectName, rerr.Err)
		}
	})

	return prefix
}

// ---- tests ----

func TestIntegration_Storage_RoundTrip(t *testing.T) {
	store, bucket := buildTestCase(t)
	raw := store.GetClient()

	ctx := context.Background()
	prefix := withIsolatedPrefix(t, raw, bucket)
	key := prefix + "round-trip"
	payload := []byte("hello integration test — round trip")

	if err := store.PutObject(ctx, bucket, key, bytes.NewReader(payload), int64(len(payload)), storage.PutObjectOptions{
		ContentType: "application/octet-stream",
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	info, err := store.StatObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	if info.ContentLength != int64(len(payload)) {
		t.Errorf("size: got %d want %d", info.ContentLength, len(payload))
	}
	if info.ETag == "" {
		t.Error("StatObject: expected non-empty ETag")
	}

	rc, _, err := store.GetObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("GetObject content mismatch: got %q want %q", got, payload)
	}

	newPayload := []byte("updated content for overwrite test")
	if err := store.PutObject(ctx, bucket, key, bytes.NewReader(newPayload), int64(len(newPayload)), storage.PutObjectOptions{
		ContentType: "application/octet-stream",
	}); err != nil {
		t.Fatalf("PutObject overwrite: %v", err)
	}

	rc, _, err = store.GetObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("GetObject after overwrite: %v", err)
	}
	got, _ = io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, newPayload) {
		t.Errorf("GetObject overwrite mismatch: got %q want %q", got, newPayload)
	}
}

func TestIntegration_Storage_NotFound(t *testing.T) {
	store, bucket := buildTestCase(t)
	raw := store.GetClient()

	ctx := context.Background()
	prefix := withIsolatedPrefix(t, raw, bucket)
	key := prefix + fmt.Sprintf("does-not-exist-%d", rand.Int63())

	_, err := store.StatObject(ctx, bucket, key)
	if err == nil {
		t.Error("StatObject: expected error for non-existent key")
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Errorf("StatObject: expected ErrObjectNotFound, got %v", err)
	}

	_, _, err = store.GetObject(ctx, bucket, key)
	if err == nil {
		t.Error("GetObject: expected error for non-existent key")
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Errorf("GetObject: expected ErrObjectNotFound, got %v", err)
	}
}

func TestIntegration_Storage_LargeMultipart(t *testing.T) {
	store, bucket := buildTestCase(t)
	raw := store.GetClient()

	ctx := context.Background()
	prefix := withIsolatedPrefix(t, raw, bucket)
	key := prefix + "large-multipart"

	const size = 20 * 1024 * 1024
	data := make([]byte, size)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)

	if err := store.PutObject(ctx, bucket, key, bytes.NewReader(data), size, storage.PutObjectOptions{
		ContentType: "application/octet-stream",
	}); err != nil {
		t.Fatalf("PutObject (20 MiB): %v", err)
	}

	info, err := store.StatObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("StatObject: %v", err)
	}
	if info.ContentLength != size {
		t.Errorf("size mismatch: got %d want %d", info.ContentLength, size)
	}

	rc, _, err := store.GetObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != size {
		t.Fatalf("GetObject length: got %d want %d", len(got), size)
	}
	if !bytes.Equal(got[:64], data[:64]) {
		t.Error("first 64 bytes mismatch")
	}
	if !bytes.Equal(got[size-64:], data[size-64:]) {
		t.Error("last 64 bytes mismatch")
	}
}

func TestIntegration_Storage_Metadata(t *testing.T) {
	store, bucket := buildTestCase(t)
	raw := store.GetClient()

	ctx := context.Background()
	prefix := withIsolatedPrefix(t, raw, bucket)
	key := prefix + "metadata-test"
	payload := []byte("metadata test payload")

	if err := store.PutObject(ctx, bucket, key, bytes.NewReader(payload), int64(len(payload)), storage.PutObjectOptions{
		ContentType:  "application/octet-stream",
		CacheControl: "public, max-age=31536000, immutable",
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	rawObj, err := raw.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("StatObject (raw): %v", err)
	}
	if rawObj.ContentType != "application/octet-stream" {
		t.Errorf("ContentType: got %q want %q", rawObj.ContentType, "application/octet-stream")
	}
	if cc := rawObj.Metadata.Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Errorf("Cache-Control: got %q want %q", cc, "public, max-age=31536000, immutable")
	}
}
