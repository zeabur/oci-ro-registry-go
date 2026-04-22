package push

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/zeabur/stratus/pkg/storage"
)

func TestUploadWithRetryRetriesUntilSuccess(t *testing.T) {
	origSleep := uploadRetrySleep
	uploadRetrySleep = func(time.Duration) {}
	defer func() { uploadRetrySleep = origSleep }()

	payload := map[string]string{"foo": "bar"}
	obj, err := NewJSONUploadObject(payload, "application/vnd.test+json", "test-json")
	if err != nil {
		t.Fatalf("NewJSONUploadObject: %v", err)
	}

	mock := &mockStorage{failCount: 1}

	if err := uploadWithRetry(context.Background(), mock, "test-bucket", "repo/index.json", obj, io.Discard); err != nil {
		t.Fatalf("uploadWithRetry: %v", err)
	}

	if got := mock.PutAttempts(); got != 2 {
		t.Fatalf("expected 2 put attempts, got %d", got)
	}

	latest := mock.LastPut()
	if latest == nil {
		t.Fatalf("expected at least one put call recorded")
	}
	if latest.key != "repo/index.json" {
		t.Fatalf("unexpected key: %s", latest.key)
	}
	if latest.opts.ContentType != "application/vnd.test+json" {
		t.Fatalf("unexpected content type: %s", latest.opts.ContentType)
	}
	expectedSize := obj.Size()
	if latest.size != expectedSize {
		t.Fatalf("unexpected content length: got %d want %d", latest.size, expectedSize)
	}
}

func TestUploadTasksConcurrentlyHandlesSkip(t *testing.T) {
	fsys := fstest.MapFS{
		"blobs/sha256/d1": {Data: []byte("existing")},
		"blobs/sha256/d2": {Data: []byte("new layer")},
	}

	blobSkip := func(ctx context.Context, s storage.Storage, bucket, key string) (SkipResult, error) {
		_, err := s.StatObject(ctx, bucket, key)
		if err == nil {
			return SkipResult{Skip: true, Reason: "layer already exists"}, nil
		}
		if errors.Is(err, storage.ErrObjectNotFound) {
			return SkipResult{}, nil
		}
		return SkipResult{}, err
	}

	obj1, err := NewFSUploadObject(fsys, "blobs/sha256/d1", "application/octet-stream", CacheControlPublicImmutable, "digest-1")
	if err != nil {
		t.Fatalf("NewFSUploadObject d1: %v", err)
	}
	obj2, err := NewFSUploadObject(fsys, "blobs/sha256/d2", "application/octet-stream", CacheControlPublicImmutable, "digest-2")
	if err != nil {
		t.Fatalf("NewFSUploadObject d2: %v", err)
	}

	mock := &mockStorage{
		existingKeys: map[string]bool{
			"repo/blobs/sha256/d1": true,
		},
	}

	tasks := []UploadTask{
		{Key: "repo/blobs/sha256/d1", Object: obj1, SkipCheck: blobSkip},
		{Key: "repo/blobs/sha256/d2", Object: obj2, SkipCheck: blobSkip},
	}

	if err := uploadTasksConcurrently(context.Background(), mock, "test-bucket", tasks, 2, io.Discard); err != nil {
		t.Fatalf("uploadTasksConcurrently: %v", err)
	}

	if got := mock.PutAttempts(); got != 1 {
		t.Fatalf("expected only one upload attempt, got %d", got)
	}
	last := mock.LastPut()
	if last == nil || !strings.HasSuffix(last.key, path.Join("blobs", "sha256", "d2")) {
		t.Fatalf("unexpected last put key: %+v", last)
	}
}

func TestUploadBlobsConcurrentlySkipsExisting(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"blobs/sha256/aaa": {Data: []byte("layer-a")},
		"blobs/sha256/bbb": {Data: []byte("layer-b")},
	}

	repo := "test-global/repo"
	mock := &mockStorage{
		existingKeys: map[string]bool{
			path.Join(repo, "blobs", "sha256", "aaa"): true,
		},
	}

	err := uploadBlobsConcurrently(context.Background(), mock, "test-bucket", fsys, repo, []string{"aaa", "bbb"}, 2, io.Discard)
	if err != nil {
		t.Fatalf("uploadBlobsConcurrently: %v", err)
	}

	if got := mock.PutAttempts(); got != 1 {
		t.Fatalf("expected 1 upload attempt, got %d", got)
	}

	last := mock.LastPut()
	if last == nil || last.key != path.Join(repo, "blobs", "sha256", "bbb") {
		t.Fatalf("unexpected last put key: %+v", last)
	}
	if last.opts.CacheControl != CacheControlPublicImmutable {
		t.Fatalf("unexpected cache control: %s", last.opts.CacheControl)
	}
	if last.opts.ContentType != "application/octet-stream" {
		t.Fatalf("unexpected content type: %s", last.opts.ContentType)
	}
}

type putCall struct {
	key     string
	payload []byte
	size    int64
	opts    storage.PutObjectOptions
}

type mockStorage struct {
	mu           sync.Mutex
	putCalls     []putCall
	putAttempts  int
	failCount    int
	existingKeys map[string]bool
	objects      map[string][]byte
}

func (m *mockStorage) PutAttempts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.putAttempts
}

func (m *mockStorage) LastPut() *putCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.putCalls) == 0 {
		return nil
	}
	cp := m.putCalls[len(m.putCalls)-1]
	return &cp
}

func (m *mockStorage) StatObject(_ context.Context, _, key string) (*storage.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.existingKeys != nil && m.existingKeys[key] {
		return &storage.ObjectInfo{ContentLength: 1}, nil
	}
	if data, ok := m.objects[key]; ok {
		return &storage.ObjectInfo{ContentLength: int64(len(data))}, nil
	}
	return nil, storage.ErrObjectNotFound
}

func (m *mockStorage) GetObject(_ context.Context, _, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.objects[key]; ok {
		cp := append([]byte(nil), data...)
		return io.NopCloser(bytes.NewReader(cp)), nil, nil
	}
	return nil, nil, storage.ErrObjectNotFound
}

func (m *mockStorage) PresignGetObject(_ context.Context, _, _ string, _ time.Duration) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (m *mockStorage) PutObject(_ context.Context, _, key string, body io.Reader, size int64, opts storage.PutObjectOptions) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	m.mu.Lock()
	m.putAttempts++
	callIdx := m.putAttempts
	m.putCalls = append(m.putCalls, putCall{key: key, payload: data, size: size, opts: opts})
	if m.objects == nil {
		m.objects = make(map[string][]byte)
	}
	m.objects[key] = append([]byte(nil), data...)
	if m.existingKeys != nil {
		m.existingKeys[key] = true
	}
	m.mu.Unlock()

	if callIdx <= m.failCount {
		return errors.New("synthetic failure")
	}
	return nil
}

func (m *mockStorage) StoredObject(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.objects[key]; ok {
		cp := append([]byte(nil), data...)
		return cp
	}
	return nil
}
