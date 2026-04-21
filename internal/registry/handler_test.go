package registry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zeabur/oci-ro-registry-go/internal/registry"
	"github.com/zeabur/oci-ro-registry-go/internal/storage"
)

const testBucket = "test-bucket"

// fakeStorage is a map-backed Storage for testing.
type fakeStorage struct {
	objects      map[string]fakeObject
	presignBase  string
	rateLimitKey string // full "bucket/key" that triggers a 10058 error
}

type fakeObject struct {
	data []byte
	etag string
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		objects:     make(map[string]fakeObject),
		presignBase: "http://minio.example.com/presigned",
	}
}

func (f *fakeStorage) put(bucket, key string, data []byte, etag string) {
	f.objects[bucket+"/"+key] = fakeObject{data: data, etag: etag}
}

func (f *fakeStorage) StatObject(_ context.Context, bucket, key string) (*storage.ObjectInfo, error) {
	obj, ok := f.objects[bucket+"/"+key]
	if !ok {
		return nil, storage.ErrObjectNotFound
	}
	return &storage.ObjectInfo{
		ContentLength: int64(len(obj.data)),
		ETag:          obj.etag,
	}, nil
}

func (f *fakeStorage) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	fullKey := bucket + "/" + key
	if f.rateLimitKey == fullKey {
		return nil, nil, fmt.Errorf("request failed with error code 10058: rate limit exceeded")
	}
	obj, ok := f.objects[fullKey]
	if !ok {
		return nil, nil, storage.ErrObjectNotFound
	}
	return io.NopCloser(bytes.NewReader(obj.data)), &storage.ObjectInfo{
		ContentLength: int64(len(obj.data)),
		ETag:          obj.etag,
	}, nil
}

func (f *fakeStorage) PresignGetObject(_ context.Context, bucket, key string, _ time.Duration) (string, error) {
	return f.presignBase + "/" + bucket + "/" + key, nil
}

// helpers

func doRequest(t *testing.T, s storage.Storage, method, path string, headers map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := registry.SetupRoutes(s, testBucket).Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp
}

func assertStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want %d; body: %s", resp.StatusCode, want, body)
	}
}

func assertHeader(t *testing.T, resp *http.Response, key, want string) {
	t.Helper()
	if got := resp.Header.Get(key); got != want {
		t.Errorf("header %s: got %q, want %q", key, got, want)
	}
}

func assertBodyCode(t *testing.T, resp *http.Response, wantCode string) {
	t.Helper()
	body, _ := io.ReadAll(resp.Body)
	var er struct {
		Errors []struct {
			Code string `json:"code"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &er); err != nil || len(er.Errors) == 0 {
		t.Fatalf("could not parse error body: %s", body)
	}
	if er.Errors[0].Code != wantCode {
		t.Errorf("error code: got %q, want %q", er.Errors[0].Code, wantCode)
	}
}

func assertBodyEmpty(t *testing.T, resp *http.Response) {
	t.Helper()
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Errorf("expected empty body, got %q", body)
	}
}

// ---- TestIndex ----

func TestIndex(t *testing.T) {
	s := newFakeStorage()
	tests := []struct{ method, path string }{
		{"GET", "/v2"},
		{"GET", "/v2/"},
		{"HEAD", "/v2/"},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			resp := doRequest(t, s, tc.method, tc.path, nil)
			assertStatus(t, resp, http.StatusOK)
			if tc.method == "GET" {
				var body map[string]any
				_ = json.NewDecoder(resp.Body).Decode(&body)
				if body["success"] != true {
					t.Errorf("body: got %v, want {success:true}", body)
				}
			}
		})
	}
}

// ---- TestGetBlob ----

func TestGetBlob_GET_Existing(t *testing.T) {
	s := newFakeStorage()
	data := []byte("layer data")
	s.put(testBucket, "ns/repo/blobs/sha256/abc123", data, `"etag1"`)

	resp := doRequest(t, s, "GET", "/v2/ns/repo/blobs/sha256:abc123", nil)
	assertStatus(t, resp, http.StatusFound)

	want := s.presignBase + "/" + testBucket + "/ns/repo/blobs/sha256/abc123"
	assertHeader(t, resp, "Location", want)
}

func TestGetBlob_GET_Missing(t *testing.T) {
	resp := doRequest(t, newFakeStorage(), "GET", "/v2/ns/repo/blobs/sha256:notexist", nil)
	assertStatus(t, resp, http.StatusNotFound)
	assertBodyCode(t, resp, "BLOB_UNKNOWN")
}

func TestGetBlob_GET_EmptyBlob(t *testing.T) {
	s := newFakeStorage()
	s.put(testBucket, "ns/repo/blobs/sha256/empty", []byte{}, "")

	resp := doRequest(t, s, "GET", "/v2/ns/repo/blobs/sha256:empty", nil)
	assertStatus(t, resp, http.StatusNotFound)
	assertBodyCode(t, resp, "BLOB_UNKNOWN")
}

func TestGetBlob_GET_InvalidDigest(t *testing.T) {
	resp := doRequest(t, newFakeStorage(), "GET", "/v2/ns/repo/blobs/md5:abc123", nil)
	assertStatus(t, resp, http.StatusBadRequest)
	assertBodyCode(t, resp, "BLOB_UNKNOWN")
}

func TestGetBlob_HEAD_Existing(t *testing.T) {
	s := newFakeStorage()
	data := []byte("layer data")
	s.put(testBucket, "ns/repo/blobs/sha256/abc123", data, `"etag1"`)

	resp := doRequest(t, s, "HEAD", "/v2/ns/repo/blobs/sha256:abc123", nil)
	assertStatus(t, resp, http.StatusOK)
	assertHeader(t, resp, "Docker-Content-Digest", "sha256:abc123")
	assertHeader(t, resp, "Content-Length", fmt.Sprintf("%d", len(data)))
	assertHeader(t, resp, "ETag", `"etag1"`)
	assertBodyEmpty(t, resp)
}

func TestGetBlob_HEAD_Missing(t *testing.T) {
	resp := doRequest(t, newFakeStorage(), "HEAD", "/v2/ns/repo/blobs/sha256:notexist", nil)
	assertStatus(t, resp, http.StatusNotFound)
}

func TestGetBlob_MethodNotAllowed(t *testing.T) {
	resp := doRequest(t, newFakeStorage(), "POST", "/v2/ns/repo/blobs/sha256:abc123", nil)
	assertStatus(t, resp, http.StatusMethodNotAllowed)
}

// ---- TestGetManifest ----

const (
	testManifestDigest  = "sha256:deadbeef"
	testManifestContent = `{"schemaVersion":2,"config":{},"layers":[]}`
	testIndexJSON       = `{
		"schemaVersion": 2,
		"manifests": [{
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"digest": "sha256:deadbeef",
			"size": 43,
			"annotations": {"org.opencontainers.image.ref.name": "latest"}
		}]
	}`
)

func setupManifestFake() *fakeStorage {
	s := newFakeStorage()
	s.put(testBucket, "ns/repo/index.json", []byte(testIndexJSON), `"idx-etag"`)
	s.put(testBucket, "ns/repo/blobs/sha256/deadbeef", []byte(testManifestContent), `"manifest-etag"`)
	return s
}

func TestGetManifest_ByTag(t *testing.T) {
	resp := doRequest(t, setupManifestFake(), "GET", "/v2/ns/repo/manifests/latest", nil)
	assertStatus(t, resp, http.StatusOK)
	assertHeader(t, resp, "Content-Type", "application/vnd.oci.image.manifest.v1+json")
	assertHeader(t, resp, "Docker-Content-Digest", testManifestDigest)
	assertHeader(t, resp, "ETag", `"manifest-etag"`)

	body, _ := io.ReadAll(resp.Body)
	if string(body) != testManifestContent {
		t.Errorf("body: got %q, want %q", body, testManifestContent)
	}
}

func TestGetManifest_ByDigest(t *testing.T) {
	resp := doRequest(t, setupManifestFake(), "GET", "/v2/ns/repo/manifests/"+testManifestDigest, nil)
	assertStatus(t, resp, http.StatusOK)
	assertHeader(t, resp, "Docker-Content-Digest", testManifestDigest)
}

func TestGetManifest_HEAD(t *testing.T) {
	resp := doRequest(t, setupManifestFake(), "HEAD", "/v2/ns/repo/manifests/latest", nil)
	assertStatus(t, resp, http.StatusOK)
	assertHeader(t, resp, "Docker-Content-Digest", testManifestDigest)
	assertHeader(t, resp, "Content-Type", "application/vnd.oci.image.manifest.v1+json")
	assertBodyEmpty(t, resp)
}

func TestGetManifest_IfNoneMatch_Match(t *testing.T) {
	resp := doRequest(t, setupManifestFake(), "GET", "/v2/ns/repo/manifests/latest", map[string]string{
		"If-None-Match": `"manifest-etag"`,
	})
	assertStatus(t, resp, http.StatusNotModified)
	assertHeader(t, resp, "Docker-Content-Digest", testManifestDigest)
	assertBodyEmpty(t, resp)
}

func TestGetManifest_IfNoneMatch_NoMatch(t *testing.T) {
	resp := doRequest(t, setupManifestFake(), "GET", "/v2/ns/repo/manifests/latest", map[string]string{
		"If-None-Match": `"other-etag"`,
	})
	assertStatus(t, resp, http.StatusOK)
}

func TestGetManifest_IndexMissing(t *testing.T) {
	resp := doRequest(t, newFakeStorage(), "GET", "/v2/ns/repo/manifests/latest", nil)
	assertStatus(t, resp, http.StatusNotFound)
	assertBodyCode(t, resp, "MANIFEST_UNKNOWN")
}

func TestGetManifest_TagNotFound(t *testing.T) {
	s := setupManifestFake()
	resp := doRequest(t, s, "GET", "/v2/ns/repo/manifests/nonexistent-tag", nil)
	assertStatus(t, resp, http.StatusNotFound)
	assertBodyCode(t, resp, "MANIFEST_UNKNOWN")
}

func TestGetManifest_ManifestBlobMissing(t *testing.T) {
	s := newFakeStorage()
	s.put(testBucket, "ns/repo/index.json", []byte(testIndexJSON), `"idx-etag"`)
	// intentionally do NOT put the manifest blob

	resp := doRequest(t, s, "GET", "/v2/ns/repo/manifests/latest", nil)
	assertStatus(t, resp, http.StatusNotFound)
	assertBodyCode(t, resp, "MANIFEST_UNKNOWN")
}

func TestGetManifest_RateLimit(t *testing.T) {
	s := setupManifestFake()
	s.rateLimitKey = testBucket + "/ns/repo/index.json"

	resp := doRequest(t, s, "GET", "/v2/ns/repo/manifests/latest", nil)
	assertStatus(t, resp, http.StatusTooManyRequests)
	assertBodyCode(t, resp, "TOOMANYREQUESTS")
}

func TestGetManifest_InvalidIndexJSON(t *testing.T) {
	s := newFakeStorage()
	s.put(testBucket, "ns/repo/index.json", []byte("not json"), `"etag"`)

	resp := doRequest(t, s, "GET", "/v2/ns/repo/manifests/latest", nil)
	assertStatus(t, resp, http.StatusNotFound)
	assertBodyCode(t, resp, "MANIFEST_UNKNOWN")
}

// ---- TestPaths ----

func TestPaths(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"blobPath", registry.BlobPathExported("ns", "repo", "abc"), "ns/repo/blobs/sha256/abc"},
		{"indexPath", registry.IndexPathExported("ns", "repo"), "ns/repo/index.json"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}
