package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/zeabur/stratus/internal/storage"
)

const blobUploadConcurrency = 4

type pushOptions struct {
	logOutput io.Writer
}

type PushOptionsFn func(opts *pushOptions)

func WithLogOutput(output io.Writer) PushOptionsFn {
	return func(opts *pushOptions) {
		opts.logOutput = output
	}
}

// PushOciLayout pushes an OCI layout filesystem to the provided storage backend.
func PushOciLayout(ctx context.Context, dst storage.Storage, bucket string, src fs.FS, imageName string, tag string, options ...PushOptionsFn) error {
	pushOpts := &pushOptions{
		logOutput: os.Stderr,
	}
	for _, opt := range options {
		opt(pushOpts)
	}
	out := pushOpts.logOutput

	if src == nil {
		return fmt.Errorf("source filesystem is not configured")
	}
	if dst == nil {
		return fmt.Errorf("destination storage is not configured")
	}

	// Validate OCI layout filesystem
	if _, err := fs.Stat(src, "index.json"); err != nil {
		return fmt.Errorf("index.json not found in OCI layout: %w", err)
	}
	if _, err := fs.Stat(src, "oci-layout"); err != nil {
		return fmt.Errorf("oci-layout not found in OCI layout: %w", err)
	}

	manifestUpdater := NewOCIManifestUpdater(dst, bucket)

	// Read local index
	localIndex, err := readOciIndexFromFS(src, "index.json")
	if err != nil {
		return fmt.Errorf("read local index: %w", err)
	}
	_, _ = fmt.Fprintf(out, "📋 Loaded local index with %d manifests\n", len(localIndex.Manifests))

	if len(localIndex.Manifests) == 0 {
		return fmt.Errorf("local index has no manifests")
	}

	// Set ref.name to the tag
	if localIndex.Manifests[0].Annotations == nil {
		localIndex.Manifests[0].Annotations = make(map[string]string)
	}
	localIndex.Manifests[0].Annotations["org.opencontainers.image.ref.name"] = tag

	// Fetch existing remote index and merge
	_, _ = fmt.Fprintf(out, "🔍 Checking remote index for %s...\n", imageName)
	existingIndex, err := manifestUpdater.GetRemoteIndex(ctx, imageName)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			_, _ = fmt.Fprintf(out, "🆕 No existing remote index found\n")
		} else {
			_, _ = fmt.Fprintf(out, "⚠️ Warning: failed to fetch remote index: %v\n", err)
		}
	} else {
		_, _ = fmt.Fprintf(out, "📥 Fetched remote index with %d manifests\n", len(existingIndex.Manifests))
	}

	_, _ = fmt.Fprintf(out, "🔄 Merging indexes...\n")
	mergedIndex := manifestUpdater.MergeIndex(existingIndex, localIndex)
	_, _ = fmt.Fprintf(out, "✅ Merged index contains %d manifests\n", len(mergedIndex.Manifests))

	// Upload blobs concurrently
	blobHexDigests, err := listBlobsFromFS(src)
	if err != nil {
		return fmt.Errorf("list local blobs: %w", err)
	}
	_, _ = fmt.Fprintf(out, "📦 Found %d local blobs to upload\n", len(blobHexDigests))

	if len(blobHexDigests) > 0 {
		_, _ = fmt.Fprintf(out, "🚀 Starting concurrent blob uploads (concurrency: %d)...\n", blobUploadConcurrency)
		err = uploadBlobsConcurrently(ctx, dst, bucket, src, imageName, blobHexDigests, blobUploadConcurrency, out)
		if err != nil {
			return fmt.Errorf("upload blobs: %w", err)
		}
	}

	// Upload merged index and oci-layout (final step to avoid referencing missing blobs)
	_, _ = fmt.Fprintf(out, "📋 Uploading updated index...\n")
	indexObj, err := NewJSONUploadObject(mergedIndex, "application/vnd.oci.image.index.v1+json", "index.json")
	if err != nil {
		return fmt.Errorf("prepare index upload object: %w", err)
	}
	if err := uploadWithRetry(ctx, dst, bucket, s3IndexPath(imageName), indexObj, out); err != nil {
		return fmt.Errorf("upload index: %w", err)
	}

	_, _ = fmt.Fprintf(out, "📋 Uploading OCI layout metadata...\n")
	layoutObj, err := NewFSUploadObject(src, "oci-layout", "application/json", CacheControlNoCache, "oci-layout")
	if err != nil {
		return fmt.Errorf("prepare oci-layout upload object: %w", err)
	}
	if err := uploadWithRetry(ctx, dst, bucket, s3OciLayoutPath(imageName), layoutObj, out); err != nil {
		return fmt.Errorf("upload oci-layout: %w", err)
	}

	_, _ = fmt.Fprintf(out, "✅ Upload complete! Uploaded %d blobs and updated index for %s\n", len(blobHexDigests), imageName)
	return nil
}

func s3RepoPath(repo string) string {
	if !strings.Contains(repo, "/") {
		return "library/" + repo
	}
	return repo
}

func s3IndexPath(repo string) string {
	return s3RepoPath(repo) + "/index.json"
}

func s3OciLayoutPath(repo string) string {
	return s3RepoPath(repo) + "/oci-layout"
}

func s3BlobPath(repo, digestHexWithoutPrefix string) string {
	return s3RepoPath(repo) + "/blobs/sha256/" + digestHexWithoutPrefix
}

// readOciIndexFromFS reads and parses an OCI index.json file from a filesystem
func readOciIndexFromFS(fsys fs.FS, filePath string) (*OciIndex, error) {
	data, err := fs.ReadFile(fsys, filePath)
	if err != nil {
		return nil, err
	}
	var index OciIndex
	err = json.Unmarshal(data, &index)
	return &index, err
}

// listBlobsFromFS returns hex digests of all blob files in the OCI layout filesystem
func listBlobsFromFS(fsys fs.FS) ([]string, error) {
	blobsDir := "blobs/sha256"
	entries, err := fs.ReadDir(fsys, blobsDir)
	if err != nil {
		return nil, err
	}

	var hexDigests []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		hexDigests = append(hexDigests, entry.Name())
	}
	return hexDigests, nil
}

// uploadBlobsConcurrently uploads all blobs with controlled concurrency, skipping existing ones.
func uploadBlobsConcurrently(ctx context.Context, s storage.Storage, bucket string, fsys fs.FS, repo string, hexDigests []string, concurrency int, output io.Writer) error {
	tasks := make([]UploadTask, 0, len(hexDigests))
	for _, hex := range hexDigests {
		localBlobPath := path.Join("blobs", "sha256", hex)
		label := getBriefDigest(hex)

		uploadObj, err := NewFSUploadObject(fsys, localBlobPath, "application/octet-stream", CacheControlPublicImmutable, label)
		if err != nil {
			return fmt.Errorf("prepare blob %s: %w", label, err)
		}

		task := UploadTask{
			Key:       s3BlobPath(repo, hex),
			Object:    uploadObj,
			SkipCheck: newBlobSkipCheck(label, output),
		}

		tasks = append(tasks, task)
	}

	return uploadTasksConcurrently(ctx, s, bucket, tasks, concurrency, output)
}

func newBlobSkipCheck(label string, output io.Writer) SkipCheckFunc {
	return func(ctx context.Context, s storage.Storage, bucket, key string) (SkipResult, error) {
		_, err := s.StatObject(ctx, bucket, key)
		switch {
		case err == nil:
			return SkipResult{Skip: true, Reason: "Layer already exists, skipping"}, nil
		case errors.Is(err, storage.ErrObjectNotFound):
			_, _ = fmt.Fprintf(output, "🔍 %s | New blob found, uploading\n", label)
		default:
			_, _ = fmt.Fprintf(output, "⚠️ %s | Failed to check existence: %v, proceeding with upload\n", label, err)
		}

		_, _ = fmt.Fprintf(output, "📤 %s | Processing blob\n", label)
		return SkipResult{}, nil
	}
}

// getBriefDigest returns the first 16 characters of a digest
func getBriefDigest(digest string) string {
	if len(digest) < 16 {
		return digest
	}
	return digest[:16]
}
