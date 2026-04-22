package push

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/zeabur/stratus/pkg/storage"
)

// UploadObject describes a reusable payload that can be uploaded multiple times (re-opened on retry).
type UploadObject interface {
	Open(ctx context.Context) (io.ReadCloser, error)
	Size() int64
	ContentType() string
	CacheControl(key string) string
	Label() string
}

// UploadTask represents one unit of upload work.
type UploadTask struct {
	Key       string
	Object    UploadObject
	SkipCheck SkipCheckFunc
}

// SkipCheckFunc decides whether a task needs to run.
type SkipCheckFunc func(ctx context.Context, s storage.Storage, bucket, key string) (SkipResult, error)

// SkipResult returns both the boolean decision and an optional reason for logging.
type SkipResult struct {
	Skip   bool
	Reason string
}

const defaultMaxUploadAttempts = 3

var uploadRetrySleep = time.Sleep

// uploadWithRetry uploads a single object with retry semantics shared by all payloads.
func uploadWithRetry(ctx context.Context, s storage.Storage, bucket, key string, object UploadObject, output io.Writer) error {
	if object == nil {
		return fmt.Errorf("nil upload object for key %s", key)
	}
	label := object.Label()
	if label == "" {
		label = path.Base(key)
	}
	size := object.Size()

	var lastErr error
	for attempt := 1; attempt <= defaultMaxUploadAttempts; attempt++ {
		reader, err := object.Open(ctx)
		if err != nil {
			return fmt.Errorf("%s | open object: %w", label, err)
		}

		_, _ = fmt.Fprintf(output, "🚀 %s | Uploading (size=%d bytes, attempt %d/%d)\n", label, size, attempt, defaultMaxUploadAttempts)

		err = s.PutObject(ctx, bucket, key, reader, size, storage.PutObjectOptions{
			ContentType:  object.ContentType(),
			CacheControl: object.CacheControl(key),
		})
		_ = reader.Close()
		if err == nil {
			_, _ = fmt.Fprintf(output, "✅ %s | Upload complete (%d bytes)\n", label, size)
			return nil
		}

		lastErr = err
		_, _ = fmt.Fprintf(output, "❌ %s | Upload failed on attempt %d: %v\n", label, attempt, err)
		if attempt < defaultMaxUploadAttempts {
			uploadRetrySleep(time.Duration(attempt) * time.Second)
		}
	}

	if lastErr != nil {
		return fmt.Errorf("upload failed after %d attempts: %w", defaultMaxUploadAttempts, lastErr)
	}

	return fmt.Errorf("upload failed after %d attempts", defaultMaxUploadAttempts)
}

// uploadTasksConcurrently uploads a batch of tasks with bounded concurrency and shared summary logging.
func uploadTasksConcurrently(ctx context.Context, s storage.Storage, bucket string, tasks []UploadTask, concurrency int, output io.Writer) error {
	if concurrency <= 0 {
		concurrency = 1
	}
	if len(tasks) == 0 {
		_, _ = fmt.Fprintln(output, "📭 No upload tasks supplied")
		return nil
	}

	var uploaded atomic.Int64
	var skipped atomic.Int64

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, task := range tasks {
		g.Go(func() error {
			label := task.Object.Label()
			if label == "" {
				label = path.Base(task.Key)
			}

			if task.SkipCheck != nil {
				result, err := task.SkipCheck(ctx, s, bucket, task.Key)
				if err != nil {
					return fmt.Errorf("%s | skip check: %w", label, err)
				}
				if result.Skip {
					reason := result.Reason
					if reason == "" {
						reason = "skip predicate satisfied"
					}
					skipped.Add(1)
					_, _ = fmt.Fprintf(output, "⏭️ %s | %s\n", label, reason)
					return nil
				}
			}

			if err := uploadWithRetry(ctx, s, bucket, task.Key, task.Object, output); err != nil {
				return err
			}
			uploaded.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(output, "📊 Upload summary: %d uploaded, %d skipped\n", uploaded.Load(), skipped.Load())
	return nil
}

// NewFSUploadObject builds an UploadObject backed by a file inside an fs.FS implementation.
func NewFSUploadObject(fsys fs.FS, filePath, contentType, cacheControl, label string) (UploadObject, error) {
	stat, err := fs.Stat(fsys, filePath)
	if err != nil {
		return nil, err
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if cacheControl == "" {
		cacheControl = CacheControlNoCache
	}
	if label == "" {
		label = path.Base(filePath)
	}

	return &fsUploadObject{
		fsys:         fsys,
		filePath:     filePath,
		size:         stat.Size(),
		contentType:  contentType,
		cacheControl: cacheControl,
		label:        label,
	}, nil
}

// NewJSONUploadObject marshals obj and stores it as an UploadObject.
func NewJSONUploadObject(obj any, contentType, label string) (UploadObject, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal upload json: %w", err)
	}
	if contentType == "" {
		contentType = "application/json"
	}
	return &jsonUploadObject{
		data:        data,
		contentType: contentType,
		label:       label,
	}, nil
}

type fsUploadObject struct {
	fsys         fs.FS
	filePath     string
	size         int64
	contentType  string
	cacheControl string
	label        string
}

func (o *fsUploadObject) Open(ctx context.Context) (io.ReadCloser, error) {
	return o.fsys.Open(o.filePath)
}

func (o *fsUploadObject) Size() int64 {
	return o.size
}

func (o *fsUploadObject) ContentType() string {
	return o.contentType
}

func (o *fsUploadObject) CacheControl(string) string {
	return o.cacheControl
}

func (o *fsUploadObject) Label() string {
	return o.label
}

type jsonUploadObject struct {
	data        []byte
	contentType string
	label       string
}

func (o *jsonUploadObject) Open(ctx context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

func (o *jsonUploadObject) Size() int64 {
	return int64(len(o.data))
}

func (o *jsonUploadObject) ContentType() string {
	return o.contentType
}

func (o *jsonUploadObject) CacheControl(string) string {
	return CacheControlNoCache
}

func (o *jsonUploadObject) Label() string {
	return o.label
}
