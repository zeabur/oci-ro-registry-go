package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

var ErrObjectNotFound = errors.New("object not found")

type ObjectInfo struct {
	ContentLength int64
	ETag          string
}

type PutObjectOptions struct {
	ContentType  string
	CacheControl string
}

type ReadStorage interface {
	StatObject(ctx context.Context, bucket, key string) (*ObjectInfo, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error)
	PresignGetObject(ctx context.Context, bucket, key string, expiry time.Duration) (string, error)
}

type WriteStorage interface {
	PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, opts PutObjectOptions) error
}

type Storage interface {
	ReadStorage
	WriteStorage
}
