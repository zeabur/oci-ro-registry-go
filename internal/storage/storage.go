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

type Storage interface {
	StatObject(ctx context.Context, bucket, key string) (*ObjectInfo, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error)
	PresignGetObject(ctx context.Context, bucket, key string, expiry time.Duration) (string, error)
}
