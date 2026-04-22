package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zeabur/stratus/v2/pkg/config"
)

var ErrMissingConfig = fmt.Errorf("missing required configuration")

type MinioStorage struct {
	client                     *minio.Client
	multipartUploadConcurrency uint
}

type minioStorageFactoryConfig struct {
	useSSL                     bool
	pathStyle                  bool
	multipartUploadConcurrency uint
}

type MinioStorageOption func(*minioStorageFactoryConfig)

func WithMinioMultipartUploadConcurrency(n uint) MinioStorageOption {
	return func(cfg *minioStorageFactoryConfig) {
		cfg.multipartUploadConcurrency = n
	}
}

func WithMinioPathStyle(pathStyle bool) MinioStorageOption {
	return func(cfg *minioStorageFactoryConfig) {
		cfg.pathStyle = pathStyle
	}
}

func WithMinioUseSSL(useSSL bool) MinioStorageOption {
	return func(cfg *minioStorageFactoryConfig) {
		cfg.useSSL = useSSL
	}
}

func NewMinioStorage(endpoint, accessKeyID, secretKey, region string, options ...MinioStorageOption) (*MinioStorage, error) {
	cfg := &minioStorageFactoryConfig{
		useSSL:                     true,
		pathStyle:                  false,
		multipartUploadConcurrency: 4,
	}
	for _, opt := range options {
		opt(cfg)
	}

	lookup := minio.BucketLookupDNS
	if cfg.pathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKeyID, secretKey, ""),
		Secure:       cfg.useSSL,
		Region:       region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	return &MinioStorage{client: client, multipartUploadConcurrency: cfg.multipartUploadConcurrency}, nil
}

func MinioStorageFromConfig(cfg config.Config) (*MinioStorage, error) {
	if cfg.S3AccessKeyID == "" || cfg.S3Endpoint == "" || cfg.S3SecretKey == "" {
		return nil, ErrMissingConfig
	}

	return NewMinioStorage(
		cfg.S3Endpoint,
		cfg.S3AccessKeyID,
		cfg.S3SecretKey,
		cfg.S3Region,
		WithMinioUseSSL(cfg.S3UseSSL),
		WithMinioPathStyle(cfg.S3PathStyle),
		WithMinioMultipartUploadConcurrency(cfg.S3MultipartUploadConcurrency),
	)
}

func (s *MinioStorage) StatObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	info, err := s.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if isMinioNotFound(err) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return &ObjectInfo{
		ContentLength: info.Size,
		ETag:          info.ETag,
	}, nil
}

func (s *MinioStorage) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error) {
	obj, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		if isMinioNotFound(err) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}

	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		if isMinioNotFound(err) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}

	return obj, &ObjectInfo{
		ContentLength: stat.Size,
		ETag:          stat.ETag,
	}, nil
}

func (s *MinioStorage) PresignGetObject(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	u, err := s.client.PresignedGetObject(ctx, bucket, key, expiry, url.Values{})
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *MinioStorage) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, opts PutObjectOptions) error {
	_, err := s.client.PutObject(ctx, bucket, key, body, size, minio.PutObjectOptions{
		ContentType:  opts.ContentType,
		CacheControl: opts.CacheControl,
		NumThreads:   uint(s.multipartUploadConcurrency),
	})
	return err
}

// GetClient exposes the underlying Minio client for advanced operations not covered by the Storage interface.
// It is mainly for integration test and you should not use it in production code.
func (s *MinioStorage) GetClient() *minio.Client {
	return s.client
}

func isMinioNotFound(err error) bool {
	errResp := minio.ToErrorResponse(err)
	return errResp.StatusCode == 404 || errResp.Code == "NoSuchKey" || errResp.Code == "NotFound"
}
