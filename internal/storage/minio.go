package storage

import (
	"context"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioStorage struct {
	client *minio.Client
}

func NewMinioStorage(endpoint, accessKeyID, secretKey, region string, useSSL, pathStyle bool) (*MinioStorage, error) {
	lookup := minio.BucketLookupDNS
	if pathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKeyID, secretKey, ""),
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	return &MinioStorage{client: client}, nil
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
		NumThreads:   4,
	})
	return err
}

func isMinioNotFound(err error) bool {
	errResp := minio.ToErrorResponse(err)
	return errResp.StatusCode == 404 || errResp.Code == "NoSuchKey" || errResp.Code == "NotFound"
}
