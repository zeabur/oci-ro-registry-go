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

func NewMinioStorage(endpoint, accessKeyID, secretKey, region string, useSSL bool) (*MinioStorage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretKey, ""),
		Secure: useSSL,
		Region: region,
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
		obj.Close()
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

func isMinioNotFound(err error) bool {
	errResp := minio.ToErrorResponse(err)
	return errResp.StatusCode == 404 || errResp.Code == "NoSuchKey" || errResp.Code == "NotFound"
}
