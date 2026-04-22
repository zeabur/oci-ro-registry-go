package push

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/zeabur/stratus/internal/storage"
)

// NewStorageFromEnvironment creates a MinioStorage and bucket from environment variables.
// STORAGE_TYPE selects the backend: "s3" / "minio" (default) or "cos".
func NewStorageFromEnvironment(_ context.Context) (*storage.MinioStorage, string, error) {
	storageType := os.Getenv("STORAGE_TYPE")
	if storageType == "" {
		storageType = "s3"
	}

	switch storageType {
	case "s3", "minio":
		return NewMinioStorageFromEnvironment()
	case "cos":
		return newCOSMinioStorageFromEnvironment()
	default:
		return nil, "", fmt.Errorf("invalid storage type: %s", storageType)
	}
}

func newCOSMinioStorageFromEnvironment() (*storage.MinioStorage, string, error) {
	secretID := os.Getenv("PUSHER_COS_SECRET_ID")
	secretKey := os.Getenv("PUSHER_COS_SECRET_KEY")
	bucketURL := os.Getenv("PUSHER_COS_BUCKET_URL")

	if secretID == "" || secretKey == "" || bucketURL == "" {
		return nil, "", fmt.Errorf("missing COS environment variables: PUSHER_COS_SECRET_ID, PUSHER_COS_SECRET_KEY, PUSHER_COS_BUCKET_URL")
	}

	u, err := url.Parse(bucketURL)
	if err != nil {
		return nil, "", fmt.Errorf("parse COS bucket URL: %w", err)
	}

	// BucketURL is https://{bucket}.cos.{region}.myqcloud.com — strip bucket prefix.
	host := u.Host
	dotIdx := strings.Index(host, ".")
	if dotIdx < 0 {
		return nil, "", fmt.Errorf("invalid COS bucket URL host %q: cannot extract bucket name", host)
	}
	bucket := host[:dotIdx]
	endpoint := host[dotIdx+1:]
	useSSL := u.Scheme == "https"

	st, err := storage.NewMinioStorage(endpoint, secretID, secretKey, "", useSSL, false)
	if err != nil {
		return nil, "", fmt.Errorf("create minio storage for COS: %w", err)
	}
	return st, bucket, nil
}

// MinioConfig holds minio-go compatible storage configuration.
type MinioConfig struct {
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	Endpoint        string
	Region          string
	UseSSL          bool
	PathStyle       bool
}

func loadMinioConfig() (*MinioConfig, error) {
	accessKeyID := os.Getenv("PUSHER_S3_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("PUSHER_S3_SECRET_ACCESS_KEY")
	bucket := os.Getenv("PUSHER_S3_BUCKET")
	rawEndpoint := os.Getenv("PUSHER_S3_ENDPOINT")
	region := os.Getenv("PUSHER_S3_REGION")

	if accessKeyID == "" || secretAccessKey == "" || bucket == "" || rawEndpoint == "" || region == "" {
		return nil, fmt.Errorf("missing S3 environment variables: PUSHER_S3_ACCESS_KEY_ID, PUSHER_S3_SECRET_ACCESS_KEY, PUSHER_S3_BUCKET, PUSHER_S3_ENDPOINT, PUSHER_S3_REGION")
	}

	endpoint, useSSL := parseEndpoint(rawEndpoint)
	if v := os.Getenv("PUSHER_S3_USE_SSL"); v != "" {
		useSSL = v == "true"
	}
	pathStyle := os.Getenv("PUSHER_S3_PATH_STYLE") == "true"

	return &MinioConfig{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Bucket:          bucket,
		Endpoint:        endpoint,
		Region:          region,
		UseSSL:          useSSL,
		PathStyle:       pathStyle,
	}, nil
}

// NewMinioStorageFromEnvironment creates a MinioStorage and bucket from PUSHER_S3_* env vars.
func NewMinioStorageFromEnvironment() (*storage.MinioStorage, string, error) {
	cfg, err := loadMinioConfig()
	if err != nil {
		return nil, "", err
	}
	st, err := storage.NewMinioStorage(cfg.Endpoint, cfg.AccessKeyID, cfg.SecretAccessKey, cfg.Region, cfg.UseSSL, cfg.PathStyle)
	if err != nil {
		return nil, "", fmt.Errorf("create minio storage: %w", err)
	}
	return st, cfg.Bucket, nil
}

func parseEndpoint(rawEndpoint string) (host string, useSSL bool) {
	u, err := url.Parse(rawEndpoint)
	if err == nil && u.Host != "" {
		return u.Host, u.Scheme == "https"
	}
	return rawEndpoint, true
}
