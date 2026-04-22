package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port                         string
	BucketName                   string
	S3Endpoint                   string
	S3AccessKeyID                string
	S3SecretKey                  string
	S3UseSSL                     bool
	S3Region                     string
	S3PathStyle                  bool
	S3MultipartUploadConcurrency uint
}

func Load() Config {
	return Config{
		Port:                         getEnvOrDefault("PORT", "3000"),
		BucketName:                   getEnvOrDefault("S3_BUCKET_NAME", "zeabur-oci-registry"),
		S3Endpoint:                   os.Getenv("S3_ENDPOINT"),
		S3AccessKeyID:                os.Getenv("S3_ACCESS_KEY_ID"),
		S3SecretKey:                  os.Getenv("S3_SECRET_ACCESS_KEY"),
		S3UseSSL:                     os.Getenv("S3_USE_SSL") == "true",
		S3Region:                     getEnvOrDefault("S3_REGION", "us-east-1"),
		S3PathStyle:                  os.Getenv("S3_PATH_STYLE") == "true",
		S3MultipartUploadConcurrency: getEnvOrDefaultUint("S3_MULTIPART_UPLOAD_CONCURRENCY", 4),
	}
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvOrDefaultUint(key string, fallback uint) uint {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err == nil {
			return uint(n)
		}
	}
	return fallback
}
