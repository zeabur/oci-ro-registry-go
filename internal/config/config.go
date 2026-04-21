package config

import "os"

type Config struct {
	Port          string
	BucketName    string
	S3Endpoint    string
	S3AccessKeyID string
	S3SecretKey   string
	S3UseSSL      bool
	S3Region      string
}

func Load() *Config {
	return &Config{
		Port:          getEnvOrDefault("PORT", "3000"),
		BucketName:    getEnvOrDefault("S3_BUCKET_NAME", "zeabur-oci-registry"),
		S3Endpoint:    os.Getenv("S3_ENDPOINT"),
		S3AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		S3SecretKey:   os.Getenv("S3_SECRET_ACCESS_KEY"),
		S3UseSSL:      os.Getenv("S3_USE_SSL") == "true",
		S3Region:      getEnvOrDefault("S3_REGION", "us-east-1"),
	}
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
