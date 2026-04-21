package main

import (
	"log"

	"github.com/zeabur/oci-ro-registry-go/internal/config"
	"github.com/zeabur/oci-ro-registry-go/internal/registry"
	"github.com/zeabur/oci-ro-registry-go/internal/storage"
)

func main() {
	cfg := config.Load()

	s3, err := storage.NewMinioStorage(cfg.S3Endpoint, cfg.S3AccessKeyID, cfg.S3SecretKey, cfg.S3Region, cfg.S3UseSSL)
	if err != nil {
		log.Fatalf("failed to create storage client: %v", err)
	}

	app := registry.SetupRoutes(s3, cfg.BucketName)
	log.Fatal(app.Listen(":" + cfg.Port))
}
