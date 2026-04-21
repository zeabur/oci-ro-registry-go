package main

import (
	"log/slog"
	"os"

	"github.com/zeabur/oci-ro-registry-go/internal/config"
	"github.com/zeabur/oci-ro-registry-go/internal/registry"
	"github.com/zeabur/oci-ro-registry-go/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := config.Load()

	s3, err := storage.NewMinioStorage(cfg.S3Endpoint, cfg.S3AccessKeyID, cfg.S3SecretKey, cfg.S3Region, cfg.S3UseSSL)
	if err != nil {
		slog.Error("failed to create storage client", "error", err)
		os.Exit(1)
	}

	app := registry.SetupRoutes(s3, cfg.BucketName)
	if err := app.Listen(":" + cfg.Port); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
