package main

import (
	"log/slog"
	"os"

	"github.com/zeabur/stratus/v2/internal/registryapi"
	"github.com/zeabur/stratus/v2/pkg/config"
	"github.com/zeabur/stratus/v2/pkg/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := config.Load()

	s3, err := storage.MinioStorageFromConfig(cfg)
	if err != nil {
		slog.Error("failed to create storage client", "error", err)
		os.Exit(1)
	}

	app := registryapi.SetupRoutes(s3, cfg.BucketName)
	if err := app.Listen(":" + cfg.Port); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
