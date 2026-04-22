package registry

import (
	"log/slog"

	"github.com/gofiber/fiber/v3"
	slogfiber "github.com/samber/slog-fiber"
	"github.com/zeabur/stratus/internal/storage"
)

func SetupRoutes(s storage.ReadStorage, bucketName string) *fiber.App {
	app := fiber.New()
	app.Use(slogfiber.New(slog.Default()))

	h := NewHandler(s, bucketName)

	app.Get("/v2", h.Index)
	app.Head("/v2", h.Index)

	app.Get("/v2/:namespace/:repository/blobs/:digest", h.GetBlob)
	app.Head("/v2/:namespace/:repository/blobs/:digest", h.GetBlob)

	app.Get("/v2/:namespace/:repository/manifests/:reference", h.GetManifest)
	app.Head("/v2/:namespace/:repository/manifests/:reference", h.GetManifest)

	return app
}
