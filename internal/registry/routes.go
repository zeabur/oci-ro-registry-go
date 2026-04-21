package registry

import (
	"github.com/gofiber/fiber/v3"
	"github.com/zeabur/oci-ro-registry-go/internal/storage"
)

func SetupRoutes(s storage.Storage, bucketName string) *fiber.App {
	app := fiber.New()

	h := NewHandler(s, bucketName)

	app.Get("/v2", h.Index)
	app.Head("/v2", h.Index)
	app.Get("/v2/", h.Index)
	app.Head("/v2/", h.Index)

	app.Get("/v2/:namespace/:repository/blobs/:digest", h.GetBlob)
	app.Head("/v2/:namespace/:repository/blobs/:digest", h.GetBlob)

	app.Get("/v2/:namespace/:repository/manifests/:reference", h.GetManifest)
	app.Head("/v2/:namespace/:repository/manifests/:reference", h.GetManifest)

	return app
}
