package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/zeabur/oci-ro-registry-go/internal/storage"
)

const (
	sha256Prefix  = "sha256:"
	presignExpiry = 30 * time.Minute
	manifestCT    = "application/vnd.oci.image.manifest.v1+json"
)

type Handler struct {
	storage    storage.Storage
	bucketName string
}

func NewHandler(s storage.Storage, bucketName string) *Handler {
	return &Handler{storage: s, bucketName: bucketName}
}

func (h *Handler) Index(c fiber.Ctx) error {
	return c.JSON(fiber.Map{"success": true})
}

func (h *Handler) GetBlob(c fiber.Ctx) error {
	namespace := c.Params("namespace")
	repository := c.Params("repository")
	digest := c.Params("digest")

	if !strings.HasPrefix(digest, sha256Prefix) {
		return c.Status(fiber.StatusBadRequest).JSON(errResponse("BLOB_UNKNOWN", "invalid digest: must start with sha256:"))
	}

	digestWithoutPrefix := digest[len(sha256Prefix):]
	key := blobPath(namespace, repository, digestWithoutPrefix)

	info, err := h.storage.StatObject(c.Context(), h.bucketName, key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(errResponse("BLOB_UNKNOWN", "blob unknown to registry"))
		}
		return err
	}

	if info.ContentLength == 0 {
		return c.Status(fiber.StatusNotFound).JSON(errResponse("BLOB_UNKNOWN", "blob unknown to registry (empty or missing)"))
	}

	c.Set("Content-Length", fmt.Sprintf("%d", info.ContentLength))
	c.Set("Docker-Content-Digest", digest)
	if info.ETag != "" {
		c.Set("ETag", info.ETag)
	}

	if c.Method() == fiber.MethodHead {
		c.Status(fiber.StatusOK)
		return nil
	}

	presignedURL, err := h.storage.PresignGetObject(c.Context(), h.bucketName, key, presignExpiry)
	if err != nil {
		return err
	}

	return c.Redirect().Status(fiber.StatusFound).To(presignedURL)
}

func (h *Handler) GetManifest(c fiber.Ctx) error {
	namespace := c.Params("namespace")
	repository := c.Params("repository")
	reference := c.Params("reference")

	idxBody, _, err := h.storage.GetObject(c.Context(), h.bucketName, indexPath(namespace, repository))
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(errResponse("MANIFEST_UNKNOWN", "manifest unknown to registry"))
		}
		if strings.Contains(err.Error(), "10058") {
			return c.Status(fiber.StatusTooManyRequests).JSON(errResponse("TOOMANYREQUESTS", fmt.Sprintf("too many requests to object %q", namespace+"/"+repository+":"+reference)))
		}
		return err
	}
	defer func() {
		_ = idxBody.Close()
	}()

	var idx OciManifestIndex
	if err := json.NewDecoder(idxBody).Decode(&idx); err != nil || idx.SchemaVersion != 2 {
		return c.Status(fiber.StatusNotFound).JSON(errResponse("MANIFEST_UNKNOWN", "manifest unknown to registry (invalid index)"))
	}

	var found *OciManifest
	for i := range idx.Manifests {
		m := &idx.Manifests[i]
		if m.Annotations["org.opencontainers.image.ref.name"] == reference || m.Digest == reference {
			found = m
			break
		}
	}
	if found == nil {
		return c.Status(fiber.StatusNotFound).JSON(errResponse("MANIFEST_UNKNOWN", "manifest unknown to registry (no such tag)"))
	}

	manifestDigestWithoutPrefix := found.Digest[len(sha256Prefix):]
	manifestKey := blobPath(namespace, repository, manifestDigestWithoutPrefix)

	manifestBody, manifestInfo, err := h.storage.GetObject(c.Context(), h.bucketName, manifestKey)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(errResponse("MANIFEST_UNKNOWN", "manifest unknown to registry (no manifest body)"))
		}
		if strings.Contains(err.Error(), "10058") {
			return c.Status(fiber.StatusTooManyRequests).JSON(errResponse("TOOMANYREQUESTS", fmt.Sprintf("too many requests to object %q", namespace+"/"+repository+":"+reference)))
		}
		return err
	}
	defer func() {
		_ = manifestBody.Close()
	}()

	ifNoneMatch := c.Get("If-None-Match")
	if ifNoneMatch != "" && manifestInfo.ETag != "" && ifNoneMatch == manifestInfo.ETag {
		c.Set("Docker-Content-Digest", found.Digest)
		c.Set("ETag", manifestInfo.ETag)
		return c.SendStatus(fiber.StatusNotModified)
	}

	c.Set("Content-Type", manifestCT)
	c.Set("Content-Length", fmt.Sprintf("%d", found.Size))
	c.Set("Docker-Content-Digest", found.Digest)
	c.Set("Docker-Content-Length", fmt.Sprintf("%d", found.Size))
	if manifestInfo.ETag != "" {
		c.Set("ETag", manifestInfo.ETag)
	}

	if c.Method() == fiber.MethodHead {
		c.Status(fiber.StatusOK)
		return nil
	}

	return c.SendStream(manifestBody, int(found.Size))
}
