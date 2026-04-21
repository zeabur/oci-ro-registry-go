# OCI Read-Only Registry — Go

## What this is

A read-only OCI Distribution Spec v1 registry (Docker Registry V2 API) backed by MinIO/S3.
Images are served directly from S3 — blobs via presigned-URL redirect, manifests streamed.
Write operations are not implemented.

## Project layout

```
main.go                          entry point: wires config → storage → routes
internal/config/config.go        env loading
internal/storage/storage.go      Storage interface + ErrObjectNotFound sentinel
internal/storage/minio.go        MinioStorage backed by minio-go/v7
internal/registry/types.go       OCI + error JSON types
internal/registry/paths.go       S3 key helpers (blobPath, indexPath)
internal/registry/handler.go     Index / GetBlob / GetManifest handlers
internal/registry/routes.go      SetupRoutes() wires fiber app
internal/registry/handler_test.go  unit tests via fake storage
internal/registry/export_test.go   exposes unexported helpers for tests
Dockerfile                       multi-stage build → distroless
```

## S3 storage layout (must match the bucket)

```
<namespace>/<repository>/index.json                   OCI image index
<namespace>/<repository>/blobs/sha256/<hex>           blob or manifest content
```

## Environment variables

| Variable              | Default              | Notes                             |
|-----------------------|----------------------|-----------------------------------|
| `PORT`                | `3000`               |                                   |
| `S3_BUCKET_NAME`      | `zeabur-oci-registry`|                                   |
| `S3_ENDPOINT`         | —                    | MinIO host:port, no scheme        |
| `S3_ACCESS_KEY_ID`    | —                    |                                   |
| `S3_SECRET_ACCESS_KEY`| —                    |                                   |
| `S3_USE_SSL`          | `false`              | Set `true` for HTTPS              |
| `S3_REGION`           | `us-east-1`          |                                   |

## Common commands

```bash
# Install / sync dependencies
go mod tidy

# Build
go build ./...

# Test (all packages, verbose)
go test -v ./...

# Vet
go vet ./...

# Run locally (requires MinIO env vars)
go run .

# Build Docker image (single platform)
docker build -t zeabur/oci-ro-registry:2 .

# Build multi-platform image with docker-bake.hcl (linux/amd64 + linux/arm64)
docker buildx bake                         # uses VERSION=2.0.0 default
docker buildx bake --set "*.VERSION=2.0.0" # explicit version
VERSION=2.0.0 docker buildx bake --push    # build and push all tags
```

## Adding a new endpoint

1. Add the handler method to `Handler` in `internal/registry/handler.go`
2. Register the route in `SetupRoutes` in `internal/registry/routes.go`
3. Add test cases in `handler_test.go` using `doRequest` + `assertStatus`

## Testing approach

Tests live in `package registry_test` and use a map-backed `fakeStorage`.
No real S3/MinIO connection is required to run tests.
`fiber.App.Test(*http.Request)` is used to exercise the full HTTP stack.
