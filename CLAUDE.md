# Stratus — A stateless OCI Registry (Go)

## What this is

A monorepo with two components:

- **`cmd/stratus`** — Read-only OCI Distribution Spec v1 registry (Docker Registry V2 API) backed by S3. Blobs are served via presigned-URL redirect; manifests are streamed. Write operations are not implemented.
- **`pkg/push`** — Go library for pushing a BuildKit OCI image (`--output type=oci`) to the S3 registry. Accepts an `fs.FS` in OCI layout format, uploads blobs concurrently, and atomically merges the index.

## Project layout

```
cmd/stratus/main.go              entry point: wires config → storage → routes
internal/config/config.go        env loading
internal/storage/storage.go      Storage interface + ErrObjectNotFound sentinel
internal/storage/minio.go        MinioStorage backed by minio-go/v7
internal/registry/types.go       OCI + error JSON types
internal/registry/paths.go       S3 key helpers (blobPath, indexPath)
internal/registry/handler.go     Index / GetBlob / GetManifest handlers
internal/registry/routes.go      SetupRoutes() wires fiber app
internal/registry/handler_test.go  unit tests via fake storage
internal/registry/export_test.go   exposes unexported helpers for tests
pkg/push/pusher.go               PushOciLayout() — top-level push entrypoint
pkg/push/uploader.go             concurrent upload with skip-if-exists logic
pkg/push/manifest_updater.go     remote index fetch + merge
pkg/push/storage.go              Storage interface used by pkg/push
pkg/push/storage_builder.go      builds a storage.Storage from env vars
Dockerfile                       multi-stage build → distroless (cmd/stratus)
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
docker build -t zeabur/stratus:2 .

# Build multi-platform image with docker-bake.hcl (linux/amd64 + linux/arm64)
docker buildx bake                         # uses VERSION=2.1.1 default
docker buildx bake --set "*.VERSION=2.1.1" # explicit version
VERSION=2.1.1 docker buildx bake --push    # build and push all tags
```

## Adding a new endpoint

1. Add the handler method to `Handler` in `internal/registry/handler.go`
2. Register the route in `SetupRoutes` in `internal/registry/routes.go`
3. Add test cases in `handler_test.go` using `doRequest` + `assertStatus`

## Testing approach

Tests live in `package registry_test` and use a map-backed `fakeStorage`.
No real S3/MinIO connection is required to run tests.
`fiber.App.Test(*http.Request)` is used to exercise the full HTTP stack.

## Integration tests

Integration tests require real S3 credentials and are gated behind the `integration` build tag.

```bash
S3_ACCESS_KEY_ID=ACCESS_KEY_ID \
S3_SECRET_ACCESS_KEY=SECRET_ACCESS_KEY \
S3_BUCKET_NAME=BUCKET \
S3_REGION=REGION \
S3_ENDPOINT=ENDPOINT \
S3_USE_SSL=true \
S3_PATH_STYLE=false \
go test -tags=integration -v ./... -run Integration
```

Note that ENDPOINT should not include a protocol. Specify with `S3_USE_SSL` instead.
