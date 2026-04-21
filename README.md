# S3 OCI Registry - Read-Only API

A read-only OCI Distribution Spec v1 registry (Docker Registry V2 API) backed by S3. Images are stored in S3 and served to Docker/containerd clients via blob redirect and manifest streaming.

## Implemented endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET, HEAD | `/v2/` | Registry support check (end-1) |
| GET, HEAD | `/v2/:ns/:repo/blobs/:digest` | Blob pull via presigned-URL redirect (end-2) |
| GET, HEAD | `/v2/:ns/:repo/manifests/:reference` | Manifest pull, tag or digest (end-3) |

Write endpoints (push, delete, catalog) are not implemented — this is intentional.

## Configuration

Set the following environment variables before starting:

```bash
S3_ENDPOINT=minio.example.com:9000
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET_NAME=zeabur-oci-registry   # optional, this is the default
S3_USE_SSL=false                      # set true for HTTPS
S3_REGION=us-east-1                   # optional
PORT=3000                             # optional
```

## Running with Docker

```bash
docker build -t zeabur/oci-ro-registry:2 .

docker run -p 3000:3000 \
  -e S3_ENDPOINT=minio.example.com:9000 \
  -e S3_ACCESS_KEY_ID=minioadmin \
  -e S3_SECRET_ACCESS_KEY=minioadmin \
  zeabur/oci-ro-registry:2
```

## Building multi-platform images

[docker-bake.hcl](./docker-bake.hcl) builds `linux/amd64` and `linux/arm64` and tags the image as `2.0.0`, `2.0`, `2`, and `latest`.

```bash
# Build locally (no push)
VERSION=2.0.0 docker buildx bake

# Build and push to Docker Hub
VERSION=2.0.0 docker buildx bake --push
```

Override `REGISTRY` or `IMAGE` variables to target a different registry:

```bash
REGISTRY=ghcr.io IMAGE=your-org/oci-ro-registry VERSION=2.0.0 docker buildx bake --push
```

## S3 bucket layout

The registry expects images to be stored using the following key structure:

```
<namespace>/<repository>/index.json
<namespace>/<repository>/blobs/sha256/<hex-digest>
```

`index.json` is an [OCI image index](https://github.com/opencontainers/image-spec/blob/main/image-index.md) with `schemaVersion: 2`.
Each manifest entry must include an `org.opencontainers.image.ref.name` annotation for tag-based pulls.

## Development

See [CLAUDE.md](./CLAUDE.md) for development commands and architecture notes.
