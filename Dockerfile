# syntax=docker/dockerfile:1.23

FROM golang:1.26 AS build
WORKDIR /src
RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/stratus ./cmd/stratus

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/stratus /bin/stratus
EXPOSE 3000
ENTRYPOINT ["/bin/stratus"]
