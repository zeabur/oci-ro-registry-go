// push-image pulls any image from a remote registry and pushes it to Stratus.
//
// Usage:
//
//	push-image <source-image> <namespace/repo> <tag>
//
// Example:
//
//	push-image ubuntu:22.04 library/ubuntu 22.04
//
// Configuration is read from environment variables (S3_ENDPOINT, S3_ACCESS_KEY_ID, etc.).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	stratusconfig "github.com/zeabur/stratus/v2/pkg/config"
	stratuspush "github.com/zeabur/stratus/v2/pkg/push"
	stratusstorage "github.com/zeabur/stratus/v2/pkg/storage"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <source-image> <namespace/repo> <tag>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  Example: %s ubuntu:22.04 library/ubuntu 22.04\n", os.Args[0])
		os.Exit(1)
	}

	srcRef := os.Args[1]
	imageName := os.Args[2]
	tag := os.Args[3]

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, srcRef, imageName, tag); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, srcRef, imageName, tag string) error {
	fmt.Fprintf(os.Stderr, "⬇️  Pulling %s...\n", srcRef)
	img, err := crane.Pull(srcRef, crane.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("pull %s: %w", srcRef, err)
	}

	tmpDir, err := os.MkdirTemp("", "stratus-push-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	lp, err := layout.Write(tmpDir, empty.Index)
	if err != nil {
		return fmt.Errorf("init oci layout: %w", err)
	}
	if err := lp.AppendImage(img); err != nil {
		return fmt.Errorf("write oci layout: %w", err)
	}

	cfg := stratusconfig.Load()

	storage, err := stratusstorage.MinioStorageFromConfig(cfg)
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}

	return stratuspush.PushOciLayout(
		ctx,
		storage,
		cfg.BucketName,
		os.DirFS(tmpDir),
		imageName,
		tag,
		stratuspush.WithLogOutput(os.Stderr),
	)
}
