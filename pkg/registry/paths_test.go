package registry_test

import (
	"testing"

	"github.com/zeabur/stratus/pkg/registry"
)

func TestBlobPath(t *testing.T) {
	tests := []struct{ repo, digest, want string }{
		{"ns/repo", "abc", "ns/repo/blobs/sha256/abc"},
		{"nginx", "def", "library/nginx/blobs/sha256/def"},
	}
	for _, tc := range tests {
		if got := registry.BlobPath(tc.repo, tc.digest); got != tc.want {
			t.Errorf("BlobPath(%q, %q) = %q, want %q", tc.repo, tc.digest, got, tc.want)
		}
	}
}

func TestIndexPath(t *testing.T) {
	tests := []struct{ repo, want string }{
		{"ns/repo", "ns/repo/index.json"},
		{"nginx", "library/nginx/index.json"},
	}
	for _, tc := range tests {
		if got := registry.IndexPath(tc.repo); got != tc.want {
			t.Errorf("IndexPath(%q) = %q, want %q", tc.repo, got, tc.want)
		}
	}
}

func TestOciLayoutPath(t *testing.T) {
	tests := []struct{ repo, want string }{
		{"ns/repo", "ns/repo/oci-layout"},
		{"nginx", "library/nginx/oci-layout"},
	}
	for _, tc := range tests {
		if got := registry.OciLayoutPath(tc.repo); got != tc.want {
			t.Errorf("OciLayoutPath(%q) = %q, want %q", tc.repo, got, tc.want)
		}
	}
}
