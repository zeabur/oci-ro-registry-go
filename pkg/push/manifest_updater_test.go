package push

import (
	"testing"

	"github.com/zeabur/stratus/v2/pkg/registry"
)

func TestMergeIndex(t *testing.T) {
	updater := NewOCIManifestUpdater(nil, "")

	incoming := &registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:aaa",
				Size:      123,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "latest",
				},
			},
		},
	}

	existing := &registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:aaa",
				Size:      123,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "latest",
				},
			},
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:bbb",
				Size:      456,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "1.0.0",
				},
			},
		},
	}

	merged := updater.MergeIndex(existing, incoming)

	if merged.SchemaVersion != 2 {
		t.Fatalf("expected schema version 2, got %d", merged.SchemaVersion)
	}

	if len(merged.Manifests) != 2 {
		t.Fatalf("expected 2 manifests, got %d", len(merged.Manifests))
	}

	refDigestPairs := map[string]string{}
	for _, m := range merged.Manifests {
		refDigestPairs[m.Annotations["org.opencontainers.image.ref.name"]] = m.Digest
	}

	if refDigestPairs["latest"] != "sha256:aaa" {
		t.Fatalf("expected latest digest sha256:aaa, got %s", refDigestPairs["latest"])
	}
	if refDigestPairs["1.0.0"] != "sha256:bbb" {
		t.Fatalf("expected 1.0.0 digest sha256:bbb, got %s", refDigestPairs["1.0.0"])
	}
}

func TestMergeIndex_ExistingNil(t *testing.T) {
	updater := NewOCIManifestUpdater(nil, "")

	incoming := &registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:aaa",
				Size:      123,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "latest",
				},
			},
		},
	}

	merged := updater.MergeIndex(nil, incoming)

	if merged == nil {
		t.Fatal("expected merged index, got nil")
	}

	if merged != incoming {
		t.Fatal("expected merged to be the same reference as incoming when existing is nil")
	}

	if merged.SchemaVersion != 2 {
		t.Fatalf("expected schema version 2, got %d", merged.SchemaVersion)
	}

	if len(merged.Manifests) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(merged.Manifests))
	}

	if merged.Manifests[0].Digest != "sha256:aaa" {
		t.Fatalf("expected digest sha256:aaa, got %s", merged.Manifests[0].Digest)
	}
}

func TestMergeIndex_IncomingNil(t *testing.T) {
	updater := NewOCIManifestUpdater(nil, "")

	existing := &registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:aaa",
				Size:      123,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "latest",
				},
			},
		},
	}

	merged := updater.MergeIndex(existing, nil)

	if merged == nil {
		t.Fatal("expected merged index, got nil")
	}

	if merged != existing {
		t.Fatal("expected merged to be the same reference as existing when incoming is nil")
	}

	if merged.SchemaVersion != 2 {
		t.Fatalf("expected schema version 2, got %d", merged.SchemaVersion)
	}

	if len(merged.Manifests) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(merged.Manifests))
	}

	if merged.Manifests[0].Digest != "sha256:aaa" {
		t.Fatalf("expected digest sha256:aaa, got %s", merged.Manifests[0].Digest)
	}
}

func TestMergeIndex_LatestWins(t *testing.T) {
	updater := NewOCIManifestUpdater(nil, "")

	incoming := &registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:new",
				Size:      999,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "latest",
				},
			},
		},
	}

	existing := &registry.OciManifestIndex{
		SchemaVersion: 2,
		Manifests: []registry.OciManifest{
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:old",
				Size:      111,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "latest",
				},
			},
			{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    "sha256:other",
				Size:      222,
				Annotations: map[string]string{
					"org.opencontainers.image.ref.name": "other-tag",
				},
			},
		},
	}

	merged := updater.MergeIndex(existing, incoming)

	if merged.SchemaVersion != 2 {
		t.Fatalf("expected schema version 2, got %d", merged.SchemaVersion)
	}

	if len(merged.Manifests) != 2 {
		t.Fatalf("expected 2 manifests, got %d", len(merged.Manifests))
	}

	refDigestPairs := map[string]string{}
	for _, m := range merged.Manifests {
		refDigestPairs[m.Annotations["org.opencontainers.image.ref.name"]] = m.Digest
	}

	if refDigestPairs["latest"] != "sha256:new" {
		t.Fatalf("expected latest digest sha256:new (from incoming), got %s", refDigestPairs["latest"])
	}

	if refDigestPairs["other-tag"] != "sha256:other" {
		t.Fatalf("expected other-tag digest sha256:other, got %s", refDigestPairs["other-tag"])
	}
}
