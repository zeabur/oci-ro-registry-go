package registry

type OciManifest struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations"`
}

type OciManifestIndex struct {
	SchemaVersion int           `json:"schemaVersion"`
	Manifests     []OciManifest `json:"manifests"`
}
