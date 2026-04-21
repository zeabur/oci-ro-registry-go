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

type RegistryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Errors []RegistryError `json:"errors"`
}

func errResponse(code, message string) *ErrorResponse {
	return &ErrorResponse{
		Errors: []RegistryError{{Code: code, Message: message}},
	}
}
