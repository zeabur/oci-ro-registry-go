package registry

import "strings"

// NormalizeRepo adds a "library/" namespace prefix when repo has no "/" separator.
func NormalizeRepo(repo string) string {
	if !strings.Contains(repo, "/") {
		return "library/" + repo
	}
	return repo
}

func BlobPath(repo, digestHex string) string {
	return NormalizeRepo(repo) + "/blobs/sha256/" + digestHex
}

func IndexPath(repo string) string {
	return NormalizeRepo(repo) + "/index.json"
}

func OciLayoutPath(repo string) string {
	return NormalizeRepo(repo) + "/oci-layout"
}
