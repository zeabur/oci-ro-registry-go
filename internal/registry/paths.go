package registry

import "strings"

func blobPath(namespace, repository, digestWithoutPrefix string) string {
	return strings.Join([]string{namespace, repository, "blobs", "sha256", digestWithoutPrefix}, "/")
}

func indexPath(namespace, repository string) string {
	return strings.Join([]string{namespace, repository, "index.json"}, "/")
}
