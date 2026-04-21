package registry

// Expose unexported path helpers for external tests.
var (
	BlobPathExported  = blobPath
	IndexPathExported = indexPath
)
