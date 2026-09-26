package storage

import (
	"net/http"
	"time"
)

type UploadOptions struct {
	MaxBytes  int64
	ExpiresAt time.Time
}

type StorageAdapter interface {
	GenerateUpload(opts ...UploadOptions) (fileID string, url string, headers map[string]string, err error)
	GenerateDownload(fileID string) (url string, err error)
	Delete(fileID string) error

	ServeHTTP(w http.ResponseWriter, r *http.Request) bool // for local storage adapter to mount serving routes
}

// Matcher is implemented by adapters that serve browser-facing HTTP routes.
// The engine uses Matches to apply the WebSocket origin check and CORS headers
// before the adapter writes a response. Adapters that only mint presigned URLs
// can omit it and return false from ServeHTTP.
type Matcher interface {
	Matches(r *http.Request) bool
}
