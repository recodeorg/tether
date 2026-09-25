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
	GenerateUpload(mimeType string, opts ...UploadOptions) (fileID string, url string, headers map[string]string, err error)
	GenerateDownload(fileID string) (url string, err error)
	Delete(fileID string) error

	MountRoutes(mux *http.ServeMux) // for local storage adapter to mount serving routes
}
