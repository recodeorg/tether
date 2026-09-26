package storage

import (
	"context"
	"net/http"
	"time"
)

type UploadOptions struct {
	MaxBytes  int64
	ExpiresIn time.Duration
}

type UploadInfo struct {
	FileID    string `json:"fileID"`
	UploadURL string `json:"uploadURL"`
}

type StorageAdapter interface {
	UploadStream(ctx context.Context, fileID string, contentType string, r *http.Request) error
	ServeFile(fileID string, w http.ResponseWriter, r *http.Request) error
	Delete(fileID string) error
}
