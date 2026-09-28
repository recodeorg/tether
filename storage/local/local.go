// Package local provides a storage adapter that keeps files in a directory
// on the server's disk.
package local

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// LocalStorage is a [storage.StorageAdapter] that stores each file in
// UploadDir under its file ID. It suits single-instance deployments; engines
// on different servers cannot read each other's files.
type LocalStorage struct {
	UploadDir string
}

// NewLocalStorage returns a LocalStorage that stores files in uploadDir,
// creating the directory if it does not exist. It returns an error if the
// directory cannot be created.
func NewLocalStorage(uploadDir string) (*LocalStorage, error) {
	err := os.MkdirAll(uploadDir, os.ModePerm)
	if err != nil {
		slog.Error("Failed to create directory", "error", err)
		return nil, err
	}
	return &LocalStorage{
		UploadDir: uploadDir,
	}, nil
}

// UploadStream writes the request body to the file fileID, removing the
// partial file if the copy fails.
func (l *LocalStorage) UploadStream(ctx context.Context, fileID string, contentType string, r *http.Request) error {
	dstPath, err := l.safePath(fileID)
	if err != nil {
		return err
	}
	file, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, r.Body)
	if err != nil {
		os.Remove(dstPath)
		return err
	}
	return nil
}

// ServeFile serves the file fileID with [http.ServeFile].
func (l *LocalStorage) ServeFile(fileID string, w http.ResponseWriter, r *http.Request) error {
	filePath, err := l.safePath(fileID)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return err
	}
	http.ServeFile(w, r, filePath)
	return nil
}

// Delete removes the file fileID.
func (l *LocalStorage) Delete(fileID string) error {
	path, err := l.safePath(fileID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

// safePath resolves fileID as a single file inside UploadDir. Parent segments
// and absolute paths are rejected so Join cannot escape the upload root.
func (l *LocalStorage) safePath(fileID string) (string, error) {
	if fileID == "" || fileID == "." || fileID == ".." || filepath.IsAbs(fileID) || fileID != filepath.Base(fileID) {
		return "", fmt.Errorf("invalid file id")
	}
	root, err := filepath.Abs(l.UploadDir)
	if err != nil {
		return "", err
	}
	root = filepath.Clean(root)
	candidate := filepath.Join(root, fileID)
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return "", fmt.Errorf("invalid file id")
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid file id")
	}
	return candidate, nil
}

// Name returns "tether/storage/local".
func (l *LocalStorage) Name() string {
	return "tether/storage/local"
}
