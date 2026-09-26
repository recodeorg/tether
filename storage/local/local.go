package local

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type LocalStorage struct {
	UploadDir string
}

func NewLocalStorage(uploadDir string) *LocalStorage {
	os.MkdirAll(uploadDir, os.ModePerm)
	return &LocalStorage{
		UploadDir: uploadDir,
	}
}

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

	os.WriteFile(dstPath+".mime", []byte(contentType), 0644)
	return nil
}

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

func (l *LocalStorage) Delete(fileID string) error {
	path, err := l.safePath(fileID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if err := os.Remove(path + ".mime"); err != nil {
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
