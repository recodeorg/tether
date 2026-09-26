package local

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	dstPath := filepath.Join(l.UploadDir, fileID)
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
	filePath := filepath.Join(l.UploadDir, fileID)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return err
	}
	http.ServeFile(w, r, filePath)
	return nil
}

func (l *LocalStorage) Delete(fileID string) error {
	os.Remove(filepath.Join(l.UploadDir, fileID))
	os.Remove(filepath.Join(l.UploadDir, fileID+".mime"))
	return nil
}
