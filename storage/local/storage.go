package local

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/recodeorg/tether/storage"
)

type ephemeralToken struct {
	FileID    string
	MaxBytes  int64
	ExpiresAt time.Time
}

type LocalStorage struct {
	UploadDir      string
	BaseURL        string
	mu             sync.RWMutex
	uploadTokens   map[string]ephemeralToken
	downloadTokens map[string]ephemeralToken
}

func NewLocalStorage(uploadDir string, baseURL string) *LocalStorage {
	os.MkdirAll(uploadDir, os.ModePerm)
	ls := &LocalStorage{
		UploadDir:      uploadDir,
		BaseURL:        baseURL,
		uploadTokens:   make(map[string]ephemeralToken),
		downloadTokens: make(map[string]ephemeralToken),
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			now := time.Now()
			ls.mu.Lock()
			for token, data := range ls.uploadTokens {
				if now.After(data.ExpiresAt) {
					delete(ls.uploadTokens, token)
				}
			}
			for token, data := range ls.downloadTokens {
				if now.After(data.ExpiresAt) {
					delete(ls.downloadTokens, token)
				}
			}
			ls.mu.Unlock()
		}
	}()
	return ls
}

func (l *LocalStorage) GenerateUpload(opts ...storage.UploadOptions) (fileID string, url string, headers map[string]string, err error) {
	fileID = uuid.New().String()
	uploadToken := ephemeralToken{
		FileID:    fileID,
		MaxBytes:  1024 * 1024 * 20, // 20MB
		ExpiresAt: time.Now().Add(10 * time.Minute),
	}
	if len(opts) > 0 {
		if opts[0].MaxBytes != 0 {
			uploadToken.MaxBytes = opts[0].MaxBytes
		}
		if opts[0].ExpiresAt != (time.Time{}) {
			uploadToken.ExpiresAt = opts[0].ExpiresAt
		}
	}
	l.mu.Lock()
	l.uploadTokens[fileID] = uploadToken
	l.mu.Unlock()
	uploadUrl := fmt.Sprintf("%s/tether/storage/upload/%s", l.BaseURL, fileID)
	return fileID, uploadUrl, nil, nil
}

func (l *LocalStorage) GenerateDownload(fileID string) (url string, err error) {
	downloadID := uuid.New().String()
	downloadToken := ephemeralToken{
		FileID:    fileID,
		ExpiresAt: time.Now().Add(60 * time.Minute),
	}
	l.mu.Lock()
	l.downloadTokens[downloadID] = downloadToken
	l.mu.Unlock()
	downloadUrl := fmt.Sprintf("%s/tether/storage/file/%s", l.BaseURL, downloadID)
	return downloadUrl, nil
}

func (l *LocalStorage) Delete(fileID string) error {
	filePath := filepath.Join(l.UploadDir, fileID)
	err := os.Remove(filePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (l *LocalStorage) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	fileID := filepath.Base(r.URL.Path)
	l.mu.Lock()
	tokenData, ok := l.uploadTokens[fileID]
	if ok {
		delete(l.uploadTokens, fileID)
	}
	l.mu.Unlock()
	if !ok || time.Now().After(tokenData.ExpiresAt) {
		http.Error(w, "Invalid file ID", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, int64(tokenData.MaxBytes))

	dstPath := filepath.Join(l.UploadDir, fileID)
	file, err := os.Create(dstPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	_, err = io.Copy(file, r.Body)
	if err != nil {
		os.Remove(dstPath)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create the .mime sidecar file with the content-type header
	mimePath := dstPath + ".mime"
	mimeFile, err := os.Create(mimePath)
	if err != nil {
		os.Remove(dstPath)
		http.Error(w, "Failed to create .mime file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = mimeFile.WriteString(contentType)
	closeErr := mimeFile.Close()
	if err != nil || closeErr != nil {
		os.Remove(dstPath)
		os.Remove(mimePath)
		http.Error(w, "Failed to write .mime file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "File uploaded successfully")
}

func (l *LocalStorage) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	downloadID := filepath.Base(r.URL.Path)
	l.mu.RLock()
	tokenData, ok := l.downloadTokens[downloadID]
	l.mu.RUnlock()
	if !ok {
		http.Error(w, "Invalid download ID", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(l.UploadDir, tokenData.FileID)
	mimePath := filePath + ".mime"
	if _, err := os.Stat(mimePath); err == nil {
		mimeContent, err := os.ReadFile(mimePath)
		if err == nil {
			contentType := string(mimeContent)
			w.Header().Set("Content-Type", contentType)
		}
	}
	http.ServeFile(w, r, filePath)
}

func (l *LocalStorage) ServeHTTP(w http.ResponseWriter, r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, "/tether/storage/upload/") {
		l.handleUpload(w, r)
		return true
	}
	if strings.HasPrefix(r.URL.Path, "/tether/storage/file/") {
		l.handleDownload(w, r)
		return true
	}
	return false
}
