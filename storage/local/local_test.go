package local

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeleteRejectsPathEscape(t *testing.T) {
	root := t.TempDir()
	uploadDir := filepath.Join(root, "uploads")
	store := NewLocalStorage(uploadDir)

	sibling := filepath.Join(root, "outside.txt")
	if err := os.WriteFile(sibling, []byte("sibling"), 0o644); err != nil {
		t.Fatalf("write sibling: %v", err)
	}
	decoy := filepath.Join(uploadDir, "outside.txt")
	if err := os.WriteFile(decoy, []byte("decoy"), 0o644); err != nil {
		t.Fatalf("write decoy: %v", err)
	}

	absTarget := filepath.Join(root, "absolute.txt")
	if err := os.WriteFile(absTarget, []byte("absolute"), 0o644); err != nil {
		t.Fatalf("write absolute target: %v", err)
	}
	absDecoy := filepath.Join(uploadDir, "absolute.txt")
	if err := os.WriteFile(absDecoy, []byte("decoy"), 0o644); err != nil {
		t.Fatalf("write absolute decoy: %v", err)
	}

	if err := store.Delete("../outside.txt"); err == nil {
		t.Fatal("parent path delete returned nil")
	}
	if err := store.Delete(absTarget); err == nil {
		t.Fatal("absolute path delete returned nil")
	}

	for _, path := range []string{sibling, decoy, absTarget, absDecoy} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("file %s was removed or became unreadable: %v", path, err)
		}
	}
}

func TestDeletePropagatesMissingFile(t *testing.T) {
	store := NewLocalStorage(t.TempDir())
	err := store.Delete("550e8400-e29b-41d4-a716-446655440000")
	if err == nil {
		t.Fatal("delete of missing file returned nil")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("delete error = %v, want not exist", err)
	}
}

func TestDeleteRemovesStoredObject(t *testing.T) {
	store := NewLocalStorage(t.TempDir())
	fileID := "550e8400-e29b-41d4-a716-446655440000"
	path := filepath.Join(store.UploadDir, fileID)
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.WriteFile(path+".mime", []byte("text/plain"), 0o644); err != nil {
		t.Fatalf("write mime: %v", err)
	}

	if err := store.Delete(fileID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stored file still present: %v", err)
	}
	if _, err := os.Stat(path + ".mime"); !os.IsNotExist(err) {
		t.Fatalf("mime file still present: %v", err)
	}
}
