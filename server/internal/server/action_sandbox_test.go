package server

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/datafiles"
	gonvexsandbox "github.com/gonvex/gonvex/server/internal/sandbox"
)

type sandboxStorage struct {
	meta gonvex.FileMetadata
	data []byte
}

func (*sandboxStorage) GenerateUploadURL(gonvex.UploadOptions) (gonvex.UploadTarget, error) {
	return gonvex.UploadTarget{}, nil
}
func (*sandboxStorage) GetURL(string) (string, error)                             { return "", nil }
func (*sandboxStorage) GenerateDownloadURL(string, time.Duration) (string, error) { return "", nil }
func (storage *sandboxStorage) GetMetadata(string) (gonvex.FileMetadata, error) {
	return storage.meta, nil
}
func (*sandboxStorage) Delete(string) error { return nil }
func (*sandboxStorage) Store([]byte, gonvex.UploadOptions) (gonvex.FileMetadata, error) {
	return gonvex.FileMetadata{}, nil
}
func (storage *sandboxStorage) Open(string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(storage.data)), nil
}

func TestActionSandboxImportsAuthorizedCSVIntoPrivateDuckDB(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test worker is a POSIX script")
	}
	root := t.TempDir()
	worker := filepath.Join(root, "worker")
	if err := os.WriteFile(worker, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := gonvexsandbox.New(gonvexsandbox.Config{Enabled: true, Root: filepath.Join(root, "sandboxes"), WorkerBinary: worker, AllowUnconfined: true})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	scope := gonvexsandbox.Scope{ProjectID: "project", TenantID: "tenant", AccountID: "account"}
	handle, err := manager.Create(scope, true, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	storage := &sandboxStorage{meta: gonvex.FileMetadata{ID: "file-1", TenantID: "tenant", OwnerID: "account", Visibility: gonvex.FileVisibilityPrivate, Status: gonvex.FileStatusUploaded}, data: []byte("region,amount\nnorth,7\nsouth,4\n")}
	bridge := &actionSandbox{manager: manager, dataFiles: datafiles.NewManager(filepath.Join(root, "data")), storage: storage, scope: scope}
	value, err := bridge.importFile(context.Background(), handle.SandboxID, "file-1", "sales.csv")
	if err != nil {
		t.Fatal(err)
	}
	imported, ok := value.(gonvexsandbox.Import)
	if !ok || imported.Alias != "data_csv_file_1" || len(imported.Tables) != 1 {
		t.Fatalf("import = %#v", value)
	}

	storage.meta.OwnerID = "another-account"
	second, err := manager.Create(scope, true, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.importFile(context.Background(), second.SandboxID, "file-1", "sales.csv"); err == nil {
		t.Fatal("private file owned by another account was imported")
	}
}
