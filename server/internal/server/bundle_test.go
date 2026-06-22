package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
	"github.com/gonvex/gonvex/server/internal/config"
)

func TestDevSyncLoadsProjectBundle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go plugin bundles are not supported on Windows; use Linux/macOS runtime for plugin-backed sync tests")
	}

	moduleRoot, err := gonvexModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(moduleRoot, "pkg", "projectbundle", "testdata", "app", "register.go"))
	if err != nil {
		t.Fatal(err)
	}

	bundle := manifest.SourceBundle{
		ModulePath:  "gonvexapp/sync-test",
		PackageName: "app",
		Files: map[string]string{
			"app/register.go": projectbundle.EncodeFile(source),
		},
	}
	bundle.Hash = projectbundle.HashFiles(bundle.Files)

	payload, err := json.Marshal(map[string]any{
		"project":     "sync-test",
		"generatedAt": "now",
		"functions": map[string]any{
			"sample.echo": map[string]any{"kind": "query", "handler": "SampleEcho", "file": "app/register.go"},
		},
		"schema": map[string]any{"tables": map[string]any{}},
		"bundle": bundle,
	})
	if err != nil {
		t.Fatal(err)
	}

	server := New(config.Config{GonvexModuleRoot: moduleRoot})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", bytes.NewReader(payload)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	app := server.runtime.AppForProject("sync-test")
	if app == nil {
		t.Fatal("expected synced project app")
	}
	result, err := server.executeQuery(context.Background(), "sync-test", "sample.echo", json.RawMessage(`{"name":"Ada"}`))
	if err != nil {
		t.Fatalf("execute synced query: %v", err)
	}
	payloadMap, ok := result.(map[string]any)
	if !ok || payloadMap["name"] != "Ada" {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func gonvexModuleRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrInvalid
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..")), nil
}
