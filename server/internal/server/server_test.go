package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestHealth(t *testing.T) {
	server := New(config.Config{PostgresURL: "postgres://example", S3Endpoint: "http://localhost:9000", S3Bucket: "gonvex-dev"})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
}

func TestDataTablesWithoutDatabase(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Tables []any `json:"tables"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Tables) != 0 {
		t.Fatalf("expected no tables, got %d", len(payload.Tables))
	}
}

func TestDataRowsWithoutDatabase(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Table   string `json:"table"`
		Columns []any  `json:"columns"`
		Rows    []any  `json:"rows"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Table != "tasks" || len(payload.Columns) != 0 || len(payload.Rows) != 0 {
		t.Fatalf("unexpected rows payload: %+v", payload)
	}
}

func TestDataRowsRejectsInvalidTableName(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables/bad-name/rows", nil))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestInsertDataRowsWithoutDatabaseFails(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"id":"task_1","title":"Test"}`)

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/data/tables/tasks/rows", body))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestInsertDataRowsRejectsInvalidTableName(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"id":"task_1"}`)

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/data/tables/bad-name/rows", body))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestDevSyncStoresManifest(t *testing.T) {
	server := New(config.Config{})
	body := bytes.NewBufferString(`{"project":"test","generatedAt":"now","functions":{"tasks.list":{"kind":"query","handler":"List","file":"gonvex/tasks.go"}},"schema":{}}`)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	manifest := server.runtime.Manifest()
	if len(manifest.Functions) != 1 {
		t.Fatalf("expected 1 function, got %d", len(manifest.Functions))
	}
}

func TestManifestForUnknownProjectIsEmpty(t *testing.T) {
	server := New(config.Config{})
	body := bytes.NewBufferString(`{"project":"app","generatedAt":"now","functions":{"tasks.list":{"kind":"query","handler":"List","file":"gonvex/tasks.go"}},"schema":{}}`)

	server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/dev/manifest", nil)
	request.Header.Set("x-gonvex-project-id", "testing")
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Project   string         `json:"project"`
		Functions map[string]any `json:"functions"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Project != "testing" {
		t.Fatalf("expected testing project, got %q", payload.Project)
	}
	if len(payload.Functions) != 0 {
		t.Fatalf("expected no functions for unknown project, got %d", len(payload.Functions))
	}
}

func TestMetricsTracksDataCacheAndFunctionCalls(t *testing.T) {
	server := New(config.Config{})

	server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows", nil))
	if _, err := server.executeQuery(context.Background(), "", "tasks.grid", nil); err != nil {
		t.Fatalf("execute query: %v", err)
	}
	if _, err := server.executeMutation(context.Background(), "", "missing.mutation", nil); err == nil {
		t.Fatal("expected missing mutation to fail")
	}

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/metrics", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Functions map[string]struct {
			Calls  int64 `json:"calls"`
			Errors int64 `json:"errors"`
		} `json:"functions"`
		Cache struct {
			Bypasses int64 `json:"bypasses"`
		} `json:"cache"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Functions["tasks.grid"].Calls != 1 {
		t.Fatalf("expected tasks.grid call to be tracked, got %+v", payload.Functions["tasks.grid"])
	}
	if payload.Functions["missing.mutation"].Errors != 1 {
		t.Fatalf("expected missing mutation error to be tracked, got %+v", payload.Functions["missing.mutation"])
	}
	if payload.Cache.Bypasses != 1 {
		t.Fatalf("expected cache bypass to be tracked, got %d", payload.Cache.Bypasses)
	}
}
