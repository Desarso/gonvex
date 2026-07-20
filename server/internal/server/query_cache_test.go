package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gorilla/websocket"
)

type queryCacheTestArgs struct {
	Value string `json:"value"`
}

func TestWebSocketQueryLogsRedisAndDatabaseSources(t *testing.T) {
	redisServer := miniredis.RunT(t)
	var executions atomic.Int32
	app := gonvex.NewApp()
	app.Query("bulk.tasksByWorkspace", func(_ *gonvex.QueryCtx, args queryCacheTestArgs) (map[string]string, error) {
		executions.Add(1)
		return map[string]string{"value": args.Value}, nil
	})
	runtime := NewWithApp(config.Config{
		QueryCacheEnabled: true,
		ValkeyURL:         "redis://" + redisServer.Addr(),
		RowsCacheTTL:      time.Minute,
	}, app)
	t.Cleanup(func() { _ = runtime.cache.close() })
	httpServer := httptest.NewServer(runtime.Handler())
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws?project=project-a"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var ready serverMessage
	if err := connection.ReadJSON(&ready); err != nil {
		t.Fatal(err)
	}

	query := func(id string) {
		t.Helper()
		if err := connection.WriteJSON(clientMessage{
			Type: "query.subscribe",
			ID:   id,
			Path: "bulk.tasksByWorkspace",
			Args: json.RawMessage(`{"value":"same"}`),
		}); err != nil {
			t.Fatal(err)
		}
		var result serverMessage
		if err := connection.ReadJSON(&result); err != nil {
			t.Fatal(err)
		}
		if result.Type != "query.result" {
			t.Fatalf("unexpected query result: %+v", result)
		}
	}

	query("query-db")
	query("query-redis")
	if got := executions.Load(); got != 1 {
		t.Fatalf("expected Redis to avoid the second function execution, got %d executions", got)
	}

	snapshot := runtime.metrics.snapshot(manifest.Manifest{}, 0, 0, "project-a")
	sources := map[string]int{}
	for _, entry := range snapshot.Logs {
		if entry.Path == "bulk.tasksByWorkspace" {
			sources[entry.Source+":"+entry.Cache]++
		}
	}
	if sources["database:miss"] != 1 || sources["redis:hit"] != 1 {
		t.Fatalf("expected one database miss and one Redis hit, got %+v logs=%+v", sources, snapshot.Logs)
	}

	runtime.cache.invalidateQueries(context.Background(), "project-a", "project-a", []string{"supportSessions"})
	query("query-after-unrelated-invalidate")
	if got := executions.Load(); got != 1 {
		t.Fatalf("expected an unrelated table invalidation to preserve the cached result, got %d executions", got)
	}

	runtime.cache.invalidateQueries(context.Background(), "project-a", "project-a", []string{"tasks"})
	query("query-after-invalidate")
	if got := executions.Load(); got != 2 {
		t.Fatalf("expected invalidation to force a database execution, got %d executions", got)
	}
}

func TestQueryCacheDirectiveIsStableAndScopeSafe(t *testing.T) {
	server := New(config.Config{QueryCacheEnabled: true})
	if err := server.runtime.SyncManifest(manifest.Manifest{
		Project: "project-a",
		Functions: map[string]manifest.FunctionEntry{
			"tasks.list": {Kind: manifest.FunctionKindQuery, Handler: "ListTasks", File: "tasks.go"},
		},
		Schema: manifest.EmptySchema(),
		Bundle: &manifest.SourceBundle{Hash: "bundle-a", Files: map[string]string{}},
	}); err != nil {
		t.Fatal(err)
	}

	caller := callerContext{
		user:        &gonvex.User{ID: "user-a"},
		permissions: map[string]any{"role": "member", "tasks:read": true},
	}
	first := server.queryCacheDirective("project-a", "tenant-a", caller)
	second := server.queryCacheDirective("project-a", "tenant-a", caller)
	if first == nil || second == nil {
		t.Fatal("expected browser query cache directive")
	}
	if first.Scope != second.Scope || first.Epoch != second.Epoch {
		t.Fatalf("expected stable directive, got %#v and %#v", first, second)
	}
	if first.ProtocolVersion != 1 || first.MaxAgeMS <= 0 {
		t.Fatalf("unexpected cache policy: %#v", first)
	}

	otherTenant := server.queryCacheDirective("project-a", "tenant-b", caller)
	otherUser := server.queryCacheDirective("project-a", "tenant-a", callerContext{
		user:        &gonvex.User{ID: "user-b"},
		permissions: caller.permissions,
	})
	otherPermissions := server.queryCacheDirective("project-a", "tenant-a", callerContext{
		user:        caller.user,
		permissions: map[string]any{"role": "viewer", "tasks:read": true},
	})
	for name, directive := range map[string]*queryCacheDirective{
		"tenant":      otherTenant,
		"user":        otherUser,
		"permissions": otherPermissions,
	} {
		if directive == nil || directive.Scope == first.Scope {
			t.Fatalf("expected %s change to produce a distinct scope", name)
		}
	}
}

func TestQueryCacheDirectiveChangesWithRuntimeManifest(t *testing.T) {
	server := New(config.Config{QueryCacheEnabled: true})
	manifestA := manifest.Manifest{
		Project:   "project-a",
		Functions: map[string]manifest.FunctionEntry{},
		Schema:    manifest.EmptySchema(),
		Bundle:    &manifest.SourceBundle{Hash: "bundle-a", Files: map[string]string{}},
	}
	if err := server.runtime.SyncManifest(manifestA); err != nil {
		t.Fatal(err)
	}
	before := server.queryCacheDirective("project-a", "tenant-a", callerContext{})

	manifestA.Bundle.Hash = "bundle-b"
	if err := server.runtime.SyncManifest(manifestA); err != nil {
		t.Fatal(err)
	}
	after := server.queryCacheDirective("project-a", "tenant-a", callerContext{})
	if before == nil || after == nil || before.Epoch == after.Epoch || before.Scope == after.Scope {
		t.Fatalf("expected manifest change to invalidate cache scope: before=%#v after=%#v", before, after)
	}
}

func TestQueryCacheDirectiveChangesWithTenantDatabaseRoute(t *testing.T) {
	server := New(config.Config{
		QueryCacheEnabled: true,
		ProjectDatabases:  map[string]string{"project-a": "postgres://db/project"},
		TenantDatabases:   map[string]string{"project-a:tenant-a": "postgres://db/tenant-a"},
	})
	before := server.queryCacheDirective("project-a", "tenant-a", callerContext{})

	server.projectMu.Lock()
	server.config.TenantDatabases["project-a:tenant-a"] = "postgres://db/tenant-b"
	server.projectMu.Unlock()
	after := server.queryCacheDirective("project-a", "tenant-a", callerContext{})

	if before == nil || after == nil || before.Epoch == after.Epoch || before.Scope == after.Scope {
		t.Fatalf("expected database route change to invalidate cache scope: before=%#v after=%#v", before, after)
	}
}

func TestClearProjectCacheDoesNotClearOtherProjects(t *testing.T) {
	redisServer := miniredis.RunT(t)
	cache, err := newRowsCache("redis://"+redisServer.Addr(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.close() })
	ctx := context.Background()
	projectAQuery := cache.queryKey("project-a", "tenant-a", "generation", "scope", "tasks.list", nil)
	projectBQuery := cache.queryKey("project-b", "tenant-b", "generation", "scope", "tasks.list", nil)
	projectARows := cache.rowsKey("project-a", "tenant-a", "tasks", nil)
	cache.set(ctx, projectAQuery, []byte("a"))
	cache.set(ctx, projectBQuery, []byte("b"))
	cache.set(ctx, projectARows, []byte("rows"))

	if cleared := cache.clearProject(ctx, "project-a"); cleared != 2 {
		t.Fatalf("cleared entries = %d, want 2", cleared)
	}
	if _, outcome := cache.read(ctx, projectAQuery); outcome != "miss" {
		t.Fatalf("project A query outcome = %q, want miss", outcome)
	}
	if _, outcome := cache.read(ctx, projectARows); outcome != "miss" {
		t.Fatalf("project A rows outcome = %q, want miss", outcome)
	}
	if _, outcome := cache.read(ctx, projectBQuery); outcome != "hit" {
		t.Fatalf("project B query outcome = %q, want hit", outcome)
	}
}

func TestQueryCacheDirectiveCanBeDisabled(t *testing.T) {
	server := New(config.Config{})
	if directive := server.queryCacheDirective("project-a", "tenant-a", callerContext{}); directive != nil {
		t.Fatalf("expected disabled cache to omit directive, got %#v", directive)
	}
}

func TestWebSocketAdvertisesAndReturnsQueryCacheMetadata(t *testing.T) {
	app := gonvex.NewApp()
	app.Query("cache.echo", func(_ *gonvex.QueryCtx, args queryCacheTestArgs) (map[string]string, error) {
		return map[string]string{"value": args.Value}, nil
	})
	runtime := NewWithApp(config.Config{QueryCacheEnabled: true}, app)
	httpServer := httptest.NewServer(runtime.Handler())
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws?project=project-a"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	var ready serverMessage
	if err := connection.ReadJSON(&ready); err != nil {
		t.Fatal(err)
	}
	if ready.Type != "session.ready" || ready.QueryCache == nil || ready.QueryCache.Scope == "" {
		t.Fatalf("expected cache-capable session.ready, got %#v", ready)
	}

	if err := connection.WriteJSON(clientMessage{
		Type: "query.subscribe",
		ID:   "query-1",
		Path: "cache.echo",
		Args: json.RawMessage(`{"value":"fresh"}`),
	}); err != nil {
		t.Fatal(err)
	}
	var result serverMessage
	if err := connection.ReadJSON(&result); err != nil {
		t.Fatal(err)
	}
	if result.Type != "query.result" || result.CacheScope != ready.QueryCache.Scope || result.CacheRevision == "" {
		t.Fatalf("expected scoped query result, got %#v", result)
	}
}
