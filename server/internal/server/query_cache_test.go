package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gorilla/websocket"
)

type queryCacheTestArgs struct {
	Value string `json:"value"`
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
