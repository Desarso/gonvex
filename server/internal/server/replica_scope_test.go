package server

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

func TestReplicaDirectiveIsMandatoryStableAndScopeSafe(t *testing.T) {
	server := New(config.Config{})
	caller := callerContext{
		user:        &gonvex.Account{ID: "account-a"},
		permissions: map[string]any{"role": "member", "tasks:read": true},
	}
	first := server.replicaDirective("project-a", "tenant-a", caller)
	second := server.replicaDirective("project-a", "tenant-a", caller)
	if first == nil || second == nil {
		t.Fatal("Local Replica directives cannot be disabled")
	}
	if first.Scope != second.Scope || first.Epoch != second.Epoch || first.VisibilityScope != second.VisibilityScope {
		t.Fatalf("directive is not deterministic: %#v %#v", first, second)
	}
	if first.ProtocolVersion != replicaProtocolVersion || first.VisibilityScope == "" {
		t.Fatalf("incomplete replica directive: %#v", first)
	}

	variants := map[string]*replicaDirective{
		"tenant": server.replicaDirective("project-a", "tenant-b", caller),
		"account": server.replicaDirective("project-a", "tenant-a", callerContext{
			user: &gonvex.Account{ID: "account-b"}, permissions: caller.permissions,
		}),
		"permissions": server.replicaDirective("project-a", "tenant-a", callerContext{
			user: caller.user, permissions: map[string]any{"role": "viewer"},
		}),
	}
	for name, directive := range variants {
		if directive.Scope == first.Scope || directive.VisibilityScope == first.VisibilityScope {
			t.Fatalf("%s did not isolate replica scope: %#v", name, directive)
		}
	}
}

func TestVisibilityScopeOwnsReplicaCursorEpoch(t *testing.T) {
	server := New(config.Config{})
	base := server.replicaDirective("project-a", "tenant-a", callerContext{user: &gonvex.Account{ID: "account-a"}})
	other := server.replicaDirective("project-a", "tenant-a", callerContext{user: &gonvex.Account{ID: "account-b"}})
	clock := replicaClock{DatabaseEpoch: "db-epoch", Revision: 7}
	definition := manifest.ReplicaCollectionDefinition{Table: "tasks", Key: "id"}
	first := replicaCursorForClock(clock, definition, base.VisibilityScope)
	if again := replicaCursorForClock(clock, definition, base.VisibilityScope); again != first {
		t.Fatalf("same visibility produced different cursors: %#v %#v", first, again)
	}
	if crossAccount := replicaCursorForClock(clock, definition, other.VisibilityScope); crossAccount.Epoch == first.Epoch {
		t.Fatal("different visibility scopes shared a replica cursor epoch")
	}
}

func TestReplicaDirectiveChangesWithTenantDatabaseRoute(t *testing.T) {
	server := New(config.Config{
		ProjectDatabases: map[string]string{"project-a": "postgres://db/project"},
		TenantDatabases:  map[string]string{"project-a:tenant-a": "postgres://db/tenant-a"},
	})
	before := server.replicaDirective("project-a", "tenant-a", callerContext{})
	server.projectMu.Lock()
	server.config.TenantDatabases["project-a:tenant-a"] = "postgres://db/tenant-b"
	server.projectMu.Unlock()
	after := server.replicaDirective("project-a", "tenant-a", callerContext{})
	if before.Epoch == after.Epoch || before.Scope == after.Scope {
		t.Fatalf("database route did not rotate replica scope: %#v %#v", before, after)
	}
}

func TestClearDataExplorerCacheDoesNotClearOtherProjects(t *testing.T) {
	redisServer := miniredis.RunT(t)
	cache, err := newRowsCache("redis://"+redisServer.Addr(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.close() })
	ctx := context.Background()
	projectARows := cache.rowsKey(ctx, "project-a", "tenant-a", "tasks", nil)
	projectBRows := cache.rowsKey(ctx, "project-b", "tenant-b", "tasks", nil)
	cache.set(ctx, projectARows, []byte("rows"))
	cache.set(ctx, projectBRows, []byte("rows"))
	if cleared := cache.clearProject(ctx, "project-a"); cleared != 1 {
		t.Fatalf("cleared entries = %d, want 1", cleared)
	}
	if _, outcome := cache.read(ctx, projectARows); outcome != "miss" {
		t.Fatalf("project A row outcome = %q", outcome)
	}
	if _, outcome := cache.read(ctx, projectBRows); outcome != "hit" {
		t.Fatalf("project B row outcome = %q", outcome)
	}
}
