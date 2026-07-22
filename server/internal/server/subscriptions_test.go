package server

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

func TestSubscriptionTokensAreDistinctMapKeys(t *testing.T) {
	tokens := make(map[*subscriptionToken]struct{}, 10_000)
	for range 10_000 {
		tokens[newSubscriptionToken()] = struct{}{}
	}
	if len(tokens) != 10_000 {
		t.Fatalf("distinct subscription tokens = %d, want 10000", len(tokens))
	}
}

func TestSubscriptionRunnerSerializesAndCoalescesBurst(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var running atomic.Int32
	var maximum atomic.Int32
	var executions atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string) (any, error) {
		current := running.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		call := executions.Add(1)
		if call == 1 {
			close(started)
			<-release
		}
		running.Add(-1)
		return []map[string]any{{"id": "task-1", "title": "same"}}, nil
	}
	group := &sharedSubscription{
		manager: manager, project: "project-a", tenant: "tenant-a", path: "tasks.list",
		ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{},
	}
	token := newSubscriptionToken()
	group.listeners[token] = querySubscription{token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}}

	group.request("initial", 0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first execution did not start")
	}
	for index := 0; index < 20; index++ {
		group.request("invalidate", float64(index+1))
	}
	close(release)
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 2
	})
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent executions = %d, want 1", maximum.Load())
	}
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.RerunsCoalesced; got != 20 {
		t.Fatalf("reruns coalesced = %d, want 20", got)
	}
}

func TestDependencyIndexSelectsOnlyMatchingTenantTableAndColumns(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	manager := server.subscriptions
	tasks := &sharedSubscription{manager: manager, project: "p", tenant: "a", path: "tasks.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "tasks", Columns: []string{"title"}}}}
	users := &sharedSubscription{manager: manager, project: "p", tenant: "a", path: "users.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "users"}}}
	otherTenant := &sharedSubscription{manager: manager, project: "p", tenant: "b", path: "tasks.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "tasks"}}}
	manager.mu.Lock()
	manager.indexGroupLocked(tasks)
	manager.indexGroupLocked(users)
	manager.indexGroupLocked(otherTenant)
	manager.mu.Unlock()

	manager.requestChange(tableChange{project: "p", tenant: "a", table: "tasks", operation: "update", changedColumns: []string{"description"}})
	if tasks.requested != 0 || users.requested != 0 || otherTenant.requested != 0 {
		t.Fatalf("irrelevant column selected a subscription: tasks=%d users=%d other=%d", tasks.requested, users.requested, otherTenant.requested)
	}
	manager.requestChange(tableChange{project: "p", tenant: "a", table: "tasks", operation: "update", changedColumns: []string{"title"}})
	if tasks.requested != 1 || users.requested != 0 || otherTenant.requested != 0 {
		t.Fatalf("dependency selection mismatch: tasks=%d users=%d other=%d", tasks.requested, users.requested, otherTenant.requested)
	}
}

func TestSharedKeyRequiresExplicitPermissionSharing(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	server.runtime.SyncManifest(manifest.Manifest{Project: "p", Functions: map[string]manifest.FunctionEntry{
		"tasks.list": {Kind: manifest.FunctionKindQuery, Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{{Table: "tasks"}}, ShareByPermissions: true}},
	}, Schema: manifest.EmptySchema()})
	base := querySubscription{project: "p", tenant: "a", path: "tasks.list", args: json.RawMessage(`{"status":"open"}`), caller: callerContext{user: &gonvex.User{ID: "one"}, permissions: map[string]any{"role": "member"}}}
	other := base
	other.caller.user = &gonvex.User{ID: "two"}
	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(other)
	if firstKey != secondKey {
		t.Fatal("same permission scope should share when explicitly enabled")
	}
	other.tenant = "b"
	thirdKey, _, _ := server.subscriptions.groupKeyAndDependencies(other)
	if firstKey == thirdKey {
		t.Fatal("different tenants must never share")
	}
}

func TestSharedKeySeparatesUsersAndBundleVersionsByDefault(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	current := manifest.Manifest{
		Project: "p",
		Functions: map[string]manifest.FunctionEntry{
			"tasks.list": {Kind: manifest.FunctionKindQuery, Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{{Table: "tasks"}}}},
		},
		Schema: manifest.EmptySchema(),
		Bundle: &manifest.SourceBundle{Hash: "bundle-a"},
	}
	if err := server.runtime.SyncManifest(current); err != nil {
		t.Fatal(err)
	}
	base := querySubscription{project: "p", tenant: "a", path: "tasks.list", args: json.RawMessage(`{"status":"open"}`), caller: callerContext{user: &gonvex.User{ID: "one"}, permissions: map[string]any{"role": "member"}}}
	otherUser := base
	otherUser.caller.user = &gonvex.User{ID: "two"}
	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(otherUser)
	if firstKey == secondKey {
		t.Fatal("different users must not share unless permission-only sharing is explicit")
	}

	current.Bundle.Hash = "bundle-b"
	if err := server.runtime.SyncManifest(current); err != nil {
		t.Fatal(err)
	}
	afterDeploy, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	if firstKey == afterDeploy {
		t.Fatal("bundle deployment must create a distinct shared key")
	}
}

func TestKeyedResultPatch(t *testing.T) {
	patch, ok := keyedResultPatch(
		json.RawMessage(`[{"id":"a","title":"old"},{"id":"b","title":"keep"}]`),
		json.RawMessage(`[{"id":"b","title":"keep"},{"id":"a","title":"new"},{"id":"c","title":"added"}]`),
	)
	if !ok || len(patch.Inserted) != 1 || len(patch.Updated) != 1 || len(patch.Deleted) != 0 {
		t.Fatalf("unexpected patch: %#v", patch)
	}
	if got := patch.Order; len(got) != 3 || got[0] != "b" || got[2] != "c" {
		t.Fatalf("unexpected order: %v", got)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied")
}
