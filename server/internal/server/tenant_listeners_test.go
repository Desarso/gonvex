package server

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestParseSyncNotifyPayloadUsesTablesOnlyWithRevisionIdentity(t *testing.T) {
	parsed := parseSyncNotifyPayload(`{"revision":42,"epoch":"epoch-a","tables":["tasks","statuses","tasks"]}`)
	if parsed.Epoch != "epoch-a" || parsed.Revision != 42 || !reflect.DeepEqual(parsed.Tables, []string{"statuses", "tasks"}) {
		t.Fatalf("unexpected parsed sync notification: %#v", parsed)
	}

	for name, raw := range map[string]string{
		"legacy":      `{"revision":42,"epoch":"epoch-a"}`,
		"empty":       `{"revision":42,"epoch":"epoch-a","tables":[]}`,
		"no epoch":    `{"revision":42,"tables":["tasks"]}`,
		"no revision": `{"epoch":"epoch-a","tables":["tasks"]}`,
		"malformed":   `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if got := parseSyncNotifyPayload(raw); len(got.Tables) != 0 {
				t.Fatalf("fallback payload retained filterable tables: %#v", got)
			}
		})
	}
}

func TestTenantListenerReconnectsWhenDatabaseRouteChanges(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit:       8,
		TenantListenerIdleTimeout: time.Minute,
		TenantDatabases: map[string]string{
			"project-a:tenant-a": "postgres://listener.test/old-database",
		},
	})
	manager := runtime.subscriptions.listeners
	key := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	oldContext, oldCancel := context.WithCancel(context.Background())
	oldListener := &tenantListener{
		key: key, databaseURL: "postgres://listener.test/old-database",
		refs: 2, cancel: oldCancel, ready: make(chan struct{}), connected: true,
	}
	close(oldListener.ready)
	manager.active[key] = oldListener

	runtime.projectMu.Lock()
	runtime.config.TenantDatabases["project-a:tenant-a"] = "postgres://listener.test/new-database"
	runtime.projectMu.Unlock()

	ready := manager.acquire(key.project, key.tenant)
	manager.mu.Lock()
	current := manager.active[key]
	manager.mu.Unlock()
	if current == oldListener {
		t.Fatal("database route change reused a listener connected to the old tenant database")
	}
	select {
	case <-oldContext.Done():
	default:
		t.Fatal("database route change did not stop the old PostgreSQL listener")
	}
	if ready == nil || current == nil || ready != current.ready {
		t.Fatal("database route change did not publish the replacement readiness barrier")
	}
	current.cancel()
}

func TestTenantListenerReadinessResetsAcrossDisconnect(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit: 8,
		TenantDatabases: map[string]string{
			"project-a:tenant-a": "postgres://listener.test/database",
		},
	})
	manager := runtime.subscriptions.listeners
	key := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	listener := &tenantListener{
		key: key, databaseURL: "postgres://listener.test/database", ready: make(chan struct{}),
	}
	manager.active[key] = listener

	first := manager.acquire(key.project, key.tenant)
	if first == nil {
		t.Fatal("existing tenant listener must expose a readiness barrier")
	}
	select {
	case <-first:
		t.Fatal("a listener must not be ready before PostgreSQL LISTEN succeeds")
	default:
	}
	manager.markReady(listener)
	select {
	case <-first:
	default:
		t.Fatal("the readiness barrier did not open after LISTEN succeeded")
	}

	manager.markDisconnected(listener)
	second := manager.acquire(key.project, key.tenant)
	if second == nil || second == first {
		t.Fatal("a disconnected listener must publish a new readiness barrier")
	}
	select {
	case <-second:
		t.Fatal("new replicas must wait while the tenant listener reconnects")
	default:
	}
	manager.markReady(listener)
	select {
	case <-second:
	default:
		t.Fatal("reconnected listener did not release waiting replicas")
	}
}

func TestFreshnessActionsAreSerializedWithListenerDisconnect(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit: 8,
		TenantDatabases: map[string]string{
			"project-a:tenant-a": "postgres://listener.test/database",
		},
	})
	manager := runtime.subscriptions.listeners
	key := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	ready := make(chan struct{})
	close(ready)
	listener := &tenantListener{key: key, ready: ready, connected: true}
	manager.active[key] = listener

	actions := 0
	if _, ok := manager.whileConnected(key.project, key.tenant, func() { actions++ }); !ok {
		t.Fatal("a connected listener must permit a freshness-sensitive action")
	}
	manager.markDisconnected(listener)
	next, ok := manager.whileConnected(key.project, key.tenant, func() { actions++ })
	if ok || next == nil || next == ready {
		t.Fatal("a stale readiness barrier must not permit a late authoritative action")
	}
	if actions != 1 {
		t.Fatalf("freshness action ran %d times, want exactly once before disconnect", actions)
	}
}

func TestSyncAttachFailsClosedWithoutLiveListener(t *testing.T) {
	runtime := New(config.Config{TenantListenerLimit: 0})
	connection := &wsConn{
		server:   runtime,
		project:  "project-a",
		tenant:   "tenant-a",
		replicas: map[string]*replicaSubscription{},
	}
	subscription := &replicaSubscription{
		conn: connection, id: "sync-a", path: "tasks.sync",
		project: connection.project, tenant: connection.tenant,
	}

	if err := connection.attachReplica(context.Background(), subscription); err == nil {
		t.Fatal("sync must remain non-authoritative when no live listener can close the handoff race")
	}
	if len(connection.replicas) != 0 || subscription.listenerAcquired {
		t.Fatal("failed listener acquisition must not attach an apparently live subscription")
	}
}
