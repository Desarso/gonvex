package server

import (
	"context"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestTenantListenerReadinessResetsAcrossDisconnect(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit: 8,
		TenantDatabases: map[string]string{
			"tenant-a": "postgres://listener.test/database",
		},
	})
	manager := runtime.subscriptions.listeners
	key := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	listener := &tenantListener{key: key, ready: make(chan struct{})}
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
		t.Fatal("new syncs must wait while the tenant listener reconnects")
	default:
	}
	manager.markReady(listener)
	select {
	case <-second:
	default:
		t.Fatal("reconnected listener did not release waiting syncs")
	}
}

func TestFreshnessActionsAreSerializedWithListenerDisconnect(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit: 8,
		TenantDatabases: map[string]string{
			"tenant-a": "postgres://listener.test/database",
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
		server:  runtime,
		project: "project-a",
		tenant:  "tenant-a",
		syncs:   map[string]*syncSubscription{},
	}
	subscription := &syncSubscription{
		conn: connection, id: "sync-a", path: "tasks.sync",
		project: connection.project, tenant: connection.tenant,
	}

	if err := connection.attachSync(context.Background(), subscription); err == nil {
		t.Fatal("sync must remain non-authoritative when no live listener can close the handoff race")
	}
	if len(connection.syncs) != 0 || subscription.listenerAcquired {
		t.Fatal("failed listener acquisition must not attach an apparently live subscription")
	}
}
