package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

func TestEnsureRuntimeTenantDatabaseCoalescesConcurrentProvisioning(t *testing.T) {
	const (
		project     = "whagons-5"
		tenantID    = "calaluna"
		databaseURL = "postgres://postgres:postgres@127.0.0.1:5432/calaluna_whagons_5?sslmode=disable"
	)

	server := New(config.Config{})
	server.tenants[tenantStoreKey(project, tenantID)] = tenantTarget{
		ID:           tenantID,
		ProjectID:    project,
		databaseURL:  databaseURL,
		databaseName: "calaluna_whagons_5",
		Provisioned:  false,
	}

	var provisionCalls atomic.Int32
	firstProvisionStarted := make(chan struct{})
	releaseProvision := make(chan struct{})
	server.provisionTenant = func(context.Context, string, manifest.Schema) error {
		if provisionCalls.Add(1) == 1 {
			close(firstProvisionStarted)
		}
		<-releaseProvision
		return nil
	}

	const callers = 32
	start := make(chan struct{})
	errors := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			<-start
			_, err := server.ensureRuntimeTenantDatabase(context.Background(), project, tenantID, databaseURL)
			errors <- err
		}()
	}
	close(start)
	<-firstProvisionStarted
	// Keep the first provision in flight long enough for the other requests to
	// enter ensureRuntimeTenantDatabase too. Without per-tenant coalescing they
	// each start their own schema apply and exhaust PostgreSQL connections.
	time.Sleep(100 * time.Millisecond)
	close(releaseProvision)
	wait.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Fatalf("ensure tenant database: %v", err)
		}
	}
	if got := provisionCalls.Load(); got != 1 {
		t.Fatalf("expected one tenant schema provision, got %d", got)
	}
}
