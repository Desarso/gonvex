package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/schema"
)

func TestApplyTenantSchemasRunsIndependentDatabasesConcurrently(t *testing.T) {
	tenants := make([]tenantTarget, 6)
	for index := range tenants {
		tenants[index] = tenantTarget{
			ID:          string(rune('a' + index)),
			ProjectID:   "project-a",
			Database:    string(rune('a' + index)),
			databaseURL: "postgres://tenant-" + string(rune('a'+index)),
		}
	}

	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	apply := func(context.Context, string, manifest.Schema) (schema.Result, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		<-release
		return schema.Result{}, nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := applyTenantSchemas(context.Background(), tenants, manifest.EmptySchema(), apply)
		done <- err
	}()

	deadline := time.Now().Add(time.Second)
	for maximum.Load() < tenantSchemaApplyConcurrency && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got != tenantSchemaApplyConcurrency {
		t.Fatalf("maximum tenant schema concurrency = %d, want %d", got, tenantSchemaApplyConcurrency)
	}
}
