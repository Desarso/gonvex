package server

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

func TestRowsCacheKeyIncludesTenant(t *testing.T) {
	cache := &rowsCache{ttl: time.Second}
	query := url.Values{"limit": []string{"10"}}

	a := cache.rowsKey(context.Background(), "project-a", "tenant-a", "tasks", query)
	b := cache.rowsKey(context.Background(), "project-a", "tenant-b", "tasks", query)

	if a == b {
		t.Fatalf("expected tenant-scoped cache keys to differ: %q", a)
	}
	if !strings.Contains(a, "project-a:tenant-a:tasks") {
		t.Fatalf("expected key to contain project and tenant scope, got %q", a)
	}
}

func TestRowsCacheInvalidationAdvancesGenerationWithoutScanningKeys(t *testing.T) {
	redisServer := miniredis.RunT(t)
	cache, err := newRowsCache("redis://"+redisServer.Addr(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.close() })
	ctx := context.Background()

	before := cache.rowsKey(ctx, "project-a", "tenant-a", "tasks", nil)
	cache.set(ctx, before, []byte("stale"))
	cache.invalidateRows(ctx, "project-a", "tenant-a", "tasks")
	after := cache.rowsKey(ctx, "project-a", "tenant-a", "tasks", nil)

	if before == after {
		t.Fatalf("row cache generation did not advance: %q", before)
	}
	if value, outcome := cache.read(ctx, before); outcome != "hit" || string(value) != "stale" {
		t.Fatalf("invalidation scanned/deleted old entry instead of retiring it by generation: outcome=%q value=%q", outcome, value)
	}
	if _, outcome := cache.read(ctx, after); outcome != "miss" {
		t.Fatalf("new generation unexpectedly reused stale entry: %q", outcome)
	}
}
