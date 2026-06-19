package server

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestRowsCacheKeyIncludesTenant(t *testing.T) {
	cache := &rowsCache{ttl: time.Second}
	query := url.Values{"limit": []string{"10"}}

	a := cache.rowsKey("project-a", "tenant-a", "tasks", query)
	b := cache.rowsKey("project-a", "tenant-b", "tasks", query)

	if a == b {
		t.Fatalf("expected tenant-scoped cache keys to differ: %q", a)
	}
	if !strings.Contains(a, "project-a:tenant-a:tasks") {
		t.Fatalf("expected key to contain project and tenant scope, got %q", a)
	}
}
