package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestSlugNormalizesProjectNames(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "lowercase words", in: "Acme Dashboard", want: "acme-dashboard"},
		{name: "trims punctuation", in: "  ***Acme!!!  ", want: "acme"},
		{name: "keeps digits", in: "Project 2026", want: "project-2026"},
		{name: "collapses separators", in: "one---two___three", want: "one-two-three"},
		{name: "empty after normalization", in: "!!!", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := slug(test.in); got != test.want {
				t.Fatalf("expected %q, got %q", test.want, got)
			}
		})
	}
}

func TestUniqueNameAddsNumericSuffix(t *testing.T) {
	taken := map[string]bool{"app": true, "app_2": true}

	got := uniqueName("app", func(value string) bool {
		return taken[value]
	})

	if got != "app_3" {
		t.Fatalf("expected app_3, got %q", got)
	}
}

func TestUniqueProjectIDChecksRuntimeAndConfiguredDatabases(t *testing.T) {
	server := New(config.Config{ProjectDatabases: map[string]string{"acme": "postgres://example/acme"}})
	server.projects["acme-2"] = projectTarget{ID: "acme-2"}

	server.projectMu.Lock()
	got := server.uniqueProjectIDLocked("Acme")
	server.projectMu.Unlock()

	if got != "acme_2" {
		t.Fatalf("expected configured database collision to produce acme_2, got %q", got)
	}
}

func TestUniqueProjectIDFallsBackForPunctuationOnlyNames(t *testing.T) {
	server := New(config.Config{})

	server.projectMu.Lock()
	got := server.uniqueProjectIDLocked("!!!")
	server.projectMu.Unlock()

	if got != "project" {
		t.Fatalf("expected project fallback, got %q", got)
	}
}

func TestUniqueDatabaseNameUsesProjectIDSlug(t *testing.T) {
	server := New(config.Config{})
	server.projects["existing"] = projectTarget{databaseName: "gonvex_acme_app"}

	server.projectMu.Lock()
	got := server.uniqueDatabaseNameLocked("acme-app")
	server.projectMu.Unlock()

	if got != "gonvex_acme_app_2" {
		t.Fatalf("expected gonvex_acme_app_2, got %q", got)
	}
}

func TestDatabaseURLRewritesPathAndPreservesConnectionOptions(t *testing.T) {
	got, err := databaseURL("postgres://user:pass@localhost:5432/old_db?sslmode=disable", "new_db")
	if err != nil {
		t.Fatal(err)
	}

	if got != "postgres://user:pass@localhost:5432/new_db?sslmode=disable" {
		t.Fatalf("unexpected database URL: %q", got)
	}
}

func TestDatabaseURLRejectsInvalidBaseURL(t *testing.T) {
	if _, err := databaseURL("://bad-url", "new_db"); err == nil {
		t.Fatal("expected invalid base URL to fail")
	}
}

func TestServerProjectQuoteIdentEscapesQuotes(t *testing.T) {
	got := quoteIdent(`bad"name`)
	if got != `"bad""name"` {
		t.Fatalf("unexpected quoted identifier: %q", got)
	}
	if strings.Contains(got, `bad"name"`) {
		t.Fatalf("quoteIdent did not escape embedded quote: %q", got)
	}
}

func TestTenantIDPrefersHeaderThenQuery(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows?tenant=query-tenant", nil)
	request.Header.Set("x-gonvex-tenant-id", "header-tenant")
	if got := tenantID(request); got != "header-tenant" {
		t.Fatalf("expected header tenant, got %q", got)
	}

	request = httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows?tenant=query-tenant", nil)
	if got := tenantID(request); got != "query-tenant" {
		t.Fatalf("expected query tenant, got %q", got)
	}

	request = httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows", nil)
	if got := tenantID(request); got != "" {
		t.Fatalf("expected empty tenant, got %q", got)
	}
}

func TestTenantIDFromRequestDefaultsToProjectThenDefault(t *testing.T) {
	if got := tenantIDFromRequest("project-a", ""); got != "project-a" {
		t.Fatalf("expected project fallback, got %q", got)
	}
	if got := tenantIDFromRequest("project-a", "tenant-a"); got != "tenant-a" {
		t.Fatalf("expected explicit tenant, got %q", got)
	}
	if got := tenantIDFromRequest("", ""); got != "default" {
		t.Fatalf("expected default fallback, got %q", got)
	}
}

func TestDatabaseURLForTenantPrefersProjectTenantMap(t *testing.T) {
	server := New(config.Config{
		PostgresURL: "postgres://example/base",
		ProjectDatabases: map[string]string{
			"project-a": "postgres://example/project",
		},
		TenantDatabases: map[string]string{
			"project-a:tenant-a": "postgres://example/tenant",
		},
	})

	if got := server.databaseURLForTenant("project-a", "tenant-a"); got != "postgres://example/tenant" {
		t.Fatalf("expected tenant database URL, got %q", got)
	}
	if got := server.databaseURLForTenant("project-a", ""); got != "postgres://example/project" {
		t.Fatalf("expected project database URL, got %q", got)
	}
}

func TestUniqueTenantIDChecksProjectScopedCollisions(t *testing.T) {
	server := New(config.Config{TenantDatabases: map[string]string{"project-a:acme": "postgres://example/acme"}})
	server.tenants["project-a:acme-2"] = tenantTarget{ID: "acme-2", ProjectID: "project-a"}

	server.projectMu.Lock()
	got := server.uniqueTenantIDLocked("project-a", "Acme")
	server.projectMu.Unlock()

	if got != "acme_2" {
		t.Fatalf("expected acme_2, got %q", got)
	}
}

func TestTenantDatabaseNameIncludesProjectAndTenant(t *testing.T) {
	got := tenantDatabaseName("Acme App", "West Coast")
	if got != "gonvex_acme_app_west_coast" {
		t.Fatalf("unexpected tenant database name: %q", got)
	}
}

func TestTenantStoreResolverReturnsNoopStoreWithoutDatabaseURL(t *testing.T) {
	resolver := newTenantStoreResolver(&config.Config{})

	store, err := resolver.TenantStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if store.TenantID != "tenant-a" || store.DB != nil || store.DatabaseURL != "" {
		t.Fatalf("unexpected store: %#v", store)
	}
}

func TestTenantStoreResolverReapsIdleStores(t *testing.T) {
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	resolver := newTenantStoreResolver(&config.Config{})
	resolver.now = func() time.Time { return now }
	resolver.idleTTL = time.Minute

	store, err := resolver.TenantStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	resolver.stores["tenant-a"] = store
	now = now.Add(2 * time.Minute)

	if got := resolver.ReapIdle(); got != 1 {
		t.Fatalf("expected one reaped store, got %d", got)
	}
	if len(resolver.stores) != 0 {
		t.Fatalf("expected empty stores after reap, got %#v", resolver.stores)
	}
}
