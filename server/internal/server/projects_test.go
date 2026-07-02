package server

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestGenerateProjectIDReturnsUUID(t *testing.T) {
	got, err := generateProjectID()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 36 {
		t.Fatalf("expected UUID length, got %d for %q", len(got), got)
	}
	if got[8] != '-' || got[13] != '-' || got[18] != '-' || got[23] != '-' {
		t.Fatalf("expected UUID separators, got %q", got)
	}
	if got[14] != '4' {
		t.Fatalf("expected UUID v4 marker, got %q", got)
	}
}

func TestConfiguredProjectsHydrateIntoProjectList(t *testing.T) {
	server := New(config.Config{
		ProjectDatabases: map[string]string{
			"whagons-5": "postgres://postgres:postgres@127.0.0.1:5432/gonvex_whagons_5?sslmode=disable",
		},
		ProjectKeys: map[string]string{
			"whagons-5": "secret",
		},
	})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/projects", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
	var payload struct {
		Projects []projectTarget `json:"projects"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Projects) != 1 {
		t.Fatalf("expected one configured project, got %d", len(payload.Projects))
	}
	project := payload.Projects[0]
	if project.ID != "whagons-5" || project.Database != "gonvex_whagons_5" || project.RuntimeCreated || project.TestTab {
		t.Fatalf("unexpected configured project: %+v", project)
	}
}

func TestUpdateProjectPersistsDatabaseMode(t *testing.T) {
	server := New(config.Config{})
	server.projects["whagons-5"] = projectTarget{
		ID:           "whagons-5",
		Name:         "whagons 5",
		Database:     "gonvex_dev",
		DatabaseMode: "single",
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPatch, "/dev/projects/whagons-5", bytes.NewBufferString(`{"databaseMode":"multiTenant"}`))

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Project projectTarget `json:"project"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Project.DatabaseMode != "multiTenant" {
		t.Fatalf("expected multiTenant mode, got %q", payload.Project.DatabaseMode)
	}
	if got := server.projects["whagons-5"].DatabaseMode; got != "multiTenant" {
		t.Fatalf("expected in-memory project mode to update, got %q", got)
	}
}

func TestTenantsEndpointIncludesGlobalTenantsForProject(t *testing.T) {
	server := New(config.Config{
		TenantDatabases: map[string]string{
			"global-tenant":      "postgres://postgres:postgres@127.0.0.1:5432/gonvex_global_tenant?sslmode=disable",
			"other:project-only": "postgres://postgres:postgres@127.0.0.1:5432/gonvex_other_project_only?sslmode=disable",
			"whagons-5:local":    "postgres://postgres:postgres@127.0.0.1:5432/gonvex_whagons_5_local?sslmode=disable",
		},
	})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/tenants?project=whagons-5", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
	var payload struct {
		Tenants []tenantTarget `json:"tenants"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	got := map[string]bool{}
	for _, tenant := range payload.Tenants {
		got[tenant.ID] = true
	}
	if !got["global-tenant"] || !got["local"] {
		t.Fatalf("expected global and project tenants, got %+v", got)
	}
	if got["project-only"] {
		t.Fatalf("did not expect other project's tenant, got %+v", got)
	}
}

func TestProjectKeyEndpointReturnsConfiguredProjectKey(t *testing.T) {
	server := New(config.Config{
		AdminKey: "admin-secret",
		ProjectDatabases: map[string]string{
			"whagons-5": "postgres://postgres:postgres@127.0.0.1:5432/gonvex_whagons_5?sslmode=disable",
		},
		ProjectKeys: map[string]string{
			"whagons-5": "secret",
		},
	})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/dev/projects/whagons-5/key", nil)
	request.Header.Set("authorization", "Bearer admin-secret")

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
	var payload struct {
		ProjectKey string `json:"projectKey"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.ProjectKey != "secret" {
		t.Fatalf("expected configured key, got %q", payload.ProjectKey)
	}
}

func TestProjectKeyEndpointRequiresAdminKey(t *testing.T) {
	server := New(config.Config{
		AdminKey: "admin-secret",
		ProjectDatabases: map[string]string{
			"whagons-5": "postgres://postgres:postgres@127.0.0.1:5432/gonvex_whagons_5?sslmode=disable",
		},
		ProjectKeys: map[string]string{
			"whagons-5": "secret",
		},
	})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/projects/whagons-5/key", nil))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
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

func TestTelemetryDatabaseNameIsPerProject(t *testing.T) {
	if got := telemetryDatabaseName("Whagons 5"); got != "gonvex_whagons_5_telemetry" {
		t.Fatalf("expected project telemetry database name, got %q", got)
	}
	if got := telemetryDatabaseName(""); got != "gonvex_default_telemetry" {
		t.Fatalf("expected default telemetry database name, got %q", got)
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

func TestGenerateProjectKeyHasExpectedShape(t *testing.T) {
	first, err := generateProjectKey("whagons-5")
	if err != nil {
		t.Fatal(err)
	}
	second, err := generateProjectKey("whagons-5")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first, "gvx_") || len(first) < 40 {
		t.Fatalf("unexpected project key shape: %q", first)
	}
	if first == second {
		t.Fatal("expected generated project keys to be unique")
	}
	if got := projectIDFromProjectKey(first); got != "whagons-5" {
		t.Fatalf("expected key to encode project id, got %q", got)
	}
}

func TestProjectIDFromProjectKeyRejectsLegacyOrMalformedKeys(t *testing.T) {
	for _, key := range []string{"", "secret", "gvx_onlytwo", "gvx_!!!_secret"} {
		if got := projectIDFromProjectKey(key); got != "" {
			t.Fatalf("expected malformed key %q to decode empty project, got %q", key, got)
		}
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

func TestTenantDatabaseNameUsesAliasWithScopedSuffix(t *testing.T) {
	projectID := "a7f9f7df-6a7b-45f7-b44d-bde2068dca27"
	got := tenantDatabaseNameWithAlias(projectID, "west-coast", "testing")
	want := "testing_a7f9f7df_6a7b_45f7_b44d_bde2068dca27"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
	if got == "testing" {
		t.Fatalf("expected scoped physical database name, got %q", got)
	}
	if len(got) > 63 {
		t.Fatalf("expected Postgres-safe name length, got %d for %q", len(got), got)
	}
	otherProject := tenantDatabaseNameWithAlias("Other App", "West Coast", "testing")
	if otherProject == got {
		t.Fatalf("expected project-scoped tenant database names, got %q for both projects", got)
	}
	otherTenant := tenantDatabaseNameWithAlias(projectID, "east-coast", "testing")
	if otherTenant != got {
		t.Fatalf("expected same project and alias to map to the same physical DB before collision guard, got %q and %q", got, otherTenant)
	}
}

func TestPersistedTenantDatabaseNamePrefersExistingDatabaseAlias(t *testing.T) {
	got := tenantDatabaseNameForPersistedTenant(
		"whagons-5",
		"calaluna",
		"calaluna",
		"calaluna",
		map[string]bool{"calaluna": true, "calaluna_whagons_5": true},
	)

	if got != "calaluna" {
		t.Fatalf("expected existing database alias to win, got %q", got)
	}
}

func TestPersistedTenantDatabaseNameFallsBackToProjectScopedName(t *testing.T) {
	got := tenantDatabaseNameForPersistedTenant(
		"whagons-5",
		"calaluna",
		"calaluna",
		"calaluna",
		map[string]bool{"calaluna_whagons_5": true},
	)

	if got != "calaluna_whagons_5" {
		t.Fatalf("expected project-scoped fallback, got %q", got)
	}
}

func TestTenantDatabaseAliasTakenChecksProjectScope(t *testing.T) {
	server := New(config.Config{})
	server.tenants["project-a:testing"] = tenantTarget{
		ID:        "testing",
		ProjectID: "project-a",
		Database:  "testing",
	}

	if !server.tenantDatabaseAliasTakenLocked("project-a", "testing", "project-a:other") {
		t.Fatal("expected same-project tenant database alias collision")
	}
	if server.tenantDatabaseAliasTakenLocked("project-b", "testing", "project-b:testing") {
		t.Fatal("did not expect cross-project tenant database alias collision")
	}
	if server.tenantDatabaseAliasTakenLocked("project-a", "testing", "project-a:testing") {
		t.Fatal("did not expect current tenant key to collide with itself")
	}
}

func TestTenantReferenceAliasesIncludeTenantIdentityAndDatabase(t *testing.T) {
	got := tenantReferenceAliases(tenantTarget{
		ID:           "kh7y5pbycsqxej1d5pq388d5gs84je8c",
		Name:         "Cala Luna",
		Database:     "calaluna",
		databaseName: "calaluna",
		domain:       "calaluna",
	})

	want := map[string]bool{
		"kh7y5pbycsqxej1d5pq388d5gs84je8c": true,
		"calaluna":                         true,
		"Cala Luna":                        true,
	}
	for _, value := range got {
		delete(want, value)
	}
	if len(want) > 0 {
		t.Fatalf("missing aliases: %+v from %v", want, got)
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
