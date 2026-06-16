package server

import (
	"strings"
	"testing"

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
