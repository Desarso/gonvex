package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestParseRegistrationsIncludesPublicHTTPAsHTTP(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "register.go")
	source := `package backend

func Register(app interface{ PublicHTTP(string, any) }) {
	app.PublicHTTP("/webhooks/provider", ProviderWebhook)
}
`
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, err := parseRegistrations(dir, path)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := entries["/webhooks/provider"]
	if !ok || entry.Kind != manifest.FunctionKindHTTP || entry.Handler != "ProviderWebhook" {
		t.Fatalf("public HTTP registration = %#v", entry)
	}
}

func TestParseRegistrationsIncludesDependencyOptions(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "tasks.go")
	source := `package app
import "github.com/gonvex/gonvex/pkg/gonvex"
func Register(app *gonvex.App) {
  app.Query("tasks.list", ListTasks,
    gonvex.Reads("tasks").Columns("id", "title").Filters("status").OrdersBy("updated_at").Windowed(),
    gonvex.ShareByPermissions(),
  )
  app.Mutation("tasks.update", UpdateTask, gonvex.Writes("tasks").Columns("title"))
}`
	if err := os.WriteFile(file, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := parseRegistrations(root, file)
	if err != nil {
		t.Fatal(err)
	}
	query := entries["tasks.list"]
	if len(query.Dependencies.Reads) != 1 || !query.Dependencies.Reads[0].Windowed || !query.Dependencies.ShareByPermissions {
		t.Fatalf("query dependencies = %#v", query.Dependencies)
	}
	mutation := entries["tasks.update"]
	if len(mutation.Dependencies.Writes) != 1 || mutation.Dependencies.Writes[0].Table != "tasks" {
		t.Fatalf("mutation dependencies = %#v", mutation.Dependencies)
	}
}

func TestParseSchemaScopesTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.go")
	source := `package backend

import "github.com/gonvex/gonvex/pkg/gonvex"

func Schema(s *gonvex.Schema) {
	s.LandlordTable("billing_accounts", func(t *gonvex.Table) {
		t.ID("id")
		t.String("tenant_id")
	})
	s.TenantTable("tasks", func(t *gonvex.Table) {
		t.ID("id")
		t.String("title")
	})
	s.Table("messages", func(t *gonvex.Table) {
		t.ID("id")
		t.String("body")
		t.TrigramIndex("body_trgm", "body")
	})
}
`
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}

	schema, err := parseSchema(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := schema.LandlordTables["billing_accounts"]; !ok {
		t.Fatalf("expected billing_accounts in landlord tables")
	}
	if _, ok := schema.TenantTables["tasks"]; !ok {
		t.Fatalf("expected tasks in tenant tables")
	}
	if _, ok := schema.TenantTables["messages"]; !ok {
		t.Fatalf("expected legacy Table shorthand in tenant tables")
	}
	if _, ok := schema.Tables["billing_accounts"]; ok {
		t.Fatalf("did not expect landlord table in legacy tenant tables")
	}
	index := schema.TenantTables["messages"].Indexes["body_trgm"]
	if index.Kind != "trigram" {
		t.Fatalf("expected trigram index kind, got %q", index.Kind)
	}
	if len(index.Columns) != 1 || index.Columns[0] != "body" {
		t.Fatalf("expected body trigram index column, got %#v", index.Columns)
	}
}

func TestWriteBindingsWritesScopedSchemaFiles(t *testing.T) {
	root := t.TempDir()
	m := manifest.Manifest{
		Project:     "test-project",
		GeneratedAt: "2026-01-01T00:00:00Z",
		Functions:   map[string]manifest.FunctionEntry{},
		Schema: manifest.Schema{
			Tables: map[string]manifest.Table{
				"tasks": {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}, Indexes: map[string]manifest.Index{}},
			},
			LandlordTables: map[string]manifest.Table{
				"billing_accounts": {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}, Indexes: map[string]manifest.Index{}},
			},
			TenantTables: map[string]manifest.Table{
				"tasks": {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}, Indexes: map[string]manifest.Index{}},
			},
		},
	}

	if err := writeBindings(root, m); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{
		"gonvex/_generated/schema.ts",
		"gonvex/_generated/landlord/schema.ts",
		"gonvex/_generated/landlord/tables.ts",
		"gonvex/_generated/tenant/schema.ts",
		"gonvex/_generated/tenant/tables.ts",
	} {
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			t.Fatalf("expected generated file %s: %v", rel, err)
		}
	}

	topLevel, err := os.ReadFile(filepath.Join(root, "gonvex/_generated/schema.ts"))
	if err != nil {
		t.Fatal(err)
	}
	content := string(topLevel)
	if !strings.Contains(content, "billing_accounts") || !strings.Contains(content, "tasks") {
		t.Fatalf("expected top-level schema to contain landlord and tenant tables:\n%s", content)
	}
}
