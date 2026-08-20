package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

func TestBuildManifestShipsRootMigrationFiles(t *testing.T) {
	root := t.TempDir()
	backend := filepath.Join(root, "gonvex")
	if err := os.MkdirAll(filepath.Join(root, "migrations"), 0o755); err != nil {
		t.Fatal(err)
	}
	goFile := filepath.Join(backend, "register.go")
	if err := os.MkdirAll(backend, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goFile, []byte("package app\nfunc Register(any) {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sql := []byte("-- gonvex:scope tenant\nSELECT 1;\n")
	if err := os.WriteFile(filepath.Join(root, "migrations", "0001_start.sql"), sql, 0o644); err != nil {
		t.Fatal(err)
	}
	bundle, err := buildManifest(root, []string{goFile}, "project")
	if err != nil {
		t.Fatal(err)
	}
	if got := bundle.Bundle.Files["migrations/0001_start.sql"]; got != projectbundle.EncodeFile(sql) {
		t.Fatalf("migration was not bundled: %q", got)
	}
}

func TestParseRegistrationsIncludesDependencyOptions(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "tasks.go")
	source := `package app
import "github.com/gonvex/gonvex/pkg/gonvex"
func Register(app *gonvex.App) {
	  app.LiveQuery("tasks.list", ListTasks,
	    gonvex.LivePlan(gonvex.LiveTable("tasks").Select("id", "title").Filter(gonvex.Eq("status", gonvex.Arg("status"))).SortArgs("sort", "direction", "updated_at", "desc", "updated_at").WindowArgs("offset", "limit", 100, 200)),
	    gonvex.ShareByPermissions(),
	    gonvex.ShareResultFrom("internal.tasksShared", "query"),
	  )
  app.Reducer("tasks.update", UpdateTask, gonvex.OnlineOnlyNonOptimistic("test fixture"))
  app.Reducer("presence.beat", Beat, gonvex.OnlineOnlyNonOptimistic("test fixture"))
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
	if query.Dependencies.ShareResultFrom != "internal.tasksShared" || query.Dependencies.ShareResultField != "query" {
		t.Fatalf("query sharing dependencies = %#v", query.Dependencies)
	}
	if reducer := entries["tasks.update"]; reducer.Kind != manifest.FunctionKindReducer {
		t.Fatalf("reducer registration = %#v", reducer)
	}
}

func TestParseRegistrationsIncludesCompleteProgressiveReplicaCollectionDefinition(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "sync.go")
	source := `package app
import "github.com/gonvex/gonvex/pkg/gonvex"
func Register(app *gonvex.App) {
  app.ReplicaCollection("sync.tasks", ListTasks,
    gonvex.ReplicaTable("tasks").
      Key("_id").
      EqualArg("tenantId").
      VisibilityDependsOn("taskAcks", "taskApprovalInstances", "taskWorkspaceContexts").
      OrderBy("id", "desc").
      Progressive().
      Budget(100, 4194304),
  )
}`
	if err := os.WriteFile(file, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := parseRegistrations(root, file)
	if err != nil {
		t.Fatal(err)
	}
	entry := entries["sync.tasks"]
	if entry.Replica == nil {
		t.Fatal("sync definition was not parsed")
	}
	if entry.Replica.Mode != "progressive" || entry.Replica.MaxRows != 100 || entry.Replica.MaxBytes != 4194304 {
		t.Fatalf("progressive sync budget = %#v", entry.Replica)
	}
	if got := strings.Join(entry.Replica.VisibilityTables, ","); got != "taskAcks,taskApprovalInstances,taskWorkspaceContexts" {
		t.Fatalf("visibility dependencies = %q", got)
	}
	if len(entry.Dependencies.Reads) != 0 {
		t.Fatalf("Replica Collection unexpectedly declared query reads = %#v", entry.Dependencies.Reads)
	}
}

func TestParseSchemaScopesTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.go")
	source := `package backend

import "github.com/gonvex/gonvex/pkg/gonvex"

func Schema(s *gonvex.Schema) {
	s.ControlPlaneTable("billing_accounts", func(t *gonvex.Table) {
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
	if _, ok := schema.ControlPlaneTables["billing_accounts"]; !ok {
		t.Fatalf("expected billing_accounts in Control Plane tables")
	}
	if _, ok := schema.TenantTables["tasks"]; !ok {
		t.Fatalf("expected tasks in tenant tables")
	}
	if _, ok := schema.TenantTables["messages"]; !ok {
		t.Fatalf("expected legacy Table shorthand in tenant tables")
	}
	if _, ok := schema.Tables["billing_accounts"]; ok {
		t.Fatalf("did not expect Control Plane table in tenant tables")
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
			ControlPlaneTables: map[string]manifest.Table{
				"billing_accounts": {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}, Indexes: map[string]manifest.Index{}},
			},
			TenantTables: map[string]manifest.Table{
				"tasks": {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}, Indexes: map[string]manifest.Index{}},
			},
		},
	}
	legacyDir := filepath.Join(root, "gonvex/_generated/landlord")
	if err := os.MkdirAll(legacyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "schema.ts"), []byte("legacy"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := writeBindings(root, m); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{
		"gonvex/_generated/schema.ts",
		"gonvex/_generated/control-plane/schema.ts",
		"gonvex/_generated/control-plane/tables.ts",
		"gonvex/_generated/tenant/schema.ts",
		"gonvex/_generated/tenant/tables.ts",
	} {
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			t.Fatalf("expected generated file %s: %v", rel, err)
		}
	}
	if _, err := os.Stat(legacyDir); !os.IsNotExist(err) {
		t.Fatalf("expected obsolete generated directory to be removed, got %v", err)
	}

	topLevel, err := os.ReadFile(filepath.Join(root, "gonvex/_generated/schema.ts"))
	if err != nil {
		t.Fatal(err)
	}
	content := string(topLevel)
	if !strings.Contains(content, "billing_accounts") || !strings.Contains(content, "tasks") {
		t.Fatalf("expected top-level schema to contain Control Plane and tenant tables:\n%s", content)
	}
}
