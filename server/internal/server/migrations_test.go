package server

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/sqlmigration"
)

func TestMigrationsFromManifestReadsTypeScriptModuleArtifact(t *testing.T) {
	source := "-- gonvex:scope tenant\nSELECT 1;\n"
	current := manifest.Manifest{Module: &manifest.ModuleArtifact{Files: map[string]string{
		"migrations/0001_tasks.sql": base64.StdEncoding.EncodeToString([]byte(source)),
		"gonvex/index.ts":           base64.StdEncoding.EncodeToString([]byte("export {};")),
	}}}
	migrations, err := migrationsFromManifest(current)
	if err != nil {
		t.Fatalf("read TypeScript migrations: %v", err)
	}
	if len(migrations) != 1 || migrations[0].Name != "0001_tasks.sql" || string(migrations[0].SQL) != source {
		t.Fatalf("unexpected migrations: %#v", migrations)
	}
}

func TestTenantSQLMigrationsContinueAfterFailure(t *testing.T) {
	tenants := []tenantTarget{{ID: "a", databaseURL: "db-a"}, {ID: "b", databaseURL: "db-b"}, {ID: "c", databaseURL: "db-c"}}
	called := map[string]bool{}
	var calledMu sync.Mutex
	result, err := applyTenantSQLMigrations(context.Background(), tenants, []sqlmigration.Migration{{Name: "0001_x.sql"}}, false,
		func(_ context.Context, databaseURL string, _ []sqlmigration.Migration, _ bool) (sqlmigration.Result, error) {
			calledMu.Lock()
			called[databaseURL] = true
			calledMu.Unlock()
			if databaseURL == "db-b" {
				return sqlmigration.Result{}, errors.New("broken")
			}
			return sqlmigration.Result{Applied: []string{"0001_x.sql"}}, nil
		})
	if err == nil || !strings.Contains(err.Error(), "tenant b") {
		t.Fatalf("expected labeled aggregate error, got %v", err)
	}
	if len(called) != 3 {
		t.Fatalf("failure stopped fleet: %#v", called)
	}
	if len(result.Applied) != 2 {
		t.Fatalf("successful tenants not reported: %#v", result)
	}
}

func TestIntersectColumnSetsRequiresEveryTenant(t *testing.T) {
	got := intersectColumnSets([]map[string]bool{{"items.absent_elsewhere": true, "items.blocked": true, "items.all": true}, {"items.blocked": false, "items.all": true}})
	if !got["items.absent_elsewhere"] || got["items.blocked"] || !got["items.all"] {
		t.Fatalf("unexpected intersection: %#v", got)
	}
}
