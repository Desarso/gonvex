//go:build integration

package schema

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestCreateIndexesReconcilesUniqueness(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tableName := fmt.Sprintf("schema_index_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE TABLE ` + quoteIdent(tableName) + ` (value text)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableName)) })
	physicalName := tableName + "_by_value"
	if _, err := db.Exec(`CREATE INDEX ` + quoteIdent(physicalName) + ` ON ` + quoteIdent(tableName) + ` (value)`); err != nil {
		t.Fatal(err)
	}

	table := manifest.Table{Indexes: map[string]manifest.Index{
		"by_value": {Columns: []string{"value"}, Unique: true},
	}}
	if _, err := createIndexes(context.Background(), db, tableName, table); err != nil {
		t.Fatal(err)
	}
	exists, unique, err := existingIndexUniqueness(context.Background(), db, physicalName)
	if err != nil || !exists || !unique {
		t.Fatalf("reconciled index exists=%v unique=%v err=%v", exists, unique, err)
	}
}

func TestCreateIndexesRestoresPriorIndexWhenUniquenessCannotBeStrengthened(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tableName := fmt.Sprintf("schema_duplicate_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE TABLE ` + quoteIdent(tableName) + ` (value text); INSERT INTO ` + quoteIdent(tableName) + ` (value) VALUES ('same'), ('same')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableName)) })
	physicalName := tableName + "_by_value"
	if _, err := db.Exec(`CREATE INDEX ` + quoteIdent(physicalName) + ` ON ` + quoteIdent(tableName) + ` (value)`); err != nil {
		t.Fatal(err)
	}

	table := manifest.Table{Indexes: map[string]manifest.Index{
		"by_value": {Columns: []string{"value"}, Unique: true},
	}}
	if _, err := createIndexes(context.Background(), db, tableName, table); err == nil {
		t.Fatal("duplicate data unexpectedly accepted a unique index")
	}
	exists, unique, err := existingIndexUniqueness(context.Background(), db, physicalName)
	if err != nil || !exists || unique {
		t.Fatalf("restored index exists=%v unique=%v err=%v", exists, unique, err)
	}
}

func TestInstallNotifyTriggersSkipsCompleteInstall(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tableName := fmt.Sprintf("schema_notify_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE TABLE ` + quoteIdent(tableName) + ` (id text primary key)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableName))
		for _, suffix := range []string{"insert", "update", "delete"} {
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_notify_"+tableName+"_"+suffix) + `()`)
		}
	})

	tables := map[string]manifest.Table{
		tableName: {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}},
	}
	first, err := InstallNotifyTriggers(context.Background(), db, tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("first trigger install applied %d changes, want 1: %#v", len(first), first)
	}

	second, err := InstallNotifyTriggers(context.Background(), db, tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("unchanged trigger install applied %d changes, want 0: %#v", len(second), second)
	}
}
