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

func TestApplySkipsAllDDLForUnchangedSchema(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tables := map[string]manifest.Table{}
	for index := range 5 {
		tableName := fmt.Sprintf("snapshot_%d_%d", time.Now().UnixNano(), index)
		tables[tableName] = manifest.Table{
			Columns: map[string]manifest.Column{
				"id":    {Type: "id", PrimaryKey: true},
				"value": {Type: "string", Nullable: true},
			},
			Indexes: map[string]manifest.Index{
				"by_value": {Columns: []string{"value"}},
			},
		}
	}
	t.Cleanup(func() {
		for tableName := range tables {
			_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableName))
			for _, suffix := range []string{"insert", "update", "delete"} {
				_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_notify_"+tableName+"_"+suffix) + `()`)
			}
		}
	})

	desired := manifest.Schema{Tables: tables}
	first, err := Apply(context.Background(), databaseURL, desired)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Applied) == 0 {
		t.Fatal("first schema apply unexpectedly made no changes")
	}
	second, err := Apply(context.Background(), databaseURL, desired)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 {
		t.Fatalf("unchanged schema still executed DDL: %#v", second.Applied)
	}
}

func TestApplyDropsOnlyEmptyApprovedUndeclaredColumns(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tableName := fmt.Sprintf("schema_drop_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE TABLE ` + quoteIdent(tableName) + ` (id text primary key, empty_value text, kept_value text); INSERT INTO ` + quoteIdent(tableName) + ` (id, kept_value) VALUES ('1', 'constant')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableName)) })
	desired := manifest.Schema{Tables: map[string]manifest.Table{tableName: {Columns: map[string]manifest.Column{"id": {Type: "id", PrimaryKey: true}}}}}
	candidates, err := EmptyUndeclaredColumns(context.Background(), databaseURL, desired)
	if err != nil {
		t.Fatal(err)
	}
	if !candidates[tableName+".empty_value"] || candidates[tableName+".kept_value"] {
		t.Fatalf("wrong candidates: %#v", candidates)
	}
	result, err := ApplyWithOptions(context.Background(), databaseURL, desired, nil, ApplyOptions{DropEmptyUndeclaredColumns: candidates})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Applied) == 0 {
		t.Fatal("empty column was not dropped")
	}
	columns, err := existingColumns(context.Background(), db, tableName)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := columns["empty_value"]; exists {
		t.Fatal("empty undeclared column remains")
	}
	if _, exists := columns["kept_value"]; !exists {
		t.Fatal("non-empty undeclared column was dropped")
	}
	if _, exists := columns["id"]; !exists {
		t.Fatal("declared column was dropped")
	}
}
