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
