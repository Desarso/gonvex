//go:build integration

package sqlmigration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestMigrationLifecycleOrderingAndNoTransaction(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	table := "gonvex_migration_test_" + suffix
	index := table + "_value"
	names := []string{"9101_create_" + suffix + ".sql", "9102_insert_" + suffix + ".sql", "9103_index_" + suffix + ".sql"}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + `"` + table + `"`)
		for _, name := range names {
			_, _ = db.Exec(`DELETE FROM gonvex_migrations WHERE name = $1`, name)
		}
	})
	parsed, err := Parse(map[string][]byte{
		names[1]: []byte("-- gonvex:scope tenant\nINSERT INTO \"" + table + "\" (value) VALUES ('ordered');"),
		names[0]: []byte("-- gonvex:scope tenant\nCREATE TABLE \"" + table + "\" (value text);"),
		names[2]: []byte("-- gonvex:scope tenant\n-- gonvex:no-transaction\nCREATE INDEX CONCURRENTLY \"" + index + "\" ON \"" + table + "\" (value);"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := applyDB(context.Background(), db, parsed, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(first.Applied, ",") != strings.Join(names, ",") {
		t.Fatalf("wrong order: %#v", first.Applied)
	}
	second, err := applyDB(context.Background(), db, parsed, false)
	if err != nil || len(second.Applied) != 0 {
		t.Fatalf("migration reran: result=%#v err=%v", second, err)
	}
	changed := append([]Migration(nil), parsed...)
	changed[0].Checksum = strings.Repeat("0", 64)
	if _, err := applyDB(context.Background(), db, changed, false); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum failure, got %v", err)
	}
}

func TestNoTransactionFailureDoesNotRecordMigration(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	name := "9201_partial_" + suffix + ".sql"
	table := "gonvex_partial_" + suffix
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS "` + table + `"`)
		_, _ = db.Exec(`DELETE FROM gonvex_migrations WHERE name = $1`, name)
	})
	parsed, err := Parse(map[string][]byte{name: []byte("-- gonvex:scope tenant\n-- gonvex:no-transaction\nCREATE TABLE \"" + table + "\" (id int); SELECT definitely_missing();")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyDB(context.Background(), db, parsed, false); err == nil {
		t.Fatal("expected statement failure")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM gonvex_migrations WHERE name = $1`, name).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("partially applied migration was recorded")
	}
}
