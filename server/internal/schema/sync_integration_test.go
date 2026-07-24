//go:build integration

package schema

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestDurableSyncLogOrdersConcurrentTransactionsAndProjectsColumns(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tableName := fmt.Sprintf("sync_stress_%d", time.Now().UnixNano())
	table := manifest.Table{
		Columns: map[string]manifest.Column{
			"id":          {Type: "id", PrimaryKey: true},
			"workspaceId": {Type: "string"},
			"title":       {Type: "string"},
			"secret":      {Type: "string", Nullable: true},
			"updatedAt":   {Type: "int64"},
			"deletedAt":   {Type: "int64", Nullable: true},
		},
	}
	definition := manifest.SyncDefinition{
		Table:          tableName,
		Key:            "id",
		Columns:        []string{"id", "workspaceId", "title", "updatedAt", "deletedAt"},
		EqualFilters:   map[string]string{"workspaceId": "workspaceId"},
		ExcludeWhenSet: []string{"deletedAt"},
		OrderBy:        "updatedAt",
		OrderDirection: "desc",
		Mode:           "progressive",
		MaxRows:        100,
	}
	if _, err := ApplyWithSync(
		context.Background(),
		databaseURL,
		manifest.Schema{Tables: map[string]manifest.Table{tableName: table}},
		map[string]manifest.SyncDefinition{tableName: definition},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableName))
		_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent(syncArtifactName(tableName, "stage")) + `()`)
	})

	// One multi-row mutation must remain one atomic cursor revision.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for index := range 10 {
		if _, err := tx.Exec(
			`INSERT INTO `+quoteIdent(tableName)+` ("id", "workspaceId", "title", "secret", "updatedAt") VALUES ($1, $2, $3, $4, $5)`,
			fmt.Sprintf("batch-%d", index), "workspace-a", "visible", "must-not-leak", index,
		); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var batchRevisions int
	var batchOrdinals int
	var leakedSecrets int
	if err := db.QueryRow(`
		SELECT count(DISTINCT revision), count(DISTINCT ordinal),
		       count(*) FILTER (WHERE new_value ? 'secret')
		FROM _gonvex_sync_changes
		WHERE table_name = $1 AND row_id LIKE 'batch-%'
	`, tableName).Scan(&batchRevisions, &batchOrdinals, &leakedSecrets); err != nil {
		t.Fatal(err)
	}
	if batchRevisions != 1 || batchOrdinals != 10 || leakedSecrets != 0 {
		t.Fatalf("batch revisions=%d ordinals=%d leakedSecrets=%d", batchRevisions, batchOrdinals, leakedSecrets)
	}

	// Concurrent writers contend on the commit clock. Every committed mutation
	// must receive a distinct, fully assigned revision with no lost events.
	const writers = 100
	var wait sync.WaitGroup
	errors := make(chan error, writers)
	for index := range writers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, execErr := db.Exec(
				`INSERT INTO `+quoteIdent(tableName)+` ("id", "workspaceId", "title", "updatedAt") VALUES ($1, $2, $3, $4)`,
				fmt.Sprintf("concurrent-%03d", index), "workspace-a", "stress", index,
			)
			if execErr != nil {
				errors <- execErr
			}
		}(index)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	var events int
	var revisions int
	var unassigned int
	if err := db.QueryRow(`
		SELECT count(*), count(DISTINCT revision),
		       count(*) FILTER (WHERE revision IS NULL OR ordinal IS NULL)
		FROM _gonvex_sync_changes
		WHERE table_name = $1 AND row_id LIKE 'concurrent-%'
	`, tableName).Scan(&events, &revisions, &unassigned); err != nil {
		t.Fatal(err)
	}
	if events != writers || revisions != writers || unassigned != 0 {
		t.Fatalf("concurrent events=%d revisions=%d unassigned=%d", events, revisions, unassigned)
	}
}
