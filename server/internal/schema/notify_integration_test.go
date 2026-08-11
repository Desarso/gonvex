package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestTableNotificationIncludesTransactionMutationID(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	tableName := fmt.Sprintf("notify_commit_%d", time.Now().UnixNano())
	table := manifest.Table{Columns: map[string]manifest.Column{
		"id":    {Type: "id", PrimaryKey: true},
		"value": {Type: "string"},
	}}
	if _, err := Apply(ctx, databaseURL, manifest.Schema{Tables: map[string]manifest.Table{tableName: table}}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS ` + quoteIdent(tableName))
		for _, operation := range []string{"insert", "update", "delete"} {
			_, _ = db.Exec(`DROP FUNCTION IF EXISTS ` + quoteIdent("gonvex_notify_"+tableName+"_"+operation) + `()`)
		}
		_ = db.Close()
	})

	listener, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(ctx)
	if _, err := listener.Exec(ctx, `LISTEN `+NotifyChannel); err != nil {
		t.Fatal(err)
	}

	const mutationID = "mutation-db-one"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('gonvex.mutation_id', $1, true)`, mutationID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+quoteIdent(tableName)+` ("id", "value") VALUES ('row-1', 'latest')`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		notification, err := listener.WaitForNotification(waitCtx)
		if err != nil {
			t.Fatalf("wait for table notification: %v", err)
		}
		var payload struct {
			Table      string `json:"table"`
			MutationID string `json:"mutationId"`
		}
		if json.Unmarshal([]byte(notification.Payload), &payload) != nil || payload.Table != tableName {
			continue
		}
		if payload.MutationID != mutationID {
			t.Fatalf("table notification mutationId = %q, want %q; payload=%s", payload.MutationID, mutationID, notification.Payload)
		}
		return
	}
}
