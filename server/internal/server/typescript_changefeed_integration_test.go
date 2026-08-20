package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
)

// This is the cutover invariant for SQL-owned TypeScript modules: installing
// the module after its migrations must discover the committed application
// schema and attach the authoritative durable transaction feed.
func TestTypeScriptSQLSchemaInstallsAuthoritativeChangeFeed(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	databaseURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_ts_feed_"+tenantRegistryTestSuffix(t))

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE tasks (
		id text PRIMARY KEY,
		title text NOT NULL,
		status text NOT NULL
	)`); err != nil {
		t.Fatalf("apply TypeScript SQL migration: %v", err)
	}

	result, err := installModuleChangeFeedForDatabase(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("install TypeScript module change feed: %v", err)
	}
	if len(result.Applied) == 0 {
		t.Fatal("change-feed installation reported no applied artifacts")
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(context.Background(), `SELECT set_config('gonvex.command_id', 'command-ts-1', true)`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(context.Background(), `INSERT INTO tasks (id, title, status) VALUES ('task-1', 'Freezer', 'ready')`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var (
		revision  int64
		commandID string
		tableName string
		rowID     string
		operation string
		newValue  []byte
	)
	if err := db.QueryRowContext(context.Background(), `SELECT
		revision, command_id, table_name, row_id, operation, new_value
		FROM _gonvex_sync_changes
		WHERE table_name = 'tasks' AND row_id = 'task-1'`).Scan(
		&revision, &commandID, &tableName, &rowID, &operation, &newValue,
	); err != nil {
		t.Fatalf("read committed TypeScript change: %v", err)
	}
	if revision <= 0 || commandID != "command-ts-1" || tableName != "tasks" || rowID != "task-1" || operation != "insert" {
		t.Fatalf("unexpected committed change: revision=%d commandId=%q table=%q row=%q operation=%q", revision, commandID, tableName, rowID, operation)
	}
	var row map[string]any
	if err := json.Unmarshal(newValue, &row); err != nil {
		t.Fatalf("decode committed row: %v", err)
	}
	if row["title"] != "Freezer" || row["status"] != "ready" {
		t.Fatalf("committed row = %#v", row)
	}
}
