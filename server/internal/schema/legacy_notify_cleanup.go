package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

const NotifySchemaVersion = manifest.NotifySchemaVersion

// RemoveLegacyNotifyTriggers is migration cleanup only. The statement-level
// invalidation architecture has no installer, listener, or runtime fallback;
// the durable row-level change feed is the sole realtime source of truth.
func RemoveLegacyNotifyTriggers(ctx context.Context, db *sql.DB, tables map[string]manifest.Table) ([]string, error) {
	var applied []string
	for _, tableName := range sortedTableNames(tables) {
		if !validIdent(tableName) {
			return applied, fmt.Errorf("invalid table name %q", tableName)
		}
		tableIdent := quoteIdent(tableName)
		for _, suffix := range []string{"notify", "notify_insert", "notify_update", "notify_delete"} {
			name := quoteIdent("gonvex_" + tableName + "_" + suffix)
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", name, tableIdent)); err != nil {
				return applied, err
			}
		}
		for _, operation := range []string{"insert", "update", "delete"} {
			name := quoteIdent("gonvex_notify_" + tableName + "_" + operation)
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", name)); err != nil {
				return applied, err
			}
		}
		applied = append(applied, fmt.Sprintf("removed legacy invalidation triggers for %s", tableName))
	}
	for version := 1; version <= 14; version++ {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS gonvex_notify_schema_v%d()", version)); err != nil {
			return applied, err
		}
	}
	return applied, nil
}
