package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gonvex/gonvex/pkg/manifest"
)

const NotifyChannel = "gonvex_table_change"

func InstallNotifyTriggers(ctx context.Context, db *sql.DB, tables map[string]manifest.Table) ([]string, error) {
	var applied []string
	for _, tableName := range sortedTableNames(tables) {
		installed, err := notifyTriggersInstalled(ctx, db, tableName)
		if err != nil {
			return applied, err
		}
		if installed {
			continue
		}
		table := tables[tableName]
		statement, err := notifySQLForTable(tableName, table)
		if err != nil {
			return applied, err
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return applied, err
		}
		applied = append(applied, fmt.Sprintf("ensured notify triggers for %s", tableName))
	}
	return applied, nil
}

// notifyTriggersInstalled avoids rewriting three functions and three triggers
// for every table on every schema sync. Those definitions are independent of
// ordinary column changes; they only need installation for a new table or when
// an artifact is missing. Rebuilding hundreds of unchanged triggers across
// every tenant can otherwise exceed reverse-proxy request timeouts before the
// new manifest is persisted.
func notifyTriggersInstalled(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	triggerPrefix := "gonvex_" + tableName + "_notify_"
	functionPrefix := "gonvex_notify_" + tableName + "_"
	var triggerCount int
	var functionCount int
	err := db.QueryRowContext(ctx, `
		SELECT
			(
				SELECT count(*)
				FROM pg_catalog.pg_trigger t
				JOIN pg_catalog.pg_class relation ON relation.oid = t.tgrelid
				JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname = current_schema()
					AND relation.relname = $1
					AND NOT t.tgisinternal
					AND t.tgname IN ($2, $3, $4)
			),
			(
				SELECT count(*)
				FROM pg_catalog.pg_proc p
				JOIN pg_catalog.pg_namespace namespace ON namespace.oid = p.pronamespace
				WHERE namespace.nspname = current_schema()
					AND p.proname IN ($5, $6, $7)
					AND p.pronargs = 0
			)
	`,
		tableName,
		triggerPrefix+"insert", triggerPrefix+"update", triggerPrefix+"delete",
		functionPrefix+"insert", functionPrefix+"update", functionPrefix+"delete",
	).Scan(&triggerCount, &functionCount)
	if err != nil {
		return false, err
	}
	return triggerCount == 3 && functionCount == 3, nil
}

func NotifySQLForTable(tableName string, table manifest.Table) (string, error) {
	return notifySQLForTable(tableName, table)
}

func notifySQLForTable(tableName string, table manifest.Table) (string, error) {
	if !validIdent(tableName) {
		return "", fmt.Errorf("invalid table name %q", tableName)
	}
	hasID := false
	if column, ok := table.Columns["id"]; ok && column.Type != "" {
		hasID = true
	}

	functionPrefix := "gonvex_notify_" + tableName
	insertFunction := quoteIdent(functionPrefix + "_insert")
	updateFunction := quoteIdent(functionPrefix + "_update")
	deleteFunction := quoteIdent(functionPrefix + "_delete")
	insertTrigger := quoteIdent("gonvex_" + tableName + "_notify_insert")
	updateTrigger := quoteIdent("gonvex_" + tableName + "_notify_update")
	deleteTrigger := quoteIdent("gonvex_" + tableName + "_notify_delete")
	tableIdent := quoteIdent(tableName)

	return strings.Join([]string{
		notifyFunctionSQL(insertFunction, tableName, "new_rows", hasID, true),
		notifyFunctionSQL(updateFunction, tableName, "new_rows", hasID, false),
		notifyFunctionSQL(deleteFunction, tableName, "old_rows", hasID, true),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", quoteIdent("gonvex_"+tableName+"_notify"), tableIdent),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", insertTrigger, tableIdent),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", updateTrigger, tableIdent),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", deleteTrigger, tableIdent),
		fmt.Sprintf(`CREATE TRIGGER %s
AFTER INSERT ON %s
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION %s();`, insertTrigger, tableIdent, insertFunction),
		fmt.Sprintf(`CREATE TRIGGER %s
AFTER UPDATE ON %s
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION %s();`, updateTrigger, tableIdent, updateFunction),
		fmt.Sprintf(`CREATE TRIGGER %s
AFTER DELETE ON %s
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION %s();`, deleteTrigger, tableIdent, deleteFunction),
	}, "\n\n"), nil
}

func notifyFunctionSQL(functionName string, tableName string, transitionTable string, hasID bool, broad bool) string {
	idRead := fmt.Sprintf(`SELECT count(*), COALESCE(array_agg(id::text), ARRAY[]::text[])
  INTO row_count, ids
  FROM (SELECT id FROM %s WHERE id IS NOT NULL LIMIT 500) limited;`, transitionTable)
	if !hasID {
		idRead = fmt.Sprintf(`SELECT count(*)
  INTO row_count
  FROM %s;
  ids := ARRAY[]::text[];`, transitionTable)
	}

	broadExpression := "row_count >= 500"
	idsExpression := "CASE WHEN row_count < 500 THEN ids ELSE ARRAY[]::text[] END"
	if broad || !hasID {
		broadExpression = "true"
		idsExpression = "ARRAY[]::text[]"
	}

	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s()
RETURNS trigger AS $$
DECLARE
  row_count integer;
  ids text[];
BEGIN
  %s

  PERFORM pg_notify(%s, json_build_object(
    'table', %s,
    'broad', %s,
    'count', row_count,
    'ids', %s
  )::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;`, functionName, idRead, quoteLiteral(NotifyChannel), quoteLiteral(tableName), broadExpression, idsExpression)
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
