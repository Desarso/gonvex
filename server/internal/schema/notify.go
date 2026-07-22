package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gonvex/gonvex/pkg/manifest"
)

const NotifyChannel = "gonvex_table_change"
const NotifySchemaVersion = manifest.NotifySchemaVersion
const notifySchemaVersionFunction = "gonvex_notify_schema_v2"

func InstallNotifyTriggers(ctx context.Context, db *sql.DB, tables map[string]manifest.Table) ([]string, error) {
	artifacts, err := loadNotifyArtifacts(ctx, db)
	if err != nil {
		return nil, err
	}
	var applied []string
	for _, tableName := range sortedTableNames(tables) {
		if artifacts.installed(tableName) {
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
	if !artifacts.functions[notifySchemaVersionFunction] {
		if _, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION gonvex_notify_schema_v2() RETURNS integer AS $$ BEGIN RETURN 2; END; $$ LANGUAGE plpgsql IMMUTABLE;`); err != nil {
			return applied, err
		}
	}
	return applied, nil
}

type notifyArtifacts struct {
	triggers  map[string]bool
	functions map[string]bool
}

func (artifacts notifyArtifacts) installed(tableName string) bool {
	triggerPrefix := "gonvex_" + tableName + "_notify_"
	functionPrefix := "gonvex_notify_" + tableName + "_"
	return artifacts.triggers[triggerPrefix+"insert"] &&
		artifacts.triggers[triggerPrefix+"update"] &&
		artifacts.triggers[triggerPrefix+"delete"] &&
		artifacts.functions[functionPrefix+"insert"] &&
		artifacts.functions[functionPrefix+"update"] &&
		artifacts.functions[functionPrefix+"delete"] &&
		artifacts.functions[notifySchemaVersionFunction]
}

func loadNotifyArtifacts(ctx context.Context, db *sql.DB) (notifyArtifacts, error) {
	artifacts := notifyArtifacts{
		triggers:  map[string]bool{},
		functions: map[string]bool{},
	}
	triggerRows, err := db.QueryContext(ctx, `
		SELECT t.tgname
		FROM pg_catalog.pg_trigger t
		JOIN pg_catalog.pg_class relation ON relation.oid = t.tgrelid
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = current_schema() AND NOT t.tgisinternal
	`)
	if err != nil {
		return artifacts, err
	}
	for triggerRows.Next() {
		var name string
		if err := triggerRows.Scan(&name); err != nil {
			triggerRows.Close()
			return artifacts, err
		}
		artifacts.triggers[name] = true
	}
	if err := triggerRows.Close(); err != nil {
		return artifacts, err
	}
	if err := triggerRows.Err(); err != nil {
		return artifacts, err
	}

	functionRows, err := db.QueryContext(ctx, `
		SELECT p.proname
		FROM pg_catalog.pg_proc p
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = p.pronamespace
		WHERE namespace.nspname = current_schema() AND p.pronargs = 0
	`)
	if err != nil {
		return artifacts, err
	}
	for functionRows.Next() {
		var name string
		if err := functionRows.Scan(&name); err != nil {
			functionRows.Close()
			return artifacts, err
		}
		artifacts.functions[name] = true
	}
	if err := functionRows.Close(); err != nil {
		return artifacts, err
	}
	if err := functionRows.Err(); err != nil {
		return artifacts, err
	}
	return artifacts, nil
}

// notifyTriggersInstalled avoids rewriting three functions and three triggers
// for every table on every schema sync. Those definitions are independent of
// ordinary column changes; they only need installation for a new table or when
// an artifact is missing. Rebuilding hundreds of unchanged triggers across
// every tenant can otherwise exceed reverse-proxy request timeouts before the
// new manifest is persisted.
func notifyTriggersInstalled(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	artifacts, err := loadNotifyArtifacts(ctx, db)
	if err != nil {
		return false, err
	}
	return artifacts.installed(tableName), nil
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
		notifyFunctionSQL(insertFunction, tableName, "new_rows", hasID, true, "insert", nil),
		notifyFunctionSQL(updateFunction, tableName, "new_rows", hasID, false, "update", sortedColumnNames(table.Columns)),
		notifyFunctionSQL(deleteFunction, tableName, "old_rows", hasID, true, "delete", nil),
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
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION %s();`, updateTrigger, tableIdent, updateFunction),
		fmt.Sprintf(`CREATE TRIGGER %s
AFTER DELETE ON %s
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION %s();`, deleteTrigger, tableIdent, deleteFunction),
	}, "\n\n"), nil
}

func notifyFunctionSQL(functionName string, tableName string, transitionTable string, hasID bool, broad bool, operation string, columns []string) string {
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
	if operation == "update" {
		broadExpression = "(" + broadExpression + " OR cardinality(changed_columns) > 100)"
	}
	changedColumnsSQL := "changed_columns := ARRAY[]::text[];"
	if operation == "update" {
		if hasID {
			// Join the transition tables once and inspect their JSON keys. Running
			// one OLD/NEW join per schema column makes wide bulk updates needlessly
			// expensive. The 101-row cap is enough to detect the broad (>100)
			// fallback without growing the NOTIFY payload.
			changedColumnsSQL = fmt.Sprintf(`SELECT COALESCE(array_agg(column_name ORDER BY column_name), ARRAY[]::text[])
  INTO changed_columns
  FROM (
    SELECT DISTINCT changed.column_name
    FROM old_rows old_row
    FULL OUTER JOIN new_rows new_row USING (%s)
    CROSS JOIN LATERAL jsonb_object_keys(
      COALESCE(to_jsonb(old_row), '{}'::jsonb) || COALESCE(to_jsonb(new_row), '{}'::jsonb)
    ) AS changed(column_name)
    WHERE to_jsonb(old_row) -> changed.column_name IS DISTINCT FROM to_jsonb(new_row) -> changed.column_name
    LIMIT 101
  ) changed_columns_limited;`, quoteIdent("id"))
		} else {
			quoted := make([]string, 0, len(columns))
			for _, column := range columns {
				quoted = append(quoted, quoteLiteral(column))
			}
			changedColumnsSQL = "changed_columns := ARRAY[" + strings.Join(quoted, ", ") + "]::text[];"
		}
	}

	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s()
RETURNS trigger AS $$
DECLARE
  row_count integer;
  ids text[];
  changed_columns text[];
BEGIN
  %s

  %s

  PERFORM pg_notify(%s, json_build_object(
    'table', %s,
    'operation', %s,
    'broad', %s,
    'count', row_count,
    'ids', %s,
    'changedColumns', CASE WHEN cardinality(changed_columns) <= 100 THEN changed_columns ELSE ARRAY[]::text[] END
  )::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;`, functionName, idRead, changedColumnsSQL, quoteLiteral(NotifyChannel), quoteLiteral(tableName), quoteLiteral(operation), broadExpression, idsExpression)
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
