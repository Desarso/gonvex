package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
)

// ChangeFeedNotifyChannel is deliberately only a wake-up signal. The durable
// _gonvex_sync_changes rows are the source of truth; listeners always read the
// committed revision instead of trusting data embedded in NOTIFY payloads.
const ChangeFeedNotifyChannel = "gonvex_change_feed"

// SyncStorageInstalled reports whether a database has the durable sync clock
// and change log. Schema fingerprint equality is not sufficient: a restored
// or partially provisioned database may have the expected application schema
// while these runtime-owned tables are absent.
func SyncStorageInstalled(ctx context.Context, databaseURL string) (bool, error) {
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return false, err
	}
	defer db.Close()
	var installed bool
	err = db.QueryRowContext(ctx, `
		SELECT to_regclass('public._gonvex_sync_clock') IS NOT NULL
		   AND to_regclass('public._gonvex_sync_changes') IS NOT NULL
	`).Scan(&installed)
	return installed, err
}

// InstallSyncLog installs the authoritative, transaction-ordered change feed
// for every application table. Replica Collections and Live Queries consume
// the same committed rows, so there is no second invalidation trigger system.
func InstallSyncLog(ctx context.Context, db *sql.DB, schema manifest.Schema, _ map[string]manifest.ReplicaCollectionDefinition) ([]string, error) {
	if _, err := db.ExecContext(ctx, syncInfrastructureSQL); err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, actionOutboxInfrastructureSQL); err != nil {
		return nil, err
	}
	var applied []string
	for _, tableName := range sortedTableNames(schema.Tables) {
		table := schema.Tables[tableName]
		definition, err := changeFeedDefinition(tableName, table)
		if err != nil {
			return applied, err
		}
		columns, err := syncColumns(tableName, table, definition)
		if err != nil {
			return applied, err
		}
		statement, err := syncTriggerSQL(tableName, definition.Key, columns)
		if err != nil {
			return applied, err
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return applied, err
		}
		applied = append(applied, fmt.Sprintf("ensured durable change feed for %s", tableName))
	}
	return applied, nil
}

const actionOutboxInfrastructureSQL = `
CREATE TABLE IF NOT EXISTS _gonvex_action_outbox (
  id text PRIMARY KEY,
  action_path text NOT NULL,
  args jsonb NOT NULL,
  actor_user_id text,
  actor_email text,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing')),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE _gonvex_action_outbox ADD COLUMN IF NOT EXISTS locked_at timestamptz;
CREATE INDEX IF NOT EXISTS _gonvex_action_outbox_pending
  ON _gonvex_action_outbox (available_at, created_at)
  WHERE status = 'pending';`

func changeFeedDefinition(tableName string, table manifest.Table) (manifest.ReplicaCollectionDefinition, error) {
	key := ""
	columns := make([]string, 0, len(table.Columns))
	for columnName, column := range table.Columns {
		columns = append(columns, columnName)
		if column.PrimaryKey && (key == "" || columnName < key) {
			key = columnName
		}
	}
	if key == "" {
		return manifest.ReplicaCollectionDefinition{}, fmt.Errorf("change-feed table %q has no primary key", tableName)
	}
	sort.Strings(columns)
	return manifest.ReplicaCollectionDefinition{Table: tableName, Key: key, Columns: columns}, nil
}

func syncColumns(tableName string, table manifest.Table, definition manifest.ReplicaCollectionDefinition) ([]string, error) {
	values := append([]string{}, definition.Columns...)
	values = append(values, definition.Key, definition.OrderBy)
	values = append(values, definition.ExcludeWhenSet...)
	for column := range definition.EqualFilters {
		values = append(values, column)
	}
	seen := map[string]bool{}
	columns := make([]string, 0, len(values))
	for _, column := range values {
		column = strings.TrimSpace(column)
		if column == "" || seen[column] {
			continue
		}
		if !validIdent(column) {
			return nil, fmt.Errorf("invalid sync column %q for %s", column, tableName)
		}
		if _, ok := table.Columns[column]; !ok {
			return nil, fmt.Errorf("sync column %s.%s is not declared in the active schema", tableName, column)
		}
		seen[column] = true
		columns = append(columns, column)
	}
	sort.Strings(columns)
	if len(columns) == 0 {
		return nil, fmt.Errorf("sync table %s has no projected columns", tableName)
	}
	if !seen[definition.Key] {
		return nil, fmt.Errorf("sync key %s.%s is not projected", tableName, definition.Key)
	}
	return columns, nil
}

func syncTriggerSQL(tableName, key string, columns []string) (string, error) {
	if !validIdent(tableName) || !validIdent(key) {
		return "", fmt.Errorf("invalid sync table or key %q.%q", tableName, key)
	}
	stageFunction := quoteIdent(syncArtifactName(tableName, "stage"))
	stageTrigger := quoteIdent(syncArtifactName(tableName, "stage_trigger"))
	finalizeTrigger := quoteIdent(syncArtifactName(tableName, "finalize_trigger"))
	tableIdent := quoteIdent(tableName)
	oldProjection := jsonProjection("OLD", columns)
	newProjection := jsonProjection("NEW", columns)

	return fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$
DECLARE
  old_data jsonb;
  new_data jsonb;
  changed_columns text[];
  row_key text;
BEGIN
  IF TG_OP = 'INSERT' THEN
    old_data := NULL;
    new_data := %s;
    row_key := NEW.%s::text;
    changed_columns := ARRAY(SELECT jsonb_object_keys(new_data));
  ELSIF TG_OP = 'DELETE' THEN
    old_data := %s;
    new_data := NULL;
    row_key := OLD.%s::text;
    changed_columns := ARRAY(SELECT jsonb_object_keys(old_data));
  ELSE
    old_data := %s;
    new_data := %s;
    row_key := NEW.%s::text;
    changed_columns := ARRAY(
      SELECT key
      FROM jsonb_object_keys(old_data || new_data) AS changed(key)
      WHERE old_data -> key IS DISTINCT FROM new_data -> key
      ORDER BY key
    );
  END IF;

  INSERT INTO _gonvex_sync_changes (
    transaction_id, table_name, row_id, operation, old_value, new_value, changed_columns
  ) VALUES (
    txid_current()::bigint, %s, row_key, lower(TG_OP), old_data, new_data, changed_columns
  );
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS %s ON %s;
DROP TRIGGER IF EXISTS %s ON %s;
CREATE TRIGGER %s
AFTER INSERT OR UPDATE OR DELETE ON %s
FOR EACH ROW EXECUTE FUNCTION %s();
CREATE CONSTRAINT TRIGGER %s
AFTER INSERT OR UPDATE OR DELETE ON %s
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION gonvex_sync_finalize_transaction();
`,
		stageFunction,
		newProjection, quoteIdent(key),
		oldProjection, quoteIdent(key),
		oldProjection, newProjection, quoteIdent(key),
		quoteLiteral(tableName),
		stageTrigger, tableIdent,
		finalizeTrigger, tableIdent,
		stageTrigger, tableIdent, stageFunction,
		finalizeTrigger, tableIdent,
	), nil
}

func jsonProjection(alias string, columns []string) string {
	parts := make([]string, 0, len(columns)*2)
	for _, column := range columns {
		parts = append(parts, quoteLiteral(column), alias+"."+quoteIdent(column))
	}
	return "jsonb_build_object(" + strings.Join(parts, ", ") + ")"
}

func syncArtifactName(tableName, suffix string) string {
	name := "gonvex_sync_" + tableName + "_" + suffix
	if len(name) <= 63 {
		return name
	}
	hash := sha256.Sum256([]byte(name))
	return name[:50] + fmt.Sprintf("_%x", hash[:6])
}

const syncInfrastructureSQL = `
CREATE TABLE IF NOT EXISTS _gonvex_sync_clock (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  epoch text NOT NULL,
  revision bigint NOT NULL DEFAULT 0,
  retained_revision bigint NOT NULL DEFAULT 0
);
ALTER TABLE _gonvex_sync_clock
  ADD COLUMN IF NOT EXISTS retained_revision bigint NOT NULL DEFAULT 0;
INSERT INTO _gonvex_sync_clock (singleton, epoch, revision)
VALUES (true, md5(random()::text || clock_timestamp()::text), 0)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS _gonvex_sync_changes (
  event_id bigserial PRIMARY KEY,
  transaction_id bigint NOT NULL,
  revision bigint,
  ordinal integer,
  command_id text,
  table_name text NOT NULL,
  row_id text NOT NULL,
  operation text NOT NULL,
  old_value jsonb,
  new_value jsonb,
  changed_columns text[] NOT NULL DEFAULT ARRAY[]::text[],
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = '_gonvex_sync_changes'
      AND column_name = 'mutation_id'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = '_gonvex_sync_changes'
        AND column_name = 'command_id'
    ) THEN
      ALTER TABLE _gonvex_sync_changes RENAME COLUMN mutation_id TO command_id;
    ELSE
      UPDATE _gonvex_sync_changes
      SET command_id = COALESCE(command_id, mutation_id)
      WHERE command_id IS NULL AND mutation_id IS NOT NULL;
      ALTER TABLE _gonvex_sync_changes DROP COLUMN mutation_id;
    END IF;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_revision
  ON _gonvex_sync_changes (revision, ordinal)
  WHERE revision IS NOT NULL;
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_created_at
  ON _gonvex_sync_changes (created_at);

CREATE OR REPLACE FUNCTION gonvex_sync_finalize_transaction() RETURNS trigger AS $$
DECLARE
  revision_text text;
  next_revision bigint;
  current_epoch text;
  changed_tables text[];
  notify_payload jsonb;
BEGIN
  revision_text := current_setting('gonvex.sync_revision', true);
  IF revision_text IS NULL OR revision_text = '' THEN
    UPDATE _gonvex_sync_clock
    SET revision = revision + 1
    WHERE singleton = true
    RETURNING revision, epoch INTO next_revision, current_epoch;

    PERFORM set_config('gonvex.sync_revision', next_revision::text, true);

    WITH ranked AS (
      SELECT event_id, row_number() OVER (ORDER BY event_id)::integer AS row_ordinal
      FROM _gonvex_sync_changes
      WHERE transaction_id = txid_current()::bigint AND revision IS NULL
    )
    UPDATE _gonvex_sync_changes changes
    SET revision = next_revision,
        ordinal = ranked.row_ordinal,
        command_id = NULLIF(current_setting('gonvex.command_id', true), '')
    FROM ranked
    WHERE changes.event_id = ranked.event_id;

    SELECT array_agg(DISTINCT table_name ORDER BY table_name)
    INTO changed_tables
    FROM _gonvex_sync_changes
    WHERE transaction_id = txid_current()::bigint
      AND revision = next_revision;

    notify_payload := jsonb_build_object(
      'epoch', current_epoch,
      'revision', next_revision,
      'tables', changed_tables
    );
    IF octet_length(notify_payload::text) > 7000 THEN
      notify_payload := jsonb_build_object(
        'epoch', current_epoch,
        'revision', next_revision
      );
    END IF;

    PERFORM pg_notify(
      'gonvex_change_feed',
      notify_payload::text
    );
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
`
