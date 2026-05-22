package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

const taskNotifyChannel = "gonvex_table_change"

type taskNotifyPayload struct {
	Table string   `json:"table"`
	Broad bool     `json:"broad"`
	IDs   []string `json:"ids"`
	Count int      `json:"count"`
}

func (s *Server) startPostgresNotifications() {
	if s.config.PostgresURL == "" {
		return
	}
	go s.listenPostgresNotifications()
}

func (s *Server) listenPostgresNotifications() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := pgx.Connect(ctx, s.config.PostgresURL)
	cancel()
	if err != nil {
		return
	}
	defer conn.Close(context.Background())

	if err := ensureTaskNotifyTrigger(context.Background(), conn); err != nil {
		return
	}
	if _, err := conn.Exec(context.Background(), "LISTEN "+taskNotifyChannel); err != nil {
		return
	}

	for {
		notification, err := conn.WaitForNotification(context.Background())
		if err != nil {
			return
		}
		if notification.Payload == "tasks" {
			s.broadcastTableChange("tasks")
			continue
		}
		var payload taskNotifyPayload
		if err := json.Unmarshal([]byte(notification.Payload), &payload); err == nil && payload.Table == "tasks" {
			if payload.Broad {
				s.broadcastTableChange("tasks")
			} else {
				s.broadcastRowIDChange("tasks", payload.IDs)
			}
		}
	}
}

func ensureTaskNotifyTrigger(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
CREATE OR REPLACE FUNCTION gonvex_notify_tasks_insert()
RETURNS trigger AS $$
DECLARE
  row_count integer;
  ids text[];
BEGIN
  SELECT count(*), COALESCE(array_agg(id), ARRAY[]::text[])
  INTO row_count, ids
  FROM (SELECT id FROM new_rows WHERE id IS NOT NULL LIMIT 500) limited;

  PERFORM pg_notify('gonvex_table_change', json_build_object(
    'table', 'tasks',
    'broad', true,
    'count', row_count,
    'ids', ARRAY[]::text[]
  )::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gonvex_notify_tasks_update()
RETURNS trigger AS $$
DECLARE
  row_count integer;
  ids text[];
BEGIN
  SELECT count(*), COALESCE(array_agg(id), ARRAY[]::text[])
  INTO row_count, ids
  FROM (SELECT id FROM new_rows WHERE id IS NOT NULL LIMIT 500) limited;

  PERFORM pg_notify('gonvex_table_change', json_build_object(
    'table', 'tasks',
    'broad', row_count >= 500,
    'count', row_count,
    'ids', CASE WHEN row_count < 500 THEN ids ELSE ARRAY[]::text[] END
  )::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gonvex_notify_tasks_delete()
RETURNS trigger AS $$
DECLARE
  row_count integer;
  ids text[];
BEGIN
  SELECT count(*), COALESCE(array_agg(id), ARRAY[]::text[])
  INTO row_count, ids
  FROM (SELECT id FROM old_rows WHERE id IS NOT NULL LIMIT 500) limited;

  PERFORM pg_notify('gonvex_table_change', json_build_object(
    'table', 'tasks',
    'broad', true,
    'count', row_count,
    'ids', ARRAY[]::text[]
  )::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS gonvex_tasks_notify ON tasks;
DROP TRIGGER IF EXISTS gonvex_tasks_notify_insert ON tasks;
DROP TRIGGER IF EXISTS gonvex_tasks_notify_update ON tasks;
DROP TRIGGER IF EXISTS gonvex_tasks_notify_delete ON tasks;

DO $$
BEGIN
  IF to_regclass('public.tasks') IS NOT NULL THEN
    CREATE TRIGGER gonvex_tasks_notify_insert
    AFTER INSERT ON tasks
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION gonvex_notify_tasks_insert();

    CREATE TRIGGER gonvex_tasks_notify_update
    AFTER UPDATE ON tasks
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION gonvex_notify_tasks_update();

    CREATE TRIGGER gonvex_tasks_notify_delete
    AFTER DELETE ON tasks
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT EXECUTE FUNCTION gonvex_notify_tasks_delete();
  END IF;
END $$;
`)
	return err
}
