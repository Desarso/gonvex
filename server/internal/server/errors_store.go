package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func ensureErrorSchema(ctx context.Context, db telemetrySchemaDB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS gonvex_error_events (
			project_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL,
			tenant_id TEXT NOT NULL DEFAULT '',
			release TEXT NOT NULL DEFAULT '',
			user_id TEXT NOT NULL DEFAULT '',
			device_id TEXT NOT NULL DEFAULT '',
			payload JSONB NOT NULL,
			PRIMARY KEY (project_id, event_id)
		)`,
		`CREATE INDEX IF NOT EXISTS gonvex_error_events_group ON gonvex_error_events (project_id, fingerprint, occurred_at DESC)`,
		`CREATE INDEX IF NOT EXISTS gonvex_error_events_tenant ON gonvex_error_events (project_id, tenant_id, occurred_at DESC)`,
		`CREATE TABLE IF NOT EXISTS gonvex_error_groups (
			project_id TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			title TEXT NOT NULL,
			culprit TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'unresolved',
			priority TEXT NOT NULL DEFAULT 'medium',
			assignee TEXT NOT NULL DEFAULT '',
			first_seen TIMESTAMPTZ NOT NULL,
			last_seen TIMESTAMPTZ NOT NULL,
			event_count BIGINT NOT NULL DEFAULT 0,
			tenants JSONB NOT NULL DEFAULT '{}'::jsonb,
			releases JSONB NOT NULL DEFAULT '{}'::jsonb,
			environments JSONB NOT NULL DEFAULT '{}'::jsonb,
			users JSONB NOT NULL DEFAULT '{}'::jsonb,
			devices JSONB NOT NULL DEFAULT '{}'::jsonb,
			latest_event JSONB NOT NULL,
			regression BOOLEAN NOT NULL DEFAULT false,
			PRIMARY KEY (project_id, fingerprint)
		)`,
		`CREATE INDEX IF NOT EXISTS gonvex_error_groups_inbox ON gonvex_error_groups (project_id, status, last_seen DESC)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) openErrorDB(ctx context.Context, project string) (*sql.DB, error) {
	db, err := s.openProjectTelemetryDB(ctx, project)
	if err != nil || db == nil {
		return db, err
	}
	if err := ensureErrorSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// persistError atomically records the event and updates its aggregate. The
// advisory lock makes count maps safe across multiple runtime replicas.
func (s *Server) persistError(ctx context.Context, event capturedError) (available bool, accepted bool, err error) {
	db, err := s.openErrorDB(ctx, event.Project)
	if err != nil || db == nil {
		return db != nil, false, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return true, false, err
	}
	defer tx.Rollback()

	payload, err := json.Marshal(event)
	if err != nil {
		return true, false, err
	}
	when := eventTime(event.Timestamp)
	result, err := tx.ExecContext(ctx, `INSERT INTO gonvex_error_events
		(project_id, event_id, fingerprint, occurred_at, tenant_id, release, user_id, device_id, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
		ON CONFLICT (project_id, event_id) DO NOTHING`, event.Project, event.EventID, fingerprint(event), when, event.Tenant, event.Release, errorUserID(event), event.DeviceID, payload)
	if err != nil {
		return true, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows == 0 {
		return true, false, err
	}

	fp := fingerprint(event)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, event.Project+":"+fp); err != nil {
		return true, false, err
	}
	group, err := scanErrorGroup(tx.QueryRowContext(ctx, errorGroupSelect+` WHERE project_id=$1 AND fingerprint=$2`, event.Project, fp))
	if errors.Is(err, sql.ErrNoRows) {
		group = newErrorGroup(event, fp, when)
	} else if err != nil {
		return true, false, err
	} else {
		applyErrorToGroup(group, event, when)
	}
	if err := upsertErrorGroup(ctx, tx, group); err != nil {
		return true, false, err
	}
	if err := tx.Commit(); err != nil {
		return true, false, err
	}
	return true, true, nil
}

const errorGroupSelect = `SELECT fingerprint, project_id, title, culprit, status, priority, assignee,
	first_seen, last_seen, event_count, tenants, releases, environments, users, devices, latest_event, regression
	FROM gonvex_error_groups`

type rowScanner interface{ Scan(...any) error }

func scanErrorGroup(row rowScanner) (*errorGroup, error) {
	group := &errorGroup{}
	var firstSeen, lastSeen time.Time
	var tenants, releases, environments, users, devices, latest []byte
	if err := row.Scan(&group.Fingerprint, &group.Project, &group.Title, &group.Culprit, &group.Status, &group.Priority, &group.Assignee,
		&firstSeen, &lastSeen, &group.Count, &tenants, &releases, &environments, &users, &devices, &latest, &group.Regression); err != nil {
		return nil, err
	}
	group.FirstSeen = firstSeen.UTC().Format(time.RFC3339Nano)
	group.LastSeen = lastSeen.UTC().Format(time.RFC3339Nano)
	group.Tenants = decodeCountMap(tenants)
	group.Releases = decodeCountMap(releases)
	group.Environments = decodeCountMap(environments)
	group.Users = decodeCountMap(users)
	group.Devices = decodeCountMap(devices)
	if err := json.Unmarshal(latest, &group.Latest); err != nil {
		return nil, err
	}
	return group, nil
}

func (s *Server) persistentErrorGroups(ctx context.Context, project, status string) ([]*errorGroup, bool, error) {
	db, err := s.openErrorDB(ctx, project)
	if err != nil || db == nil {
		return nil, db != nil, err
	}
	defer db.Close()
	query := errorGroupSelect + ` WHERE project_id=$1`
	args := []any{project}
	if status != "" {
		query += ` AND status=$2`
		args = append(args, status)
	}
	query += ` ORDER BY last_seen DESC LIMIT 500`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	groups := []*errorGroup{}
	for rows.Next() {
		group, err := scanErrorGroup(rows)
		if err != nil {
			return nil, true, err
		}
		groups = append(groups, group)
	}
	return groups, true, rows.Err()
}

func (s *Server) persistentErrorGroup(ctx context.Context, project, fp string) (*errorGroup, bool, error) {
	db, err := s.openErrorDB(ctx, project)
	if err != nil || db == nil {
		return nil, db != nil, err
	}
	defer db.Close()
	group, err := scanErrorGroup(db.QueryRowContext(ctx, errorGroupSelect+` WHERE project_id=$1 AND fingerprint=$2`, project, fp))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, true, nil
	}
	return group, true, err
}

func (s *Server) updatePersistentErrorGroup(ctx context.Context, project, fp string, update errorGroupUpdate) (*errorGroup, bool, error) {
	db, err := s.openErrorDB(ctx, project)
	if err != nil || db == nil {
		return nil, db != nil, err
	}
	defer db.Close()
	result, err := db.ExecContext(ctx, `UPDATE gonvex_error_groups SET
		status=COALESCE(NULLIF($3,''),status), priority=COALESCE(NULLIF($4,''),priority), assignee=CASE WHEN $5 THEN $6 ELSE assignee END
		WHERE project_id=$1 AND fingerprint=$2`, project, fp, update.Status, update.Priority, update.AssigneeSet, update.Assignee)
	if err != nil {
		return nil, true, err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, true, nil
	}
	group, _, err := s.persistentErrorGroup(ctx, project, fp)
	return group, true, err
}

func upsertErrorGroup(ctx context.Context, tx *sql.Tx, group *errorGroup) error {
	latest, _ := json.Marshal(group.Latest)
	_, err := tx.ExecContext(ctx, `INSERT INTO gonvex_error_groups
		(project_id,fingerprint,title,culprit,status,priority,assignee,first_seen,last_seen,event_count,tenants,releases,environments,users,devices,latest_event,regression)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17)
		ON CONFLICT (project_id,fingerprint) DO UPDATE SET title=EXCLUDED.title,culprit=EXCLUDED.culprit,status=EXCLUDED.status,
		priority=EXCLUDED.priority,assignee=EXCLUDED.assignee,last_seen=EXCLUDED.last_seen,event_count=EXCLUDED.event_count,
		tenants=EXCLUDED.tenants,releases=EXCLUDED.releases,environments=EXCLUDED.environments,users=EXCLUDED.users,devices=EXCLUDED.devices,
		latest_event=EXCLUDED.latest_event,regression=EXCLUDED.regression`, group.Project, group.Fingerprint, group.Title, group.Culprit,
		group.Status, group.Priority, group.Assignee, group.FirstSeen, group.LastSeen, group.Count, encodeJSON(group.Tenants), encodeJSON(group.Releases),
		encodeJSON(group.Environments), encodeJSON(group.Users), encodeJSON(group.Devices), latest, group.Regression)
	return err
}

func eventTime(raw string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || parsed.After(time.Now().Add(5*time.Minute)) {
		return time.Now().UTC()
	}
	return parsed.UTC()
}

func encodeJSON(value any) []byte { encoded, _ := json.Marshal(value); return encoded }
func decodeCountMap(raw []byte) map[string]int {
	value := map[string]int{}
	_ = json.Unmarshal(raw, &value)
	return value
}
func errorUserID(event capturedError) string {
	if id := stringValue(event.User["id"]); id != "" {
		return id
	}
	return stringValue(event.User["email"])
}

func validateErrorGroupUpdate(update errorGroupUpdate) error {
	if update.Status != "" && update.Status != "unresolved" && update.Status != "resolved" && update.Status != "ignored" {
		return fmt.Errorf("invalid error status")
	}
	if update.Priority != "" && update.Priority != "low" && update.Priority != "medium" && update.Priority != "high" && update.Priority != "critical" {
		return fmt.Errorf("invalid error priority")
	}
	return nil
}
