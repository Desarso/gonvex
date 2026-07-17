package server

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"time"

	"github.com/gonvex/gonvex/server/internal/dbpool"
)

type telemetrySchemaDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *Server) recordTransactionTelemetry(entry transactionTelemetryEntry) {
	if !s.config.TelemetryEnabled {
		return
	}
	entry.Project = strings.TrimSpace(entry.Project)
	if entry.Project == "" {
		entry.Project = "default"
	}
	s.metrics.recordTransaction(entry)
	select {
	case s.telemetryWrites <- struct{}{}:
		go func() {
			defer func() { <-s.telemetryWrites }()
			s.persistTransactionTelemetry(entry)
		}()
	default:
		slog.Debug("telemetry write dropped because persistence queue is full", "project", entry.Project, "path", entry.Path)
	}
}

func (s *Server) persistTransactionTelemetry(entry transactionTelemetryEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	db, err := s.openProjectTelemetryDB(ctx, entry.Project)
	if err != nil || db == nil {
		if err != nil {
			slog.Debug("telemetry database unavailable", "project", entry.Project, "error", err)
		}
		return
	}
	defer db.Close()

	deviceJSON := strings.TrimSpace(entry.DeviceJSON)
	if deviceJSON == "" {
		deviceJSON = "{}"
	}
	eventTime := entry.Time
	if eventTime == "" {
		eventTime = time.Now().UTC().Format(time.RFC3339Nano)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO telemetry_events (
		event_time, tenant_id, operation_id, kind, path, phase, reason, outcome, error,
		client_sent_at_ms, client_received_at_ms, client_duration_ms,
		server_received_at_ms, server_committed_at_ms, server_completed_at_ms, server_sent_at_ms, change_committed_at_ms,
		server_duration_ms, server_commit_ms, client_to_commit_ms, client_round_trip_ms, server_to_browser_ms, change_to_browser_ms, subscription_duration_ms,
		browser_name, browser_version, device_type, platform, user_agent, language, timezone, viewport_width, viewport_height, device
	) VALUES (
		$1::timestamptz, $2, $3, $4, $5, $6, $7, $8, $9,
		$10, $11, $12,
		$13, $14, $15, $16, $17,
		$18, $19, $20, $21, $22, $23, $24,
		$25, $26, $27, $28, $29, $30, $31, $32, $33, $34::jsonb
	)`,
		eventTime,
		entry.Tenant,
		entry.OperationID,
		entry.Kind,
		entry.Path,
		entry.Phase,
		entry.Reason,
		entry.Outcome,
		entry.Error,
		nullFloat(entry.ClientSentAtMS),
		nullFloat(entry.ClientReceivedAtMS),
		nullFloat(entry.ClientDurationMS),
		nullFloat(entry.ServerReceivedAtMS),
		nullFloat(entry.ServerCommittedAtMS),
		nullFloat(entry.ServerCompletedAtMS),
		nullFloat(entry.ServerSentAtMS),
		nullFloat(entry.ChangeCommittedAtMS),
		nullFloat(entry.ServerDurationMS),
		nullFloat(entry.ServerCommitMS),
		nullFloat(entry.ClientToCommitMS),
		nullFloat(entry.ClientRoundTripMS),
		nullFloat(entry.ServerToBrowserMS),
		nullFloat(entry.ChangeToBrowserMS),
		nullFloat(entry.SubscriptionDurationMS),
		entry.BrowserName,
		entry.BrowserVersion,
		entry.DeviceType,
		entry.Platform,
		entry.UserAgent,
		entry.Language,
		entry.Timezone,
		nullInt(entry.ViewportWidth),
		nullInt(entry.ViewportHeight),
		deviceJSON,
	)
	if err != nil {
		slog.Debug("telemetry insert failed", "project", entry.Project, "path", entry.Path, "error", err)
	}
}

func (s *Server) openProjectTelemetryDB(ctx context.Context, projectID string) (*sql.DB, error) {
	baseURL := s.projectTelemetryBaseURL(projectID)
	if strings.TrimSpace(baseURL) == "" {
		return nil, nil
	}
	databaseName := telemetryDatabaseName(projectID)
	telemetryURL, err := databaseURL(baseURL, databaseName)
	if err != nil {
		return nil, err
	}
	db, err := dbpool.Open(telemetryURL)
	if err == nil {
		if pingErr := db.PingContext(ctx); pingErr == nil {
			return db, ensureTelemetrySchema(ctx, db)
		}
		_ = db.Close()
	}
	if _, createErr := createProjectDatabase(ctx, baseURL, databaseName); createErr != nil {
		// If another connection created it first, the second open below will succeed.
		slog.Debug("telemetry database create failed", "project", projectID, "database", databaseName, "error", createErr)
	}
	db, err = dbpool.Open(telemetryURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, ensureTelemetrySchema(ctx, db)
}

func (s *Server) projectTelemetryBaseURL(projectID string) string {
	if strings.TrimSpace(s.config.PostgresURL) != "" {
		return s.config.PostgresURL
	}
	if strings.TrimSpace(projectID) != "" {
		if s.config.ProjectDatabases != nil && strings.TrimSpace(s.config.ProjectDatabases[projectID]) != "" {
			return s.config.ProjectDatabases[projectID]
		}
		s.projectMu.RLock()
		project := s.projects[projectID]
		s.projectMu.RUnlock()
		if strings.TrimSpace(project.databaseURL) != "" {
			return project.databaseURL
		}
	}
	if strings.TrimSpace(s.config.LandlordURL) != "" {
		return s.config.LandlordURL
	}
	return ""
}

func ensureTelemetrySchema(ctx context.Context, db telemetrySchemaDB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS telemetry_events (
		id BIGSERIAL PRIMARY KEY,
		recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
		tenant_id TEXT NOT NULL DEFAULT '',
		operation_id TEXT NOT NULL DEFAULT '',
		kind TEXT NOT NULL,
		path TEXT NOT NULL,
		phase TEXT NOT NULL,
		reason TEXT NOT NULL DEFAULT '',
		outcome TEXT NOT NULL,
		error TEXT NOT NULL DEFAULT '',
		client_sent_at_ms DOUBLE PRECISION,
		client_received_at_ms DOUBLE PRECISION,
		client_duration_ms DOUBLE PRECISION,
		server_received_at_ms DOUBLE PRECISION,
		server_committed_at_ms DOUBLE PRECISION,
		server_completed_at_ms DOUBLE PRECISION,
		server_sent_at_ms DOUBLE PRECISION,
		change_committed_at_ms DOUBLE PRECISION,
		server_duration_ms DOUBLE PRECISION,
		server_commit_ms DOUBLE PRECISION,
		client_to_commit_ms DOUBLE PRECISION,
		client_round_trip_ms DOUBLE PRECISION,
		server_to_browser_ms DOUBLE PRECISION,
		change_to_browser_ms DOUBLE PRECISION,
		subscription_duration_ms DOUBLE PRECISION,
		browser_name TEXT NOT NULL DEFAULT '',
		browser_version TEXT NOT NULL DEFAULT '',
		device_type TEXT NOT NULL DEFAULT '',
		platform TEXT NOT NULL DEFAULT '',
		user_agent TEXT NOT NULL DEFAULT '',
		language TEXT NOT NULL DEFAULT '',
		timezone TEXT NOT NULL DEFAULT '',
		viewport_width INTEGER,
		viewport_height INTEGER,
		device JSONB NOT NULL DEFAULT '{}'::jsonb
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS telemetry_events_time ON telemetry_events (event_time DESC)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS telemetry_events_path ON telemetry_events (kind, path, event_time DESC)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS telemetry_events_device ON telemetry_events (browser_name, device_type, event_time DESC)`); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS telemetry_events_tenant ON telemetry_events (tenant_id, event_time DESC)`)
	return err
}

func telemetryDatabaseName(projectID string) string {
	base := strings.ReplaceAll(slug(projectID), "-", "_")
	if base == "" {
		base = "default"
	}
	return "gonvex_" + base + "_telemetry"
}

func nullFloat(value float64) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullInt(value int) any {
	if value == 0 {
		return nil
	}
	return value
}
