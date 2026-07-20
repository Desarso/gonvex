package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
)

const mutationLogQueueSize = 256

type runtimeMutationLogStore interface {
	LoadRecent(context.Context, int) ([]runtimeLogEntry, error)
	Append(context.Context, runtimeLogEntry) error
}

type postgresRuntimeMutationLogStore struct {
	server *Server
}

func (s postgresRuntimeMutationLogStore) database(ctx context.Context) (*sql.DB, error) {
	db, err := s.server.pooledProjectRegistry(ctx)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("project registry database is not configured")
	}
	return db, nil
}

func (s postgresRuntimeMutationLogStore) LoadRecent(ctx context.Context, limit int) ([]runtimeLogEntry, error) {
	db, err := s.database(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT entry
		FROM (
			SELECT id, entry
			FROM gonvex_runtime_mutation_logs
			ORDER BY id DESC
			LIMIT $1
		) recent
		ORDER BY id`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]runtimeLogEntry, 0, limit)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var entry runtimeLogEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (s postgresRuntimeMutationLogStore) Append(ctx context.Context, entry runtimeLogEntry) error {
	db, err := s.database(ctx)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_mutation_logs (project_id, kind, entry)
		VALUES ($1, $2, $3::jsonb)`, entry.Project, entry.Kind, string(raw))
	return err
}

func (m *runtimeMetrics) startMutationLogPersistence(store runtimeMutationLogStore) {
	if m == nil || store == nil {
		return
	}
	m.mu.Lock()
	if m.mutationWrites != nil {
		m.mu.Unlock()
		return
	}
	m.mutationWrites = make(chan runtimeLogEntry, mutationLogQueueSize)
	writes := m.mutationWrites
	m.mu.Unlock()

	go m.runMutationLogPersistence(store, writes)
}

func (m *runtimeMetrics) runMutationLogPersistence(store runtimeMutationLogStore, writes <-chan runtimeLogEntry) {
	ctx := context.Background()
	entries, err := store.LoadRecent(ctx, metricsLogLimit)
	if err != nil {
		slog.Error("restore runtime mutation logs", "error", err)
	} else {
		m.restoreMutationLogs(entries)
	}

	for entry := range writes {
		if err := store.Append(ctx, entry); err != nil {
			slog.Error("persist runtime mutation log", "project", entry.Project, "path", entry.Path, "kind", entry.Kind, "error", err)
		}
	}
}

func (m *runtimeMetrics) restoreMutationLogs(entries []runtimeLogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(entries, m.logs...)
	if len(m.logs) > metricsLogLimit {
		m.logs = m.logs[len(m.logs)-metricsLogLimit:]
	}
}

func (m *runtimeMetrics) persistMutationLog(entry runtimeLogEntry) {
	if entry.Kind != "mutation" && entry.Kind != "internalMutation" {
		return
	}
	m.mu.Lock()
	writes := m.mutationWrites
	m.mu.Unlock()
	if writes != nil {
		// Backpressure is preferable to silently losing durable history when the
		// database is slower than the mutation rate.
		writes <- entry
	}
}
