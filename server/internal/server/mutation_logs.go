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
			FROM gonvex_runtime_reducer_logs
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
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_reducer_logs (project_id, kind, entry)
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
	m.logs = append(entries, m.logs...)
	if len(m.logs) > metricsLogLimit {
		m.logs = m.logs[len(m.logs)-metricsLogLimit:]
	}
	onFunctionError := m.onFunctionError
	m.mu.Unlock()

	// Rehydrate the Errors inbox from the same durable failures visible in
	// Logs. Event IDs make this replay idempotent when the error event was
	// already captured before the runtime restarted.
	if onFunctionError != nil {
		for _, entry := range entries {
			if entry.Outcome == "error" {
				onFunctionError(entry)
			}
		}
	}
}

// runtimeLogIsDurable decides what outlives the in-memory ring.
//
// Reducers are the durable audit trail. FAILURES of every kind are durable too:
// the ring holds metricsLogLimit entries shared by all projects and tenants, which
// at production traffic is a matter of minutes — so the single log line explaining
// a failed query or action was routinely gone before anyone went looking for it.
// Successful queries/actions stay memory-only; they are the high-volume, low-value
// half of the stream.
func runtimeLogIsDurable(entry runtimeLogEntry) bool {
	if entry.Outcome == "error" {
		return true
	}
	return entry.Kind == "reducer"
}

func (m *runtimeMetrics) persistMutationLog(entry runtimeLogEntry) {
	if !runtimeLogIsDurable(entry) {
		return
	}
	m.mu.Lock()
	writes := m.mutationWrites
	m.mu.Unlock()
	if writes == nil {
		return
	}
	if entry.Kind != "reducer" {
		// Failure diagnostics are best-effort: a queue this far behind means the
		// registry database is struggling, and stalling live request handlers to
		// record a log line would turn a slow database into an outage.
		select {
		case writes <- entry:
		default:
			slog.Warn("dropped runtime error log", "project", entry.Project, "path", entry.Path, "kind", entry.Kind)
		}
		return
	}
	// Backpressure is preferable to silently losing durable history when the
	// database is slower than the Reducer rate.
	writes <- entry
}
