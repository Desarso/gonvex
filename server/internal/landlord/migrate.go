package landlord

import (
	"context"

	"github.com/gonvex/gonvex/server/internal/dbpool"
)

type Result struct {
	Applied []string `json:"applied"`
}

func Apply(ctx context.Context, databaseURL string) (Result, error) {
	if databaseURL == "" {
		return Result{}, nil
	}

	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return Result{}, err
	}

	result := Result{}
	for _, statement := range migrationStatements() {
		if _, err := db.ExecContext(ctx, statement.sql); err != nil {
			return result, err
		}
		result.Applied = append(result.Applied, statement.name)
	}
	return result, nil
}

type migrationStatement struct {
	name string
	sql  string
}

func migrationStatements() []migrationStatement {
	return []migrationStatement{
		{name: "users", sql: `CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			email TEXT UNIQUE,
			name TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`},
		{name: "tenants", sql: `CREATE TABLE IF NOT EXISTS tenants (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			database_url TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`},
		{name: "memberships", sql: `CREATE TABLE IF NOT EXISTS memberships (
			user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
			role TEXT NOT NULL DEFAULT 'member',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, tenant_id)
		)`},
		{name: "sessions", sql: `CREATE TABLE IF NOT EXISTS sessions (
			token_hash TEXT PRIMARY KEY,
			user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			active_tenant_id TEXT REFERENCES tenants(id) ON DELETE SET NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`},
		{name: "memberships_by_tenant", sql: `CREATE INDEX IF NOT EXISTS memberships_by_tenant ON memberships (tenant_id, user_id)`},
		{name: "sessions_by_user", sql: `CREATE INDEX IF NOT EXISTS sessions_by_user ON sessions (user_id, expires_at)`},
	}
}
