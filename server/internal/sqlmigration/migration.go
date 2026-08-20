package sqlmigration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gonvex/gonvex/server/internal/dbpool"
)

type Scope string

const (
	ScopeTenant       Scope = "tenant"
	ScopeControlPlane Scope = "control-plane"
	legacyScopeName         = "landlord"
)

var migrationName = regexp.MustCompile(`^[0-9]{4}_[A-Za-z0-9][A-Za-z0-9_-]*\.sql$`)

type Migration struct {
	Name          string `json:"name"`
	Checksum      string `json:"checksum"`
	Scope         Scope  `json:"scope"`
	NoTransaction bool   `json:"noTransaction"`
	SQL           string `json:"-"`
}

type Result struct {
	Applied []string `json:"applied"`
	Pending []string `json:"pending,omitempty"`
}

func Parse(files map[string][]byte) ([]Migration, error) {
	migrations := make([]Migration, 0, len(files))
	for name, contents := range files {
		if !migrationName.MatchString(name) {
			return nil, fmt.Errorf("migration %q must be named NNNN_description.sql", name)
		}
		text := string(contents)
		scope, explicit, noTransaction, err := directives(text)
		if err != nil {
			return nil, fmt.Errorf("migration %s: %w", name, err)
		}
		if !explicit {
			return nil, fmt.Errorf("migration %s: missing required -- gonvex:scope tenant|control-plane directive (tenant is the safe parser default)", name)
		}
		sum := sha256.Sum256(contents)
		migrations = append(migrations, Migration{
			Name: name, Checksum: hex.EncodeToString(sum[:]), Scope: scope,
			NoTransaction: noTransaction, SQL: text,
		})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Name < migrations[j].Name })
	return migrations, nil
}

func directives(contents string) (Scope, bool, bool, error) {
	scope := ScopeTenant
	explicit := false
	noTransaction := false
	for _, line := range strings.Split(contents, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "--") {
			break
		}
		directive := strings.TrimSpace(strings.TrimPrefix(line, "--"))
		switch {
		case strings.HasPrefix(directive, "gonvex:scope"):
			value := strings.TrimSpace(strings.TrimPrefix(directive, "gonvex:scope"))
			if value == legacyScopeName {
				value = string(ScopeControlPlane)
			}
			if value != string(ScopeTenant) && value != string(ScopeControlPlane) {
				return scope, explicit, noTransaction, fmt.Errorf("invalid scope %q", value)
			}
			scope, explicit = Scope(value), true
		case directive == "gonvex:no-transaction":
			noTransaction = true
		}
	}
	return scope, explicit, noTransaction, nil
}

func Filter(migrations []Migration, scope Scope) []Migration {
	result := make([]Migration, 0, len(migrations))
	for _, migration := range migrations {
		if migration.Scope == scope {
			result = append(result, migration)
		}
	}
	return result
}

func Apply(ctx context.Context, databaseURL string, migrations []Migration, dryRun bool) (Result, error) {
	if databaseURL == "" || len(migrations) == 0 {
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
	conn, err := db.Conn(ctx)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext('gonvex_sql_migrations:' || current_database()))`); err != nil {
		return Result{}, err
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext('gonvex_sql_migrations:' || current_database()))`)
	return applyDB(ctx, conn, migrations, dryRun)
}

type database interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func applyDB(ctx context.Context, db database, migrations []Migration, dryRun bool) (Result, error) {
	if dryRun {
		var exists bool
		if err := db.QueryRowContext(ctx, `SELECT to_regclass(current_schema() || '.gonvex_migrations') IS NOT NULL`).Scan(&exists); err != nil {
			return Result{}, err
		}
		if !exists {
			result := Result{}
			for _, migration := range migrations {
				result.Pending = append(result.Pending, migration.Name)
			}
			return result, nil
		}
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_migrations (
		name text PRIMARY KEY,
		checksum text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now(),
		duration_ms bigint NOT NULL
	)`); err != nil {
		return Result{}, fmt.Errorf("create gonvex_migrations: %w", err)
	}
	result := Result{}
	for _, migration := range migrations {
		var recorded string
		err := db.QueryRowContext(ctx, `SELECT checksum FROM gonvex_migrations WHERE name = $1`, migration.Name).Scan(&recorded)
		if err == nil {
			if recorded != migration.Checksum {
				return result, fmt.Errorf("migration %s checksum mismatch: database has %s, bundle has %s", migration.Name, recorded, migration.Checksum)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return result, fmt.Errorf("check migration %s: %w", migration.Name, err)
		}
		result.Pending = append(result.Pending, migration.Name)
		if dryRun {
			continue
		}
		started := time.Now()
		if migration.NoTransaction {
			err = applyWithoutTransaction(ctx, db, migration, started)
		} else {
			err = applyInTransaction(ctx, db, migration, started)
		}
		if err != nil {
			return result, fmt.Errorf("apply migration %s: %w", migration.Name, err)
		}
		result.Applied = append(result.Applied, migration.Name)
	}
	return result, nil
}

func applyInTransaction(ctx context.Context, db database, migration Migration, started time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, migration.SQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_migrations (name, checksum, duration_ms) VALUES ($1, $2, $3)`, migration.Name, migration.Checksum, time.Since(started).Milliseconds()); err != nil {
		return err
	}
	return tx.Commit()
}

func applyWithoutTransaction(ctx context.Context, db database, migration Migration, started time.Time) error {
	statements, err := splitStatements(migration.SQL)
	if err != nil {
		return err
	}
	for index, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("statement %d of %d: %w", index+1, len(statements), err)
		}
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_migrations (name, checksum, duration_ms) VALUES ($1, $2, $3)`, migration.Name, migration.Checksum, time.Since(started).Milliseconds())
	return err
}

// splitStatements handles PostgreSQL strings, quoted identifiers, comments,
// and dollar-quoted function bodies. It intentionally rejects unterminated input.
func splitStatements(source string) ([]string, error) {
	var result []string
	start := 0
	quote := byte(0)
	lineComment, blockComment := false, 0
	dollarTag := ""
	for i := 0; i < len(source); i++ {
		if lineComment {
			if source[i] == '\n' {
				lineComment = false
			}
			continue
		}
		if blockComment > 0 {
			if i+1 < len(source) && source[i:i+2] == "/*" {
				blockComment++
				i++
				continue
			}
			if i+1 < len(source) && source[i:i+2] == "*/" {
				blockComment--
				i++
				continue
			}
			continue
		}
		if dollarTag != "" {
			if strings.HasPrefix(source[i:], dollarTag) {
				i += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}
		if quote != 0 {
			if source[i] == quote {
				if i+1 < len(source) && source[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		if i+1 < len(source) && source[i:i+2] == "--" {
			lineComment = true
			i++
			continue
		}
		if i+1 < len(source) && source[i:i+2] == "/*" {
			blockComment = 1
			i++
			continue
		}
		if source[i] == '\'' || source[i] == '"' {
			quote = source[i]
			continue
		}
		if source[i] == '$' {
			if end := strings.IndexByte(source[i+1:], '$'); end >= 0 {
				tag := source[i : i+end+2]
				valid := true
				for _, char := range tag[1 : len(tag)-1] {
					if !(char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
						valid = false
					}
				}
				if valid {
					dollarTag = tag
					i += len(tag) - 1
					continue
				}
			}
		}
		if source[i] == ';' {
			if statement := strings.TrimSpace(source[start : i+1]); statement != "" {
				result = append(result, statement)
			}
			start = i + 1
		}
	}
	if quote != 0 || dollarTag != "" || blockComment != 0 {
		return nil, fmt.Errorf("unterminated SQL quote or comment")
	}
	if statement := strings.TrimSpace(source[start:]); statement != "" {
		result = append(result, statement)
	}
	return result, nil
}
