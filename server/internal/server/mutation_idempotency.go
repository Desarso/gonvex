package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Idempotency rows only need to outlive client replays: the outbox resends a
// queued write with its original key after an offline stretch or an app
// restart. A key older than this window can only arrive from a client that
// stayed offline longer than any realistic replay, and re-executing that
// write is a better failure mode than letting the table grow without bound.
const mutationIdempotencySweepInterval = time.Hour

const mutationIdempotencySQL = `
CREATE TABLE IF NOT EXISTS _gonvex_mutation_idempotency (
  subject text NOT NULL DEFAULT '',
  idempotency_key text NOT NULL,
  path text NOT NULL,
  result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (subject, idempotency_key)
);
CREATE INDEX IF NOT EXISTS gonvex_mutation_idempotency_created_at
  ON _gonvex_mutation_idempotency (created_at);
`

const mutationIdempotencySweepSQL = `
DELETE FROM _gonvex_mutation_idempotency WHERE created_at < now() - interval '7 days'
`

type mutationIdempotencyContextKey struct{}

// mutationIdempotency identifies one client write across replays. The subject
// is the authenticated caller: a stored result must never be replayable by a
// different user who guesses (or reuses) the same key.
type mutationIdempotency struct {
	Key     string
	Subject string
}

func withMutationIdempotency(ctx context.Context, key string, subject string) context.Context {
	key = strings.TrimSpace(key)
	if key == "" {
		return ctx
	}
	return context.WithValue(ctx, mutationIdempotencyContextKey{}, mutationIdempotency{Key: key, Subject: subject})
}

func mutationIdempotencyFromContext(ctx context.Context) (mutationIdempotency, bool) {
	value, ok := ctx.Value(mutationIdempotencyContextKey{}).(mutationIdempotency)
	return value, ok && value.Key != ""
}

func (s *Server) ensureMutationIdempotencyStorage(ctx context.Context, db *sql.DB, databaseURL string) error {
	s.mutationIdempotencyMu.Lock()
	if s.mutationIdempotencyReady == nil {
		s.mutationIdempotencyReady = map[string]bool{}
	}
	ready := s.mutationIdempotencyReady[databaseURL]
	s.mutationIdempotencyMu.Unlock()
	if ready {
		return nil
	}
	// Concurrent CREATE TABLE IF NOT EXISTS statements can still collide in
	// the pg_type catalog, so in-process installs are serialized per database
	// and a catalog conflict from another process counts as success once the
	// table is observably there.
	_, err, _ := s.mutationIdempotencyInstalls.Do(databaseURL, func() (any, error) {
		if _, err := db.ExecContext(ctx, mutationIdempotencySQL); err != nil {
			var installed bool
			probe := db.QueryRowContext(ctx, `SELECT to_regclass('public._gonvex_mutation_idempotency') IS NOT NULL`).Scan(&installed)
			if probe != nil || !installed {
				return nil, fmt.Errorf("install mutation idempotency storage: %w", err)
			}
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	s.mutationIdempotencyMu.Lock()
	s.mutationIdempotencyReady[databaseURL] = true
	s.mutationIdempotencyMu.Unlock()
	return nil
}

// claimMutationIdempotency records the key inside the mutation's own
// transaction, so the claim and the mutation's writes commit or roll back
// together. A concurrent duplicate blocks on the primary key until the first
// transaction resolves: after a commit it observes the conflict and must be
// served the stored result; after a rollback its own claim proceeds.
func claimMutationIdempotency(ctx context.Context, tx *sql.Tx, claim mutationIdempotency, path string) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		INSERT INTO _gonvex_mutation_idempotency (subject, idempotency_key, path)
		VALUES ($1, $2, $3)
		ON CONFLICT (subject, idempotency_key) DO NOTHING
	`, claim.Subject, claim.Key, path)
	if err != nil {
		return false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return inserted > 0, nil
}

func storeMutationIdempotencyResult(ctx context.Context, tx *sql.Tx, claim mutationIdempotency, resultValue any) error {
	encoded, err := json.Marshal(resultValue)
	if err != nil {
		// The result crosses the wire as JSON anyway, so this only happens for
		// values that would fail serialization there too. Keep the claim with
		// a null result rather than fail an otherwise committed mutation.
		return nil
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE _gonvex_mutation_idempotency SET result = $3
		WHERE subject = $1 AND idempotency_key = $2
	`, claim.Subject, claim.Key, encoded)
	return err
}

// replayMutationIdempotencyResult serves the committed outcome of the first
// delivery to a duplicate send without re-executing the handler.
func replayMutationIdempotencyResult(ctx context.Context, db *sql.DB, claim mutationIdempotency, path string) (any, error) {
	var encoded []byte
	var storedPath string
	err := db.QueryRowContext(ctx, `
		SELECT result, path FROM _gonvex_mutation_idempotency
		WHERE subject = $1 AND idempotency_key = $2
	`, claim.Subject, claim.Key).Scan(&encoded, &storedPath)
	if err != nil {
		return nil, fmt.Errorf("mutation %q was already accepted under idempotency key %q but its stored result could not be read: %w", path, claim.Key, err)
	}
	if storedPath != path {
		return nil, fmt.Errorf("idempotency key %q was already used by mutation %q; refusing to replay it for %q", claim.Key, storedPath, path)
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	var result any
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("mutation %q stored an unreadable idempotent result for key %q: %w", path, claim.Key, err)
	}
	return result, nil
}

// maybeSweepMutationIdempotency drops expired claims at most once per sweep
// interval per database, off the mutation's latency path.
func (s *Server) maybeSweepMutationIdempotency(db *sql.DB, databaseURL string) {
	s.mutationIdempotencyMu.Lock()
	if s.mutationIdempotencySweptAt == nil {
		s.mutationIdempotencySweptAt = map[string]time.Time{}
	}
	if time.Since(s.mutationIdempotencySweptAt[databaseURL]) < mutationIdempotencySweepInterval {
		s.mutationIdempotencyMu.Unlock()
		return
	}
	s.mutationIdempotencySweptAt[databaseURL] = time.Now()
	s.mutationIdempotencyMu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, mutationIdempotencySweepSQL)
	}()
}
