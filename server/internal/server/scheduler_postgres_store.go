package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type scheduledJobDatabase func(context.Context) (*sql.DB, error)

type postgresScheduledJobStore struct {
	database scheduledJobDatabase
}

func newPostgresScheduledJobStore(database scheduledJobDatabase) *postgresScheduledJobStore {
	if database == nil {
		return nil
	}
	return &postgresScheduledJobStore{database: database}
}

func (store *postgresScheduledJobStore) db(ctx context.Context) (*sql.DB, error) {
	db, err := store.database(ctx)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("scheduler database is not configured")
	}
	return db, nil
}

func (store *postgresScheduledJobStore) enqueue(ctx context.Context, job scheduledJob) error {
	db, err := store.db(ctx)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_scheduled_jobs
		(id, project_id, tenant_id, function_path, args, run_at, scheduled_for, cron_name)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.ProjectID, job.TenantID, job.FunctionPath, nullableSchedulerArgs(job.Args),
		job.RunAt, job.ScheduledFor, job.CronName,
	)
	return err
}

func nullableSchedulerArgs(args json.RawMessage) any {
	if len(args) == 0 {
		return nil
	}
	return []byte(args)
}

func (store *postgresScheduledJobStore) claimDue(ctx context.Context, now time.Time, limit int, owner string) ([]scheduledJob, error) {
	if limit <= 0 {
		return nil, nil
	}
	db, err := store.db(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `WITH candidates AS (
		SELECT id
		FROM gonvex_scheduled_jobs
		WHERE status = 'pending'
		  AND run_at <= $1
		  AND (claim_token = '' OR lease_until IS NULL OR lease_until <= now())
		ORDER BY (cron_name <> '') ASC, run_at ASC, id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $2
	)
	UPDATE gonvex_scheduled_jobs AS jobs
	SET claim_sequence = jobs.claim_sequence + 1,
		claim_token = $3 || ':' || (jobs.claim_sequence + 1)::text,
		lease_until = now() + ($4 * interval '1 millisecond'),
		updated_at = now()
	FROM candidates
	WHERE jobs.id = candidates.id
	RETURNING jobs.id, jobs.project_id, jobs.tenant_id, jobs.function_path,
		jobs.args, jobs.run_at, jobs.scheduled_for, jobs.cron_name, jobs.claim_token`,
		now, limit, owner, scheduledJobLease.Milliseconds(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]scheduledJob, 0, limit)
	for rows.Next() {
		var job scheduledJob
		var args []byte
		if err := rows.Scan(
			&job.ID, &job.ProjectID, &job.TenantID, &job.FunctionPath,
			&args, &job.RunAt, &job.ScheduledFor, &job.CronName, &job.ClaimToken,
		); err != nil {
			return nil, err
		}
		job.Args = append(json.RawMessage(nil), args...)
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

type scheduledJobAdoption int

const (
	scheduledJobBusy scheduledJobAdoption = iota
	scheduledJobAdopted
	scheduledJobAlreadyCompleted
)

// adoptClaim moves a job claimed from the pre-Postgres Valkey queue into the
// durable table while retaining the same fencing token. This lets a new
// runtime drain work created by an overlapping old runtime without executing
// the same deterministic cron occurrence twice.
func (store *postgresScheduledJobStore) adoptClaim(ctx context.Context, job scheduledJob, _ time.Time) (scheduledJobAdoption, error) {
	db, err := store.db(ctx)
	if err != nil {
		return scheduledJobBusy, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return scheduledJobBusy, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_scheduled_jobs
		(id, project_id, tenant_id, function_path, args, run_at, scheduled_for, cron_name)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.ProjectID, job.TenantID, job.FunctionPath, nullableSchedulerArgs(job.Args),
		job.RunAt, job.ScheduledFor, job.CronName,
	); err != nil {
		return scheduledJobBusy, err
	}
	var adopted bool
	err = tx.QueryRowContext(ctx, `UPDATE gonvex_scheduled_jobs
		SET claim_sequence = claim_sequence + 1,
			claim_token = $2,
			lease_until = now() + ($3 * interval '1 millisecond'),
			updated_at = now()
		WHERE id = $1
		  AND status = 'pending'
		  AND (claim_token = '' OR claim_token = $2 OR lease_until IS NULL OR lease_until <= now())
		RETURNING TRUE`, job.ID, job.ClaimToken, scheduledJobLease.Milliseconds()).Scan(&adopted)
	if err == nil && adopted {
		if err := tx.Commit(); err != nil {
			return scheduledJobBusy, err
		}
		return scheduledJobAdopted, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return scheduledJobBusy, err
	}
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM gonvex_scheduled_jobs WHERE id = $1`, job.ID).Scan(&status); err != nil {
		return scheduledJobBusy, err
	}
	if err := tx.Commit(); err != nil {
		return scheduledJobBusy, err
	}
	if status == "completed" {
		return scheduledJobAlreadyCompleted, nil
	}
	return scheduledJobBusy, nil
}

func (store *postgresScheduledJobStore) renew(ctx context.Context, id string, owner string) (bool, error) {
	db, err := store.db(ctx)
	if err != nil {
		return false, err
	}
	result, err := db.ExecContext(ctx, `UPDATE gonvex_scheduled_jobs
		SET lease_until = now() + ($3 * interval '1 millisecond'), updated_at = now()
		WHERE id = $1 AND status = 'pending' AND claim_token = $2`, id, owner, scheduledJobLease.Milliseconds())
	if err != nil {
		return false, err
	}
	updated, err := result.RowsAffected()
	return updated == 1, err
}

func (store *postgresScheduledJobStore) complete(ctx context.Context, id string, owner string) error {
	db, err := store.db(ctx)
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, `UPDATE gonvex_scheduled_jobs
		SET status = 'completed', completed_at = COALESCE(completed_at, now()),
			claim_token = '', lease_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'pending' AND claim_token = $2`, id, owner)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil || updated == 1 {
		return err
	}
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM gonvex_scheduled_jobs WHERE id = $1`, id).Scan(&status); err != nil {
		return err
	}
	if status == "completed" {
		return nil
	}
	return fmt.Errorf("scheduled job %s completion rejected: claim is missing or owned by another runtime", id)
}

func (store *postgresScheduledJobStore) release(ctx context.Context, id string, owner string) error {
	db, err := store.db(ctx)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `UPDATE gonvex_scheduled_jobs
		SET claim_token = '', lease_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'pending' AND claim_token = $2`, id, owner)
	return err
}
