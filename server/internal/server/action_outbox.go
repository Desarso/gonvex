package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	"github.com/google/uuid"
)

type postgresActionOutbox struct {
	tx   *sql.Tx
	user *gonvex.User
}

func (o postgresActionOutbox) Enqueue(ctx context.Context, actionPath string, args any) (string, error) {
	actionPath = strings.TrimSpace(actionPath)
	if o.tx == nil || actionPath == "" {
		return "", fmt.Errorf("gonvex: outbox requires a transaction and Action path")
	}
	raw, err := json.Marshal(args)
	if err != nil {
		return "", err
	}
	id, userID, email := uuid.NewString(), "", ""
	if o.user != nil {
		userID, email = o.user.ID, o.user.Email
	}
	_, err = o.tx.ExecContext(ctx, `INSERT INTO _gonvex_action_outbox
      (id, action_path, args, actor_user_id, actor_email) VALUES ($1, $2, $3::jsonb, NULLIF($4, ''), NULLIF($5, ''))`,
		id, actionPath, raw, userID, email)
	return id, err
}

type claimedActionOutbox struct {
	id, path      string
	args          json.RawMessage
	userID, email string
	attempts      int
}

func (s *Server) drainActionOutbox(project, tenant string) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	databaseURL := s.databaseURLForTenant(project, tenantIDFromRequest(project, tenant))
	if strings.TrimSpace(databaseURL) == "" {
		return
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return
	}
	defer db.Close()
	for range 100 {
		row, ok, err := claimActionOutbox(ctx, db)
		if err != nil || !ok {
			return
		}
		caller := callerContext{}
		if row.userID != "" {
			caller.user = &gonvex.User{ID: row.userID, Email: row.email}
		}
		_, actionErr := s.executeTenantActionForCaller(withMutationID(ctx, row.id), project, tenant, caller, row.path, row.args)
		if actionErr == nil {
			_, _ = db.ExecContext(ctx, `DELETE FROM _gonvex_action_outbox WHERE id = $1`, row.id)
			continue
		}
		delay := time.Duration(min(row.attempts, 10)) * time.Second
		if delay < time.Second {
			delay = time.Second
		}
		_, _ = db.ExecContext(ctx, `UPDATE _gonvex_action_outbox SET status = 'pending', locked_at = NULL, available_at = now() + $2::interval, last_error = $3 WHERE id = $1`, row.id, delay.String(), actionErr.Error())
		time.AfterFunc(delay, func() { go s.drainActionOutbox(project, tenant) })
		return
	}
	// The bounded drain prevents one busy tenant monopolizing a worker. If the
	// batch filled, promptly continue with another bounded claim loop.
	go s.drainActionOutbox(project, tenant)
}

func claimActionOutbox(ctx context.Context, db *sql.DB) (claimedActionOutbox, bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return claimedActionOutbox{}, false, err
	}
	defer tx.Rollback()
	row := claimedActionOutbox{}
	err = tx.QueryRowContext(ctx, `SELECT id, action_path, args, COALESCE(actor_user_id, ''), COALESCE(actor_email, ''), attempts + 1
    FROM _gonvex_action_outbox WHERE (status = 'pending' AND available_at <= now()) OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
    ORDER BY available_at, created_at FOR UPDATE SKIP LOCKED LIMIT 1`).
		Scan(&row.id, &row.path, &row.args, &row.userID, &row.email, &row.attempts)
	if err == sql.ErrNoRows {
		return claimedActionOutbox{}, false, nil
	}
	if err != nil {
		return claimedActionOutbox{}, false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE _gonvex_action_outbox SET status = 'processing', locked_at = now(), attempts = $2 WHERE id = $1`, row.id, row.attempts); err != nil {
		return claimedActionOutbox{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return claimedActionOutbox{}, false, err
	}
	return row, true, nil
}
