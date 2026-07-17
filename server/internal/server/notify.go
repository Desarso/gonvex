package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	"github.com/gonvex/gonvex/server/internal/schema"
	"github.com/jackc/pgx/v5"
)

type tableNotifyPayload struct {
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
	db, err := dbpool.Open(s.config.PostgresURL)
	if err != nil {
		cancel()
		return
	}
	defer db.Close()
	connection, err := db.Conn(ctx)
	cancel()
	if err != nil {
		return
	}
	defer connection.Close()

	_ = connection.Raw(func(raw any) error {
		conn, ok := dbpool.PGXConn(raw)
		if !ok {
			return nil
		}
		if err := ensureBaseNotifyTriggers(context.Background(), conn); err != nil {
			return err
		}
		if _, err := conn.Exec(context.Background(), "LISTEN "+schema.NotifyChannel); err != nil {
			return err
		}

		for {
			notification, err := conn.WaitForNotification(context.Background())
			if err != nil {
				return err
			}
			if notification.Payload != "" && notification.Payload[0] != '{' {
				s.broadcastTableChange("", notification.Payload)
				continue
			}
			var payload tableNotifyPayload
			if err := json.Unmarshal([]byte(notification.Payload), &payload); err == nil && payload.Table != "" {
				if payload.Broad {
					s.broadcastTableChange("", payload.Table)
				} else {
					s.broadcastRowIDChange("", payload.Table, payload.IDs)
				}
			}
		}
	})
}

func ensureBaseNotifyTriggers(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'tasks'
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	table := manifest.Table{Columns: map[string]manifest.Column{}}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return err
		}
		table.Columns[column] = manifest.Column{Type: "string"}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(table.Columns) == 0 {
		return nil
	}

	statement, err := schema.NotifySQLForTable("tasks", table)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, statement)
	return err
}
