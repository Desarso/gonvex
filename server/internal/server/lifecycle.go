package server

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

const (
	// A stable, deployment-wide PostgreSQL advisory lock. Every runtime sharing
	// the landlord database competes for this session lock, so rolling replicas
	// can serve requests concurrently while only one process dispatches jobs.
	schedulerLeaderLockID   int64 = 0x476f6e7665785363
	schedulerLeaseRetry           = time.Second
	schedulerLeaseHeartbeat       = 2 * time.Second
)

// Close stops background work and closes live WebSockets. Coolify only sends
// SIGTERM to the old container after the replacement passes readiness, so
// clients can reconnect immediately to the healthy replacement.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.cancel()

	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.close()
	}

	if s.tenantStores != nil {
		s.tenantStores.Close()
	}
	if s.cache != nil {
		_ = s.cache.close()
	}
}

func (s *Server) startDistributedScheduler() {
	go s.runDistributedScheduler(s.ctx)
}

func (s *Server) runDistributedScheduler(ctx context.Context) {
	databaseURL := s.projectRegistryURL()
	if databaseURL == "" {
		slog.Warn("scheduler leader lease unavailable; running single-process scheduler")
		s.scheduler.run(ctx)
		return
	}

	for ctx.Err() == nil {
		if err := s.holdSchedulerLeadership(ctx, databaseURL); err != nil && ctx.Err() == nil {
			slog.Warn("scheduler leader lease interrupted", "error", err)
		}
		if !waitForSchedulerRetry(ctx) {
			return
		}
	}
}

func (s *Server) holdSchedulerLeadership(ctx context.Context, databaseURL string) error {
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	connection, err := database.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()

	var acquired bool
	if err := connection.QueryRowContext(
		ctx,
		"SELECT pg_try_advisory_lock($1)",
		schedulerLeaderLockID,
	).Scan(&acquired); err != nil {
		return err
	}
	if !acquired {
		return nil
	}

	leaderContext, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.scheduler.run(leaderContext)
	}()

	ticker := time.NewTicker(schedulerLeaseHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancel()
			<-done
			return ctx.Err()
		case <-ticker.C:
			var alive int
			if err := connection.QueryRowContext(ctx, "SELECT 1").Scan(&alive); err != nil {
				cancel()
				<-done
				return err
			}
		}
	}
}

func waitForSchedulerRetry(ctx context.Context) bool {
	timer := time.NewTimer(schedulerLeaseRetry)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
