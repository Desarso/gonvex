package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/gonvex/gonvex/server/internal/landlord"
)

func (s *Server) startLandlordMigrations() {
	if s.config.LandlordURL == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result, err := landlord.Apply(ctx, s.config.LandlordURL)
		if err != nil {
			slog.Error("landlord migration failed", "error", err)
			return
		}
		slog.Info("landlord migration complete", "applied", len(result.Applied))
	}()
}
