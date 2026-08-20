package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/gonvex/gonvex/server/internal/controlplane/legacyidentity"
)

// startControlPlaneMigrations installs the legacy identity compatibility
// tables while the account/member Control Plane migration is rolling out.
// LandlordURL remains a deprecated configuration alias for this endpoint.
func (s *Server) startControlPlaneMigrations() {
	if s.config.ControlPlaneURL == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result, err := legacyidentity.Apply(ctx, s.config.ControlPlaneURL)
		if err != nil {
			slog.Error("control plane migration failed", "error", err)
			return
		}
		slog.Info("control plane migration complete", "applied", len(result.Applied))
	}()
}
