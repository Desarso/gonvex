package runtime

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/server"
)

func Handler(app *gonvex.App) http.Handler {
	runtime, err := server.NewRequiredWithApp(config.FromEnv(), app)
	if err != nil {
		panic(fmt.Errorf("gonvex runtime startup failed: %w", err))
	}
	return runtime.Handler()
}

func ListenAndServe(app *gonvex.App) error {
	cfg := config.FromEnv()
	runtime, err := server.NewRequiredWithApp(cfg, app)
	if err != nil {
		return fmt.Errorf("gonvex runtime startup failed: %w", err)
	}
	slog.Info("starting gonvex runtime", "addr", cfg.Addr)
	return http.ListenAndServe(cfg.Addr, runtime.Handler())
}
