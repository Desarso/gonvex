package runtime

import (
	"log/slog"
	"net/http"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/server"
)

func Handler(app *gonvex.App) http.Handler {
	return server.NewWithApp(config.FromEnv(), app).Handler()
}

func ListenAndServe(app *gonvex.App) error {
	cfg := config.FromEnv()
	runtime := server.NewWithApp(cfg, app)
	slog.Info("starting gonvex runtime", "addr", cfg.Addr)
	return http.ListenAndServe(cfg.Addr, runtime.Handler())
}
