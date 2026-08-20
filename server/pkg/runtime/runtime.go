package runtime

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/server"
)

func Handler() http.Handler {
	runtime, err := server.NewRequired(config.FromEnv())
	if err != nil {
		panic(fmt.Errorf("gonvex runtime startup failed: %w", err))
	}
	return runtime.Handler()
}

func ListenAndServe() error {
	cfg := config.FromEnv()
	runtime, err := server.NewRequired(cfg)
	if err != nil {
		return fmt.Errorf("gonvex runtime startup failed: %w", err)
	}
	slog.Info("starting gonvex runtime", "addr", cfg.Addr)
	return server.NewHTTPServer(cfg.Addr, runtime.Handler()).ListenAndServe()
}
