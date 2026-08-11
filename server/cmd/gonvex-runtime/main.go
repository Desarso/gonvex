package main

import (
	"log/slog"
	"os"

	"github.com/gonvex/gonvex/server/internal/config"
	gonvexruntime "github.com/gonvex/gonvex/server/internal/server"
)

func main() {
	cfg := config.FromEnv()
	runtime := gonvexruntime.New(cfg)

	slog.Info("starting gonvex runtime", "addr", cfg.Addr)
	if err := gonvexruntime.NewHTTPServer(cfg.Addr, runtime.Handler()).ListenAndServe(); err != nil {
		slog.Error("gonvex runtime stopped", "error", err)
		os.Exit(1)
	}
}
