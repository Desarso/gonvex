package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/gonvex/gonvex/server/internal/config"
	gonvexruntime "github.com/gonvex/gonvex/server/internal/server"
)

func main() {
	cfg := config.FromEnv()
	runtime := gonvexruntime.New(cfg)

	slog.Info("starting gonvex runtime", "addr", cfg.Addr)
	if err := http.ListenAndServe(cfg.Addr, runtime.Handler()); err != nil {
		slog.Error("gonvex runtime stopped", "error", err)
		os.Exit(1)
	}
}
