package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
	gonvexruntime "github.com/gonvex/gonvex/server/internal/server"
)

func main() {
	run(config.FromEnv())
}

func run(cfg config.Config) {
	runtime, err := gonvexruntime.NewRequired(cfg)
	if err != nil {
		slog.Error("gonvex runtime startup failed", "error", err)
		os.Exit(1)
	}

	defer runtime.Close()

	server := gonvexruntime.NewHTTPServer(cfg.Addr, runtime.Handler())
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("starting gonvex runtime", "addr", cfg.Addr)
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			slog.Warn("graceful HTTP shutdown timed out", "error", err)
		}
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("gonvex runtime stopped", "error", err)
			os.Exit(1)
		}
	}
}
