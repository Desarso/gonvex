package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
	gonvexruntime "github.com/gonvex/gonvex/server/internal/server"
	"github.com/gonvex/gonvex/server/internal/supervisor"
)

func main() {
	cfg := config.FromEnv()
	if reloadSupervisorEnabled() {
		runSupervisor(cfg)
		return
	}
	runWorker(cfg)
}

func reloadSupervisorEnabled() bool {
	enabled := strings.TrimSpace(strings.ToLower(os.Getenv("GONVEX_RELOAD_SUPERVISOR")))
	worker := strings.TrimSpace(os.Getenv("GONVEX_RUNTIME_WORKER"))
	return (enabled == "1" || enabled == "true" || enabled == "yes" || enabled == "on") && worker != "1"
}

func runSupervisor(cfg config.Config) {
	runtimeSupervisor, err := supervisor.Start(context.Background(), supervisor.Config{})
	if err != nil {
		slog.Error("gonvex runtime supervisor startup failed", "error", err)
		os.Exit(1)
	}
	defer runtimeSupervisor.Close()

	server := gonvexruntime.NewHTTPServer(cfg.Addr, runtimeSupervisor.Handler())
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("starting supervised gonvex runtime gateway", "addr", cfg.Addr)
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			slog.Warn("graceful gateway shutdown timed out", "error", err)
		}
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("gonvex runtime gateway stopped", "error", err)
			os.Exit(1)
		}
	}
}

func runWorker(cfg config.Config) {
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
