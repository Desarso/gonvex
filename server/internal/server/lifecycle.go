package server

import (
	"context"
	"log/slog"
	"time"
)

// moduleHostShutdownGrace bounds how long Close waits for the module host to
// finish its in-flight calls and exit before it is killed.
const moduleHostShutdownGrace = 15 * time.Second

// Close stops background work and closes live WebSockets. Coolify only sends
// SIGTERM to the old container after the replacement passes readiness, so
// clients can reconnect immediately to the healthy replacement.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.cancel()
	s.membershipProjectorMu.Lock()
	s.membershipProjectorClosing = true
	s.membershipProjectorMu.Unlock()
	projectionDone := make(chan struct{})
	go func() {
		s.membershipProjectorWG.Wait()
		close(projectionDone)
	}()
	select {
	case <-projectionDone:
	case <-time.After(moduleHostShutdownGrace):
		slog.Warn("membership projectors did not shut down cleanly")
	}

	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.close()
	}

	// The module host runs as a separate process, so shutting it down is
	// bounded and explicit: it is asked to drain, then killed if it does not.
	// Skipping this would leave an orphan holding V8 heaps and a socket.
	if s.runtime != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), moduleHostShutdownGrace)
		if err := s.runtime.Close(shutdown); err != nil {
			slog.Warn("module host did not shut down cleanly", "error", err)
		}
		cancel()
	}

	if s.tenantStores != nil {
		s.tenantStores.Close()
	}
	if s.cache != nil {
		_ = s.cache.close()
	}
}
