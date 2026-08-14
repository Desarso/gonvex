package server

// Close stops background work and closes live WebSockets. Coolify only sends
// SIGTERM to the old container after the replacement passes readiness, so
// clients can reconnect immediately to the healthy replacement.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.cancel()

	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.close()
	}

	if s.tenantStores != nil {
		s.tenantStores.Close()
	}
	if s.cache != nil {
		_ = s.cache.close()
	}
}
