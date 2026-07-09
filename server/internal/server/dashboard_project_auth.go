package server

import (
	"net/http"
	"strings"
)

func (s *Server) withDashboardProjectAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.needsDashboardProjectAuth(r) {
			next.ServeHTTP(w, r)
			return
		}
		// The dev CLI (`gonvex dev`) polls GET /dev/manifest after each sync to
		// confirm the runtime still holds its manifest, authenticating with the
		// project sync key — the same credential POST /dev/sync accepts — not a
		// dashboard session. Honor that key here for the read-only manifest
		// check. Without this the poll 401s, runtimeHasManifest reads any
		// non-200 as "state missing", and the watch loop resyncs every couple of
		// seconds forever.
		if r.Method == http.MethodGet && r.URL.Path == "/dev/manifest" {
			if project := projectID(r); project != "" && s.acceptsSyncKey(project, syncKey(r)) {
				next.ServeHTTP(w, r)
				return
			}
		}
		actor, ok := s.dashboardActorFromRequest(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
			return
		}
		project := projectID(r)
		if project == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
			return
		}
		if !s.canAccessProject(r.Context(), actor, project) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "project access is required"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) needsDashboardProjectAuth(r *http.Request) bool {
	if s.dashboardAuthOptional() {
		return false
	}
	path := r.URL.Path
	if !strings.HasPrefix(path, "/dev/") {
		return false
	}
	switch {
	case strings.HasPrefix(path, "/dev/auth/"):
		return false
	case path == "/dev/projects" || strings.HasPrefix(path, "/dev/projects/"):
		return false
	case path == "/dev/sync":
		return false
	case path == "/dev/logs/stream":
		return false
	case path == "/dev/metrics/stream":
		return false
	default:
		return true
	}
}
