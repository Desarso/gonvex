package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// projectEnvVar is a single project-scoped environment variable. These live in
// the runtime registry (gonvex_project_env), not in any browsable tenant/project
// database, and are hidden from the Data browser.
type projectEnvVar struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Source    string `json:"source"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

func projectFromEnvRequest(r *http.Request) string {
	if p := strings.TrimSpace(r.PathValue("project")); p != "" {
		return p
	}
	return projectID(r)
}

const projectEnvCacheTTL = 30 * time.Second

type projectEnvCacheEntry struct {
	values    map[string]string
	fetchedAt time.Time
}

// projectEnvValues returns the project's env store as a name→value map for
// injection into function runtime contexts. Values are cached briefly so the
// registry database isn't hit on every function call; the cache is dropped on
// any env write so dashboard edits apply on the next call.
func (s *Server) projectEnvValues(ctx context.Context, project string) map[string]string {
	if project == "" {
		return nil
	}
	s.projectEnvMu.Lock()
	entry, ok := s.projectEnvCache[project]
	s.projectEnvMu.Unlock()
	if ok && time.Since(entry.fetchedAt) < projectEnvCacheTTL {
		return entry.values
	}

	vars, err := s.loadProjectEnv(ctx, project)
	if err != nil || vars == nil {
		// vars == nil (with nil err) means the registry handle wasn't available —
		// distinct from an empty store, which returns an empty non-nil slice.
		// Never cache that as "project has no env" or functions would silently
		// lose their keys for a full TTL window.
		slog.Warn("project env load failed; functions fall back to process env", "project", project, "error", err)
		if ok {
			return entry.values
		}
		return nil
	}
	values := make(map[string]string, len(vars))
	for _, v := range vars {
		values[v.Name] = v.Value
	}
	s.projectEnvMu.Lock()
	if s.projectEnvCache == nil {
		s.projectEnvCache = map[string]projectEnvCacheEntry{}
	}
	s.projectEnvCache[project] = projectEnvCacheEntry{values: values, fetchedAt: time.Now()}
	s.projectEnvMu.Unlock()
	return values
}

func (s *Server) invalidateProjectEnvCache(project string) {
	s.projectEnvMu.Lock()
	delete(s.projectEnvCache, project)
	s.projectEnvMu.Unlock()
}

func (s *Server) loadProjectEnv(ctx context.Context, project string) ([]projectEnvVar, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT name, value, updated_at FROM gonvex_project_env WHERE project_id = $1 ORDER BY name`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	vars := make([]projectEnvVar, 0)
	for rows.Next() {
		var v projectEnvVar
		var updated sql.NullTime
		if err := rows.Scan(&v.Name, &v.Value, &updated); err != nil {
			return nil, err
		}
		v.Source = "project"
		if updated.Valid {
			v.UpdatedAt = updated.Time.UTC().Format(time.RFC3339)
		}
		vars = append(vars, v)
	}
	return vars, rows.Err()
}

// GET /dev/projects/{project}/env
func (s *Server) handleProjectEnv(w http.ResponseWriter, r *http.Request) {
	project := projectFromEnvRequest(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	if !s.canAccessProject(r.Context(), actor, project) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project access is required"})
		return
	}
	vars, err := s.loadProjectEnv(r.Context(), project)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"variables": vars})
}

// POST /dev/projects/{project}/env  { name, value }
func (s *Server) handleSetProjectEnv(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	project := projectFromEnvRequest(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	defer s.invalidateProjectEnvCache(project)
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	if !s.canManageProject(r.Context(), actor, project) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}
	var payload struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "variable name is required"})
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": registryUnavailableError(err)})
		return
	}
	defer db.Close()
	if _, err := db.ExecContext(r.Context(),
		`INSERT INTO gonvex_project_env (project_id, name, value, updated_at) VALUES ($1, $2, $3, now())
		 ON CONFLICT (project_id, name) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
		project, name, payload.Value); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// PUT /dev/projects/{project}/env  { content?: ".env text", variables?: [{name,value}] }
// Replaces the project's entire env set. Used by the dashboard "paste .env" mode.
func (s *Server) handleBulkProjectEnv(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	project := projectFromEnvRequest(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	defer s.invalidateProjectEnvCache(project)
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	if !s.canManageProject(r.Context(), actor, project) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}
	var payload struct {
		Content   string `json:"content"`
		Variables []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	pairs := map[string]string{}
	order := make([]string, 0)
	add := func(name, value string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, seen := pairs[name]; !seen {
			order = append(order, name)
		}
		pairs[name] = value
	}
	if strings.TrimSpace(payload.Content) != "" {
		for _, kv := range parseDotEnv(payload.Content) {
			add(kv[0], kv[1])
		}
	}
	for _, v := range payload.Variables {
		add(v.Name, v.Value)
	}

	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": registryUnavailableError(err)})
		return
	}
	defer db.Close()

	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(r.Context(), `DELETE FROM gonvex_project_env WHERE project_id = $1`, project); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for _, name := range order {
		if _, err := tx.ExecContext(r.Context(),
			`INSERT INTO gonvex_project_env (project_id, name, value, updated_at) VALUES ($1, $2, $3, now())`,
			project, name, pairs[name]); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": len(order)})
}

// DELETE /dev/projects/{project}/env  { name }
func (s *Server) handleDeleteProjectEnv(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	project := projectFromEnvRequest(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	defer s.invalidateProjectEnvCache(project)
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	if !s.canManageProject(r.Context(), actor, project) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}
	var payload struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "variable name is required"})
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": registryUnavailableError(err)})
		return
	}
	defer db.Close()
	if _, err := db.ExecContext(r.Context(), `DELETE FROM gonvex_project_env WHERE project_id = $1 AND name = $2`, project, name); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func registryUnavailableError(err error) string {
	if err != nil {
		return err.Error()
	}
	return "project registry database is not configured"
}

// parseDotEnv parses .env-style text into ordered name/value pairs. It ignores
// blank lines and comments, strips an optional leading "export ", and removes
// surrounding single/double quotes from values.
func parseDotEnv(content string) [][2]string {
	pairs := make([][2]string, 0)
	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		name := strings.TrimSpace(line[:eq])
		value := strings.TrimSpace(line[eq+1:])
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		if name == "" {
			continue
		}
		pairs = append(pairs, [2]string{name, value})
	}
	return pairs
}
