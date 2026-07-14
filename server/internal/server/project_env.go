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

// acceptsProjectEnvKey is deliberately stricter than acceptsSyncKey. A sync key
// may be runtime-wide for bootstrapping local projects, but environment values
// are project-scoped secrets and require a credential bound to the exact project
// in the route. Persisted/configured project keys are authoritative. The legacy
// single-key runtime configuration remains supported only for generated Gonvex
// keys, whose project identifier is encoded in the key itself.
func (s *Server) acceptsProjectEnvKey(project string, provided string) bool {
	project = strings.TrimSpace(project)
	provided = strings.TrimSpace(provided)
	if project == "" || provided == "" {
		return false
	}

	s.hydrateProjects()
	s.projectMu.RLock()
	expected := strings.TrimSpace(s.config.ProjectKeys[project])
	if expected == "" {
		if target, ok := s.projects[project]; ok {
			expected = strings.TrimSpace(target.syncKey)
		}
	}
	devKey := strings.TrimSpace(s.config.DevSyncKey)
	s.projectMu.RUnlock()

	if expected != "" {
		return constantTimeString(provided, expected)
	}
	return devKey != "" &&
		constantTimeString(provided, devKey) &&
		projectIDFromProjectKey(provided) == project
}

// authorizeProjectEnvRequest accepts either an owner/admin dashboard session or
// the exact project key used by the CLI. Project keys must be project-bound and
// non-empty; unlike the local-dev sync endpoint, this never treats an
// unconfigured key as open access.
func (s *Server) authorizeProjectEnvRequest(w http.ResponseWriter, r *http.Request, project string, manage bool) bool {
	if s.acceptsProjectEnvKey(project, syncKey(r)) {
		return true
	}

	actor, ok := s.projectEnvDashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in or project key is required"})
		return false
	}
	permission := permissionProjectsEnvRead
	if manage {
		permission = permissionProjectsEnvWrite
	}
	if !actor.hasAccountPermission(permission) {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error":      "personal access token does not grant the required permission",
			"permission": permission,
		})
		return false
	}
	if manage {
		if !s.canManageProject(r.Context(), actor, project) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
			return false
		}
		return true
	}
	if !s.canAccessProject(r.Context(), actor, project) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project access is required"})
		return false
	}
	return true
}

// projectEnvDashboardActorFromRequest accepts signed dashboard sessions,
// permission-scoped personal access tokens, and the explicitly configured
// runtime admin key. It intentionally does not use the legacy
// DevSyncKey-as-admin fallback: allowing a runtime-wide sync credential here
// would bypass project-key scoping.
func (s *Server) projectEnvDashboardActorFromRequest(r *http.Request) (dashboardActor, bool) {
	token := strings.TrimSpace(r.Header.Get("authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[len("Bearer "):])
	}
	if actor, ok := s.verifyDashboardToken(token); ok {
		actor.credentialKind = "session"
		return actor, true
	}
	if adminKey := strings.TrimSpace(s.config.AdminKey); adminKey != "" && constantTimeString(token, adminKey) {
		return dashboardActor{Email: "admin@gonvex.local", Name: "Gonvex Admin", Role: "admin", credentialKind: "adminKey"}, true
	}
	if strings.HasPrefix(token, "gvx_pat_") {
		if actor, ok := s.verifyAccountAccessToken(r.Context(), token); ok {
			return actor, true
		}
	}
	if s.dashboardAuthOptional() {
		return dashboardActor{Email: "local@gonvex.dev", Name: "Local Developer", Role: "admin", credentialKind: "local"}, true
	}
	return dashboardActor{}, false
}

// GET /dev/projects/{project}/env
func (s *Server) handleProjectEnv(w http.ResponseWriter, r *http.Request) {
	project := projectFromEnvRequest(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.authorizeProjectEnvRequest(w, r, project, false) {
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
	if !s.authorizeProjectEnvRequest(w, r, project, true) {
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
// Replaces the project's entire env set. Used by the dashboard "paste .env"
// mode and `gonvex env push`.
func (s *Server) handleBulkProjectEnv(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	project := projectFromEnvRequest(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	defer s.invalidateProjectEnvCache(project)
	if !s.authorizeProjectEnvRequest(w, r, project, true) {
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
	if !s.authorizeProjectEnvRequest(w, r, project, true) {
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
