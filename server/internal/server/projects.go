package server

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/gonvex/gonvex/pkg/manifest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type projectTarget struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Environment    string `json:"environment"`
	Database       string `json:"database"`
	DatabaseMode   string `json:"databaseMode"`
	StorageBucket  string `json:"storageBucket"`
	Status         string `json:"status"`
	Description    string `json:"description"`
	Provisioned    bool   `json:"provisioned"`
	RuntimeCreated bool   `json:"runtimeCreated"`
	TestTab        bool   `json:"testTab"`
	OwnerEmail     string `json:"ownerEmail,omitempty"`
	Role           string `json:"role,omitempty"`
	databaseURL    string
	databaseName   string
	syncKey        string
}

type createProjectResponse struct {
	Project    projectTarget `json:"project"`
	ProjectKey string        `json:"projectKey"`
}

func (s *Server) handleProjects(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	s.hydrateProjects()

	s.projectMu.RLock()
	projects := make([]projectTarget, 0, len(s.projects))
	for _, project := range s.projects {
		if s.dashboardAuthOptional() || s.canAccessProject(r.Context(), actor, project.ID) {
			if project.OwnerEmail == "" && s.dashboardAuthOptional() {
				project.OwnerEmail = actor.Email
			}
			projects = append(projects, project)
		}
	}
	s.projectMu.RUnlock()

	sort.Slice(projects, func(i, j int) bool {
		return strings.ToLower(projects[i].Name) < strings.ToLower(projects[j].Name)
	})
	writeJSON(w, http.StatusOK, map[string]any{"projects": projects})
}

func (s *Server) hydrateProjects() {
	s.hydratePersistedProjects()
	s.hydrateConfiguredProjects()
}

func normalizedDatabaseMode(mode string) string {
	switch strings.TrimSpace(mode) {
	case "", "single":
		return "single"
	case "multiTenant":
		return "multiTenant"
	default:
		return ""
	}
}

func normalizedDatabaseModeWithDefault(mode string) string {
	normalized := normalizedDatabaseMode(mode)
	if normalized == "" {
		return "single"
	}
	return normalized
}

func (s *Server) hydratePersistedProjects() {
	projects, err := s.loadProjectRegistry(context.Background())
	if err != nil {
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	for _, project := range projects {
		if _, ok := s.projects[project.ID]; ok {
			continue
		}
		s.projects[project.ID] = project
		if s.config.ProjectDatabases == nil {
			s.config.ProjectDatabases = map[string]string{}
		}
		if s.config.ProjectKeys == nil {
			s.config.ProjectKeys = map[string]string{}
		}
		s.config.ProjectDatabases[project.ID] = project.databaseURL
		if project.syncKey != "" {
			s.config.ProjectKeys[project.ID] = project.syncKey
		}
	}
}

func (s *Server) hydrateConfiguredProjects() {
	if len(s.config.ProjectDatabases) == 0 {
		return
	}

	var imported []projectTarget
	s.projectMu.Lock()
	for projectID, databaseURL := range s.config.ProjectDatabases {
		projectID = strings.TrimSpace(projectID)
		if projectID == "" {
			continue
		}
		if _, ok := s.projects[projectID]; ok {
			continue
		}
		project := projectTarget{
			ID:            projectID,
			Name:          projectNameFromID(projectID),
			Environment:   "local dev",
			Database:      databaseNameFromURL(databaseURL, projectID),
			DatabaseMode:  "single",
			StorageBucket: projectID + "-dev",
			Status:        "local",
			Description:   "Configured project database.",
			Provisioned:   true,
			databaseURL:   databaseURL,
			databaseName:  databaseNameFromURL(databaseURL, projectID),
			syncKey:       s.config.ProjectKeys[projectID],
		}
		s.projects[projectID] = project
		imported = append(imported, project)
	}
	s.projectMu.Unlock()

	for _, project := range imported {
		_ = s.saveProjectRegistry(context.Background(), project)
	}
}

func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	defer r.Body.Close()
	var payload struct {
		Name         string `json:"name"`
		DatabaseMode string `json:"databaseMode"`
		TestTab      bool   `json:"testTab"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project name is required"})
		return
	}
	databaseMode := normalizedDatabaseMode(payload.DatabaseMode)
	if databaseMode == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "databaseMode must be single or multiTenant"})
		return
	}
	if s.config.PostgresURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "DATABASE_URL is not configured"})
		return
	}

	s.hydrateProjects()

	s.projectMu.Lock()
	defer s.projectMu.Unlock()

	projectID, err := s.uniqueRuntimeProjectIDLocked()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	databaseName := s.uniqueDatabaseNameLocked(projectID)
	databaseURL, err := createProjectDatabase(r.Context(), s.config.PostgresURL, databaseName)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.config.ProjectDatabases == nil {
		s.config.ProjectDatabases = map[string]string{}
	}
	if s.config.ProjectKeys == nil {
		s.config.ProjectKeys = map[string]string{}
	}
	projectKey, err := generateProjectKey(projectID)
	if err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.config.ProjectDatabases[projectID] = databaseURL
	s.config.ProjectKeys[projectID] = projectKey
	project := projectTarget{
		ID:             projectID,
		Name:           name,
		Environment:    "local dev",
		Database:       databaseName,
		DatabaseMode:   databaseMode,
		StorageBucket:  projectID + "-dev",
		Status:         "local",
		Description:    "Runtime-created project database.",
		Provisioned:    true,
		RuntimeCreated: true,
		TestTab:        payload.TestTab,
		OwnerEmail:     actor.Email,
		Role:           "owner",
		databaseURL:    databaseURL,
		databaseName:   databaseName,
		syncKey:        projectKey,
	}
	s.projects[projectID] = project
	if err := s.saveProjectRegistry(r.Context(), project); err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		delete(s.projects, projectID)
		delete(s.config.ProjectDatabases, projectID)
		delete(s.config.ProjectKeys, projectID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := s.ensureProjectOwnerMember(r.Context(), project.ID, actor); err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		_ = s.deleteProjectRegistry(context.Background(), projectID)
		delete(s.projects, projectID)
		delete(s.config.ProjectDatabases, projectID)
		delete(s.config.ProjectKeys, projectID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, createProjectResponse{Project: project, ProjectKey: projectKey})
}

func (s *Server) handleUpdateProject(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	defer r.Body.Close()
	projectID := strings.TrimSpace(r.PathValue("project"))
	if !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}
	var payload struct {
		DatabaseMode *string `json:"databaseMode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if payload.DatabaseMode == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no project fields provided"})
		return
	}
	databaseMode := normalizedDatabaseMode(*payload.DatabaseMode)
	if databaseMode == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "databaseMode must be single or multiTenant"})
		return
	}

	s.hydrateProjects()

	s.projectMu.Lock()
	project, ok := s.projects[projectID]
	if ok {
		project.DatabaseMode = databaseMode
		s.projects[projectID] = project
	}
	s.projectMu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "project not found"})
		return
	}
	if err := s.saveProjectRegistry(r.Context(), project); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"project": project})
}

func (s *Server) handleProjectKey(w http.ResponseWriter, r *http.Request) {
	adminKey := s.acceptsAdminKey(syncKey(r))
	actor, signedIn := s.dashboardActorFromRequest(r)
	if !signedIn && !adminKey {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid Gonvex admin key"})
		return
	}
	s.hydrateProjects()
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if signedIn && !adminKey && !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	project, ok := s.projects[projectID]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "project not found"})
		return
	}
	projectKey := project.syncKey
	if projectKey == "" && s.config.ProjectKeys != nil {
		projectKey = s.config.ProjectKeys[projectID]
	}
	if projectKey == "" {
		var err error
		projectKey, err = generateProjectKey(projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if s.config.ProjectKeys == nil {
			s.config.ProjectKeys = map[string]string{}
		}
		s.config.ProjectKeys[projectID] = projectKey
	}
	project.syncKey = projectKey
	s.projects[projectID] = project
	if err := s.saveProjectRegistry(r.Context(), project); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"projectKey": projectKey})
}

func (s *Server) handleDeleteProject(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	project, ok := s.projects[projectID]
	if !ok || !project.RuntimeCreated {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "runtime-created project not found"})
		return
	}
	if err := dropProjectDatabase(r.Context(), s.config.PostgresURL, project.databaseName); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	_ = dropProjectDatabase(r.Context(), s.config.PostgresURL, telemetryDatabaseName(projectID))
	delete(s.projects, projectID)
	delete(s.config.ProjectDatabases, projectID)
	delete(s.config.ProjectKeys, projectID)
	if err := s.deleteProjectRegistry(r.Context(), projectID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.cache.invalidateRows(r.Context(), projectID, tenantIDFromRequest(projectID, ""), "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) projectRegistryURL() string {
	if strings.TrimSpace(s.config.LandlordURL) != "" {
		return s.config.LandlordURL
	}
	return s.config.PostgresURL
}

func (s *Server) openProjectRegistry(ctx context.Context) (*sql.DB, error) {
	registryURL := s.projectRegistryURL()
	if strings.TrimSpace(registryURL) == "" {
		return nil, nil
	}
	db, err := sql.Open("pgx", registryURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureProjectRegistry(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func ensureProjectRegistry(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_runtime_projects (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		environment TEXT NOT NULL,
		database_name TEXT NOT NULL,
		database_mode TEXT NOT NULL DEFAULT 'single',
		database_url TEXT NOT NULL,
		storage_bucket TEXT NOT NULL,
		status TEXT NOT NULL,
		description TEXT NOT NULL,
		project_key TEXT NOT NULL DEFAULT '',
		provisioned BOOLEAN NOT NULL DEFAULT TRUE,
		runtime_created BOOLEAN NOT NULL DEFAULT TRUE,
		test_tab BOOLEAN NOT NULL DEFAULT FALSE,
		owner_email TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS test_tab BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS database_mode TEXT NOT NULL DEFAULT 'single'`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE gonvex_runtime_projects ADD COLUMN IF NOT EXISTS owner_email TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_dashboard_users (
		email TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'user',
		password_hash TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_project_members (
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		email TEXT NOT NULL,
		name TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL DEFAULT 'dev',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (project_id, email)
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS gonvex_project_members_by_email ON gonvex_project_members (email, project_id)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_project_invitations (
		id TEXT PRIMARY KEY,
		project_id TEXT NOT NULL REFERENCES gonvex_runtime_projects(id) ON DELETE CASCADE,
		email TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'dev',
		token_hash TEXT NOT NULL,
		invited_by TEXT NOT NULL DEFAULT '',
		expires_at TIMESTAMPTZ NOT NULL,
		accepted_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_runtime_manifests (
		project_id TEXT PRIMARY KEY,
		manifest JSONB NOT NULL,
		bundle_hash TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	// Project environment variables, stored in the runtime registry (not in any
	// browsable tenant/project database) and hidden from the Data browser.
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS gonvex_project_env (
		project_id TEXT NOT NULL,
		name TEXT NOT NULL,
		value TEXT NOT NULL DEFAULT '',
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (project_id, name)
	)`)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) loadProjectRegistry(ctx context.Context) ([]projectTarget, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT id, name, environment, database_name, database_url, storage_bucket, status, description, project_key, provisioned, runtime_created, COALESCE(test_tab, false), COALESCE(NULLIF(database_mode, ''), 'single'), COALESCE(owner_email, '') FROM gonvex_runtime_projects ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []projectTarget
	for rows.Next() {
		var project projectTarget
		if err := rows.Scan(&project.ID, &project.Name, &project.Environment, &project.databaseName, &project.databaseURL, &project.StorageBucket, &project.Status, &project.Description, &project.syncKey, &project.Provisioned, &project.RuntimeCreated, &project.TestTab, &project.DatabaseMode, &project.OwnerEmail); err != nil {
			return nil, err
		}
		project.DatabaseMode = normalizedDatabaseModeWithDefault(project.DatabaseMode)
		project.Database = project.databaseName
		projects = append(projects, project)
	}
	return projects, rows.Err()
}

func (s *Server) saveProjectRegistry(ctx context.Context, project projectTarget) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()

	databaseName := project.databaseName
	if databaseName == "" {
		databaseName = project.Database
	}
	project.DatabaseMode = normalizedDatabaseModeWithDefault(project.DatabaseMode)
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_projects (
		id, name, environment, database_name, database_mode, database_url, storage_bucket, status, description, project_key, provisioned, runtime_created, test_tab, owner_email, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now())
	ON CONFLICT (id) DO UPDATE SET
		name = EXCLUDED.name,
		environment = EXCLUDED.environment,
		database_name = EXCLUDED.database_name,
		database_mode = EXCLUDED.database_mode,
		database_url = EXCLUDED.database_url,
		storage_bucket = EXCLUDED.storage_bucket,
		status = EXCLUDED.status,
		description = EXCLUDED.description,
		project_key = EXCLUDED.project_key,
		provisioned = EXCLUDED.provisioned,
		runtime_created = EXCLUDED.runtime_created,
		test_tab = EXCLUDED.test_tab,
		owner_email = EXCLUDED.owner_email,
		updated_at = now()`,
		project.ID,
		project.Name,
		project.Environment,
		databaseName,
		project.DatabaseMode,
		project.databaseURL,
		project.StorageBucket,
		project.Status,
		project.Description,
		project.syncKey,
		project.Provisioned,
		project.RuntimeCreated,
		project.TestTab,
		project.OwnerEmail,
	)
	return err
}

func (s *Server) deleteProjectRegistry(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_runtime_manifests WHERE project_id = $1`, projectID); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `DELETE FROM gonvex_runtime_projects WHERE id = $1`, projectID)
	return err
}

func (s *Server) ensureProjectOwnerMember(ctx context.Context, projectID string, actor dashboardActor) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	name := strings.TrimSpace(actor.Name)
	if name == "" {
		name = displayNameFromEmail(actor.Email)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_project_members (
		project_id, email, name, role
	) VALUES ($1, $2, $3, 'owner')
	ON CONFLICT (project_id, email) DO UPDATE SET
		name = EXCLUDED.name,
		role = 'owner'`,
		projectID, actor.Email, name)
	return err
}

func (s *Server) saveRuntimeManifest(ctx context.Context, next manifest.Manifest) error {
	projectID := strings.TrimSpace(next.Project)
	if projectID == "" {
		return nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	bundleHash := ""
	if next.Bundle != nil {
		bundleHash = next.Bundle.Hash
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_manifests (
		project_id, manifest, bundle_hash, updated_at
	) VALUES ($1, $2::jsonb, $3, now())
	ON CONFLICT (project_id) DO UPDATE SET
		manifest = EXCLUDED.manifest,
		bundle_hash = EXCLUDED.bundle_hash,
		updated_at = now()`,
		projectID,
		string(payload),
		bundleHash,
	)
	return err
}

func (s *Server) loadRuntimeManifests(ctx context.Context) ([]manifest.Manifest, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT manifest FROM gonvex_runtime_manifests ORDER BY updated_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var manifests []manifest.Manifest
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var next manifest.Manifest
		if err := json.Unmarshal(payload, &next); err != nil {
			return nil, err
		}
		if next.Functions == nil {
			next.Functions = map[string]manifest.FunctionEntry{}
		}
		if next.Schema.Tables == nil {
			next.Schema = manifest.EmptySchema()
		}
		manifests = append(manifests, next)
	}
	return manifests, rows.Err()
}

func (s *Server) loadRuntimeManifest(ctx context.Context, projectID string) (manifest.Manifest, bool, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return manifest.Manifest{}, false, nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return manifest.Manifest{}, false, err
	}
	defer db.Close()
	var payload []byte
	err = db.QueryRowContext(ctx, `SELECT manifest FROM gonvex_runtime_manifests WHERE project_id = $1`, projectID).Scan(&payload)
	if err == sql.ErrNoRows {
		return manifest.Manifest{}, false, nil
	}
	if err != nil {
		return manifest.Manifest{}, false, err
	}
	var next manifest.Manifest
	if err := json.Unmarshal(payload, &next); err != nil {
		return manifest.Manifest{}, false, err
	}
	if next.Project == "" {
		next.Project = projectID
	}
	if next.Functions == nil {
		next.Functions = map[string]manifest.FunctionEntry{}
	}
	if next.Schema.Tables == nil {
		next.Schema = manifest.EmptySchema()
	}
	return next, true, nil
}

func generateProjectKey(projectID string) (string, error) {
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate project key: %w", err)
	}
	encodedProject := base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(projectID)))
	return "gvx_" + encodedProject + "." + base64.RawURLEncoding.EncodeToString(bytes[:]), nil
}

func projectNameFromID(projectID string) string {
	value := strings.TrimSpace(strings.ReplaceAll(projectID, "-", " "))
	if value == "" {
		return "Project"
	}
	return value
}

func databaseNameFromURL(databaseURL string, projectID string) string {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "gonvex_" + strings.ReplaceAll(slug(projectID), "-", "_")
	}
	name := strings.Trim(strings.TrimSpace(parsed.Path), "/")
	if name == "" {
		return "gonvex_" + strings.ReplaceAll(slug(projectID), "-", "_")
	}
	return name
}

func projectIDFromProjectKey(key string) string {
	payload, ok := strings.CutPrefix(strings.TrimSpace(key), "gvx_")
	if !ok {
		return ""
	}
	encodedProject, _, ok := strings.Cut(payload, ".")
	if !ok {
		parts := strings.Split(strings.TrimSpace(key), "_")
		if len(parts) != 3 || parts[0] != "gvx" {
			return ""
		}
		encodedProject = parts[1]
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encodedProject)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(decoded))
}

func generateProjectID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate project id: %w", err)
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16]), nil
}

func (s *Server) uniqueRuntimeProjectIDLocked() (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		projectID, err := generateProjectID()
		if err != nil {
			return "", err
		}
		if _, ok := s.projects[projectID]; ok {
			continue
		}
		if s.config.ProjectDatabases != nil && s.config.ProjectDatabases[projectID] != "" {
			continue
		}
		return projectID, nil
	}
	return "", fmt.Errorf("generate project id: exhausted collision retries")
}

func (s *Server) uniqueProjectIDLocked(name string) string {
	base := slug(name)
	if base == "" {
		base = "project"
	}
	return uniqueName(base, func(value string) bool {
		if _, ok := s.projects[value]; ok {
			return true
		}
		if s.config.ProjectDatabases != nil && s.config.ProjectDatabases[value] != "" {
			return true
		}
		return false
	})
}

func (s *Server) uniqueDatabaseNameLocked(projectID string) string {
	base := "gonvex_" + strings.ReplaceAll(slug(projectID), "-", "_")
	return uniqueName(base, func(value string) bool {
		for _, project := range s.projects {
			if project.databaseName == value {
				return true
			}
		}
		return false
	})
}

func uniqueName(base string, taken func(string) bool) string {
	if !taken(base) {
		return base
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("%s_%d", base, suffix)
		if !taken(candidate) {
			return candidate
		}
	}
}

func createProjectDatabase(ctx context.Context, baseURL string, databaseName string) (string, error) {
	db, err := openMaintenanceDB(baseURL)
	if err != nil {
		return "", err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(databaseName)); err != nil {
		return "", err
	}
	return databaseURL(baseURL, databaseName)
}

func dropProjectDatabase(ctx context.Context, baseURL string, databaseName string) error {
	db, err := openMaintenanceDB(baseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	quoted := quoteIdent(databaseName)
	_, _ = db.ExecContext(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", databaseName)
	_, err = db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoted)
	return err
}

func openMaintenanceDB(baseURL string) (*sql.DB, error) {
	maintenanceURL, err := databaseURL(baseURL, "postgres")
	if err != nil {
		return nil, err
	}
	return sql.Open("pgx", maintenanceURL)
}

func databaseURL(baseURL string, databaseName string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + databaseName
	return parsed.String(), nil
}

var slugPattern = regexp.MustCompile(`[^a-z0-9]+`)

func slug(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return '-'
	}, value)
	value = slugPattern.ReplaceAllString(value, "-")
	return strings.Trim(value, "-")
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
