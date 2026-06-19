package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/schema"
)

type tenantTarget struct {
	ID             string `json:"id"`
	ProjectID      string `json:"projectId"`
	Name           string `json:"name"`
	Database       string `json:"database"`
	Status         string `json:"status"`
	Description    string `json:"description"`
	Provisioned    bool   `json:"provisioned"`
	RuntimeCreated bool   `json:"runtimeCreated"`
	databaseURL    string
	databaseName   string
}

func (s *Server) handleTenants(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	s.projectMu.RLock()
	tenants := make([]tenantTarget, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		if project == "" || tenant.ProjectID == project {
			tenants = append(tenants, tenant)
		}
	}
	s.projectMu.RUnlock()

	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].ProjectID == tenants[j].ProjectID {
			return strings.ToLower(tenants[i].Name) < strings.ToLower(tenants[j].Name)
		}
		return tenants[i].ProjectID < tenants[j].ProjectID
	})
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload struct {
		Name      string `json:"name"`
		ProjectID string `json:"projectId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	project := strings.TrimSpace(payload.ProjectID)
	if project == "" {
		project = projectID(r)
	}
	if project == "" {
		project = "default"
	}
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant name is required"})
		return
	}
	if s.config.PostgresURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "DATABASE_URL is not configured"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()

	tenantID := s.uniqueTenantIDLocked(project, name)
	databaseName := s.uniqueTenantDatabaseNameLocked(project, tenantID)
	databaseURL, err := createProjectDatabase(r.Context(), s.config.PostgresURL, databaseName)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := provisionTenantDatabase(r.Context(), databaseURL, s.runtime.ManifestForProject(project).Schema); err != nil {
		_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.config.TenantDatabases == nil {
		s.config.TenantDatabases = map[string]string{}
	}
	s.config.TenantDatabases[tenantStoreKey(project, tenantID)] = databaseURL
	tenant := tenantTarget{
		ID:             tenantID,
		ProjectID:      project,
		Name:           name,
		Database:       databaseName,
		Status:         "local",
		Description:    "Runtime-created tenant database.",
		Provisioned:    true,
		RuntimeCreated: true,
		databaseURL:    databaseURL,
		databaseName:   databaseName,
	}
	s.tenants[tenantStoreKey(project, tenantID)] = tenant

	writeJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
}

func (s *Server) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if project == "" {
		project = "default"
	}
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	if tenantID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant id is required"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()
	key := tenantStoreKey(project, tenantID)
	tenant, ok := s.tenants[key]
	if !ok || !tenant.RuntimeCreated {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "runtime-created tenant not found"})
		return
	}
	if err := dropProjectDatabase(r.Context(), s.config.PostgresURL, tenant.databaseName); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	delete(s.tenants, key)
	delete(s.config.TenantDatabases, key)
	s.tenantStores.Close()
	s.cache.invalidateRows(r.Context(), project, tenantID, "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) uniqueTenantIDLocked(projectID string, name string) string {
	base := slug(name)
	if base == "" {
		base = "tenant"
	}
	return uniqueName(base, func(value string) bool {
		if _, ok := s.tenants[tenantStoreKey(projectID, value)]; ok {
			return true
		}
		if s.config.TenantDatabases != nil && s.config.TenantDatabases[tenantStoreKey(projectID, value)] != "" {
			return true
		}
		return false
	})
}

func (s *Server) uniqueTenantDatabaseNameLocked(projectID string, tenantID string) string {
	base := tenantDatabaseName(projectID, tenantID)
	return uniqueName(base, func(value string) bool {
		for _, tenant := range s.tenants {
			if tenant.databaseName == value {
				return true
			}
		}
		return false
	})
}

func provisionTenantDatabase(ctx context.Context, databaseURL string, desiredSchema manifest.Schema) error {
	if databaseURL == "" {
		return fmt.Errorf("tenant database URL is not configured")
	}
	if _, err := schema.Apply(ctx, databaseURL, desiredSchema); err != nil {
		return err
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if err := ensureTenantLocalTables(ctx, db); err != nil {
		return err
	}
	return nil
}

func ensureTenantLocalTables(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS members (
			user_id TEXT PRIMARY KEY,
			role TEXT NOT NULL DEFAULT 'member',
			permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS members_by_role ON members (role)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
