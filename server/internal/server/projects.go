package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type projectTarget struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Environment    string `json:"environment"`
	Database       string `json:"database"`
	StorageBucket  string `json:"storageBucket"`
	Status         string `json:"status"`
	Description    string `json:"description"`
	Provisioned    bool   `json:"provisioned"`
	RuntimeCreated bool   `json:"runtimeCreated"`
	databaseURL    string
	databaseName   string
}

func (s *Server) handleProjects(w http.ResponseWriter, _ *http.Request) {
	s.projectMu.RLock()
	projects := make([]projectTarget, 0, len(s.projects))
	for _, project := range s.projects {
		projects = append(projects, project)
	}
	s.projectMu.RUnlock()

	sort.Slice(projects, func(i, j int) bool {
		return strings.ToLower(projects[i].Name) < strings.ToLower(projects[j].Name)
	})
	writeJSON(w, http.StatusOK, map[string]any{"projects": projects})
}

func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload struct {
		Name string `json:"name"`
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
	if s.config.PostgresURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "DATABASE_URL is not configured"})
		return
	}

	s.projectMu.Lock()
	defer s.projectMu.Unlock()

	projectID := s.uniqueProjectIDLocked(name)
	databaseName := s.uniqueDatabaseNameLocked(projectID)
	databaseURL, err := createProjectDatabase(r.Context(), s.config.PostgresURL, databaseName)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.config.ProjectDatabases == nil {
		s.config.ProjectDatabases = map[string]string{}
	}
	s.config.ProjectDatabases[projectID] = databaseURL
	project := projectTarget{
		ID:             projectID,
		Name:           name,
		Environment:    "local dev",
		Database:       databaseName,
		StorageBucket:  projectID + "-dev",
		Status:         "local",
		Description:    "Runtime-created project database.",
		Provisioned:    true,
		RuntimeCreated: true,
		databaseURL:    databaseURL,
		databaseName:   databaseName,
	}
	s.projects[projectID] = project

	writeJSON(w, http.StatusCreated, map[string]any{"project": project})
}

func (s *Server) handleDeleteProject(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
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
	delete(s.projects, projectID)
	delete(s.config.ProjectDatabases, projectID)
	s.cache.invalidateRows(r.Context(), projectID, "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
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
