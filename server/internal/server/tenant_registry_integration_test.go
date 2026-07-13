package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func tenantRegistryTestPostgresURL(t *testing.T) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv("GONVEX_TEST_POSTGRES_URL"))
	if value == "" {
		t.Skip("set GONVEX_TEST_POSTGRES_URL to run PostgreSQL tenant-registry integration tests")
	}
	return value
}

func createTenantRegistryTestDatabase(t *testing.T, baseURL string, name string) string {
	t.Helper()
	databaseURL, err := createProjectDatabase(context.Background(), baseURL, name)
	if err != nil {
		t.Fatalf("create test database %s: %v", name, err)
	}
	t.Cleanup(func() {
		if err := dropProjectDatabase(context.Background(), baseURL, name); err != nil {
			t.Errorf("drop test database %s: %v", name, err)
		}
	})
	return databaseURL
}

func tenantRegistryTestSuffix(t *testing.T) string {
	t.Helper()
	id, err := generateRelationshipID()
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(id[:18], "-", "_")
}

func TestPostgresUUIDv6ProjectIgnoresUnrelatedAppDatabase(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlName := "gonvex_rel_control_" + suffix
	unrelatedName := "phantom_tenant_" + suffix
	controlURL := createTenantRegistryTestDatabase(t, baseURL, controlName)
	unrelatedURL := createTenantRegistryTestDatabase(t, baseURL, unrelatedName)

	unrelatedDB, err := sql.Open("pgx", unrelatedURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unrelatedDB.ExecContext(context.Background(), `CREATE TABLE tasks (id TEXT PRIMARY KEY); CREATE TABLE workspaces (id TEXT PRIMARY KEY)`); err != nil {
		_ = unrelatedDB.Close()
		t.Fatalf("seed unrelated app database: %v", err)
	}
	_ = unrelatedDB.Close()

	cfg := config.Config{
		LandlordURL:      controlURL,
		PostgresURL:      baseURL,
		ProjectDatabases: map[string]string{},
		ProjectKeys:      map[string]string{},
	}
	server := New(cfg)
	createProjectRequest := httptest.NewRequest(http.MethodPost, "/dev/projects", bytes.NewBufferString(`{"name":"Relationship Test","databaseMode":"multiTenant"}`))
	createProjectRequest.Header.Set("content-type", "application/json")
	createProjectRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(createProjectRecorder, createProjectRequest)
	if createProjectRecorder.Code != http.StatusCreated {
		t.Fatalf("create project: expected %d, got %d: %s", http.StatusCreated, createProjectRecorder.Code, createProjectRecorder.Body.String())
	}
	var createdProject createProjectResponse
	if err := json.NewDecoder(createProjectRecorder.Body).Decode(&createdProject); err != nil {
		t.Fatalf("decode created project: %v", err)
	}
	projectID := createdProject.Project.ID
	if !isUUIDv6(projectID) {
		t.Fatalf("new project id is not UUIDv6: %q", projectID)
	}
	t.Cleanup(func() {
		if err := dropProjectDatabase(context.Background(), baseURL, createdProject.Project.Database); err != nil {
			t.Errorf("drop created project database %s: %v", createdProject.Project.Database, err)
		}
	})

	server.hydrateProjectTenantDatabases(context.Background(), projectID)
	server.projectMu.RLock()
	for _, tenant := range server.tenants {
		if tenant.ProjectID == projectID {
			server.projectMu.RUnlock()
			t.Fatalf("UUIDv6 project adopted unrelated database %q as tenant %+v", unrelatedName, tenant)
		}
	}
	server.projectMu.RUnlock()

	createTenantRequest := httptest.NewRequest(http.MethodPost, "/dev/tenants", bytes.NewBufferString(`{"name":"Acme","projectId":"`+projectID+`"}`))
	createTenantRequest.Header.Set("content-type", "application/json")
	createTenantRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(createTenantRecorder, createTenantRequest)
	if createTenantRecorder.Code != http.StatusCreated {
		t.Fatalf("create tenant: expected %d, got %d: %s", http.StatusCreated, createTenantRecorder.Code, createTenantRecorder.Body.String())
	}
	var createdTenant struct {
		Tenant tenantTarget `json:"tenant"`
	}
	if err := json.NewDecoder(createTenantRecorder.Body).Decode(&createdTenant); err != nil {
		t.Fatalf("decode created tenant: %v", err)
	}
	if !isUUIDv6(createdTenant.Tenant.ID) || createdTenant.Tenant.RelationshipID != createdTenant.Tenant.ID {
		t.Fatalf("new tenant identity/relationship is not UUIDv6: %+v", createdTenant.Tenant)
	}
	createdTenantDatabase := tenantDatabaseNameWithAlias(projectID, createdTenant.Tenant.ID, "acme")
	t.Cleanup(func() {
		if err := dropProjectDatabase(context.Background(), baseURL, createdTenantDatabase); err != nil {
			t.Errorf("drop created tenant database %s: %v", createdTenantDatabase, err)
		}
	})

	renameRequest := httptest.NewRequest(http.MethodPatch, "/dev/projects/"+projectID, bytes.NewBufferString(`{"name":"Renamed Relationship Test"}`))
	renameRequest.Header.Set("content-type", "application/json")
	renameRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(renameRecorder, renameRequest)
	if renameRecorder.Code != http.StatusOK {
		t.Fatalf("rename project: expected %d, got %d: %s", http.StatusOK, renameRecorder.Code, renameRecorder.Body.String())
	}
	var renamed struct {
		Project projectTarget `json:"project"`
	}
	if err := json.NewDecoder(renameRecorder.Body).Decode(&renamed); err != nil {
		t.Fatalf("decode renamed project: %v", err)
	}
	if renamed.Project.Name != "Renamed Relationship Test" || renamed.Project.ID != projectID || renamed.Project.Database != createdProject.Project.Database {
		t.Fatalf("rename changed project identity: before=%+v after=%+v", createdProject.Project, renamed.Project)
	}

	// Project keys are the CLI's machine credential. Exercise the authenticated
	// bulk env route to ensure the CLI does not need a dashboard session and a
	// key remains scoped to the project encoded in the URL.
	server.config.RequireAuth = true
	envRequest := httptest.NewRequest(http.MethodPut, "/dev/projects/"+projectID+"/env", bytes.NewBufferString(`{"content":"API_URL=https://api.example.test\nSECRET_TOKEN=shh\n"}`))
	envRequest.Header.Set("content-type", "application/json")
	envRequest.Header.Set("x-gonvex-key", createdProject.ProjectKey)
	envRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(envRecorder, envRequest)
	if envRecorder.Code != http.StatusOK {
		t.Fatalf("push project env with project key: expected %d, got %d: %s", http.StatusOK, envRecorder.Code, envRecorder.Body.String())
	}
	storedEnv, err := server.loadProjectEnv(context.Background(), projectID)
	if err != nil {
		t.Fatalf("load pushed project env: %v", err)
	}
	if len(storedEnv) != 2 || storedEnv[0].Name != "API_URL" || storedEnv[1].Name != "SECRET_TOKEN" {
		t.Fatalf("unexpected pushed project env: %+v", storedEnv)
	}

	restarted := New(cfg)
	loaded, err := restarted.loadTenantRegistry(context.Background(), projectID)
	if err != nil {
		t.Fatalf("load new tenant relationship after restart: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ID != createdTenant.Tenant.ID || loaded[0].RelationshipID != createdTenant.Tenant.RelationshipID {
		t.Fatalf("new relationship did not survive restart: %+v", loaded)
	}
	persistedProjects, err := restarted.loadProjectRegistry(context.Background())
	if err != nil {
		t.Fatalf("load renamed project after restart: %v", err)
	}
	var persistedName string
	for _, project := range persistedProjects {
		if project.ID == projectID {
			persistedName = project.Name
			break
		}
	}
	if persistedName != "Renamed Relationship Test" {
		t.Fatalf("renamed project did not survive restart: got %q", persistedName)
	}
}

func TestPostgresLegacyProjectTenantBackfillSurvivesRestart(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlName := "gonvex_legacy_control_" + suffix
	projectID := "legacy-" + strings.ReplaceAll(suffix, "_", "-")
	projectName := "gonvex_legacy_project_" + suffix
	tenantName := tenantDatabaseNameWithAlias(projectID, "acme", "acme")
	controlURL := createTenantRegistryTestDatabase(t, baseURL, controlName)
	projectURL := createTenantRegistryTestDatabase(t, baseURL, projectName)
	tenantURL := createTenantRegistryTestDatabase(t, baseURL, tenantName)

	cfg := config.Config{
		LandlordURL: controlURL,
		PostgresURL: baseURL,
		ProjectDatabases: map[string]string{
			projectID: projectURL,
		},
		ProjectKeys: map[string]string{},
	}
	project := projectTarget{
		ID:             projectID,
		Name:           "Legacy Relationship Test",
		Environment:    "test",
		Database:       projectName,
		DatabaseMode:   "multiTenant",
		StorageBucket:  projectID + "-test",
		Status:         "local",
		Description:    "Legacy tenant registry integration test.",
		Provisioned:    true,
		RuntimeCreated: true,
		databaseURL:    projectURL,
		databaseName:   projectName,
	}
	server := New(cfg)
	server.projects[projectID] = project
	if err := server.saveProjectRegistry(context.Background(), project); err != nil {
		t.Fatalf("save legacy project registry: %v", err)
	}

	server.hydrateProjectTenantDatabases(context.Background(), projectID)
	server.projectMu.RLock()
	backfilled, ok := server.tenants[tenantStoreKey(projectID, "acme")]
	server.projectMu.RUnlock()
	if !ok {
		t.Fatalf("legacy project tenant database %q was not backfilled", tenantName)
	}
	if !isUUIDv6(backfilled.RelationshipID) || !backfilled.registered || backfilled.databaseURL != tenantURL {
		t.Fatalf("unexpected backfilled relationship: %+v", backfilled)
	}

	restarted := New(cfg)
	loaded, err := restarted.loadTenantRegistry(context.Background(), projectID)
	if err != nil {
		t.Fatalf("load tenant registry after restart: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ID != "acme" || loaded[0].RelationshipID != backfilled.RelationshipID {
		t.Fatalf("legacy relationship did not survive restart: %+v", loaded)
	}
}
