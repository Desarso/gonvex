package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

type projectEnvHTTPResponse struct {
	status int
	body   []byte
}

func requestProjectEnvOverHTTP(t *testing.T, client *http.Client, runtimeURL string, method string, project string, body string, projectKey string, dashboardToken string) projectEnvHTTPResponse {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, runtimeURL+"/dev/projects/"+project+"/env", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("create project env request: %v", err)
	}
	if body != "" {
		request.Header.Set("content-type", "application/json")
	}
	if projectKey != "" {
		request.Header.Set("authorization", "Bearer "+projectKey)
		request.Header.Set("x-gonvex-key", projectKey)
	} else if dashboardToken != "" {
		request.Header.Set("authorization", "Bearer "+dashboardToken)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send project env request: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read project env response: %v", err)
	}
	return projectEnvHTTPResponse{status: response.StatusCode, body: payload}
}

func requireProjectEnvStatus(t *testing.T, response projectEnvHTTPResponse, expected int) {
	t.Helper()
	if response.status != expected {
		// Do not include response bodies here: successful GET responses contain
		// environment values and should never end up in test or CI logs.
		t.Fatalf("project env request: expected status %d, got %d", expected, response.status)
	}
}

func TestPostgresProjectEnvAuthorizationThroughRouterAndMiddleware(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_env_auth_"+suffix)
	projectA := "env-project-a-" + suffix
	projectB := "env-project-b-" + suffix
	projectWithoutKey := "env-project-unkeyed-" + suffix
	projectAKey, err := generateProjectKey(projectA)
	if err != nil {
		t.Fatal("generate first synthetic project key")
	}
	projectBKey, err := generateProjectKey(projectB)
	if err != nil {
		t.Fatal("generate second synthetic project key")
	}

	server := New(config.Config{
		LandlordURL:     controlURL,
		PostgresURL:     baseURL,
		RequireAuth:     true,
		DashboardSecret: "project-env-integration-session-secret",
		DevSyncKey:      "runtime-wide-integration-sync-key",
		ProjectKeys: map[string]string{
			projectA: projectAKey,
			projectB: projectBKey,
		},
	})

	ownerEmail := "env-owner@example.test"
	projects := []projectTarget{
		{
			ID: projectA, Name: "Environment project A", Environment: "test",
			Database: "project_a", DatabaseMode: "single", StorageBucket: projectA + "-test",
			Status: "test", Description: "Project environment authorization test.",
			Provisioned: true, RuntimeCreated: true, OwnerEmail: ownerEmail,
			databaseURL: baseURL, databaseName: "project_a", syncKey: projectAKey,
		},
		{
			ID: projectB, Name: "Environment project B", Environment: "test",
			Database: "project_b", DatabaseMode: "single", StorageBucket: projectB + "-test",
			Status: "test", Description: "Cross-project authorization test.",
			Provisioned: true, RuntimeCreated: true, OwnerEmail: "another-owner@example.test",
			databaseURL: baseURL, databaseName: "project_b", syncKey: projectBKey,
		},
		{
			ID: projectWithoutKey, Name: "Environment project without key", Environment: "test",
			Database: "project_unkeyed", DatabaseMode: "single", StorageBucket: projectWithoutKey + "-test",
			Status: "test", Description: "Unconfigured-key authorization test.",
			Provisioned: true, RuntimeCreated: true, OwnerEmail: ownerEmail,
			databaseURL: baseURL, databaseName: "project_unkeyed",
		},
	}
	server.projectMu.Lock()
	for _, project := range projects {
		server.projects[project.ID] = project
	}
	server.projectMu.Unlock()
	for _, project := range projects {
		if err := server.saveProjectRegistry(context.Background(), project); err != nil {
			t.Fatalf("save synthetic project registry: %v", err)
		}
	}
	if err := server.ensureProjectOwnerMember(context.Background(), projectA, dashboardActor{Email: ownerEmail, Name: "Environment Owner", Role: "user"}); err != nil {
		t.Fatalf("save synthetic project owner: %v", err)
	}
	registry, err := server.openProjectRegistry(context.Background())
	if err != nil {
		t.Fatalf("open synthetic project registry: %v", err)
	}
	if registry == nil {
		t.Fatal("synthetic project registry is unavailable")
	}
	if _, err := registry.ExecContext(context.Background(), `INSERT INTO gonvex_project_members (
		project_id, email, name, role
	) VALUES ($1, $2, $3, 'admin')`, projectA, "env-admin@example.test", "Environment Admin"); err != nil {
		_ = registry.Close()
		t.Fatalf("save synthetic project admin: %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("close synthetic project registry: %v", err)
	}

	// Start a fresh runtime without ProjectKeys in its process config. Every
	// request below therefore proves the persisted project-key registration is
	// hydrated before authorization, as it is after a real runtime restart.
	runtimeServer := New(config.Config{
		LandlordURL:     controlURL,
		PostgresURL:     baseURL,
		RequireAuth:     true,
		DashboardSecret: "project-env-integration-session-secret",
		DevSyncKey:      "runtime-wide-integration-sync-key",
	})
	ownerSession, err := runtimeServer.dashboardSessionForActor(dashboardActor{Email: ownerEmail, Name: "Environment Owner", Role: "user"})
	if err != nil {
		t.Fatal("create synthetic owner session")
	}
	adminSession, err := runtimeServer.dashboardSessionForActor(dashboardActor{Email: "env-admin@example.test", Name: "Environment Admin", Role: "user"})
	if err != nil {
		t.Fatal("create synthetic admin session")
	}

	runtime := httptest.NewServer(runtimeServer.Handler())
	t.Cleanup(runtime.Close)

	t.Run("matching project key can manage every method", func(t *testing.T) {
		requireProjectEnvStatus(t, requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodPost, projectA, `{"name":"AUTH_TEST_ALPHA","value":"alpha-value"}`, projectAKey, ""), http.StatusOK)

		listed := requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodGet, projectA, "", projectAKey, "")
		requireProjectEnvStatus(t, listed, http.StatusOK)
		var initial struct {
			Variables []projectEnvVar `json:"variables"`
		}
		if err := json.Unmarshal(listed.body, &initial); err != nil {
			t.Fatal("decode initial project environment response")
		}
		if len(initial.Variables) != 1 || initial.Variables[0].Name != "AUTH_TEST_ALPHA" || initial.Variables[0].Value != "alpha-value" {
			t.Fatal("POST value was not returned by GET")
		}

		requireProjectEnvStatus(t, requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodPut, projectA, `{"content":"AUTH_TEST_BETA=beta-value\nAUTH_TEST_GAMMA=gamma-value\n"}`, projectAKey, ""), http.StatusOK)
		requireProjectEnvStatus(t, requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodDelete, projectA, `{"name":"AUTH_TEST_BETA"}`, projectAKey, ""), http.StatusOK)

		remaining := requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodGet, projectA, "", projectAKey, "")
		requireProjectEnvStatus(t, remaining, http.StatusOK)
		var final struct {
			Variables []projectEnvVar `json:"variables"`
		}
		if err := json.Unmarshal(remaining.body, &final); err != nil {
			t.Fatal("decode final project environment response")
		}
		if len(final.Variables) != 1 || final.Variables[0].Name != "AUTH_TEST_GAMMA" || final.Variables[0].Value != "gamma-value" {
			t.Fatal("PUT replacement or DELETE did not persist")
		}
	})

	rejectionBodies := map[string]string{
		http.MethodGet:    "",
		http.MethodPost:   `{"name":"AUTH_TEST_REJECTED","value":"rejected-value"}`,
		http.MethodPut:    `{"content":"AUTH_TEST_REJECTED=rejected-value\n"}`,
		http.MethodDelete: `{"name":"AUTH_TEST_GAMMA"}`,
	}
	for _, testCase := range []struct {
		name       string
		project    string
		projectKey string
	}{
		{name: "another project's key", project: projectA, projectKey: projectBKey},
		{name: "arbitrary key with no matching project key", project: projectWithoutKey, projectKey: "arbitrary-integration-key"},
		{name: "runtime-wide sync key with no matching project key", project: projectWithoutKey, projectKey: "runtime-wide-integration-sync-key"},
		{name: "no credentials", project: projectA},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
				t.Run(method, func(t *testing.T) {
					response := requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, method, testCase.project, rejectionBodies[method], testCase.projectKey, "")
					requireProjectEnvStatus(t, response, http.StatusUnauthorized)
				})
			}
		})
	}

	for _, session := range []struct {
		name  string
		token string
	}{
		{name: "dashboard owner", token: ownerSession.AccessToken},
		{name: "dashboard project admin", token: adminSession.AccessToken},
	} {
		t.Run(session.name+" can manage every method", func(t *testing.T) {
			requireProjectEnvStatus(t, requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodGet, projectA, "", "", session.token), http.StatusOK)
			requireProjectEnvStatus(t, requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodPost, projectA, `{"name":"AUTH_TEST_SESSION","value":"session-value"}`, "", session.token), http.StatusOK)
			requireProjectEnvStatus(t, requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodPut, projectA, `{"content":"AUTH_TEST_SESSION=session-value\n"}`, "", session.token), http.StatusOK)
			requireProjectEnvStatus(t, requestProjectEnvOverHTTP(t, runtime.Client(), runtime.URL, http.MethodDelete, projectA, `{"name":"AUTH_TEST_SESSION"}`, "", session.token), http.StatusOK)
		})
	}
}
