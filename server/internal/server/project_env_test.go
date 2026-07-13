package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func projectEnvRouterRequest(t *testing.T, server *Server, method string, project string, body string, key string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "/dev/projects/"+project+"/env", strings.NewReader(body))
	if body != "" {
		request.Header.Set("content-type", "application/json")
	}
	if key != "" {
		// Match the CLI: it sends both forms so runtimes and reverse proxies that
		// standardize on either header see the same project credential.
		request.Header.Set("authorization", "Bearer "+key)
		request.Header.Set("x-gonvex-key", key)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func TestProjectEnvRouterAcceptsMatchingRegisteredProjectKey(t *testing.T) {
	server := New(config.Config{
		RequireAuth: true,
		ProjectKeys: map[string]string{
			"project-a": "project-a-key",
			"project-b": "project-b-key",
		},
	})

	for _, headers := range []struct {
		name  string
		apply func(*http.Request)
	}{
		{name: "x-gonvex-key", apply: func(request *http.Request) {
			request.Header.Set("x-gonvex-key", "project-a-key")
		}},
		{name: "bearer", apply: func(request *http.Request) {
			request.Header.Set("authorization", "Bearer project-a-key")
		}},
	} {
		t.Run(headers.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/dev/projects/project-a/env", nil)
			headers.apply(request)
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("matching project key did not reach the environment handler: got status %d", recorder.Code)
			}
		})
	}
}

func TestProjectEnvRouterRejectsAnotherProjectsKeyForEveryMethod(t *testing.T) {
	server := New(config.Config{
		RequireAuth: true,
		ProjectKeys: map[string]string{
			"project-a": "project-a-key",
			"project-b": "project-b-key",
		},
	})

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			recorder := projectEnvRouterRequest(t, server, method, "project-a", `{}`, "project-b-key")
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("another project's key: expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
			}
		})
	}
}

func TestProjectEnvRouterRejectsUnscopedDevSyncKeyForEveryMethod(t *testing.T) {
	server := New(config.Config{
		RequireAuth: true,
		DevSyncKey:  "runtime-wide-sync-key",
	})

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			recorder := projectEnvRouterRequest(t, server, method, "project-without-a-key", `{}`, "runtime-wide-sync-key")
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("unscoped dev sync key: expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
			}
		})
	}
}

func TestProjectEnvRouterRejectsRequestsWithoutCredentialsForEveryMethod(t *testing.T) {
	server := New(config.Config{
		RequireAuth: true,
		ProjectKeys: map[string]string{"project-a": "project-a-key"},
	})

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			recorder := projectEnvRouterRequest(t, server, method, "project-a", `{}`, "")
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("missing credentials: expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
			}
		})
	}
}

func TestProjectEnvRouterScopesLegacyGeneratedDevKeyToEncodedProject(t *testing.T) {
	projectKey, err := generateProjectKey("project-a")
	if err != nil {
		t.Fatal("generate synthetic project key")
	}
	server := New(config.Config{RequireAuth: true, DevSyncKey: projectKey})

	matching := projectEnvRouterRequest(t, server, http.MethodGet, "project-a", "", projectKey)
	if matching.Code != http.StatusOK {
		t.Fatalf("project-bound legacy key: expected status %d, got %d", http.StatusOK, matching.Code)
	}

	otherProject := projectEnvRouterRequest(t, server, http.MethodGet, "project-b", "", projectKey)
	if otherProject.Code != http.StatusUnauthorized {
		t.Fatalf("project-bound legacy key used on another project: expected status %d, got %d", http.StatusUnauthorized, otherProject.Code)
	}
}

func TestProjectEnvRouterCORSAllowsProjectKeyHeader(t *testing.T) {
	server := New(config.Config{RequireAuth: true})
	request := httptest.NewRequest(http.MethodOptions, "/dev/projects/project-a/env", nil)
	request.Header.Set("access-control-request-headers", "x-gonvex-key")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("preflight: expected status %d, got %d", http.StatusNoContent, recorder.Code)
	}
	if allowed := strings.ToLower(recorder.Header().Get("access-control-allow-headers")); !strings.Contains(allowed, "x-gonvex-key") {
		t.Fatal("preflight did not allow the project-key header")
	}
}
