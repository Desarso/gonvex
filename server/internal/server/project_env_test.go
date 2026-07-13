package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestProjectEnvAuthorizationAcceptsMatchingProjectKey(t *testing.T) {
	server := New(config.Config{
		RequireAuth: true,
		ProjectKeys: map[string]string{
			"project-a": "project-a-key",
			"project-b": "project-b-key",
		},
	})
	request := httptest.NewRequest(http.MethodPut, "/dev/projects/project-a/env", nil)
	request.Header.Set("x-gonvex-key", "project-a-key")
	recorder := httptest.NewRecorder()

	if !server.authorizeProjectEnvRequest(recorder, request, "project-a", true) {
		t.Fatalf("matching project key was rejected: %s", recorder.Body.String())
	}
}

func TestProjectEnvAuthorizationRejectsAnotherProjectsKey(t *testing.T) {
	server := New(config.Config{
		RequireAuth: true,
		ProjectKeys: map[string]string{
			"project-a": "project-a-key",
			"project-b": "project-b-key",
		},
	})
	request := httptest.NewRequest(http.MethodPut, "/dev/projects/project-a/env", nil)
	request.Header.Set("authorization", "Bearer project-b-key")
	recorder := httptest.NewRecorder()

	if server.authorizeProjectEnvRequest(recorder, request, "project-a", true) {
		t.Fatal("a key registered to another project was accepted")
	}
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d: %s", http.StatusUnauthorized, recorder.Code, recorder.Body.String())
	}
}

func TestProjectEnvAuthorizationDoesNotAcceptArbitraryKeyWhenUnconfigured(t *testing.T) {
	server := New(config.Config{RequireAuth: true})
	request := httptest.NewRequest(http.MethodGet, "/dev/projects/project-a/env", nil)
	request.Header.Set("x-gonvex-key", "made-up-key")
	recorder := httptest.NewRecorder()

	if server.authorizeProjectEnvRequest(recorder, request, "project-a", false) {
		t.Fatal("an arbitrary key was accepted for a project with no registered key")
	}
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d: %s", http.StatusUnauthorized, recorder.Code, recorder.Body.String())
	}
}
