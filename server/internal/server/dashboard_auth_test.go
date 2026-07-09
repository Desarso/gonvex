package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestDashboardSessionTokenRoundTrip(t *testing.T) {
	server := New(config.Config{DashboardSecret: "test-secret"})
	session, err := server.dashboardSessionForActor(dashboardActor{
		Email: "Owner@Example.COM",
		Name:  "Owner",
		Role:  "admin",
	})
	if err != nil {
		t.Fatalf("sign session: %v", err)
	}
	actor, ok := server.verifyDashboardToken(session.AccessToken)
	if !ok {
		t.Fatal("expected signed session token to verify")
	}
	if actor.Email != "owner@example.com" || actor.Role != "admin" {
		t.Fatalf("unexpected actor: %+v", actor)
	}
}

func TestDashboardSessionTokenRejectsExpiredToken(t *testing.T) {
	server := New(config.Config{DashboardSecret: "test-secret"})
	token, err := server.signDashboardSession(dashboardSession{
		Email:       "owner@example.com",
		Name:        "Owner",
		Role:        "admin",
		ExpiresAtMS: time.Now().Add(-time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("sign session: %v", err)
	}
	if _, ok := server.verifyDashboardToken(token); ok {
		t.Fatal("expected expired token to be rejected")
	}
}

func TestProjectsEndpointRequiresDashboardTokenWhenConfigured(t *testing.T) {
	server := New(config.Config{DashboardSecret: "test-secret"})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/projects", nil))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
}

func TestProjectsEndpointRequiresDashboardTokenWhenRequireAuth(t *testing.T) {
	server := New(config.Config{RequireAuth: true})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/projects", nil))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
}

// The `gonvex dev` watch loop verifies each sync via GET /dev/manifest using the
// project sync key (not a dashboard session). If that read 401s, the CLI reads
// it as "runtime state missing" and resyncs forever, so the manifest health
// check must accept the same key POST /dev/sync accepts.
func TestManifestEndpointAcceptsProjectSyncKey(t *testing.T) {
	server := New(config.Config{
		DashboardSecret: "test-secret",
		ProjectKeys:     map[string]string{"proj-1": "gvx_secret"},
	})

	noCreds := httptest.NewRecorder()
	server.Handler().ServeHTTP(noCreds, httptest.NewRequest(http.MethodGet, "/dev/manifest?project=proj-1", nil))
	if noCreds.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without credentials, got %d", noCreds.Code)
	}

	withKey := httptest.NewRequest(http.MethodGet, "/dev/manifest?project=proj-1", nil)
	withKey.Header.Set("x-gonvex-project-id", "proj-1")
	withKey.Header.Set("x-gonvex-key", "gvx_secret")
	keyed := httptest.NewRecorder()
	server.Handler().ServeHTTP(keyed, withKey)
	if keyed.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid project sync key, got %d", keyed.Code)
	}

	wrongKey := httptest.NewRequest(http.MethodGet, "/dev/manifest?project=proj-1", nil)
	wrongKey.Header.Set("x-gonvex-project-id", "proj-1")
	wrongKey.Header.Set("x-gonvex-key", "nope")
	rejected := httptest.NewRecorder()
	server.Handler().ServeHTTP(rejected, wrongKey)
	if rejected.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong project sync key, got %d", rejected.Code)
	}
}
