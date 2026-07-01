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
