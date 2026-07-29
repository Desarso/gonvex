package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

// Modern (UUID) projects reject slug tenant ids except for the runtime admin
// key (CI shard provisioning of subdomain-addressed tenants like
// "e2e-parallel") and the auth-optional local developer credential. Dashboard
// sessions and native app sessions stay restricted to opaque UUID v6 ids.
func TestCreateTenantSlugIDAllowedByCredentialKind(t *testing.T) {
	cases := []struct {
		kind    string
		ok      bool
		allowed bool
	}{
		{kind: "adminKey", ok: true, allowed: true},
		{kind: "local", ok: true, allowed: true},
		{kind: "session", ok: true, allowed: false},
		{kind: "nativeSession", ok: true, allowed: false},
		{kind: "", ok: false, allowed: false},
	}
	for _, test := range cases {
		if got := createTenantSlugIDAllowed(dashboardActor{credentialKind: test.kind}, test.ok); got != test.allowed {
			t.Fatalf("credentialKind %q (ok=%v): allowed = %v, want %v", test.kind, test.ok, got, test.allowed)
		}
	}
}

// HTTP-level: the admin key must get PAST the id-shape gate (the request then
// fails on the unrelated missing-project/database preconditions of a bare test
// server, never on "UUID v6").
func TestCreateTenantSlugIDGateOverHTTP(t *testing.T) {
	const slugBody = `{"id":"e2e-parallel","name":"e2e-parallel","projectId":"3db6f499-421c-42a2-a43d-1daa44ecbc4d"}`

	post := func(server *Server, token string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/dev/tenants?project=3db6f499-421c-42a2-a43d-1daa44ecbc4d", strings.NewReader(slugBody))
		if token != "" {
			request.Header.Set("authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder
	}

	secured := New(config.Config{AdminKey: "admin-secret", DashboardSecret: "dashboard-secret"})
	if recorder := post(secured, ""); recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated create should 401, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if recorder := post(secured, "admin-secret"); strings.Contains(recorder.Body.String(), "UUID v6") {
		t.Fatalf("runtime admin key should bypass the UUID id rule, got %d: %s", recorder.Code, recorder.Body.String())
	}

	// Auth-optional local runtime keeps slug creation for developers.
	local := New(config.Config{})
	if recorder := post(local, ""); strings.Contains(recorder.Body.String(), "UUID v6") {
		t.Fatalf("local dev credential should bypass the UUID id rule, got %d: %s", recorder.Code, recorder.Body.String())
	}
}
