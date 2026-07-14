package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestNormalizeAccountTokenPermissions(t *testing.T) {
	got, err := normalizeAccountTokenPermissions([]string{"projects:create", " projects:read ", "projects:create"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"projects:create", "projects:read"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("permissions: got %v, want %v", got, want)
	}

	defaults, err := normalizeAccountTokenPermissions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(defaults, []string{"projects:create", "projects:keys:read", "projects:read"}) {
		t.Fatalf("default permissions: %v", defaults)
	}
	if _, err := normalizeAccountTokenPermissions([]string{"billing:write"}); err == nil {
		t.Fatal("expected an unknown permission to be rejected")
	}
}

func TestAccountTokenPermissionWildcards(t *testing.T) {
	actor := dashboardActor{credentialKind: "personalAccessToken", tokenPermissions: []string{"projects:*"}}
	if !actor.hasAccountPermission(permissionProjectsCreate) || !actor.hasAccountPermission(permissionProjectsKeysRead) {
		t.Fatal("projects:* did not grant project permissions")
	}
	if actor.hasAccountPermission(permissionTokensCreate) {
		t.Fatal("projects:* unexpectedly granted token management")
	}
	if actor.canGrantAccountPermission("*") {
		t.Fatal("projects:* unexpectedly allowed minting a full-access token")
	}
	if actor.hasGlobalProjectAccess() {
		t.Fatal("projects:* unexpectedly granted global project administration")
	}

	userSession := dashboardActor{Role: "user", credentialKind: "session"}
	if userSession.canGrantAccountPermission(permissionAdminProjects) || userSession.canGrantAccountPermission("*") {
		t.Fatal("non-admin dashboard session could grant global permissions")
	}
	adminSession := dashboardActor{Role: "admin", credentialKind: "session"}
	if !adminSession.canGrantAccountPermission(permissionAdminProjects) {
		t.Fatal("admin dashboard session could not grant global project administration")
	}
	if adminSession.hasGlobalProjectAccess() {
		t.Fatal("admin dashboard session unexpectedly bypassed project membership")
	}
	globalToken := dashboardActor{Role: "admin", credentialKind: "personalAccessToken", tokenPermissions: []string{"projects:*", permissionAdminProjects}}
	if !globalToken.hasGlobalProjectAccess() {
		t.Fatal("admin token with explicit global permission was not global")
	}
}

func TestGenerateAccountAccessTokenRoundTrip(t *testing.T) {
	id, token, err := generateAccountAccessToken()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token, "gvx_pat_") || accountAccessTokenID(token) != id {
		t.Fatalf("unexpected personal access token shape: id=%q token=%q", id, token)
	}
	for _, malformed := range []string{"", "gvx_project.secret", "gvx_pat_missing-secret", "pat_id.secret"} {
		if got := accountAccessTokenID(malformed); got != "" {
			t.Fatalf("malformed token %q decoded as %q", malformed, got)
		}
	}
}

func accountTokenRequest(t *testing.T, client *http.Client, method string, url string, bearer string, body string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("content-type", "application/json")
	}
	if bearer != "" {
		request.Header.Set("authorization", "Bearer "+bearer)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, payload
}

func TestPostgresAccountTokenProjectProvisioning(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	suffix := tenantRegistryTestSuffix(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_account_tokens_"+suffix)
	server := New(config.Config{
		LandlordURL:     controlURL,
		PostgresURL:     baseURL,
		RequireAuth:     true,
		DashboardSecret: "account-token-test-session-secret",
	})
	user, err := server.createDashboardUser(context.Background(), "cli-owner@example.test", "CLI Owner", "correct horse battery staple", "user")
	if err != nil {
		t.Fatalf("create account user: %v", err)
	}
	if _, err := server.createDashboardUser(context.Background(), "runtime-admin@example.test", "Runtime Admin", "admin horse battery staple", "admin"); err != nil {
		t.Fatalf("create admin user: %v", err)
	}
	runtime := httptest.NewServer(server.Handler())
	t.Cleanup(runtime.Close)

	status, payload := accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/auth/login", "", `{"email":"cli-owner@example.test","password":"correct horse battery staple"}`)
	if status != http.StatusOK {
		t.Fatalf("login: got %d: %s", status, payload)
	}
	var login struct {
		Session dashboardSession `json:"session"`
	}
	if err := json.Unmarshal(payload, &login); err != nil || login.Session.AccessToken == "" {
		t.Fatalf("decode login: %v", err)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/auth/tokens", login.Session.AccessToken, `{"name":"not global","permissions":["projects:*","admin:projects"]}`)
	if status != http.StatusBadRequest || !bytes.Contains(payload, []byte(permissionAdminProjects)) {
		t.Fatalf("non-admin global token creation: got %d: %s", status, payload)
	}

	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/auth/tokens", login.Session.AccessToken, `{"name":"CLI provisioning","permissions":["projects:*"]}`)
	if status != http.StatusCreated {
		t.Fatalf("create token: got %d: %s", status, payload)
	}
	var createdToken struct {
		Token       accountAccessToken `json:"token"`
		AccessToken string             `json:"accessToken"`
	}
	if err := json.Unmarshal(payload, &createdToken); err != nil || createdToken.AccessToken == "" {
		t.Fatalf("decode created token: %v", err)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodGet, runtime.URL+"/dev/auth/tokens", login.Session.AccessToken, "")
	if status != http.StatusOK || !bytes.Contains(payload, []byte(createdToken.Token.ID)) {
		t.Fatalf("list account tokens: got %d: %s", status, payload)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/auth/tokens", login.Session.AccessToken, `{"name":"create only","permissions":["projects:create"]}`)
	if status != http.StatusCreated {
		t.Fatalf("create limited token: got %d: %s", status, payload)
	}
	var limitedToken struct {
		Token       accountAccessToken `json:"token"`
		AccessToken string             `json:"accessToken"`
	}
	if err := json.Unmarshal(payload, &limitedToken); err != nil {
		t.Fatal(err)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodGet, runtime.URL+"/dev/projects", limitedToken.AccessToken, "")
	if status != http.StatusForbidden || !bytes.Contains(payload, []byte(permissionProjectsRead)) {
		t.Fatalf("limited token project list: got %d: %s", status, payload)
	}

	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodGet, runtime.URL+"/dev/auth/me", createdToken.AccessToken, "")
	if status != http.StatusOK {
		t.Fatalf("token identity: got %d: %s", status, payload)
	}
	var identity struct {
		Account        dashboardActor `json:"account"`
		Authentication string         `json:"authentication"`
	}
	if err := json.Unmarshal(payload, &identity); err != nil {
		t.Fatal(err)
	}
	if identity.Account.Email != user.Email || identity.Authentication != "personalAccessToken" {
		t.Fatalf("unexpected token identity: %+v", identity)
	}

	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/projects", createdToken.AccessToken, `{"name":"CLI-created project","databaseMode":"single"}`)
	if status != http.StatusCreated {
		t.Fatalf("create project with account token: got %d: %s", status, payload)
	}
	var createdProject createProjectResponse
	if err := json.Unmarshal(payload, &createdProject); err != nil {
		t.Fatal(err)
	}
	if createdProject.Project.ID == "" || createdProject.ProjectKey == "" || createdProject.Project.OwnerEmail != user.Email {
		t.Fatalf("incomplete or incorrectly owned project: %+v", createdProject)
	}
	t.Cleanup(func() {
		if err := dropProjectDatabase(context.Background(), baseURL, createdProject.Project.Database); err != nil {
			t.Errorf("drop account-token-created project database: %v", err)
		}
	})

	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodGet, runtime.URL+"/dev/projects", createdToken.AccessToken, "")
	if status != http.StatusOK || !bytes.Contains(payload, []byte(createdProject.Project.ID)) {
		t.Fatalf("list projects with account token: got %d: %s", status, payload)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/projects/"+createdProject.Project.ID+"/key", createdToken.AccessToken, "")
	if status != http.StatusOK || !bytes.Contains(payload, []byte(createdProject.ProjectKey)) {
		t.Fatalf("read project key with account token: got %d", status)
	}

	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/auth/login", "", `{"email":"runtime-admin@example.test","password":"admin horse battery staple"}`)
	if status != http.StatusOK {
		t.Fatalf("admin login: got %d: %s", status, payload)
	}
	var adminLogin struct {
		Session dashboardSession `json:"session"`
	}
	if err := json.Unmarshal(payload, &adminLogin); err != nil || adminLogin.Session.AccessToken == "" {
		t.Fatalf("decode admin login: %v", err)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/auth/tokens", adminLogin.Session.AccessToken, `{"name":"global automation","permissions":["projects:*","admin:projects"]}`)
	if status != http.StatusCreated {
		t.Fatalf("create global admin token: got %d: %s", status, payload)
	}
	var adminToken struct {
		Token       accountAccessToken `json:"token"`
		AccessToken string             `json:"accessToken"`
	}
	if err := json.Unmarshal(payload, &adminToken); err != nil || adminToken.AccessToken == "" {
		t.Fatalf("decode global admin token: %v", err)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodGet, runtime.URL+"/dev/projects", adminToken.AccessToken, "")
	if status != http.StatusOK || !bytes.Contains(payload, []byte(createdProject.Project.ID)) {
		t.Fatalf("global token could not list another user's project: got %d: %s", status, payload)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodPost, runtime.URL+"/dev/projects/"+createdProject.Project.ID+"/key", adminToken.AccessToken, "")
	if status != http.StatusOK || !bytes.Contains(payload, []byte(createdProject.ProjectKey)) {
		t.Fatalf("global token could not manage another user's project: got %d", status)
	}

	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodDelete, runtime.URL+"/dev/auth/tokens/"+createdToken.Token.ID, login.Session.AccessToken, "")
	if status != http.StatusOK {
		t.Fatalf("revoke token: got %d: %s", status, payload)
	}
	status, payload = accountTokenRequest(t, runtime.Client(), http.MethodDelete, runtime.URL+"/dev/auth/tokens/"+adminToken.Token.ID, adminLogin.Session.AccessToken, "")
	if status != http.StatusOK {
		t.Fatalf("revoke global admin token: got %d: %s", status, payload)
	}
	status, _ = accountTokenRequest(t, runtime.Client(), http.MethodGet, runtime.URL+"/dev/auth/me", createdToken.AccessToken, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("revoked token identity: got %d, want %d", status, http.StatusUnauthorized)
	}
}
