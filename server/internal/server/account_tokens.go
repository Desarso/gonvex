package server

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	permissionProjectsRead         = "projects:read"
	permissionProjectsCreate       = "projects:create"
	permissionProjectsUpdate       = "projects:update"
	permissionProjectsDelete       = "projects:delete"
	permissionProjectsKeysRead     = "projects:keys:read"
	permissionProjectsKeysWrite    = "projects:keys:write"
	permissionProjectsMembersRead  = "projects:members:read"
	permissionProjectsMembersWrite = "projects:members:write"
	permissionProjectsEnvRead      = "projects:env:read"
	permissionProjectsEnvWrite     = "projects:env:write"
	permissionAdminProjects        = "admin:projects"
	permissionTokensRead           = "tokens:read"
	permissionTokensCreate         = "tokens:create"
	permissionTokensRevoke         = "tokens:revoke"
)

var accountTokenPermissions = map[string]struct{}{
	"*":                            {},
	"projects:*":                   {},
	permissionProjectsRead:         {},
	permissionProjectsCreate:       {},
	permissionProjectsUpdate:       {},
	permissionProjectsDelete:       {},
	permissionProjectsKeysRead:     {},
	permissionProjectsKeysWrite:    {},
	permissionProjectsMembersRead:  {},
	permissionProjectsMembersWrite: {},
	permissionProjectsEnvRead:      {},
	permissionProjectsEnvWrite:     {},
	permissionAdminProjects:        {},
	"tokens:*":                     {},
	permissionTokensRead:           {},
	permissionTokensCreate:         {},
	permissionTokensRevoke:         {},
}

var defaultCLIAccountTokenPermissions = []string{
	permissionProjectsRead,
	permissionProjectsCreate,
	permissionProjectsKeysRead,
}

type accountAccessToken struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Prefix      string     `json:"prefix"`
	Permissions []string   `json:"permissions"`
	CreatedAt   time.Time  `json:"createdAt"`
	ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
	LastUsedAt  *time.Time `json:"lastUsedAt,omitempty"`
	RevokedAt   *time.Time `json:"revokedAt,omitempty"`
}

func normalizeAccountTokenPermissions(values []string) ([]string, error) {
	if len(values) == 0 {
		values = defaultCLIAccountTokenPermissions
	}
	seen := map[string]bool{}
	permissions := make([]string, 0, len(values))
	for _, value := range values {
		permission := strings.ToLower(strings.TrimSpace(value))
		if permission == "" || seen[permission] {
			continue
		}
		if _, ok := accountTokenPermissions[permission]; !ok {
			return nil, fmt.Errorf("unknown account token permission %q", value)
		}
		seen[permission] = true
		permissions = append(permissions, permission)
	}
	if len(permissions) == 0 {
		return nil, fmt.Errorf("at least one account token permission is required")
	}
	sort.Strings(permissions)
	return permissions, nil
}

func (actor dashboardActor) hasAccountPermission(permission string) bool {
	if actor.credentialKind != "personalAccessToken" {
		return true
	}
	for _, granted := range actor.tokenPermissions {
		if granted == "*" || granted == permission {
			return true
		}
		if strings.HasSuffix(granted, ":*") && strings.HasPrefix(permission, strings.TrimSuffix(granted, "*")) {
			return true
		}
	}
	return false
}

func (actor dashboardActor) canGrantAccountPermission(permission string) bool {
	// Global project administration is deliberately separate from projects:*
	// so a project-scoped token can never become a runtime-wide credential.
	// Only a dashboard administrator (or an already-global admin token) may
	// mint credentials carrying this capability.
	if permission == permissionAdminProjects || permission == "*" {
		if actor.Role != "admin" {
			return false
		}
		if actor.credentialKind == "personalAccessToken" && !actor.hasGlobalProjectAccess() {
			return false
		}
	}
	if actor.credentialKind != "personalAccessToken" {
		return true
	}
	if permission == "*" || strings.HasSuffix(permission, ":*") {
		for _, granted := range actor.tokenPermissions {
			if granted == "*" || granted == permission {
				return true
			}
		}
		return false
	}
	return actor.hasAccountPermission(permission)
}

func (actor dashboardActor) hasGlobalProjectAccess() bool {
	if actor.Role != "admin" {
		return false
	}
	if actor.credentialKind == "adminKey" {
		return true
	}
	if actor.credentialKind != "personalAccessToken" {
		return false
	}
	for _, permission := range actor.tokenPermissions {
		if permission == permissionAdminProjects || permission == "*" {
			return true
		}
	}
	return false
}

func (s *Server) accountActorFromRequest(r *http.Request) (dashboardActor, bool) {
	token := bearerToken(r)
	if actor, ok := s.verifyDashboardToken(token); ok {
		actor.credentialKind = "session"
		return actor, true
	}
	if actor, ok := s.dashboardActorFromNativeSession(r.Context(), token); ok {
		return actor, true
	}
	if s.acceptsAdminKey(token) {
		return dashboardActor{Email: "admin@gonvex.local", Name: "Gonvex Admin", Role: "admin", credentialKind: "adminKey"}, true
	}
	if strings.HasPrefix(token, "gvx_pat_") {
		if actor, ok := s.verifyAccountAccessToken(r.Context(), token); ok {
			return actor, true
		}
	}
	if s.dashboardAuthOptional() {
		return dashboardActor{Email: "local@gonvex.dev", Name: "Local Developer", Role: "admin", credentialKind: "local"}, true
	}
	return dashboardActor{}, false
}

func (s *Server) authorizeAccountRequest(w http.ResponseWriter, r *http.Request, permission string) (dashboardActor, bool) {
	actor, ok := s.accountActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "account sign-in or personal access token is required"})
		return dashboardActor{}, false
	}
	if permission != "" && !actor.hasAccountPermission(permission) {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error":      "personal access token does not grant the required permission",
			"permission": permission,
		})
		return dashboardActor{}, false
	}
	return actor, true
}

func bearerToken(r *http.Request) string {
	token := strings.TrimSpace(r.Header.Get("authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return strings.TrimSpace(token[len("Bearer "):])
	}
	return token
}

func generateAccountAccessToken() (id string, accessToken string, err error) {
	id, err = randomID("pat")
	if err != nil {
		return "", "", err
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", "", fmt.Errorf("generate personal access token: %w", err)
	}
	return id, "gvx_" + id + "." + base64.RawURLEncoding.EncodeToString(secret[:]), nil
}

func accountAccessTokenID(accessToken string) string {
	accessToken = strings.TrimSpace(accessToken)
	left, _, ok := strings.Cut(accessToken, ".")
	if !ok || !strings.HasPrefix(left, "gvx_pat_") {
		return ""
	}
	id := strings.TrimPrefix(left, "gvx_")
	if !strings.HasPrefix(id, "pat_") || len(id) <= len("pat_") {
		return ""
	}
	return id
}

func (s *Server) verifyAccountAccessToken(ctx context.Context, accessToken string) (dashboardActor, bool) {
	id := accountAccessTokenID(accessToken)
	if id == "" {
		return dashboardActor{}, false
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return dashboardActor{}, false
	}
	defer db.Close()

	var ownerEmail, tokenHash string
	var permissionsJSON []byte
	var expiresAt, revokedAt sql.NullTime
	err = db.QueryRowContext(ctx, `SELECT owner_email, token_hash, permissions, expires_at, revoked_at
		FROM gonvex_account_access_tokens WHERE id = $1`, id).Scan(
		&ownerEmail, &tokenHash, &permissionsJSON, &expiresAt, &revokedAt,
	)
	if err != nil || revokedAt.Valid || (expiresAt.Valid && !expiresAt.Time.After(time.Now())) || !constantTimeString(sha256Hex(accessToken), tokenHash) {
		return dashboardActor{}, false
	}
	var permissions []string
	if err := json.Unmarshal(permissionsJSON, &permissions); err != nil {
		return dashboardActor{}, false
	}
	actor, ok := s.accountActorForEmail(ctx, db, ownerEmail)
	if !ok {
		return dashboardActor{}, false
	}
	actor.credentialKind = "personalAccessToken"
	actor.tokenID = id
	actor.tokenPermissions = permissions
	_, _ = db.ExecContext(ctx, `UPDATE gonvex_account_access_tokens SET last_used_at = now() WHERE id = $1`, id)
	return actor, true
}

func (s *Server) accountActorForEmail(ctx context.Context, db *sql.DB, email string) (dashboardActor, bool) {
	email = normalizeDashboardEmail(email)
	var actor dashboardActor
	err := db.QueryRowContext(ctx, `SELECT email, name, role FROM gonvex_dashboard_users WHERE email = $1`, email).Scan(
		&actor.Email, &actor.Name, &actor.Role,
	)
	if err == nil {
		return actor, true
	}
	if err != sql.ErrNoRows {
		return dashboardActor{}, false
	}
	bootstrapEmail := normalizeDashboardEmail(s.configDashboardUser())
	if bootstrapEmail != "" && email == bootstrapEmail {
		return dashboardActor{Email: email, Name: displayNameFromEmail(email), Role: "admin"}, true
	}
	if s.dashboardAuthOptional() && email == "local@gonvex.dev" {
		return dashboardActor{Email: email, Name: "Local Developer", Role: "admin"}, true
	}
	return dashboardActor{}, false
}

func (s *Server) createAccountAccessToken(ctx context.Context, actor dashboardActor, name string, permissions []string, expiresAt *time.Time) (accountAccessToken, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return accountAccessToken{}, "", fmt.Errorf("token name is required")
	}
	if len(name) > 120 {
		return accountAccessToken{}, "", fmt.Errorf("token name must be 120 characters or fewer")
	}
	permissions, err := normalizeAccountTokenPermissions(permissions)
	if err != nil {
		return accountAccessToken{}, "", err
	}
	for _, permission := range permissions {
		if !actor.canGrantAccountPermission(permission) {
			return accountAccessToken{}, "", fmt.Errorf("current credential cannot grant permission %q", permission)
		}
	}
	if expiresAt != nil {
		value := expiresAt.UTC()
		if !value.After(time.Now()) {
			return accountAccessToken{}, "", fmt.Errorf("token expiration must be in the future")
		}
		expiresAt = &value
	}
	id, accessToken, err := generateAccountAccessToken()
	if err != nil {
		return accountAccessToken{}, "", err
	}
	permissionsJSON, err := json.Marshal(permissions)
	if err != nil {
		return accountAccessToken{}, "", err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil {
		return accountAccessToken{}, "", err
	}
	if db == nil {
		return accountAccessToken{}, "", fmt.Errorf("account token store is unavailable")
	}
	defer db.Close()
	// Password-backed users already have this row. Runtime bootstrap or future
	// external-provider sessions may not, so register an unusable-password
	// account record before issuing a durable credential. Deleting that account
	// row later invalidates all of its personal access tokens.
	ownerName := strings.TrimSpace(actor.Name)
	if ownerName == "" {
		ownerName = displayNameFromEmail(actor.Email)
	}
	ownerRole := normalizedDashboardRole(actor.Role)
	if ownerRole == "" {
		ownerRole = "user"
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO gonvex_dashboard_users (email, name, role, password_hash)
		VALUES ($1, $2, $3, '!external-provider')
		ON CONFLICT (email) DO NOTHING`, normalizeDashboardEmail(actor.Email), ownerName, ownerRole); err != nil {
		return accountAccessToken{}, "", err
	}
	createdAt := time.Now().UTC()
	prefix := "gvx_" + id
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_account_access_tokens (
		id, owner_email, name, token_prefix, token_hash, permissions, expires_at, created_at
	) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)`,
		id, normalizeDashboardEmail(actor.Email), name, prefix, sha256Hex(accessToken), string(permissionsJSON), expiresAt, createdAt)
	if err != nil {
		return accountAccessToken{}, "", err
	}
	return accountAccessToken{
		ID: id, Name: name, Prefix: prefix, Permissions: permissions, CreatedAt: createdAt, ExpiresAt: expiresAt,
	}, accessToken, nil
}

func (s *Server) listAccountAccessTokens(ctx context.Context, ownerEmail string) ([]accountAccessToken, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("account token store is unavailable")
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT id, name, token_prefix, permissions, created_at, expires_at, last_used_at, revoked_at
		FROM gonvex_account_access_tokens WHERE owner_email = $1 ORDER BY created_at DESC`, normalizeDashboardEmail(ownerEmail))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tokens := []accountAccessToken{}
	for rows.Next() {
		var token accountAccessToken
		var permissionsJSON []byte
		if err := rows.Scan(&token.ID, &token.Name, &token.Prefix, &permissionsJSON, &token.CreatedAt, &token.ExpiresAt, &token.LastUsedAt, &token.RevokedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(permissionsJSON, &token.Permissions); err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
	return tokens, rows.Err()
}

func (s *Server) handleAccountIdentity(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.authorizeAccountRequest(w, r, "")
	if !ok {
		return
	}
	permissions := []string{"*"}
	if actor.credentialKind == "personalAccessToken" {
		permissions = append([]string(nil), actor.tokenPermissions...)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"account":        map[string]any{"email": actor.Email, "name": actor.Name, "role": actor.Role},
		"authentication": actor.credentialKind,
		"permissions":    permissions,
	})
}

func (s *Server) handleAccountTokens(w http.ResponseWriter, r *http.Request) {
	permission := permissionTokensRead
	if r.Method == http.MethodPost {
		permission = permissionTokensCreate
	}
	actor, ok := s.authorizeAccountRequest(w, r, permission)
	if !ok {
		return
	}
	if r.Method == http.MethodGet {
		tokens, err := s.listAccountAccessTokens(r.Context(), actor.Email)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"tokens": tokens})
		return
	}

	defer r.Body.Close()
	var payload struct {
		Name        string   `json:"name"`
		Permissions []string `json:"permissions"`
		ExpiresAt   string   `json:"expiresAt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid token request"})
		return
	}
	var expiresAt *time.Time
	if value := strings.TrimSpace(payload.ExpiresAt); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expiresAt must be an RFC3339 timestamp"})
			return
		}
		expiresAt = &parsed
	}
	token, accessToken, err := s.createAccountAccessToken(r.Context(), actor, payload.Name, payload.Permissions, expiresAt)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"token": token, "accessToken": accessToken})
}

func (s *Server) handleRevokeAccountToken(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.authorizeAccountRequest(w, r, permissionTokensRevoke)
	if !ok {
		return
	}
	id := strings.TrimSpace(r.PathValue("token"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "token id is required"})
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "account token store is unavailable"})
		return
	}
	defer db.Close()
	result, err := db.ExecContext(r.Context(), `UPDATE gonvex_account_access_tokens SET revoked_at = now()
		WHERE id = $1 AND owner_email = $2 AND revoked_at IS NULL`, id, normalizeDashboardEmail(actor.Email))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "active account token not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
