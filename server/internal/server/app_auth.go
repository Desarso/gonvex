package server

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	googleProvider           = "google"
	authTransactionTTL       = 10 * time.Minute
	authCodeTTL              = 5 * time.Minute
	appSessionTTL            = 7 * 24 * time.Hour
	defaultGoogleKeyCacheTTL = time.Hour
)

var pkceValuePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43,128}$`)

type appAuthUser struct {
	ID             string    `json:"id"`
	Email          string    `json:"email,omitempty"`
	EmailVerified  bool      `json:"emailVerified"`
	Name           string    `json:"name,omitempty"`
	Picture        string    `json:"picture,omitempty"`
	Provider       string    `json:"provider"`
	CreatedAt      time.Time `json:"createdAt,omitempty"`
	LastSignedInAt time.Time `json:"lastSignedInAt,omitempty"`
}

type authTransaction struct {
	ProjectID         string
	RedirectURI       string
	AppState          string
	CodeChallenge     string
	Nonce             string
	GoogleRedirectURI string
}

type googleIdentity struct {
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
	Picture       string
}

type googleKeyCache struct {
	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	expiresAt time.Time
}

// handleProjectGoogleAuth is the project-owner control-plane endpoint used by
// `gonvex auth add google`. The Google client secret belongs to the Gonvex
// installation, never to an individual app project.
func (s *Server) handleProjectGoogleAuth(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	manage := r.Method != http.MethodGet
	if !s.authorizeProjectAuthRequest(w, r, projectID, manage) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		redirectURIs, enabled, err := s.googleAuthConfiguration(r.Context(), projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"provider":          googleProvider,
			"enabled":           enabled,
			"redirectUris":      redirectURIs,
			"runtimeConfigured": s.googleAuthBrokerReady(),
			"brokerCallbackUrl": s.configuredGoogleCallbackURL(),
		})
	case http.MethodPut:
		if err := s.requireSingleDatabaseProject(r.Context(), projectID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		defer r.Body.Close()
		var payload struct {
			RedirectURI string `json:"redirectUri"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid Google auth configuration"})
			return
		}
		redirectURI, err := normalizeAppRedirectURI(payload.RedirectURI)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := s.enableGoogleAuth(r.Context(), projectID, redirectURI); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		redirectURIs, _, err := s.googleAuthConfiguration(r.Context(), projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"provider":          googleProvider,
			"enabled":           true,
			"redirectUris":      redirectURIs,
			"runtimeConfigured": s.googleAuthBrokerReady(),
			"brokerCallbackUrl": s.configuredGoogleCallbackURL(),
		})
	case http.MethodDelete:
		if err := s.disableGoogleAuth(r.Context(), projectID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"provider": googleProvider, "enabled": false})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleProjectAuthUsers(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, false) {
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "auth account store is unavailable"})
		return
	}
	defer db.Close()
	rows, err := db.QueryContext(r.Context(), `SELECT id, email, email_verified, name, picture, provider, created_at, last_signed_in_at
		FROM gonvex_auth_users WHERE project_id = $1 ORDER BY created_at DESC`, projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()
	users := []appAuthUser{}
	for rows.Next() {
		var user appAuthUser
		if err := rows.Scan(&user.ID, &user.Email, &user.EmailVerified, &user.Name, &user.Picture, &user.Provider, &user.CreatedAt, &user.LastSignedInAt); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

func (s *Server) authorizeProjectAuthRequest(w http.ResponseWriter, r *http.Request, projectID string, manage bool) bool {
	if s.acceptsProjectEnvKey(projectID, syncKey(r)) {
		return true
	}
	actor, ok := s.projectEnvDashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in or project key is required"})
		return false
	}
	permission := permissionProjectsRead
	if manage {
		permission = permissionProjectsUpdate
	}
	if !actor.hasAccountPermission(permission) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "personal access token does not grant the required permission", "permission": permission})
		return false
	}
	if manage && !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return false
	}
	if !manage && !s.canAccessProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project access is required"})
		return false
	}
	return true
}

func (s *Server) requireSingleDatabaseProject(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project registry is unavailable")
	}
	defer db.Close()
	var mode string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(database_mode, ''), 'single') FROM gonvex_runtime_projects WHERE id = $1`, projectID).Scan(&mode); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("project %q was not found", projectID)
		}
		return err
	}
	if normalizedDatabaseModeWithDefault(mode) != "single" {
		return fmt.Errorf("Google auth currently requires a single-database project; tenant membership setup is required for multi-tenant projects")
	}
	return nil
}

func normalizeAppRedirectURI(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" {
		return "", fmt.Errorf("redirect URI must be an absolute http or https URL")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return "", fmt.Errorf("redirect URI cannot contain user info or a fragment")
	}
	hostname := strings.ToLower(parsed.Hostname())
	local := hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && local) {
		return "", fmt.Errorf("redirect URI must use https (http is allowed only for localhost)")
	}
	return parsed.String(), nil
}

func (s *Server) enableGoogleAuth(ctx context.Context, projectID string, redirectURI string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_auth_providers (project_id, provider, enabled, updated_at)
		VALUES ($1, $2, TRUE, now())
		ON CONFLICT (project_id, provider) DO UPDATE SET enabled = TRUE, updated_at = now()`, projectID, googleProvider); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_auth_redirect_uris (project_id, provider, redirect_uri)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, projectID, googleProvider, redirectURI); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Server) disableGoogleAuth(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `UPDATE gonvex_auth_providers SET enabled = FALSE, updated_at = now()
		WHERE project_id = $1 AND provider = $2`, projectID, googleProvider)
	return err
}

func (s *Server) googleAuthConfiguration(ctx context.Context, projectID string) ([]string, bool, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, false, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var enabled bool
	err = db.QueryRowContext(ctx, `SELECT enabled FROM gonvex_auth_providers WHERE project_id = $1 AND provider = $2`, projectID, googleProvider).Scan(&enabled)
	if err != nil && err != sql.ErrNoRows {
		return nil, false, err
	}
	rows, err := db.QueryContext(ctx, `SELECT redirect_uri FROM gonvex_auth_redirect_uris
		WHERE project_id = $1 AND provider = $2 ORDER BY redirect_uri`, projectID, googleProvider)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	redirectURIs := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, false, err
		}
		redirectURIs = append(redirectURIs, value)
	}
	return redirectURIs, enabled, rows.Err()
}

func (s *Server) googleRedirectAllowed(ctx context.Context, projectID string, redirectURI string) (bool, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return false, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var allowed bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM gonvex_auth_providers p
		JOIN gonvex_auth_redirect_uris r ON r.project_id = p.project_id AND r.provider = p.provider
		WHERE p.project_id = $1 AND p.provider = $2 AND p.enabled = TRUE AND r.redirect_uri = $3
	)`, projectID, googleProvider, redirectURI).Scan(&allowed)
	return allowed, err
}

func (s *Server) handleAuthConfig(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.URL.Query().Get("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project is required"})
		return
	}
	_, enabled, err := s.googleAuthConfiguration(r.Context(), projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "auth configuration is unavailable"})
		return
	}
	providers := []string{}
	if enabled && s.googleAuthBrokerReady() {
		providers = append(providers, googleProvider)
	}
	writeJSON(w, http.StatusOK, map[string]any{"project": projectID, "providers": providers})
}

func (s *Server) googleAuthBrokerReady() bool {
	return strings.TrimSpace(s.config.GoogleClientID) != "" && strings.TrimSpace(s.config.GoogleClientSecret) != ""
}

func (s *Server) configuredGoogleCallbackURL() string {
	base := strings.TrimRight(strings.TrimSpace(s.config.AuthPublicURL), "/")
	if base == "" {
		return ""
	}
	return base + "/auth/google/callback"
}

func (s *Server) googleCallbackURL(r *http.Request) (string, error) {
	if configured := s.configuredGoogleCallbackURL(); configured != "" {
		return configured, nil
	}
	if r.Host == "" {
		return "", fmt.Errorf("GONVEX_AUTH_URL is required")
	}
	scheme := strings.TrimSpace(r.Header.Get("x-forwarded-proto"))
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	return scheme + "://" + r.Host + "/auth/google/callback", nil
}

func (s *Server) handleGoogleAuthorize(w http.ResponseWriter, r *http.Request) {
	if !s.googleAuthBrokerReady() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Google auth is not configured on this Gonvex runtime"})
		return
	}
	query := r.URL.Query()
	projectID := strings.TrimSpace(query.Get("project"))
	redirectURI, err := normalizeAppRedirectURI(query.Get("redirect_uri"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	state := strings.TrimSpace(query.Get("state"))
	challenge := strings.TrimSpace(query.Get("code_challenge"))
	if projectID == "" || len(state) < 16 || len(state) > 512 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project and a valid state are required"})
		return
	}
	if query.Get("code_challenge_method") != "S256" || !pkceValuePattern.MatchString(challenge) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "PKCE with a valid S256 code challenge is required"})
		return
	}
	allowed, err := s.googleRedirectAllowed(r.Context(), projectID, redirectURI)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "auth configuration is unavailable"})
		return
	}
	if !allowed {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "redirect URI is not registered for this project"})
		return
	}
	callbackURL, err := s.googleCallbackURL(r)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	transactionToken, err := randomID("oauth")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not start auth flow"})
		return
	}
	nonce, err := randomID("nonce")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not start auth flow"})
		return
	}
	if err := s.saveAuthTransaction(r.Context(), transactionToken, authTransaction{
		ProjectID: projectID, RedirectURI: redirectURI, AppState: state,
		CodeChallenge: challenge, Nonce: nonce, GoogleRedirectURI: callbackURL,
	}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not start auth flow"})
		return
	}
	authorizeURL, err := url.Parse(s.config.GoogleAuthorizeURL)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Google authorize URL is invalid"})
		return
	}
	params := authorizeURL.Query()
	params.Set("client_id", s.config.GoogleClientID)
	params.Set("redirect_uri", callbackURL)
	params.Set("response_type", "code")
	params.Set("scope", "openid email profile")
	params.Set("state", transactionToken)
	params.Set("nonce", nonce)
	authorizeURL.RawQuery = params.Encode()
	http.Redirect(w, r, authorizeURL.String(), http.StatusFound)
}

func (s *Server) saveAuthTransaction(ctx context.Context, token string, transaction authTransaction) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_transactions WHERE expires_at <= now()`)
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_transactions (
		token_hash, project_id, redirect_uri, app_state, code_challenge, nonce, google_redirect_uri, expires_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		sha256Hex(token), transaction.ProjectID, transaction.RedirectURI, transaction.AppState,
		transaction.CodeChallenge, transaction.Nonce, transaction.GoogleRedirectURI, time.Now().Add(authTransactionTTL))
	return err
}

func (s *Server) consumeAuthTransaction(ctx context.Context, token string) (authTransaction, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return authTransaction{}, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var transaction authTransaction
	err = db.QueryRowContext(ctx, `DELETE FROM gonvex_auth_transactions
		WHERE token_hash = $1 AND expires_at > now()
		RETURNING project_id, redirect_uri, app_state, code_challenge, nonce, google_redirect_uri`, sha256Hex(token)).Scan(
		&transaction.ProjectID, &transaction.RedirectURI, &transaction.AppState,
		&transaction.CodeChallenge, &transaction.Nonce, &transaction.GoogleRedirectURI,
	)
	if err == sql.ErrNoRows {
		return authTransaction{}, fmt.Errorf("invalid or expired OAuth state")
	}
	return transaction, err
}

func (s *Server) handleGoogleCallback(w http.ResponseWriter, r *http.Request) {
	transaction, err := s.consumeAuthTransaction(r.Context(), strings.TrimSpace(r.URL.Query().Get("state")))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if providerError := strings.TrimSpace(r.URL.Query().Get("error")); providerError != "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": providerError, "state": transaction.AppState})
		return
	}
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if code == "" {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "missing_google_code", "state": transaction.AppState})
		return
	}
	idToken, err := s.exchangeGoogleCode(r.Context(), code, transaction.GoogleRedirectURI)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "google_exchange_failed", "state": transaction.AppState})
		return
	}
	identity, err := s.verifyGoogleIDToken(r.Context(), idToken, transaction.Nonce)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "invalid_google_identity", "state": transaction.AppState})
		return
	}
	user, err := s.upsertAppAuthUser(r.Context(), transaction.ProjectID, identity)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "account_creation_failed", "state": transaction.AppState})
		return
	}
	authCode, err := s.createAppAuthCode(r.Context(), transaction, user.ID)
	if err != nil {
		redirectToApp(w, r, transaction.RedirectURI, map[string]string{"error": "code_creation_failed", "state": transaction.AppState})
		return
	}
	redirectToApp(w, r, transaction.RedirectURI, map[string]string{"code": authCode, "state": transaction.AppState})
}

func redirectToApp(w http.ResponseWriter, r *http.Request, destination string, values map[string]string) {
	target, err := url.Parse(destination)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "invalid app redirect"})
		return
	}
	query := target.Query()
	for key, value := range values {
		query.Set(key, value)
	}
	target.RawQuery = query.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func (s *Server) exchangeGoogleCode(ctx context.Context, code string, redirectURI string) (string, error) {
	form := url.Values{
		"code":          {code},
		"client_id":     {s.config.GoogleClientID},
		"client_secret": {s.config.GoogleClientSecret},
		"redirect_uri":  {redirectURI},
		"grant_type":    {"authorization_code"},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.GoogleTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("content-type", "application/x-www-form-urlencoded")
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	var payload struct {
		IDToken string `json:"id_token"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || payload.IDToken == "" {
		return "", fmt.Errorf("Google token exchange returned %d (%s)", response.StatusCode, payload.Error)
	}
	return payload.IDToken, nil
}

func (s *Server) upsertAppAuthUser(ctx context.Context, projectID string, identity googleIdentity) (appAuthUser, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthUser{}, fmt.Errorf("auth account store is unavailable")
	}
	defer db.Close()
	userID, err := randomID("user")
	if err != nil {
		return appAuthUser{}, err
	}
	var user appAuthUser
	err = db.QueryRowContext(ctx, `INSERT INTO gonvex_auth_users (
		id, project_id, provider, provider_subject, email, email_verified, name, picture, last_signed_in_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
	ON CONFLICT (project_id, provider, provider_subject) DO UPDATE SET
		email = EXCLUDED.email,
		email_verified = EXCLUDED.email_verified,
		name = EXCLUDED.name,
		picture = EXCLUDED.picture,
		last_signed_in_at = now(),
		updated_at = now()
	RETURNING id, email, email_verified, name, picture, provider, created_at, last_signed_in_at`,
		userID, projectID, googleProvider, identity.Subject, identity.Email, identity.EmailVerified, identity.Name, identity.Picture).Scan(
		&user.ID, &user.Email, &user.EmailVerified, &user.Name, &user.Picture, &user.Provider, &user.CreatedAt, &user.LastSignedInAt,
	)
	return user, err
}

func (s *Server) createAppAuthCode(ctx context.Context, transaction authTransaction, userID string) (string, error) {
	code, err := randomID("authcode")
	if err != nil {
		return "", err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return "", fmt.Errorf("auth code store is unavailable")
	}
	defer db.Close()
	_, _ = db.ExecContext(ctx, `DELETE FROM gonvex_auth_codes WHERE expires_at <= now() OR used_at IS NOT NULL`)
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_codes (
		code_hash, project_id, user_id, redirect_uri, code_challenge, expires_at
	) VALUES ($1, $2, $3, $4, $5, $6)`, sha256Hex(code), transaction.ProjectID, userID,
		transaction.RedirectURI, transaction.CodeChallenge, time.Now().Add(authCodeTTL))
	return code, err
}

func (s *Server) handleAppAuthToken(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	defer r.Body.Close()
	var payload struct {
		GrantType    string `json:"grantType"`
		Project      string `json:"project"`
		Code         string `json:"code"`
		CodeVerifier string `json:"codeVerifier"`
		RedirectURI  string `json:"redirectUri"`
	}
	if strings.HasPrefix(strings.ToLower(r.Header.Get("content-type")), "application/json") {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid token request"})
			return
		}
	} else {
		if err := r.ParseForm(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid token request"})
			return
		}
		payload.GrantType = r.Form.Get("grant_type")
		payload.Project = r.Form.Get("project")
		payload.Code = r.Form.Get("code")
		payload.CodeVerifier = r.Form.Get("code_verifier")
		payload.RedirectURI = r.Form.Get("redirect_uri")
	}
	if payload.GrantType != "authorization_code" || strings.TrimSpace(payload.Project) == "" || strings.TrimSpace(payload.Code) == "" || !pkceValuePattern.MatchString(payload.CodeVerifier) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "authorization code, project, redirect URI, and PKCE verifier are required"})
		return
	}
	redirectURI, err := normalizeAppRedirectURI(payload.RedirectURI)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	accessToken, expiresAt, user, err := s.exchangeAppAuthCode(r.Context(), strings.TrimSpace(payload.Project), strings.TrimSpace(payload.Code), payload.CodeVerifier, redirectURI)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accessToken": accessToken,
		"tokenType":   "Bearer",
		"expiresIn":   int(appSessionTTL.Seconds()),
		"expiresAt":   expiresAt.UnixMilli(),
		"user":        user,
	})
}

func (s *Server) exchangeAppAuthCode(ctx context.Context, projectID string, code string, verifier string, redirectURI string) (string, time.Time, appAuthUser, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return "", time.Time{}, appAuthUser{}, fmt.Errorf("auth session store is unavailable")
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	defer tx.Rollback()
	var storedProject, userID, storedRedirect, challenge string
	err = tx.QueryRowContext(ctx, `SELECT project_id, user_id, redirect_uri, code_challenge
		FROM gonvex_auth_codes WHERE code_hash = $1 AND used_at IS NULL AND expires_at > now() FOR UPDATE`, sha256Hex(code)).Scan(
		&storedProject, &userID, &storedRedirect, &challenge,
	)
	if err == sql.ErrNoRows {
		return "", time.Time{}, appAuthUser{}, fmt.Errorf("invalid or expired authorization code")
	}
	if err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	if storedProject != projectID || storedRedirect != redirectURI || !constantTimeString(challenge, pkceChallenge(verifier)) {
		return "", time.Time{}, appAuthUser{}, fmt.Errorf("authorization code does not match this client")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE gonvex_auth_codes SET used_at = now() WHERE code_hash = $1`, sha256Hex(code)); err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	sessionID, err := randomID("session")
	if err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	accessToken := "gvx_" + sessionID + "." + base64.RawURLEncoding.EncodeToString(secret[:])
	expiresAt := time.Now().Add(appSessionTTL).UTC()
	if _, err := tx.ExecContext(ctx, `INSERT INTO gonvex_auth_sessions (token_hash, project_id, user_id, expires_at)
		VALUES ($1, $2, $3, $4)`, sha256Hex(accessToken), projectID, userID, expiresAt); err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	var user appAuthUser
	if err := tx.QueryRowContext(ctx, `SELECT id, email, email_verified, name, picture, provider, created_at, last_signed_in_at
		FROM gonvex_auth_users WHERE id = $1 AND project_id = $2`, userID, projectID).Scan(
		&user.ID, &user.Email, &user.EmailVerified, &user.Name, &user.Picture, &user.Provider, &user.CreatedAt, &user.LastSignedInAt,
	); err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	if err := tx.Commit(); err != nil {
		return "", time.Time{}, appAuthUser{}, err
	}
	return accessToken, expiresAt, user, nil
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (s *Server) handleAppAuthLogout(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token != "" {
		db, err := s.openProjectRegistry(r.Context())
		if err == nil && db != nil {
			_, _ = db.ExecContext(r.Context(), `UPDATE gonvex_auth_sessions SET revoked_at = now()
				WHERE token_hash = $1 AND revoked_at IS NULL`, sha256Hex(token))
			db.Close()
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type validatedAppSession struct {
	ProjectID string
	User      appAuthUser
}

func (s *Server) validateAppSession(ctx context.Context, requestedProjectID string, token string, requestedTenantID string) (validatedAppSession, string, error) {
	if !strings.HasPrefix(strings.TrimSpace(token), "gvx_session_") {
		return validatedAppSession{}, "", fmt.Errorf("not a Gonvex app session")
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return validatedAppSession{}, "", fmt.Errorf("auth session store is unavailable")
	}
	defer db.Close()
	var session validatedAppSession
	err = db.QueryRowContext(ctx, `SELECT s.project_id, u.id, u.email, u.email_verified, u.name, u.picture, u.provider, u.created_at, u.last_signed_in_at
		FROM gonvex_auth_sessions s JOIN gonvex_auth_users u ON u.id = s.user_id AND u.project_id = s.project_id
		WHERE s.token_hash = $1 AND s.revoked_at IS NULL AND s.expires_at > now()`, sha256Hex(token)).Scan(
		&session.ProjectID, &session.User.ID, &session.User.Email, &session.User.EmailVerified, &session.User.Name,
		&session.User.Picture, &session.User.Provider, &session.User.CreatedAt, &session.User.LastSignedInAt,
	)
	if err == sql.ErrNoRows {
		return validatedAppSession{}, "", fmt.Errorf("invalid or expired app session")
	}
	if err != nil {
		return validatedAppSession{}, "", err
	}
	if requestedProjectID != "" && requestedProjectID != session.ProjectID {
		return validatedAppSession{}, "", fmt.Errorf("app session was issued for a different project")
	}
	tenantID := strings.TrimSpace(requestedTenantID)
	if tenantID == "" {
		tenantID = session.ProjectID
	}
	if tenantID != session.ProjectID {
		return validatedAppSession{}, "", fmt.Errorf("app session does not grant access to tenant %q", tenantID)
	}
	return session, tenantID, nil
}

func (s *Server) verifyGoogleIDToken(ctx context.Context, token string, expectedNonce string) (googleIdentity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return googleIdentity{}, fmt.Errorf("Google ID token is malformed")
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if err := decodeJWTPart(parts[0], &header); err != nil || header.Algorithm != "RS256" || header.KeyID == "" {
		return googleIdentity{}, fmt.Errorf("Google ID token has an invalid header")
	}
	key, err := s.googlePublicKey(ctx, header.KeyID, false)
	if err != nil {
		return googleIdentity{}, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return googleIdentity{}, fmt.Errorf("Google ID token has an invalid signature")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
		// A key can rotate before its cache TTL. Refresh once before rejecting it.
		key, refreshErr := s.googlePublicKey(ctx, header.KeyID, true)
		if refreshErr != nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
			return googleIdentity{}, fmt.Errorf("Google ID token signature is invalid")
		}
	}
	var claims struct {
		Issuer        string `json:"iss"`
		Audience      string `json:"aud"`
		Subject       string `json:"sub"`
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		Nonce         string `json:"nonce"`
		ExpiresAt     int64  `json:"exp"`
		IssuedAt      int64  `json:"iat"`
	}
	if err := decodeJWTPart(parts[1], &claims); err != nil {
		return googleIdentity{}, fmt.Errorf("Google ID token claims are malformed")
	}
	now := time.Now().Unix()
	if claims.Issuer != "https://accounts.google.com" && claims.Issuer != "accounts.google.com" {
		return googleIdentity{}, fmt.Errorf("Google ID token issuer is invalid")
	}
	if claims.Audience != s.config.GoogleClientID || claims.Subject == "" || claims.ExpiresAt <= now || claims.IssuedAt > now+300 {
		return googleIdentity{}, fmt.Errorf("Google ID token claims are invalid")
	}
	if expectedNonce == "" || !constantTimeString(claims.Nonce, expectedNonce) {
		return googleIdentity{}, fmt.Errorf("Google ID token nonce is invalid")
	}
	return googleIdentity{
		Subject: claims.Subject, Email: strings.ToLower(strings.TrimSpace(claims.Email)),
		EmailVerified: claims.EmailVerified, Name: strings.TrimSpace(claims.Name), Picture: strings.TrimSpace(claims.Picture),
	}, nil
}

func decodeJWTPart(encoded string, target any) error {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func (s *Server) googlePublicKey(ctx context.Context, keyID string, forceRefresh bool) (*rsa.PublicKey, error) {
	s.googleKeys.mu.Lock()
	defer s.googleKeys.mu.Unlock()
	if !forceRefresh && time.Now().Before(s.googleKeys.expiresAt) {
		if key := s.googleKeys.keys[keyID]; key != nil {
			return key, nil
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.config.GoogleJWKSURL, nil)
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("Google JWKS returned %d", response.StatusCode)
	}
	var document struct {
		Keys []struct {
			KeyType   string `json:"kty"`
			KeyID     string `json:"kid"`
			Algorithm string `json:"alg"`
			Modulus   string `json:"n"`
			Exponent  string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return nil, err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, item := range document.Keys {
		if item.KeyType != "RSA" || item.KeyID == "" || (item.Algorithm != "" && item.Algorithm != "RS256") {
			continue
		}
		modulus, err := base64.RawURLEncoding.DecodeString(item.Modulus)
		if err != nil {
			continue
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(item.Exponent)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
			continue
		}
		exponent := 0
		for _, value := range exponentBytes {
			exponent = exponent<<8 | int(value)
		}
		if exponent < 3 {
			continue
		}
		keys[item.KeyID] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}
	}
	s.googleKeys.keys = keys
	s.googleKeys.expiresAt = time.Now().Add(jwksCacheTTL(response.Header.Get("cache-control")))
	key := keys[keyID]
	if key == nil {
		return nil, fmt.Errorf("Google signing key %q was not found", keyID)
	}
	return key, nil
}

func jwksCacheTTL(cacheControl string) time.Duration {
	for _, part := range strings.Split(cacheControl, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || strings.ToLower(name) != "max-age" {
			continue
		}
		seconds, err := strconv.Atoi(strings.TrimSpace(value))
		if err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultGoogleKeyCacheTTL
}
