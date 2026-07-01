package server

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	dashboardTokenTTL       = 7 * 24 * time.Hour
	dashboardPasswordRounds = 210_000
)

type dashboardActor struct {
	Email string `json:"email"`
	Name  string `json:"name"`
	Role  string `json:"role"`
}

type dashboardSession struct {
	Email       string `json:"email"`
	Name        string `json:"name"`
	Role        string `json:"role"`
	Provider    string `json:"provider"`
	ExpiresAtMS int64  `json:"expiresAt"`
	AccessToken string `json:"accessToken,omitempty"`
}

type projectMember struct {
	Email string `json:"email"`
	Name  string `json:"name"`
	Role  string `json:"role"`
}

type projectInvitation struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectId"`
	Email     string `json:"email"`
	Role      string `json:"role"`
	ExpiresAt string `json:"expiresAt"`
	Accepted  bool   `json:"accepted"`
}

func (s *Server) handleDashboardLogin(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid login request"})
		return
	}
	email := normalizeDashboardEmail(payload.Email)
	if email == "" || payload.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "email and password are required"})
		return
	}
	actor, ok, err := s.authenticateDashboardPassword(r.Context(), email, payload.Password)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid email or password"})
		return
	}
	session, err := s.dashboardSessionForActor(actor)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": session})
}

func (s *Server) handleDashboardUsers(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	if actor.Role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin access is required"})
		return
	}
	if r.Method == http.MethodGet {
		users, err := s.listDashboardUsers(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"users": users})
		return
	}
	defer r.Body.Close()
	var payload struct {
		Email    string `json:"email"`
		Name     string `json:"name"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid user request"})
		return
	}
	user, err := s.createDashboardUser(r.Context(), payload.Email, payload.Name, payload.Password, payload.Role)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.acceptPendingProjectInvitations(r.Context(), user.Email); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"user": user})
}

func (s *Server) handleProjectMembers(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	projectID := strings.TrimSpace(r.PathValue("project"))
	if !s.canAccessProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project access is required"})
		return
	}
	members, err := s.listProjectMembers(r.Context(), projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	invitations, err := s.listProjectInvitations(r.Context(), projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members, "invitations": invitations})
}

func (s *Server) handleCreateProjectInvitation(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	projectID := strings.TrimSpace(r.PathValue("project"))
	if !s.canManageProject(r.Context(), actor, projectID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return
	}
	defer r.Body.Close()
	var payload struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid invitation request"})
		return
	}
	invitation, err := s.inviteProjectMember(r.Context(), projectID, payload.Email, payload.Role, actor.Email)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"invitation": invitation})
}

func (s *Server) authenticateDashboardPassword(ctx context.Context, email string, password string) (dashboardActor, bool, error) {
	bootstrapEmail := normalizeDashboardEmail(s.configDashboardUser())
	if bootstrapEmail != "" && email == bootstrapEmail && constantTimeString(password, s.configDashboardPassword()) {
		return dashboardActor{Email: bootstrapEmail, Name: displayNameFromEmail(bootstrapEmail), Role: "admin"}, true, nil
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return dashboardActor{}, false, err
	}
	defer db.Close()
	var user dashboardActor
	var passwordHash string
	err = db.QueryRowContext(ctx, `
		SELECT email, name, role, password_hash
		FROM gonvex_dashboard_users
		WHERE email = $1
	`, email).Scan(&user.Email, &user.Name, &user.Role, &passwordHash)
	if err == sql.ErrNoRows {
		return dashboardActor{}, false, nil
	}
	if err != nil {
		return dashboardActor{}, false, err
	}
	return user, verifyDashboardPassword(password, passwordHash), nil
}

func (s *Server) dashboardSessionForActor(actor dashboardActor) (dashboardSession, error) {
	expiresAt := time.Now().Add(dashboardTokenTTL)
	session := dashboardSession{
		Email:       normalizeDashboardEmail(actor.Email),
		Name:        strings.TrimSpace(actor.Name),
		Role:        normalizedDashboardRole(actor.Role),
		Provider:    "gonvex",
		ExpiresAtMS: expiresAt.UnixMilli(),
	}
	if session.Name == "" {
		session.Name = displayNameFromEmail(session.Email)
	}
	token, err := s.signDashboardSession(session)
	if err != nil {
		return dashboardSession{}, err
	}
	session.AccessToken = token
	return session, nil
}

func (s *Server) dashboardActorFromRequest(r *http.Request) (dashboardActor, bool) {
	token := strings.TrimSpace(r.Header.Get("authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[len("Bearer "):])
	}
	if actor, ok := s.verifyDashboardToken(token); ok {
		return actor, true
	}
	if s.acceptsAdminKey(token) {
		return dashboardActor{Email: "admin@gonvex.local", Name: "Gonvex Admin", Role: "admin"}, true
	}
	if s.dashboardAuthOptional() {
		return dashboardActor{Email: "local@gonvex.dev", Name: "Local Developer", Role: "admin"}, true
	}
	return dashboardActor{}, false
}

func (s *Server) dashboardAuthOptional() bool {
	if s.config.RequireAuth {
		return false
	}
	return strings.TrimSpace(s.dashboardSecret()) == "" && s.configDashboardUser() == ""
}

func (s *Server) signDashboardSession(session dashboardSession) (string, error) {
	secret := s.dashboardSecret()
	if strings.TrimSpace(secret) == "" {
		return "", fmt.Errorf("dashboard session secret is not configured")
	}
	payload, err := json.Marshal(session)
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(encodedPayload))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encodedPayload + "." + signature, nil
}

func (s *Server) verifyDashboardToken(token string) (dashboardActor, bool) {
	secret := s.dashboardSecret()
	if strings.TrimSpace(secret) == "" {
		return dashboardActor{}, false
	}
	payload, signature, ok := strings.Cut(strings.TrimSpace(token), ".")
	if !ok || payload == "" || signature == "" {
		return dashboardActor{}, false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !constantTimeString(signature, expected) {
		return dashboardActor{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return dashboardActor{}, false
	}
	var session dashboardSession
	if err := json.Unmarshal(raw, &session); err != nil {
		return dashboardActor{}, false
	}
	if session.ExpiresAtMS < time.Now().UnixMilli() {
		return dashboardActor{}, false
	}
	email := normalizeDashboardEmail(session.Email)
	if email == "" {
		return dashboardActor{}, false
	}
	return dashboardActor{Email: email, Name: session.Name, Role: normalizedDashboardRole(session.Role)}, true
}

func (s *Server) dashboardSecret() string {
	if value := strings.TrimSpace(s.config.DashboardSecret); value != "" {
		return value
	}
	return strings.TrimSpace(s.config.AdminKey)
}

func (s *Server) configDashboardUser() string {
	return strings.TrimSpace(os.Getenv("DASHBOARD_AUTH_USER"))
}

func (s *Server) configDashboardPassword() string {
	return os.Getenv("DASHBOARD_AUTH_PASSWORD")
}

func (s *Server) createDashboardUser(ctx context.Context, email string, name string, password string, role string) (dashboardActor, error) {
	email = normalizeDashboardEmail(email)
	if email == "" {
		return dashboardActor{}, fmt.Errorf("email is required")
	}
	if strings.TrimSpace(password) == "" {
		return dashboardActor{}, fmt.Errorf("password is required")
	}
	role = normalizedDashboardRole(role)
	if role == "" {
		role = "user"
	}
	if name = strings.TrimSpace(name); name == "" {
		name = displayNameFromEmail(email)
	}
	passwordHash, err := hashDashboardPassword(password)
	if err != nil {
		return dashboardActor{}, err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return dashboardActor{}, err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_dashboard_users (
		email, name, role, password_hash, updated_at
	) VALUES ($1, $2, $3, $4, now())
	ON CONFLICT (email) DO UPDATE SET
		name = EXCLUDED.name,
		role = EXCLUDED.role,
		password_hash = EXCLUDED.password_hash,
		updated_at = now()`,
		email, name, role, passwordHash)
	if err != nil {
		return dashboardActor{}, err
	}
	return dashboardActor{Email: email, Name: name, Role: role}, nil
}

func (s *Server) listDashboardUsers(ctx context.Context) ([]dashboardActor, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT email, name, role FROM gonvex_dashboard_users ORDER BY email`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := []dashboardActor{}
	for rows.Next() {
		var user dashboardActor
		if err := rows.Scan(&user.Email, &user.Name, &user.Role); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (s *Server) canAccessProject(ctx context.Context, actor dashboardActor, projectID string) bool {
	if s.dashboardAuthOptional() {
		return true
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return false
	}
	defer db.Close()
	var exists bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM gonvex_runtime_projects WHERE id = $1 AND (owner_email = $2 OR (owner_email = '' AND $3 = 'admin'))
			UNION
			SELECT 1 FROM gonvex_project_members WHERE project_id = $1 AND email = $2
		)
	`, projectID, actor.Email, actor.Role).Scan(&exists)
	return err == nil && exists
}

func (s *Server) canManageProject(ctx context.Context, actor dashboardActor, projectID string) bool {
	if s.dashboardAuthOptional() {
		return true
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return false
	}
	defer db.Close()
	var exists bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM gonvex_runtime_projects WHERE id = $1 AND (owner_email = $2 OR (owner_email = '' AND $3 = 'admin'))
			UNION
			SELECT 1 FROM gonvex_project_members WHERE project_id = $1 AND email = $2 AND role IN ('owner', 'admin')
		)
	`, projectID, actor.Email, actor.Role).Scan(&exists)
	return err == nil && exists
}

func (s *Server) listProjectMembers(ctx context.Context, projectID string) ([]projectMember, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT email, name, role FROM gonvex_project_members WHERE project_id = $1 ORDER BY role, email`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	members := []projectMember{}
	for rows.Next() {
		var member projectMember
		if err := rows.Scan(&member.Email, &member.Name, &member.Role); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

func (s *Server) listProjectInvitations(ctx context.Context, projectID string) ([]projectInvitation, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT id, project_id, email, role, expires_at, accepted_at IS NOT NULL FROM gonvex_project_invitations WHERE project_id = $1 ORDER BY created_at DESC`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	invitations := []projectInvitation{}
	for rows.Next() {
		var invitation projectInvitation
		var expiresAt time.Time
		if err := rows.Scan(&invitation.ID, &invitation.ProjectID, &invitation.Email, &invitation.Role, &expiresAt, &invitation.Accepted); err != nil {
			return nil, err
		}
		invitation.ExpiresAt = expiresAt.UTC().Format(time.RFC3339)
		invitations = append(invitations, invitation)
	}
	return invitations, rows.Err()
}

func (s *Server) inviteProjectMember(ctx context.Context, projectID string, email string, role string, invitedBy string) (projectInvitation, error) {
	email = normalizeDashboardEmail(email)
	if email == "" {
		return projectInvitation{}, fmt.Errorf("email is required")
	}
	role = normalizedProjectRole(role)
	if role == "" {
		role = "dev"
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return projectInvitation{}, err
	}
	defer db.Close()
	id, err := randomID("pinv")
	if err != nil {
		return projectInvitation{}, err
	}
	token, err := randomID("invite")
	if err != nil {
		return projectInvitation{}, err
	}
	expiresAt := time.Now().Add(14 * 24 * time.Hour).UTC()
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_project_invitations (
		id, project_id, email, role, token_hash, invited_by, expires_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		id, projectID, email, role, sha256Hex(token), invitedBy, expiresAt)
	if err != nil {
		return projectInvitation{}, err
	}
	if err := s.addProjectMemberIfUserExists(ctx, db, projectID, email, role); err != nil {
		return projectInvitation{}, err
	}
	return projectInvitation{ID: id, ProjectID: projectID, Email: email, Role: role, ExpiresAt: expiresAt.Format(time.RFC3339)}, nil
}

func (s *Server) addProjectMemberIfUserExists(ctx context.Context, db *sql.DB, projectID string, email string, role string) error {
	var name string
	err := db.QueryRowContext(ctx, `SELECT name FROM gonvex_dashboard_users WHERE email = $1`, email).Scan(&name)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_project_members (
		project_id, email, name, role
	) VALUES ($1, $2, $3, $4)
	ON CONFLICT (project_id, email) DO UPDATE SET
		name = EXCLUDED.name,
		role = EXCLUDED.role`,
		projectID, email, name, role)
	if err != nil {
		return err
	}
	if role == "owner" {
		_, err = db.ExecContext(ctx, `UPDATE gonvex_runtime_projects
			SET owner_email = $1, updated_at = now()
			WHERE id = $2 AND COALESCE(owner_email, '') = ''`,
			email, projectID)
	}
	return err
}

func (s *Server) acceptPendingProjectInvitations(ctx context.Context, email string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT project_id, role FROM gonvex_project_invitations WHERE email = $1 AND accepted_at IS NULL AND expires_at > now()`, email)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var projectID, role string
		if err := rows.Scan(&projectID, &role); err != nil {
			return err
		}
		if err := s.addProjectMemberIfUserExists(ctx, db, projectID, email, role); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE gonvex_project_invitations SET accepted_at = now() WHERE project_id = $1 AND email = $2 AND accepted_at IS NULL`, projectID, email); err != nil {
			return err
		}
	}
	return rows.Err()
}

func hashDashboardPassword(password string) (string, error) {
	var salt [16]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return "", err
	}
	hash, err := pbkdf2.Key(sha256.New, password, salt[:], dashboardPasswordRounds, 32)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbkdf2_sha256$%d$%s$%s", dashboardPasswordRounds, base64.RawURLEncoding.EncodeToString(salt[:]), base64.RawURLEncoding.EncodeToString(hash)), nil
}

func verifyDashboardPassword(password string, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2_sha256" {
		return false
	}
	var rounds int
	if _, err := fmt.Sscanf(parts[1], "%d", &rounds); err != nil || rounds <= 0 {
		return false
	}
	salt, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	expected, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil {
		return false
	}
	actual, err := pbkdf2.Key(sha256.New, password, salt, rounds, len(expected))
	if err != nil {
		return false
	}
	return hmac.Equal(actual, expected)
}

func normalizeDashboardEmail(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizedDashboardRole(role string) string {
	switch strings.TrimSpace(role) {
	case "admin":
		return "admin"
	case "user", "":
		return "user"
	default:
		return ""
	}
}

func normalizedProjectRole(role string) string {
	switch strings.TrimSpace(role) {
	case "owner", "admin", "dev", "viewer":
		return strings.TrimSpace(role)
	default:
		return ""
	}
}

func displayNameFromEmail(email string) string {
	local, _, _ := strings.Cut(email, "@")
	local = strings.TrimSpace(local)
	if local == "" {
		return email
	}
	return local
}

func randomID(prefix string) (string, error) {
	var bytes [18]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(bytes[:]), nil
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func constantTimeString(left string, right string) bool {
	return hmac.Equal([]byte(left), []byte(right))
}
