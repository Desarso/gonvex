package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var appAuthMembershipRoles = map[string]bool{
	"owner":  true,
	"admin":  true,
	"member": true,
	"viewer": true,
}

var errAppAuthInvitationRequired = errors.New("this app is invite-only; ask an administrator for a workspace invitation")
var errAppAuthOwnerRequired = errors.New("only a tenant owner can manage owner or admin access")

const appAuthInvitationTTL = 7 * 24 * time.Hour

func normalizeAppAuthMembershipRole(value string) (string, error) {
	role := strings.ToLower(strings.TrimSpace(value))
	if role == "" {
		role = "member"
	}
	if !appAuthMembershipRoles[role] {
		return "", fmt.Errorf("role must be owner, admin, member, or viewer")
	}
	return role, nil
}

func lockAppAuthMembershipChanges(ctx context.Context, db *sql.DB, projectID string) (*sql.Conn, error) {
	connection, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, "gonvex-auth-memberships:"+projectID); err != nil {
		connection.Close()
		return nil, err
	}
	return connection, nil
}

func unlockAppAuthMembershipChanges(connection *sql.Conn, projectID string) {
	if connection == nil {
		return
	}
	_, _ = connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, "gonvex-auth-memberships:"+projectID)
	_ = connection.Close()
}

func singleAppAuthTenantRelationshipID(projectID string) string {
	return "auth-single:" + strings.TrimSpace(projectID)
}

// ensureSingleAppAuthTenant gives a single-database project one central,
// project-shaped tenant record. Open signup can still use its synthetic member
// access, while invite-only projects and explicit role assignments use the same
// membership and invitation machinery as multi-tenant projects.
func (s *Server) ensureSingleAppAuthTenant(ctx context.Context, projectID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var mode string
	var name string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(database_mode, ''), 'single'), name
		FROM gonvex_runtime_projects WHERE id = $1`, projectID).Scan(&mode, &name); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("project %q was not found", projectID)
		}
		return err
	}
	if normalizedDatabaseModeWithDefault(mode) != "single" {
		return nil
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_runtime_tenants (
		relationship_id, project_id, tenant_id, name, status, description, provisioned, runtime_created, updated_at
	) VALUES ($1, $2, $2, $3, 'active', 'Single-database app membership scope.', TRUE, FALSE, now())
	ON CONFLICT (project_id, tenant_id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()`,
		singleAppAuthTenantRelationshipID(projectID), projectID, name)
	return err
}

func (s *Server) listAppAuthTenants(ctx context.Context, projectID string, userID string) ([]appAuthTenant, error) {
	db, err := s.pooledProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, fmt.Errorf("project auth store is unavailable")
	}
	var mode string
	var projectName string
	var signupMode string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(p.database_mode, ''), 'single'), p.name,
		COALESCE((SELECT NULLIF(a.signup_mode, '') FROM gonvex_auth_providers a
			WHERE a.project_id = p.id AND a.provider = $2), $3)
		FROM gonvex_runtime_projects p WHERE p.id = $1`, projectID, googleProvider, appAuthSignupPersonal).Scan(&mode, &projectName, &signupMode); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("project %q was not found", projectID)
		}
		return nil, err
	}
	if normalizedDatabaseModeWithDefault(mode) == "single" {
		tenant := appAuthTenant{ID: projectID, Name: projectName}
		var permissionsJSON []byte
		err := db.QueryRowContext(ctx, `SELECT m.role, m.permissions
			FROM gonvex_auth_memberships m
			WHERE m.project_id = $1 AND m.user_id = $2 AND m.tenant_id = $1`, projectID, userID).Scan(
			&tenant.Role, &permissionsJSON,
		)
		if err == nil {
			tenant.Permissions = map[string]any{}
			if len(permissionsJSON) > 0 {
				if err := json.Unmarshal(permissionsJSON, &tenant.Permissions); err != nil {
					return nil, err
				}
			}
			return []appAuthTenant{tenant}, nil
		}
		if err != sql.ErrNoRows {
			return nil, err
		}
		if signupMode == appAuthSignupInviteOnly {
			return []appAuthTenant{}, nil
		}
		return []appAuthTenant{{ID: projectID, Name: projectName, Role: "member", Permissions: map[string]any{}}}, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT m.tenant_id, t.name, m.role, m.permissions
		FROM gonvex_auth_memberships m
		JOIN gonvex_runtime_tenants t ON t.project_id = m.project_id AND t.tenant_id = m.tenant_id
		WHERE m.project_id = $1 AND m.user_id = $2
		ORDER BY CASE m.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 WHEN 'member' THEN 2 ELSE 3 END, lower(t.name), t.tenant_id`, projectID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tenants := []appAuthTenant{}
	for rows.Next() {
		var tenant appAuthTenant
		var permissionsJSON []byte
		if err := rows.Scan(&tenant.ID, &tenant.Name, &tenant.Role, &permissionsJSON); err != nil {
			return nil, err
		}
		tenant.Permissions = map[string]any{}
		if len(permissionsJSON) > 0 {
			if err := json.Unmarshal(permissionsJSON, &tenant.Permissions); err != nil {
				return nil, err
			}
		}
		tenants = append(tenants, tenant)
	}
	return tenants, rows.Err()
}

func selectAppAuthTenant(tenants []appAuthTenant, requested string) (appAuthTenant, error) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		if len(tenants) == 0 {
			return appAuthTenant{}, fmt.Errorf("this account does not have access to a tenant")
		}
		return tenants[0], nil
	}
	for _, tenant := range tenants {
		if tenant.ID == requested {
			return tenant, nil
		}
	}
	return appAuthTenant{}, fmt.Errorf("app session does not grant access to tenant %q", requested)
}

func (s *Server) ensureAppAuthMemberships(ctx context.Context, projectID string, user appAuthUser) error {
	mode, err := s.projectDatabaseMode(ctx, projectID)
	if err != nil {
		return err
	}
	if user.EmailVerified && user.Email != "" {
		if err := s.claimAppAuthInvitations(ctx, projectID, user); err != nil {
			return err
		}
	}
	configuration, err := s.appAuthProviderConfiguration(ctx, projectID)
	if err != nil {
		return err
	}
	tenants, err := s.listAppAuthTenants(ctx, projectID, user.ID)
	if err != nil {
		return err
	}
	if configuration.SignupMode == appAuthSignupInviteOnly {
		if len(tenants) == 0 {
			return errAppAuthInvitationRequired
		}
		return nil
	}
	if len(tenants) > 0 {
		return nil
	}
	if mode == "single" {
		return nil
	}
	_, err = s.ensurePersonalAppAuthTenant(ctx, projectID, user)
	return err
}

type appAuthPendingInvitation struct {
	tenantID        string
	role            string
	permissions     map[string]any
	permissionsJSON []byte
	invitedBy       string
	expiresAt       time.Time
}

func (s *Server) claimAppAuthInvitations(ctx context.Context, projectID string, user appAuthUser) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND email = $2 AND expires_at <= now()`, projectID, strings.ToLower(user.Email)); err != nil {
		return err
	}
	// Atomically take the invitations before granting their memberships. A
	// concurrent cancellation or replacement has a definitive winner instead of
	// returning success while an already-read invitation is still claimed.
	rows, err := db.QueryContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND email = $2 AND expires_at > now()
		RETURNING tenant_id, role, permissions, invited_by, expires_at`, projectID, strings.ToLower(user.Email))
	if err != nil {
		return err
	}
	invitations := []appAuthPendingInvitation{}
	for rows.Next() {
		var item appAuthPendingInvitation
		if err := rows.Scan(&item.tenantID, &item.role, &item.permissionsJSON, &item.invitedBy, &item.expiresAt); err != nil {
			rows.Close()
			return err
		}
		item.permissions = map[string]any{}
		if len(item.permissionsJSON) > 0 {
			if err := json.Unmarshal(item.permissionsJSON, &item.permissions); err != nil {
				rows.Close()
				return err
			}
		}
		invitations = append(invitations, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for index, item := range invitations {
		if err := s.upsertAppAuthMembership(ctx, projectID, item.tenantID, user.ID, item.role, item.permissions); err != nil {
			restoreAppAuthInvitations(context.Background(), db, projectID, strings.ToLower(user.Email), invitations[index:])
			return err
		}
	}
	return rows.Err()
}

func restoreAppAuthInvitations(ctx context.Context, db *sql.DB, projectID string, email string, invitations []appAuthPendingInvitation) {
	for _, item := range invitations {
		_, _ = db.ExecContext(ctx, `INSERT INTO gonvex_auth_membership_invitations (
			project_id, tenant_id, email, role, permissions, invited_by, expires_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, now()) ON CONFLICT DO NOTHING`,
			projectID, item.tenantID, email, item.role, string(item.permissionsJSON), item.invitedBy, item.expiresAt)
	}
}

func (s *Server) ensurePersonalAppAuthTenant(ctx context.Context, projectID string, user appAuthUser) (appAuthTenant, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return appAuthTenant{}, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	connection, err := db.Conn(ctx)
	if err != nil {
		return appAuthTenant{}, err
	}
	defer connection.Close()
	lockKey := "gonvex-auth-personal:" + projectID + ":" + user.ID
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		return appAuthTenant{}, err
	}
	defer connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)

	tenants, err := s.listAppAuthTenants(ctx, projectID, user.ID)
	if err != nil {
		return appAuthTenant{}, err
	}
	if len(tenants) > 0 {
		return tenants[0], nil
	}
	name := strings.TrimSpace(user.Name)
	if name == "" {
		name = strings.Split(user.Email, "@")[0]
	}
	if name == "" {
		name = "My"
	}
	return s.createAppAuthTenant(ctx, projectID, user.ID, name+"'s workspace")
}

func (s *Server) createAppAuthTenant(ctx context.Context, projectID string, userID string, name string) (appAuthTenant, error) {
	mode, err := s.projectDatabaseMode(ctx, projectID)
	if err != nil {
		return appAuthTenant{}, err
	}
	if mode != "multiTenant" {
		return appAuthTenant{}, fmt.Errorf("project is not configured for tenant databases")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return appAuthTenant{}, fmt.Errorf("tenant name is required")
	}
	if s.config.PostgresURL == "" {
		return appAuthTenant{}, fmt.Errorf("DATABASE_URL is not configured")
	}
	registry, err := s.openProjectRegistry(ctx)
	if err != nil || registry == nil {
		return appAuthTenant{}, fmt.Errorf("project auth store is unavailable")
	}
	defer registry.Close()
	connection, err := registry.Conn(ctx)
	if err != nil {
		return appAuthTenant{}, err
	}
	defer connection.Close()
	lockKey := "gonvex-auth-tenant-create:" + projectID
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		return appAuthTenant{}, err
	}
	defer connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)
	tenantID, err := generateRelationshipID()
	if err != nil {
		return appAuthTenant{}, err
	}
	s.hydrateProjects()
	s.hydrateProjectTenantDatabases(ctx, projectID)

	s.projectMu.Lock()
	databaseAlias := slug(name)
	if databaseAlias == "" {
		databaseAlias = "workspace"
	}
	baseAlias := databaseAlias
	for suffix := 2; s.tenantDatabaseAliasTakenLocked(projectID, databaseAlias, ""); suffix++ {
		databaseAlias = fmt.Sprintf("%s-%d", baseAlias, suffix)
	}
	databaseName := tenantDatabaseNameWithAlias(projectID, tenantID, databaseAlias)
	s.projectMu.Unlock()

	tenantDatabaseURL, err := createProjectDatabase(ctx, s.config.PostgresURL, databaseName)
	if err != nil {
		return appAuthTenant{}, err
	}
	cleanupDatabase := true
	defer func() {
		if cleanupDatabase {
			_ = dropProjectDatabase(context.Background(), s.config.PostgresURL, databaseName)
		}
	}()
	if err := provisionTenantDatabase(ctx, tenantDatabaseURL, s.runtime.ManifestForProject(projectID).Schema.TenantSchema()); err != nil {
		return appAuthTenant{}, err
	}
	target := tenantTarget{
		RelationshipID: tenantID,
		ID:             tenantID, ProjectID: projectID, Name: name, Database: databaseAlias,
		Status: "active", Description: "Auth-created tenant database.", Provisioned: true, RuntimeCreated: true,
		databaseURL: tenantDatabaseURL, databaseName: databaseName,
	}
	registered, err := s.saveTenantRegistry(ctx, target)
	if err != nil || !registered.registered {
		if err == nil {
			err = fmt.Errorf("tenant relationship registry is unavailable")
		}
		return appAuthTenant{}, err
	}
	s.mergeProjectTenants(projectID, []tenantTarget{registered})
	s.invalidateProjectTenantHydration(projectID)
	role := ""
	if userID != "" {
		if err := s.upsertAppAuthMembership(ctx, projectID, tenantID, userID, "owner", map[string]any{}); err != nil {
			_ = s.deleteTenantRegistry(context.Background(), projectID, registered)
			s.projectMu.Lock()
			delete(s.tenants, tenantStoreKey(projectID, tenantID))
			delete(s.config.TenantDatabases, tenantStoreKey(projectID, tenantID))
			s.projectMu.Unlock()
			return appAuthTenant{}, err
		}
		role = "owner"
	}
	cleanupDatabase = false
	return appAuthTenant{ID: tenantID, Name: name, Role: role, Permissions: map[string]any{}}, nil
}

func (s *Server) upsertAppAuthMembership(ctx context.Context, projectID string, tenantID string, userID string, role string, permissions map[string]any) error {
	return s.upsertAppAuthMembershipAs(ctx, projectID, tenantID, userID, role, permissions, "")
}

func (s *Server) upsertAppAuthMembershipAs(ctx context.Context, projectID string, tenantID string, userID string, role string, permissions map[string]any, actorRole string) error {
	role, err := normalizeAppAuthMembershipRole(role)
	if err != nil {
		return err
	}
	sanitizedPermissions := map[string]any{}
	for key, value := range permissions {
		if key != "role" {
			sanitizedPermissions[key] = value
		}
	}
	permissionsJSON, err := json.Marshal(sanitizedPermissions)
	if err != nil {
		return fmt.Errorf("permissions must be a JSON object: %w", err)
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM gonvex_runtime_tenants WHERE project_id = $1 AND tenant_id = $2
	)`, projectID, tenantID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("tenant %q is not registered for project %q", tenantID, projectID)
	}
	var previousRole string
	var previousPermissions []byte
	previousErr := db.QueryRowContext(ctx, `SELECT role, permissions FROM gonvex_auth_memberships
		WHERE project_id = $1 AND user_id = $2 AND tenant_id = $3`, projectID, userID, tenantID).Scan(&previousRole, &previousPermissions)
	if previousErr != nil && previousErr != sql.ErrNoRows {
		return previousErr
	}
	if actorRole == "admin" && (role == "owner" || role == "admin" || (previousErr == nil && (previousRole == "owner" || previousRole == "admin"))) {
		return errAppAuthOwnerRequired
	}
	if previousErr == nil && previousRole == "owner" && role != "owner" {
		var hasOtherActiveOwner bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM gonvex_auth_memberships m
			JOIN gonvex_auth_users u ON u.project_id = m.project_id AND u.id = m.user_id
			WHERE m.project_id = $1 AND m.tenant_id = $2 AND m.role = 'owner'
			AND m.user_id <> $3 AND u.disabled_at IS NULL
		)`, projectID, tenantID, userID).Scan(&hasOtherActiveOwner); err != nil {
			return err
		}
		if !hasOtherActiveOwner {
			return fmt.Errorf("a tenant must keep at least one owner")
		}
	}
	if err := s.syncAppAuthMembershipToTenant(ctx, projectID, tenantID, userID, role, permissionsJSON); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_memberships (project_id, user_id, tenant_id, role, permissions, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (project_id, user_id, tenant_id) DO UPDATE SET
			role = EXCLUDED.role, permissions = EXCLUDED.permissions, updated_at = now()`,
		projectID, userID, tenantID, role, string(permissionsJSON))
	if err != nil {
		if previousErr == nil {
			_ = s.syncAppAuthMembershipToTenant(context.Background(), projectID, tenantID, userID, previousRole, previousPermissions)
		} else {
			_ = s.deleteAppAuthTenantMember(context.Background(), projectID, tenantID, userID)
		}
		return err
	}
	if previousErr == nil && (previousRole != role || !equalAppAuthPermissions(previousPermissions, permissionsJSON)) {
		s.revokeAppAuthUserSessions(ctx, projectID, userID)
	}
	return nil
}

func equalAppAuthPermissions(left []byte, right []byte) bool {
	var leftValue, rightValue map[string]any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func (s *Server) syncAppAuthMembershipToTenant(ctx context.Context, projectID string, tenantID string, userID string, role string, permissionsJSON []byte) error {
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL := s.databaseURLForTenant(projectID, tenantID)
	databaseURL, err := s.ensureRuntimeTenantDatabase(ctx, projectID, tenantID, databaseURL)
	if err != nil {
		return err
	}
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, tenantID), databaseURL)
	if err != nil {
		return err
	}
	if store.DB == nil {
		return fmt.Errorf("tenant database is unavailable")
	}
	if err := ensureTenantLocalTables(ctx, store.DB); err != nil {
		return err
	}
	_, err = store.DB.ExecContext(ctx, `INSERT INTO members (user_id, role, permissions, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (user_id) DO UPDATE SET role = EXCLUDED.role, permissions = EXCLUDED.permissions, updated_at = now()`,
		userID, role, string(permissionsJSON))
	return err
}

func (s *Server) deleteAppAuthTenantMember(ctx context.Context, projectID string, tenantID string, userID string) error {
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL := s.databaseURLForTenant(projectID, tenantID)
	if databaseURL == "" {
		return nil
	}
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, tenantID), databaseURL)
	if err != nil || store.DB == nil {
		return err
	}
	_, err = store.DB.ExecContext(ctx, `DELETE FROM members WHERE user_id = $1`, userID)
	return err
}

func (s *Server) removeAppAuthMembership(ctx context.Context, projectID string, tenantID string, userID string) error {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	var targetRole string
	err = db.QueryRowContext(ctx, `SELECT role FROM gonvex_auth_memberships
		WHERE project_id = $1 AND tenant_id = $2 AND user_id = $3`, projectID, tenantID, userID).Scan(&targetRole)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if targetRole == "owner" {
		var hasOtherActiveOwner bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM gonvex_auth_memberships m
			JOIN gonvex_auth_users u ON u.project_id = m.project_id AND u.id = m.user_id
			WHERE m.project_id = $1 AND m.tenant_id = $2 AND m.role = 'owner'
			AND m.user_id <> $3 AND u.disabled_at IS NULL
		)`, projectID, tenantID, userID).Scan(&hasOtherActiveOwner); err != nil {
			return err
		}
		if !hasOtherActiveOwner {
			return fmt.Errorf("a tenant must keep at least one owner")
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_auth_memberships
		WHERE project_id = $1 AND tenant_id = $2 AND user_id = $3`, projectID, tenantID, userID); err != nil {
		return err
	}
	cleanupErr := s.deleteAppAuthTenantMember(ctx, projectID, tenantID, userID)
	// Central membership is authoritative. Revoke immediately even when the
	// best-effort tenant-local mirror cleanup needs operator attention.
	s.revokeAppAuthUserSessions(ctx, projectID, userID)
	return cleanupErr
}

func (s *Server) revokeAppAuthUserSessions(ctx context.Context, projectID string, userID string) {
	db, err := s.openProjectRegistry(ctx)
	if err == nil && db != nil {
		_, _ = db.ExecContext(ctx, `UPDATE gonvex_auth_sessions SET revoked_at = COALESCE(revoked_at, now())
			WHERE project_id = $1 AND user_id = $2`, projectID, userID)
		_, _ = db.ExecContext(ctx, `UPDATE gonvex_auth_refresh_tokens SET revoked_at = COALESCE(revoked_at, now())
			WHERE project_id = $1 AND user_id = $2`, projectID, userID)
		db.Close()
	}
	s.revokeAppAuthConnections(projectID, userID)
}

func (s *Server) inviteAppAuthMember(ctx context.Context, projectID string, tenantID string, email string, role string, permissions map[string]any, invitedBy string) error {
	return s.inviteAppAuthMemberAs(ctx, projectID, tenantID, email, role, permissions, invitedBy, "")
}

func (s *Server) inviteAppAuthMemberAs(ctx context.Context, projectID string, tenantID string, email string, role string, permissions map[string]any, invitedBy string, actorRole string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" || !strings.Contains(email, "@") {
		return fmt.Errorf("a valid email is required")
	}
	role, err := normalizeAppAuthMembershipRole(role)
	if err != nil {
		return err
	}
	if actorRole == "admin" && (role == "owner" || role == "admin") {
		return errAppAuthOwnerRequired
	}
	sanitizedPermissions := map[string]any{}
	for key, value := range permissions {
		if key != "role" {
			sanitizedPermissions[key] = value
		}
	}
	raw, err := json.Marshal(sanitizedPermissions)
	if err != nil {
		return err
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var userID string
	err = db.QueryRowContext(ctx, `SELECT id FROM gonvex_auth_users
		WHERE project_id = $1 AND lower(email) = $2 AND disabled_at IS NULL
		ORDER BY created_at LIMIT 1`, projectID, email).Scan(&userID)
	if err == nil {
		return s.upsertAppAuthMembershipAs(ctx, projectID, tenantID, userID, role, sanitizedPermissions, actorRole)
	}
	if err != sql.ErrNoRows {
		return err
	}
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	if actorRole == "admin" {
		var pendingRole string
		pendingErr := db.QueryRowContext(ctx, `SELECT role FROM gonvex_auth_membership_invitations
			WHERE project_id = $1 AND tenant_id = $2 AND email = $3 AND expires_at > now()`,
			projectID, tenantID, email).Scan(&pendingRole)
		if pendingErr != nil && pendingErr != sql.ErrNoRows {
			return pendingErr
		}
		if pendingErr == nil && (pendingRole == "owner" || pendingRole == "admin") {
			return errAppAuthOwnerRequired
		}
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_auth_membership_invitations (
		project_id, tenant_id, email, role, permissions, invited_by, expires_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
	ON CONFLICT (project_id, tenant_id, email) DO UPDATE SET
		role = EXCLUDED.role, permissions = EXCLUDED.permissions, invited_by = EXCLUDED.invited_by,
		expires_at = EXCLUDED.expires_at, updated_at = now()`,
		projectID, tenantID, email, role, string(raw), invitedBy, time.Now().Add(appAuthInvitationTTL).UTC())
	return err
}

func (s *Server) deleteAppAuthInvitation(ctx context.Context, projectID string, tenantID string, email string) error {
	return s.deleteAppAuthInvitationAs(ctx, projectID, tenantID, email, "")
}

func (s *Server) deleteAppAuthInvitationAs(ctx context.Context, projectID string, tenantID string, email string, actorRole string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return fmt.Errorf("invitation email is required")
	}
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(ctx, db, projectID)
	if err != nil {
		return err
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	if actorRole == "admin" {
		var pendingRole string
		pendingErr := db.QueryRowContext(ctx, `SELECT role FROM gonvex_auth_membership_invitations
			WHERE project_id = $1 AND tenant_id = $2 AND email = $3 AND expires_at > now()`,
			projectID, tenantID, email).Scan(&pendingRole)
		if pendingErr != nil && pendingErr != sql.ErrNoRows {
			return pendingErr
		}
		if pendingErr == nil && (pendingRole == "owner" || pendingRole == "admin") {
			return errAppAuthOwnerRequired
		}
	}
	result, err := db.ExecContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND tenant_id = $2 AND email = $3`, projectID, tenantID, email)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("invitation not found")
	}
	return nil
}

func (s *Server) handleAppAuthTenants(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	identity, err := s.loadAppSessionIdentity(r.Context(), token)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	switch r.Method {
	case http.MethodGet:
		tenants, err := s.listAppAuthTenants(r.Context(), identity.ProjectID, identity.User.ID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"project": identity.ProjectID, "tenants": tenants})
	case http.MethodPost:
		if !s.allowAppAuthRequest(w, r, "tenant-create", 10, time.Hour) {
			return
		}
		configuration, err := s.appAuthProviderConfiguration(r.Context(), identity.ProjectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if configuration.SignupMode == appAuthSignupInviteOnly {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "this project allows tenant creation only through its control plane"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		defer r.Body.Close()
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant request"})
			return
		}
		tenant, err := s.createAppAuthTenant(r.Context(), identity.ProjectID, identity.User.ID, payload.Name)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func decodeAppAuthMembershipRequest(w http.ResponseWriter, r *http.Request) (string, string, map[string]any, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	defer r.Body.Close()
	var payload struct {
		Email       string         `json:"email"`
		Role        string         `json:"role"`
		Permissions map[string]any `json:"permissions"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid membership request"})
		return "", "", nil, false
	}
	return payload.Email, payload.Role, payload.Permissions, true
}

func (s *Server) handleAppAuthMe(w http.ResponseWriter, r *http.Request) {
	identity, err := s.loadAppSessionIdentity(r.Context(), bearerToken(r))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	tenants, err := s.listAppAuthTenants(r.Context(), identity.ProjectID, identity.User.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	activeTenantID := ""
	requestedTenant := tenantID(r)
	if len(tenants) > 0 {
		active, err := selectAppAuthTenant(tenants, requestedTenant)
		if err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		activeTenantID = active.ID
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"project": identity.ProjectID, "user": identity.User,
		"tenants": tenants, "activeTenantId": activeTenantID,
	})
}

func (s *Server) handleAppAuthTenantMembers(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	session, _, err := s.validateAppSession(r.Context(), "", bearerToken(r), tenantID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && session.Tenant.Role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant owner or admin access is required"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		members, invitations, err := s.listAppAuthTenantMembers(r.Context(), session.ProjectID, tenantID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"members": members, "invitations": invitations})
	case http.MethodPost:
		if !s.allowAppAuthRequest(w, r, "tenant-invite", 60, time.Minute) {
			return
		}
		email, role, permissions, ok := decodeAppAuthMembershipRequest(w, r)
		if !ok {
			return
		}
		if role == "owner" && session.Tenant.Role != "owner" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "only an owner can grant owner access"})
			return
		}
		if err := s.inviteAppAuthMemberAs(r.Context(), session.ProjectID, tenantID, email, role, permissions, session.User.ID, session.Tenant.Role); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errAppAuthOwnerRequired) {
				status = http.StatusForbidden
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDeleteAppAuthTenantMember(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "tenant-member-delete", 60, time.Minute) {
		return
	}
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	memberID := strings.TrimSpace(r.PathValue("member"))
	session, _, err := s.validateAppSession(r.Context(), "", bearerToken(r), tenantID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && session.Tenant.Role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant owner or admin access is required"})
		return
	}
	targetRole, err := s.appAuthMembershipRole(r.Context(), session.ProjectID, tenantID, memberID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && (targetRole == "owner" || targetRole == "admin") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only an owner can remove an owner or admin"})
		return
	}
	if err := s.removeAppAuthMembership(r.Context(), session.ProjectID, tenantID, memberID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDeleteAppAuthTenantInvitation(w http.ResponseWriter, r *http.Request) {
	if !s.allowAppAuthRequest(w, r, "tenant-invitation-delete", 60, time.Minute) {
		return
	}
	tenantID := strings.TrimSpace(r.PathValue("tenant"))
	email := strings.TrimSpace(r.PathValue("email"))
	session, _, err := s.validateAppSession(r.Context(), "", bearerToken(r), tenantID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	if session.Tenant.Role != "owner" && session.Tenant.Role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "tenant owner or admin access is required"})
		return
	}
	if err := s.deleteAppAuthInvitationAs(r.Context(), session.ProjectID, tenantID, email, session.Tenant.Role); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errAppAuthOwnerRequired) {
			status = http.StatusForbidden
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) appAuthMembershipRole(ctx context.Context, projectID string, tenantID string, userID string) (string, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return "", fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	var role string
	if err := db.QueryRowContext(ctx, `SELECT role FROM gonvex_auth_memberships
		WHERE project_id = $1 AND tenant_id = $2 AND user_id = $3`, projectID, tenantID, userID).Scan(&role); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("tenant member not found")
		}
		return "", err
	}
	return role, nil
}

type appAuthMemberView struct {
	UserID      string         `json:"userId"`
	Email       string         `json:"email"`
	Name        string         `json:"name"`
	Role        string         `json:"role"`
	Permissions map[string]any `json:"permissions,omitempty"`
}

type appAuthInvitationView struct {
	Email       string         `json:"email"`
	Role        string         `json:"role"`
	Permissions map[string]any `json:"permissions,omitempty"`
	ExpiresAt   time.Time      `json:"expiresAt"`
}

func (s *Server) listAppAuthTenantMembers(ctx context.Context, projectID string, tenantID string) ([]appAuthMemberView, []appAuthInvitationView, error) {
	db, err := s.openProjectRegistry(ctx)
	if err != nil || db == nil {
		return nil, nil, fmt.Errorf("project auth store is unavailable")
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT u.id, u.email, u.name, m.role, m.permissions
		FROM gonvex_auth_memberships m JOIN gonvex_auth_users u ON u.project_id = m.project_id AND u.id = m.user_id
		WHERE m.project_id = $1 AND m.tenant_id = $2
		ORDER BY CASE m.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 WHEN 'member' THEN 2 ELSE 3 END, lower(u.email)`, projectID, tenantID)
	if err != nil {
		return nil, nil, err
	}
	members := []appAuthMemberView{}
	for rows.Next() {
		var member appAuthMemberView
		var raw []byte
		if err := rows.Scan(&member.UserID, &member.Email, &member.Name, &member.Role, &raw); err != nil {
			rows.Close()
			return nil, nil, err
		}
		member.Permissions = map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &member.Permissions); err != nil {
				rows.Close()
				return nil, nil, err
			}
		}
		members = append(members, member)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM gonvex_auth_membership_invitations
		WHERE project_id = $1 AND tenant_id = $2 AND expires_at <= now()`, projectID, tenantID); err != nil {
		return nil, nil, err
	}
	inviteRows, err := db.QueryContext(ctx, `SELECT email, role, permissions, expires_at
		FROM gonvex_auth_membership_invitations WHERE project_id = $1 AND tenant_id = $2 AND expires_at > now()
		ORDER BY lower(email)`, projectID, tenantID)
	if err != nil {
		return nil, nil, err
	}
	defer inviteRows.Close()
	invitations := []appAuthInvitationView{}
	for inviteRows.Next() {
		var invitation appAuthInvitationView
		var raw []byte
		if err := inviteRows.Scan(&invitation.Email, &invitation.Role, &raw, &invitation.ExpiresAt); err != nil {
			return nil, nil, err
		}
		invitation.Permissions = map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &invitation.Permissions); err != nil {
				return nil, nil, err
			}
		}
		invitations = append(invitations, invitation)
	}
	return members, invitations, inviteRows.Err()
}

func (s *Server) handleProjectAuthMemberships(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, r.Method != http.MethodGet) {
		return
	}
	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant"))
	if tenantID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant is required"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		members, invitations, err := s.listAppAuthTenantMembers(r.Context(), projectID, tenantID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"members": members, "invitations": invitations})
	case http.MethodPut:
		email, role, permissions, ok := decodeAppAuthMembershipRequest(w, r)
		if !ok {
			return
		}
		if err := s.inviteAppAuthMember(r.Context(), projectID, tenantID, email, role, permissions, "project-admin"); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		memberID := strings.TrimSpace(r.URL.Query().Get("user"))
		invitationEmail := strings.TrimSpace(r.URL.Query().Get("email"))
		if memberID == "" && invitationEmail == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user or invitation email is required"})
			return
		}
		var err error
		if memberID != "" {
			err = s.removeAppAuthMembership(r.Context(), projectID, tenantID, memberID)
		} else {
			err = s.deleteAppAuthInvitation(r.Context(), projectID, tenantID, invitationEmail)
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleProjectAuthTenants(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, r.Method != http.MethodGet) {
		return
	}
	if r.Method == http.MethodGet {
		db, err := s.openProjectRegistry(r.Context())
		if err != nil || db == nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "project auth store is unavailable"})
			return
		}
		defer db.Close()
		rows, err := db.QueryContext(r.Context(), `SELECT t.tenant_id, t.name, count(m.user_id)
			FROM gonvex_runtime_tenants t
			LEFT JOIN gonvex_auth_memberships m ON m.project_id = t.project_id AND m.tenant_id = t.tenant_id
			WHERE t.project_id = $1 GROUP BY t.tenant_id, t.name ORDER BY lower(t.name), t.tenant_id`, projectID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		defer rows.Close()
		tenants := []map[string]any{}
		for rows.Next() {
			var id, name string
			var memberCount int
			if err := rows.Scan(&id, &name, &memberCount); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			tenants = append(tenants, map[string]any{"id": id, "name": name, "memberCount": memberCount})
		}
		writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	defer r.Body.Close()
	var payload struct {
		Name       string `json:"name"`
		OwnerEmail string `json:"ownerEmail"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant request"})
		return
	}
	tenant, err := s.createAppAuthTenant(r.Context(), projectID, "", payload.Name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if strings.TrimSpace(payload.OwnerEmail) != "" {
		if err := s.inviteAppAuthMember(r.Context(), projectID, tenant.ID, payload.OwnerEmail, "owner", map[string]any{}, "project-admin"); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "tenant": tenant})
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
}

func (s *Server) handleProjectAuthUser(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.PathValue("project"))
	userID := strings.TrimSpace(r.PathValue("user"))
	if projectID == "" || userID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project and user are required"})
		return
	}
	if !s.authorizeProjectAuthRequest(w, r, projectID, true) {
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "project auth store is unavailable"})
		return
	}
	defer db.Close()
	membershipLock, err := lockAppAuthMembershipChanges(r.Context(), db, projectID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer unlockAppAuthMembershipChanges(membershipLock, projectID)
	switch r.Method {
	case http.MethodPatch:
		r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
		defer r.Body.Close()
		var payload struct {
			Disabled bool `json:"disabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid account request"})
			return
		}
		if payload.Disabled {
			if err := ensureAppAuthUserCanBeDeactivated(r.Context(), db, projectID, userID); err != nil {
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			}
		}
		result, err := db.ExecContext(r.Context(), `UPDATE gonvex_auth_users
			SET disabled_at = CASE WHEN $3 THEN COALESCE(disabled_at, now()) ELSE NULL END, updated_at = now()
			WHERE project_id = $1 AND id = $2`, projectID, userID, payload.Disabled)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
			return
		}
		if payload.Disabled {
			s.revokeAppAuthUserSessions(r.Context(), projectID, userID)
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "disabled": payload.Disabled})
	case http.MethodDelete:
		if err := ensureAppAuthUserCanBeDeactivated(r.Context(), db, projectID, userID); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		rows, err := db.QueryContext(r.Context(), `SELECT tenant_id FROM gonvex_auth_memberships
			WHERE project_id = $1 AND user_id = $2`, projectID, userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		tenantIDs := []string{}
		for rows.Next() {
			var tenantID string
			if err := rows.Scan(&tenantID); err != nil {
				rows.Close()
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			tenantIDs = append(tenantIDs, tenantID)
		}
		rows.Close()
		result, err := db.ExecContext(r.Context(), `DELETE FROM gonvex_auth_users WHERE project_id = $1 AND id = $2`, projectID, userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
			return
		}
		s.revokeAppAuthConnections(projectID, userID)
		for _, tenantID := range tenantIDs {
			if err := s.deleteAppAuthTenantMember(r.Context(), projectID, tenantID, userID); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "account was deleted, but tenant membership cleanup failed: " + err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func ensureAppAuthUserCanBeDeactivated(ctx context.Context, db *sql.DB, projectID string, userID string) error {
	var tenantName string
	err := db.QueryRowContext(ctx, `SELECT t.name
		FROM gonvex_auth_memberships target
		JOIN gonvex_runtime_tenants t ON t.project_id = target.project_id AND t.tenant_id = target.tenant_id
		WHERE target.project_id = $1 AND target.user_id = $2 AND target.role = 'owner'
		AND NOT EXISTS (
			SELECT 1 FROM gonvex_auth_memberships another
			JOIN gonvex_auth_users u ON u.project_id = another.project_id AND u.id = another.user_id
			WHERE another.project_id = target.project_id AND another.tenant_id = target.tenant_id
			AND another.role = 'owner' AND another.user_id <> target.user_id AND u.disabled_at IS NULL
		)
		LIMIT 1`, projectID, userID).Scan(&tenantName)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("transfer ownership of %q before disabling or deleting this account", tenantName)
}
