package server

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/controlplane/legacyidentity"
)

func (s *Server) authenticateSocket(ctx context.Context, projectID string, currentTenantID string, token string, requestedTenantID string) (*gonvex.User, map[string]any, string, string, error) {
	if err := s.requireProjectDatabase(projectID); err != nil {
		return nil, nil, "", "", err
	}
	if strings.HasPrefix(strings.TrimSpace(token), "gvx_session_") {
		session, tenantID, err := s.validateAppSession(ctx, projectID, token, requestedTenantID)
		if err != nil {
			return nil, nil, "", "", err
		}
		member, err := s.loadTenantMember(ctx, session.ProjectID, tenantID, session.User.accountID())
		if err != nil {
			return nil, nil, "", "", err
		}
		return &gonvex.User{ID: session.User.accountID(), Email: session.User.Email, Name: session.User.Name, AvatarURL: session.User.Picture}, member.Permissions, session.ProjectID, tenantID, nil
	}
	if strings.TrimSpace(s.projectRegistryURL()) != "" {
		nativeEnabled, err := s.nativeAppAuthEnabled(ctx, projectID)
		if err != nil {
			return nil, nil, "", "", fmt.Errorf("project authentication configuration is unavailable")
		}
		if nativeEnabled {
			return nil, nil, "", "", fmt.Errorf("a Gonvex app session is required")
		}
	}
	// Both fallbacks below hand the app an identity taken from the presented
	// token rather than a Gonvex session. That identity is only trustworthy
	// when the token's signature was checked, which firebaseIdentityFromToken
	// does whenever the project declares Firebase configuration.
	firebaseProjectID := s.firebaseProjectID(ctx, projectID)
	appIdentity := func() (*gonvex.User, string, error) {
		user, err := s.firebaseIdentityFromToken(ctx, firebaseProjectID, token)
		if err != nil {
			return nil, "", err
		}
		tenant := tenantIDFromRequest(projectID, requestedTenantID)
		if requestedTenantID == "" {
			tenant = tenantIDFromRequest(projectID, currentTenantID)
		}
		return user, tenant, nil
	}
	hasVerifiedAppIdentity := firebaseProjectID != ""

	if s.config.LandlordURL == "" {
		if s.config.RequireAuth && !hasVerifiedAppIdentity {
			return nil, nil, "", "", fmt.Errorf("control plane database URL is not configured")
		}
		user, tenant, err := appIdentity()
		if err != nil {
			return nil, nil, "", "", err
		}
		return user, map[string]any{}, projectID, tenant, nil
	}

	session, err := legacyidentity.ValidateSession(ctx, s.config.LandlordURL, token, requestedTenantID)
	if err != nil {
		if s.config.RequireAuth && !hasVerifiedAppIdentity {
			return nil, nil, "", "", err
		}
		user, tenant, identityErr := appIdentity()
		if identityErr != nil {
			return nil, nil, "", "", identityErr
		}
		return user, map[string]any{}, projectID, tenant, nil
	}
	user := &gonvex.User{ID: session.UserID, Email: session.Email}
	permissions, err := s.loadTenantPermissions(ctx, projectID, session.ActiveTenantID, session.UserID)
	if err != nil {
		return nil, nil, "", "", err
	}
	return user, permissions, projectID, session.ActiveTenantID, nil
}

func devUserFromJWT(token string) *gonvex.User {
	user := &gonvex.User{ID: "dev"}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return user
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return user
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return user
	}
	for _, key := range []string{"sub", "user_id", "uid"} {
		if value := strings.TrimSpace(fmt.Sprint(claims[key])); value != "" && value != "<nil>" {
			user.ID = value
			break
		}
	}
	if email := strings.TrimSpace(fmt.Sprint(claims["email"])); email != "" && email != "<nil>" {
		user.Email = email
	}
	return user
}

func (s *Server) loadTenantPermissions(ctx context.Context, projectID string, tenantID string, userID string) (map[string]any, error) {
	member, err := s.loadTenantMember(ctx, projectID, tenantID, userID)
	if err != nil {
		return nil, err
	}
	return member.Permissions, nil
}

// loadTenantMember is the final authorization check for entering a tenant.
// Control-plane directory/index rows can locate a database, but only an active
// member row in that tenant database grants access.
func (s *Server) loadTenantMember(ctx context.Context, projectID string, tenantID string, accountID string) (*gonvex.Member, error) {
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL := s.databaseURLForTenant(projectID, tenantID)
	var err error
	databaseURL, err = s.ensureRuntimeTenantDatabase(ctx, projectID, tenantIDFromRequest(projectID, tenantID), databaseURL)
	if err != nil {
		return nil, err
	}
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, tenantID), databaseURL)
	if err != nil {
		return nil, err
	}
	if store.DB == nil {
		return nil, fmt.Errorf("tenant database is unavailable")
	}
	if err := ensureTenantLocalTables(ctx, store.DB); err != nil {
		return nil, err
	}

	member := &gonvex.Member{}
	var rawPermissions []byte
	if err := store.DB.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(id, ''), user_id), COALESCE(NULLIF(account_id, ''), user_id),
			status, display_name, avatar_url, role, permissions
		FROM members
		WHERE (account_id = $1 OR user_id = $1) AND status = 'active'
	`, accountID).Scan(&member.ID, &member.AccountID, &member.Status, &member.DisplayName, &member.AvatarURL, &member.Role, &rawPermissions); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("active tenant member for account %q not found", accountID)
		}
		return nil, err
	}

	permissions := map[string]any{}
	if len(rawPermissions) > 0 {
		var parsed map[string]any
		if err := json.Unmarshal(rawPermissions, &parsed); err != nil {
			return nil, err
		}
		for key, value := range parsed {
			permissions[key] = value
		}
	}
	permissions["role"] = member.Role
	member.Permissions = permissions
	return member, nil
}
