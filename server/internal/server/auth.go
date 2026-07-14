package server

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/landlord"
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
		return &gonvex.User{ID: session.User.ID, Email: session.User.Email}, session.Permissions, session.ProjectID, tenantID, nil
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
	if s.config.LandlordURL == "" {
		if s.config.RequireAuth {
			return nil, nil, "", "", fmt.Errorf("landlord database URL is not configured")
		}
		tenant := tenantIDFromRequest(projectID, requestedTenantID)
		if requestedTenantID == "" {
			tenant = tenantIDFromRequest(projectID, currentTenantID)
		}
		return devUserFromJWT(token), map[string]any{}, projectID, tenant, nil
	}

	session, err := landlord.ValidateSession(ctx, s.config.LandlordURL, token, requestedTenantID)
	if err != nil {
		if s.config.RequireAuth {
			return nil, nil, "", "", err
		}
		tenant := tenantIDFromRequest(projectID, requestedTenantID)
		if requestedTenantID == "" {
			tenant = tenantIDFromRequest(projectID, currentTenantID)
		}
		return devUserFromJWT(token), map[string]any{}, projectID, tenant, nil
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
		return map[string]any{}, nil
	}

	var role string
	var rawPermissions []byte
	if err := store.DB.QueryRowContext(ctx, `
		SELECT role, permissions
		FROM members
		WHERE user_id = $1
	`, userID).Scan(&role, &rawPermissions); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("tenant member %q not found", userID)
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
	permissions["role"] = role
	return permissions, nil
}
