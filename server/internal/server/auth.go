package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/landlord"
)

func (s *Server) authenticateSocket(ctx context.Context, projectID string, currentTenantID string, token string, requestedTenantID string) (*gonvex.User, map[string]any, string, error) {
	if s.config.LandlordURL == "" {
		if s.config.RequireAuth {
			return nil, nil, "", fmt.Errorf("landlord database URL is not configured")
		}
		tenant := tenantIDFromRequest(projectID, requestedTenantID)
		if requestedTenantID == "" {
			tenant = tenantIDFromRequest(projectID, currentTenantID)
		}
		return &gonvex.User{ID: "dev"}, map[string]any{}, tenant, nil
	}

	session, err := landlord.ValidateSession(ctx, s.config.LandlordURL, token, requestedTenantID)
	if err != nil {
		return nil, nil, "", err
	}
	user := &gonvex.User{ID: session.UserID, Email: session.Email}
	permissions, err := s.loadTenantPermissions(ctx, projectID, session.ActiveTenantID, session.UserID)
	if err != nil {
		return nil, nil, "", err
	}
	return user, permissions, session.ActiveTenantID, nil
}

func (s *Server) loadTenantPermissions(ctx context.Context, projectID string, tenantID string, userID string) (map[string]any, error) {
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, tenantID), s.databaseURLForTenant(projectID, tenantID))
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

	permissions := map[string]any{"role": role}
	if len(rawPermissions) > 0 {
		var parsed map[string]any
		if err := json.Unmarshal(rawPermissions, &parsed); err != nil {
			return nil, err
		}
		for key, value := range parsed {
			permissions[key] = value
		}
	}
	return permissions, nil
}
