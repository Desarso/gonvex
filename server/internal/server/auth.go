package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

func (s *Server) authenticateSocket(ctx context.Context, projectID string, currentTenantID string, token string, requestedTenantID string) (*gonvex.Account, map[string]any, string, string, error) {
	if err := s.requireProjectDatabase(projectID); err != nil {
		return nil, nil, "", "", err
	}
	if strings.HasPrefix(strings.TrimSpace(token), "gvx_session_") {
		session, tenantID, err := s.validateAppSession(ctx, projectID, token, requestedTenantID)
		if err != nil {
			return nil, nil, "", "", err
		}
		member, err := s.loadTenantMember(ctx, session.ProjectID, tenantID, session.Account.ID)
		if err != nil {
			return nil, nil, "", "", err
		}
		return &gonvex.Account{ID: session.Account.ID, Email: session.Account.Email, Name: session.Account.Name, AvatarURL: session.Account.Picture}, member.Permissions, session.ProjectID, tenantID, nil
	}
	if strings.TrimSpace(token) != "" {
		return nil, nil, "", "", fmt.Errorf("only Gonvex app sessions are accepted")
	}
	return nil, nil, "", "", fmt.Errorf("a Gonvex app session is required")
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
	db, err := s.tenantMemberDB(ctx, projectID, tenantID)
	if err != nil {
		return nil, err
	}

	member := &gonvex.Member{}
	var rawPermissions []byte
	if err := db.QueryRowContext(ctx, `
		SELECT id, account_id,
			status, display_name, avatar_url, role, permissions
		FROM members
		WHERE account_id = $1 AND status = 'active'
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
