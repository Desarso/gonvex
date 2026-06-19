package landlord

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

type Session struct {
	UserID         string
	Email          string
	ActiveTenantID string
}

func ValidateSession(ctx context.Context, databaseURL string, token string, requestedTenantID string) (Session, error) {
	token = strings.TrimSpace(token)
	if databaseURL == "" {
		return Session{}, fmt.Errorf("landlord database URL is not configured")
	}
	if token == "" {
		return Session{}, fmt.Errorf("session token is required")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return Session{}, err
	}
	defer db.Close()

	hash := tokenHash(token)
	session := Session{}
	if err := db.QueryRowContext(ctx, `
		SELECT u.id, COALESCE(u.email, ''), COALESCE(s.active_tenant_id, '')
		FROM sessions s
		JOIN users u ON u.id = s.user_id
		WHERE s.token_hash = $1 AND s.expires_at > now()
	`, hash).Scan(&session.UserID, &session.Email, &session.ActiveTenantID); err != nil {
		if err == sql.ErrNoRows {
			return Session{}, fmt.Errorf("invalid or expired session")
		}
		return Session{}, err
	}

	tenantID := strings.TrimSpace(requestedTenantID)
	if tenantID == "" {
		tenantID = session.ActiveTenantID
	}
	if tenantID == "" {
		return Session{}, fmt.Errorf("active tenant is required")
	}
	if err := verifyMembership(ctx, db, session.UserID, tenantID); err != nil {
		return Session{}, err
	}
	session.ActiveTenantID = tenantID
	return session, nil
}

func verifyMembership(ctx context.Context, db *sql.DB, userID string, tenantID string) error {
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM memberships
			WHERE user_id = $1 AND tenant_id = $2
		)
	`, userID, tenantID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("user is not a member of tenant %q", tenantID)
	}
	return nil
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
