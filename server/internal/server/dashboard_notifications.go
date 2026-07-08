package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type dashboardNotification struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	ProjectID string `json:"projectId,omitempty"`
	Read      bool   `json:"read"`
	CreatedAt string `json:"createdAt"`
}

// createDashboardNotification records a per-user notification that surfaces in
// the dashboard notification tray. Failures are surfaced to the caller so the
// triggering action can decide whether to fail.
func (s *Server) createDashboardNotification(ctx context.Context, db *sql.DB, email string, notificationType string, title string, body string, projectID string) error {
	email = normalizeDashboardEmail(email)
	if email == "" || title == "" {
		return nil
	}
	if notificationType == "" {
		notificationType = "info"
	}
	id, err := randomID("ntf")
	if err != nil {
		return err
	}
	var projectRef any
	if projectID != "" {
		projectRef = projectID
	}
	_, err = db.ExecContext(ctx, `INSERT INTO gonvex_dashboard_notifications (
		id, email, type, title, body, project_id
	) VALUES ($1, $2, $3, $4, $5, $6)`,
		id, email, notificationType, title, body, projectRef)
	return err
}

// notifyProjectMembership tells a user they were added to a project.
func (s *Server) notifyProjectMembership(ctx context.Context, db *sql.DB, projectID string, email string, role string) error {
	var projectName string
	err := db.QueryRowContext(ctx, `SELECT name FROM gonvex_runtime_projects WHERE id = $1`, projectID).Scan(&projectName)
	if err == sql.ErrNoRows {
		projectName = projectID
	} else if err != nil {
		return err
	}
	if projectName == "" {
		projectName = projectID
	}
	if role == "" {
		role = "member"
	}
	title := fmt.Sprintf("Added to %s", projectName)
	body := fmt.Sprintf("You now have %s access to %s.", role, projectName)
	return s.createDashboardNotification(ctx, db, email, "project_membership", title, body, projectID)
}

func (s *Server) handleListNotifications(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "notifications are unavailable"})
		return
	}
	defer db.Close()
	rows, err := db.QueryContext(r.Context(), `
		SELECT id, type, title, body, COALESCE(project_id, ''), read_at IS NOT NULL, created_at
		FROM gonvex_dashboard_notifications
		WHERE email = $1
		ORDER BY created_at DESC
		LIMIT 50
	`, actor.Email)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()
	notifications := []dashboardNotification{}
	unread := 0
	for rows.Next() {
		var n dashboardNotification
		var createdAt time.Time
		if err := rows.Scan(&n.ID, &n.Type, &n.Title, &n.Body, &n.ProjectID, &n.Read, &createdAt); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		n.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		if !n.Read {
			unread++
		}
		notifications = append(notifications, n)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"notifications": notifications, "unread": unread})
}

func (s *Server) handleReadNotifications(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
		return
	}
	defer r.Body.Close()
	var payload struct {
		IDs []string `json:"ids"`
	}
	// An empty/absent body marks every notification read.
	_ = json.NewDecoder(r.Body).Decode(&payload)
	db, err := s.openProjectRegistry(r.Context())
	if err != nil || db == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "notifications are unavailable"})
		return
	}
	defer db.Close()
	if len(payload.IDs) == 0 {
		if _, err := db.ExecContext(r.Context(), `UPDATE gonvex_dashboard_notifications SET read_at = now() WHERE email = $1 AND read_at IS NULL`, actor.Email); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	} else if _, err := db.ExecContext(r.Context(), `UPDATE gonvex_dashboard_notifications SET read_at = now() WHERE email = $1 AND id = ANY($2) AND read_at IS NULL`, actor.Email, payload.IDs); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
