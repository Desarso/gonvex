package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type capturedError struct {
	EventID     string            `json:"eventId"`
	Timestamp   string            `json:"timestamp"`
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	Name        string            `json:"name,omitempty"`
	Stack       string            `json:"stack,omitempty"`
	Culprit     string            `json:"culprit,omitempty"`
	Project     string            `json:"project"`
	Tenant      string            `json:"tenant,omitempty"`
	Release     string            `json:"release,omitempty"`
	Environment string            `json:"environment,omitempty"`
	User        map[string]any    `json:"user,omitempty"`
	DeviceID    string            `json:"deviceId,omitempty"`
	SessionID   string            `json:"sessionId,omitempty"`
	URL         string            `json:"url,omitempty"`
	UserAgent   string            `json:"userAgent,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Context     map[string]any    `json:"context,omitempty"`
	Breadcrumbs []map[string]any  `json:"breadcrumbs,omitempty"`
}

type errorGroup struct {
	Fingerprint string         `json:"fingerprint"`
	Project     string         `json:"project"`
	Title       string         `json:"title"`
	Culprit     string         `json:"culprit,omitempty"`
	Status      string         `json:"status"`
	Priority    string         `json:"priority"`
	Assignee    string         `json:"assignee,omitempty"`
	FirstSeen   string         `json:"firstSeen"`
	LastSeen    string         `json:"lastSeen"`
	Count       int            `json:"count"`
	Tenants     map[string]int `json:"tenants"`
	Releases    map[string]int `json:"releases"`
	Users       map[string]int `json:"users"`
	Devices     map[string]int `json:"devices"`
	Latest      capturedError  `json:"latest"`
}

type errorTracker struct {
	mu                sync.RWMutex
	groups            map[string]*errorGroup
	eventIDs          map[string]struct{}
	maxEvents, events int
}

func newErrorTracker(max int) *errorTracker {
	return &errorTracker{groups: map[string]*errorGroup{}, eventIDs: map[string]struct{}{}, maxEvents: max}
}

func (t *errorTracker) capture(event capturedError) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.eventIDs[event.EventID]; event.EventID != "" && exists {
		return fingerprint(event), false
	}
	if t.events >= t.maxEvents {
		return "", false
	}
	if event.EventID != "" {
		t.eventIDs[event.EventID] = struct{}{}
	}
	fp := fingerprint(event)
	now := event.Timestamp
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	group := t.groups[fp]
	if group == nil {
		group = &errorGroup{Fingerprint: fp, Project: event.Project, Title: event.Message, Culprit: event.Culprit, Status: "unresolved", Priority: "medium", FirstSeen: now, Tenants: map[string]int{}, Releases: map[string]int{}, Users: map[string]int{}, Devices: map[string]int{}}
		t.groups[fp] = group
	}
	group.Count++
	group.LastSeen = now
	group.Latest = event
	t.events++
	if event.Tenant != "" {
		group.Tenants[event.Tenant]++
	}
	if event.Release != "" {
		group.Releases[event.Release]++
	}
	if event.DeviceID != "" {
		group.Devices[event.DeviceID]++
	}
	if id := stringValue(event.User["id"]); id != "" {
		group.Users[id]++
	} else if email := stringValue(event.User["email"]); email != "" {
		group.Users[email]++
	}
	return fp, true
}

func fingerprint(e capturedError) string {
	stack := e.Culprit
	if stack == "" {
		for _, line := range strings.Split(e.Stack, "\n") {
			if strings.Contains(line, "at ") && !strings.Contains(line, "node_modules") {
				stack = strings.TrimSpace(line)
				break
			}
		}
	}
	normalized := strings.ToLower(strings.TrimSpace(e.Name + "|" + normalizeErrorMessage(e.Message) + "|" + stack + "|" + e.Project))
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:8])
}

func normalizeErrorMessage(value string) string {
	fields := strings.Fields(value)
	for i, field := range fields {
		if len(field) > 5 && (strings.ContainsAny(field, "0123456789") || strings.HasPrefix(field, "http")) {
			fields[i] = "?"
		}
	}
	return strings.Join(fields, " ")
}

func (s *Server) handleErrorEnvelope(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 512<<10)
	var envelope struct {
		Events []capturedError `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil || len(envelope.Events) == 0 || len(envelope.Events) > 20 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid error envelope"})
		return
	}
	accepted := 0
	fingerprints := []string{}
	for _, event := range envelope.Events {
		if event.Project == "" || event.Message == "" {
			continue
		}
		fp, ok := s.errorTracker.capture(event)
		if ok {
			accepted++
			fingerprints = append(fingerprints, fp)
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted, "fingerprints": fingerprints})
}

func (s *Server) handleErrorGroups(w http.ResponseWriter, r *http.Request) {
	s.errorTracker.mu.RLock()
	defer s.errorTracker.mu.RUnlock()
	groups := make([]*errorGroup, 0, len(s.errorTracker.groups))
	project, status := projectID(r), r.URL.Query().Get("status")
	for _, group := range s.errorTracker.groups {
		if project != "" && group.Project != project {
			continue
		}
		if status != "" && group.Status != status {
			continue
		}
		clone := *group
		groups = append(groups, &clone)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].LastSeen > groups[j].LastSeen })
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
}

func (s *Server) handleErrorGroup(w http.ResponseWriter, r *http.Request) {
	s.errorTracker.mu.RLock()
	defer s.errorTracker.mu.RUnlock()
	group := s.errorTracker.groups[r.PathValue("fingerprint")]
	if group == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "error group not found"})
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (s *Server) handleUpdateErrorGroup(w http.ResponseWriter, r *http.Request) {
	var update struct {
		Status   string `json:"status"`
		Priority string `json:"priority"`
		Assignee string `json:"assignee"`
	}
	if json.NewDecoder(r.Body).Decode(&update) != nil {
		writeJSON(w, 400, map[string]any{"error": "invalid update"})
		return
	}
	s.errorTracker.mu.Lock()
	defer s.errorTracker.mu.Unlock()
	group := s.errorTracker.groups[r.PathValue("fingerprint")]
	if group == nil {
		writeJSON(w, 404, map[string]any{"error": "error group not found"})
		return
	}
	if update.Status != "" {
		group.Status = update.Status
	}
	if update.Priority != "" {
		group.Priority = update.Priority
	}
	if update.Assignee != "" {
		group.Assignee = update.Assignee
	}
	writeJSON(w, 200, group)
}

func (s *Server) handleErrorBugReport(w http.ResponseWriter, r *http.Request) {
	s.errorTracker.mu.RLock()
	defer s.errorTracker.mu.RUnlock()
	group := s.errorTracker.groups[r.PathValue("fingerprint")]
	if group == nil {
		writeJSON(w, 404, map[string]any{"error": "error group not found"})
		return
	}
	writeJSON(w, 200, map[string]any{"title": group.Title, "markdown": bugReport(group), "agentContext": map[string]any{"fingerprint": group.Fingerprint, "project": group.Project, "release": group.Latest.Release, "culprit": group.Culprit, "stack": group.Latest.Stack, "breadcrumbs": group.Latest.Breadcrumbs, "context": group.Latest.Context}})
}

func bugReport(g *errorGroup) string {
	return fmt.Sprintf("## %s\n\n**Fingerprint:** `%s`\n**Impact:** %d events, %d tenants, %d users, %d devices\n**First/last seen:** %s / %s\n**Release:** %s\n**Likely source:** `%s`\n\n### Error\n```\n%s\n```\n\n### Acceptance criteria\n- Reproduce or verify the failing path.\n- Add a regression test that fails before the fix.\n- Fix the root cause without suppressing unrelated errors.\n- Verify the fix against the affected release and tenant context.\n", g.Title, g.Fingerprint, g.Count, len(g.Tenants), len(g.Users), len(g.Devices), g.FirstSeen, g.LastSeen, g.Latest.Release, g.Culprit, g.Latest.Stack)
}
func stringValue(v any) string { value, _ := v.(string); return value }
