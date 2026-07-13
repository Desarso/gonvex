package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestErrorTrackerGroupsVariableMessagesAndTracksImpact(t *testing.T) {
	tracker := newErrorTracker(100)
	fp1, ok := tracker.capture(capturedError{EventID: "one", Project: "shop", Tenant: "acme", Release: "1.2.0", Message: "order 123456 failed", Name: "Error", Culprit: "at checkout (src/cart.ts:20)", DeviceID: "laptop", User: map[string]any{"id": "u1"}})
	fp2, _ := tracker.capture(capturedError{EventID: "two", Project: "shop", Tenant: "beta", Release: "1.2.0", Message: "order 987654 failed", Name: "Error", Culprit: "at checkout (src/cart.ts:20)", DeviceID: "phone", User: map[string]any{"id": "u2"}})
	if !ok || fp1 != fp2 {
		t.Fatalf("expected one group, got %q and %q", fp1, fp2)
	}
	group := tracker.groups[fp1]
	if group.Count != 2 || len(group.Tenants) != 2 || len(group.Users) != 2 || len(group.Devices) != 2 {
		t.Fatalf("incorrect impact: %#v", group)
	}
	if _, duplicate := tracker.capture(capturedError{EventID: "two", Project: "shop", Message: "order 987654 failed"}); duplicate {
		t.Fatal("duplicate event accepted")
	}
}

func TestBugReportIsAgentReady(t *testing.T) {
	tracker := newErrorTracker(10)
	fp, _ := tracker.capture(capturedError{EventID: "one", Project: "shop", Message: "checkout failed", Stack: "Error: checkout failed\n at checkout (src/cart.ts:20)", Release: "2.0.0"})
	report := bugReport(tracker.groups[fp])
	if len(report) < 200 {
		t.Fatalf("bug report too small: %s", report)
	}
}

func TestFingerprintSurvivesBuildAndLineNumberChanges(t *testing.T) {
	first := capturedError{Project: "shop", Name: "TypeError", Message: "Cannot read properties of undefined (reading 'total')", Culprit: "at checkout (https://app.test/assets/index-A1b2C3.js:120:44)"}
	second := capturedError{Project: "shop", Name: "TypeError", Message: "Cannot read properties of undefined (reading 'total')", Culprit: "at checkout (https://app.test/assets/index-Z9y8X7.js:994:12)"}
	if fingerprint(first) != fingerprint(second) {
		t.Fatalf("expected release build frames to group together: %s != %s", fingerprint(first), fingerprint(second))
	}
}

func TestErrorEnvelopeRequiresProjectHeaderAndOverridesPayloadProject(t *testing.T) {
	server := New(config.Config{})
	server.projects["trusted"] = projectTarget{ID: "trusted", Name: "Trusted"}
	body := bytes.NewBufferString(`{"events":[{"eventId":"one","project":"spoofed","message":"boom"}]}`)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/errors/envelope", body))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected missing project header to fail, got %d", recorder.Code)
	}

	body = bytes.NewBufferString(`{"events":[{"eventId":"two","project":"spoofed","message":"boom"}]}`)
	request := httptest.NewRequest(http.MethodPost, "/errors/envelope", body)
	request.Header.Set("x-gonvex-project-id", "trusted")
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected accepted envelope, got %d: %s", recorder.Code, recorder.Body.String())
	}
	for _, group := range server.errorTracker.groups {
		if group.Project != "trusted" {
			t.Fatalf("payload selected project %q", group.Project)
		}
	}
	if !server.projects["trusted"].ErrorTrackingEnabled {
		t.Fatal("first accepted envelope did not enable project error tracking")
	}
}

func TestErrorTrackingRegistrationIsProjectScoped(t *testing.T) {
	server := New(config.Config{})
	server.projects["shop"] = projectTarget{ID: "shop", Name: "Shop"}
	server.projects["admin"] = projectTarget{ID: "admin", Name: "Admin"}

	request := httptest.NewRequest(http.MethodPost, "/errors/register", nil)
	request.Header.Set("x-gonvex-project-id", "shop")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("registration failed: %d %s", recorder.Code, recorder.Body.String())
	}
	if !server.projects["shop"].ErrorTrackingEnabled {
		t.Fatal("registration did not mark the project as enabled")
	}

	for projectID, want := range map[string]bool{"shop": true, "admin": false} {
		request = httptest.NewRequest(http.MethodGet, "/dev/errors/status", nil)
		request.Header.Set("x-gonvex-project-id", projectID)
		recorder = httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status for %s failed: %d %s", projectID, recorder.Code, recorder.Body.String())
		}
		var payload struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Enabled != want {
			t.Fatalf("project %s enabled=%v, want %v", projectID, payload.Enabled, want)
		}
	}
}

func TestErrorSchemaPersistsGroupsAndEvents(t *testing.T) {
	statements := []string{}
	db := &recordingDB{exec: func(query string, _ ...any) { statements = append(statements, query) }}
	if err := ensureErrorSchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	for _, table := range []string{"gonvex_error_events", "gonvex_error_groups"} {
		if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS "+table) {
			t.Fatalf("missing persistent %s schema in:\n%s", table, joined)
		}
	}
}

func TestSanitizeCapturedErrorFiltersSecretsInFieldsAndFreeText(t *testing.T) {
	event := sanitizeCapturedError(capturedError{
		Message: "request failed Authorization Bearer abc.def.ghi token=plain-secret",
		Context: map[string]any{"password": "hidden", "nested": map[string]any{"apiKey": "hidden-too"}},
	})
	if strings.Contains(event.Message, "abc.def.ghi") || strings.Contains(event.Message, "plain-secret") {
		t.Fatalf("free-text secret leaked: %q", event.Message)
	}
	if event.Context["password"] != filteredErrorValue {
		t.Fatalf("password was not filtered: %#v", event.Context)
	}
}

func TestErrorHTTPFlowGroupsTenantUserDeviceAndReleaseImpact(t *testing.T) {
	server := New(config.Config{})
	body := bytes.NewBufferString(`{"events":[
		{"eventId":"one","message":"task 123456 save failed","name":"Error","culprit":"at saveTask (src/tasks.ts:40:3)","tenant":"acme","release":"5.1.0+a","deviceId":"laptop","user":{"id":"ada"}},
		{"eventId":"two","message":"task 987654 save failed","name":"Error","culprit":"at saveTask (src/tasks.ts:99:8)","tenant":"beta","release":"5.1.1+b","deviceId":"phone","user":{"id":"grace"}}
	]}`)
	request := httptest.NewRequest(http.MethodPost, "/errors/envelope", body)
	request.Header.Set("x-gonvex-project-id", "whagons-5")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("ingestion failed: %d %s", recorder.Code, recorder.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/dev/errors/groups?status=unresolved", nil)
	request.Header.Set("x-gonvex-project-id", "whagons-5")
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("group listing failed: %d %s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Groups []*errorGroup `json:"groups"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Groups) != 1 {
		t.Fatalf("expected one root-cause group, got %d: %s", len(payload.Groups), recorder.Body.String())
	}
	group := payload.Groups[0]
	if group.Count != 2 || len(group.Tenants) != 2 || len(group.Users) != 2 || len(group.Devices) != 2 || len(group.Releases) != 2 {
		t.Fatalf("incorrect grouped impact: %#v", group)
	}
}
