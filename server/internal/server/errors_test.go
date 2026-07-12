package server

import "testing"

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
