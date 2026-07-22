package gonvex

import (
	"context"
	"testing"
)

func TestContextWithSandboxIdentityRoundTrip(t *testing.T) {
	user := &User{ID: "firebase-abc", Email: "a@example.com"}
	perms := map[string]any{"create-tasks": true}
	ctx := ContextWithSandboxIdentity(context.Background(), user, perms)

	gotUser, gotPerms, ok := SandboxIdentityFromContext(ctx)
	if !ok {
		t.Fatal("expected sandbox identity")
	}
	if gotUser == nil || gotUser.ID != "firebase-abc" || gotUser.Email != "a@example.com" {
		t.Fatalf("user = %#v", gotUser)
	}
	if gotPerms["create-tasks"] != true {
		t.Fatalf("permissions = %#v", gotPerms)
	}
	if SandboxTenantFromContext(ctx) != "" {
		t.Fatalf("tenant should be empty without ContextWithSandboxSession, got %q", SandboxTenantFromContext(ctx))
	}
}

func TestContextWithSandboxSessionRoundTrip(t *testing.T) {
	user := &User{ID: "firebase-abc", Email: "a@example.com"}
	perms := map[string]any{"create-tasks": true}
	ctx := ContextWithSandboxSession(context.Background(), "tenant-testing3", user, perms)

	gotUser, gotPerms, ok := SandboxIdentityFromContext(ctx)
	if !ok {
		t.Fatal("expected sandbox identity")
	}
	if gotUser == nil || gotUser.ID != "firebase-abc" {
		t.Fatalf("user = %#v", gotUser)
	}
	if gotPerms["create-tasks"] != true {
		t.Fatalf("permissions = %#v", gotPerms)
	}
	if got := SandboxTenantFromContext(ctx); got != "tenant-testing3" {
		t.Fatalf("tenant = %q", got)
	}
}

func TestContextWithSandboxIdentityIgnoresEmptyUser(t *testing.T) {
	ctx := ContextWithSandboxIdentity(context.Background(), &User{ID: ""}, nil)
	if _, _, ok := SandboxIdentityFromContext(ctx); ok {
		t.Fatal("empty user id should not attach identity")
	}
	ctx = ContextWithSandboxIdentity(context.Background(), nil, nil)
	if _, _, ok := SandboxIdentityFromContext(ctx); ok {
		t.Fatal("nil user should not attach identity")
	}
	ctx = ContextWithSandboxSession(context.Background(), "tenant-a", nil, nil)
	if SandboxTenantFromContext(ctx) != "" {
		t.Fatal("tenant alone without user must not attach session")
	}
}

func TestSandboxIdentityFromContextMissing(t *testing.T) {
	if _, _, ok := SandboxIdentityFromContext(context.Background()); ok {
		t.Fatal("plain context should have no identity")
	}
	if _, _, ok := SandboxIdentityFromContext(nil); ok {
		t.Fatal("nil context should have no identity")
	}
	if SandboxTenantFromContext(nil) != "" {
		t.Fatal("nil context tenant should be empty")
	}
}
