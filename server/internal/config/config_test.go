package config

import (
	"testing"
	"time"
)

func TestDefaultRowsCacheTTLAllowsInvalidationToDriveFreshness(t *testing.T) {
	if defaultRowsCacheTTL != 10*time.Minute {
		t.Fatalf("defaultRowsCacheTTL = %s, want 10m", defaultRowsCacheTTL)
	}
}

func TestDefaultSubscriptionRerunConcurrency(t *testing.T) {
	t.Setenv("GONVEX_SUBSCRIPTION_RERUN_CONCURRENCY", "")
	if got := FromEnv().SubscriptionRerunConcurrency; got != 32 {
		t.Fatalf("SubscriptionRerunConcurrency = %d, want 32", got)
	}
}

func TestValkeyURLAcceptsRedisURLAlias(t *testing.T) {
	t.Setenv("VALKEY_URL", "")
	t.Setenv("REDIS_URL", "redis://redis.example:6379/4")
	if got := FromEnv().ValkeyURL; got != "redis://redis.example:6379/4" {
		t.Fatalf("ValkeyURL = %q", got)
	}

	t.Setenv("VALKEY_URL", "redis://valkey.example:6379/1")
	if got := FromEnv().ValkeyURL; got != "redis://valkey.example:6379/1" {
		t.Fatalf("VALKEY_URL did not take precedence: %q", got)
	}
}

func TestControlPlaneURLDoesNotReadLegacyLandlordEnvironment(t *testing.T) {
	t.Setenv("GONVEX_CONTROL_PLANE_DATABASE_URL", "")
	t.Setenv("GONVEX_CONTROL_PLANE_URL", "")
	t.Setenv("CONTROL_PLANE_DATABASE_URL", "")
	t.Setenv("CONTROL_PLANE_URL", "")
	t.Setenv("GONVEX_LANDLORD_DATABASE_URL", "postgres://legacy.example/landlord")
	t.Setenv("LANDLORD_DATABASE_URL", "postgres://legacy.example/landlord")
	if got := FromEnv().ControlPlaneURL; got != "" {
		t.Fatalf("legacy landlord environment configured the runtime Control Plane: %q", got)
	}

	t.Setenv("GONVEX_CONTROL_PLANE_DATABASE_URL", "  postgres://control.example/gonvex  ")
	if got := FromEnv().ControlPlaneURL; got != "postgres://control.example/gonvex" {
		t.Fatalf("ControlPlaneURL = %q", got)
	}
}
