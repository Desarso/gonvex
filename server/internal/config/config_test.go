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

func TestAgentActionsAreOptInAndSeparatelyBounded(t *testing.T) {
	t.Setenv("GONVEX_AGENT_ACTIONS_ENABLED", "")
	t.Setenv("GONVEX_AGENT_ACTION_TIMEOUT", "")
	t.Setenv("GONVEX_AGENT_ACTION_CONCURRENCY", "")
	cfg := FromEnv()
	if cfg.AgentActionsEnabled {
		t.Fatal("agent Actions defaulted on")
	}
	if cfg.AgentActionTimeout != 2*time.Minute || cfg.AgentActionConcurrency != 4 {
		t.Fatalf("agent Action defaults = %s / %d", cfg.AgentActionTimeout, cfg.AgentActionConcurrency)
	}
}

func TestSandboxIsAnIndependentOptIn(t *testing.T) {
	t.Setenv("GONVEX_SANDBOX_ENABLED", "")
	t.Setenv("GONVEX_SANDBOX_ALLOW_UNCONFINED", "")
	cfg := FromEnv()
	if cfg.SandboxEnabled || cfg.SandboxAllowUnconfined {
		t.Fatal("sandbox or unconfined execution defaulted on")
	}
	if cfg.SandboxConcurrency != 2 || cfg.SandboxMaxTotal != 128 || cfg.SandboxMaxExecutions != 16 || cfg.SandboxDefaultTimeout != 30*time.Second || cfg.SandboxMaxRows != 500 {
		t.Fatalf("unexpected sandbox defaults: concurrency=%d total=%d executions=%d timeout=%s rows=%d", cfg.SandboxConcurrency, cfg.SandboxMaxTotal, cfg.SandboxMaxExecutions, cfg.SandboxDefaultTimeout, cfg.SandboxMaxRows)
	}
}

func TestValkeyURLRequiresCanonicalEnvironment(t *testing.T) {
	t.Setenv("VALKEY_URL", "")
	t.Setenv("REDIS_URL", "redis://redis.example:6379/4")
	if got := FromEnv().ValkeyURL; got != "" {
		t.Fatalf("legacy REDIS_URL configured ValkeyURL: %q", got)
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
