package dbpool

import "testing"

func TestLimitsFromEnvironmentDefaultsToSixteenActiveAndOneWarm(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_OPEN_CONNS", "")
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "")

	limits := LimitsFromEnvironment()
	if limits.MaxOpen != 16 {
		t.Fatalf("MaxOpen = %d, want 16", limits.MaxOpen)
	}
	if limits.MaxIdle != 1 {
		t.Fatalf("MaxIdle = %d, want 1", limits.MaxIdle)
	}
}

func TestLimitsFromEnvironmentAllowsExplicitPhysicalPoolBoundaries(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_OPEN_CONNS", "240")
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "120")

	limits := LimitsFromEnvironment()
	if limits.MaxOpen != 240 || limits.MaxIdle != 120 {
		t.Fatalf("LimitsFromEnvironment() = %+v, want MaxOpen=240 MaxIdle=120", limits)
	}
}

func TestLimitsFromEnvironmentClampsIdleToBoundedOpenLimit(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_OPEN_CONNS", "40")
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "100")

	limits := LimitsFromEnvironment()
	if limits.MaxOpen != 40 || limits.MaxIdle != 40 {
		t.Fatalf("LimitsFromEnvironment() = %+v, want MaxOpen=40 MaxIdle=40", limits)
	}
}

func TestLimitsFromEnvironmentDoesNotAllowUnlimitedOpenConnections(t *testing.T) {
	t.Setenv("GONVEX_DB_MAX_OPEN_CONNS", "0")
	t.Setenv("GONVEX_DB_MAX_IDLE_CONNS", "100")

	limits := LimitsFromEnvironment()
	if limits.MaxOpen != 16 || limits.MaxIdle != 16 {
		t.Fatalf("LimitsFromEnvironment() = %+v, want MaxOpen=16 MaxIdle=16", limits)
	}
}
