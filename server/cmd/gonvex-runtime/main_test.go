package main

import "testing"

func TestReloadSupervisorEnabledOnlyInGatewayProcess(t *testing.T) {
	t.Setenv("GONVEX_RELOAD_SUPERVISOR", "true")
	t.Setenv("GONVEX_RUNTIME_WORKER", "")
	if !reloadSupervisorEnabled() {
		t.Fatal("gateway process did not enable the reload supervisor")
	}

	t.Setenv("GONVEX_RUNTIME_WORKER", "1")
	if reloadSupervisorEnabled() {
		t.Fatal("worker recursively enabled the reload supervisor")
	}

	t.Setenv("GONVEX_RUNTIME_WORKER", "")
	t.Setenv("GONVEX_RELOAD_SUPERVISOR", "false")
	if reloadSupervisorEnabled() {
		t.Fatal("disabled reload supervisor was enabled")
	}
}
