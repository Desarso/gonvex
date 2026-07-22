package main

import "testing"

func TestAssertLoopbackTarget(t *testing.T) {
	for _, target := range []string{
		"http://localhost:8080",
		"ws://127.0.0.1:8080/ws",
		"http://127.12.0.4:8080",
		"ws://[::1]:8080/ws",
	} {
		if err := assertLoopbackTarget(target); err != nil {
			t.Fatalf("loopback target %q rejected: %v", target, err)
		}
	}
	if err := assertLoopbackTarget("https://runtime.example.com"); err == nil {
		t.Fatal("expected non-loopback target to be rejected")
	}
}

func TestVariableFlagRejectsMalformedValue(t *testing.T) {
	variables := variableFlag{}
	if err := variables.Set("workspaceId=abc"); err != nil {
		t.Fatalf("valid variable rejected: %v", err)
	}
	if variables["workspaceId"] != "abc" {
		t.Fatalf("variable was not stored: %#v", variables)
	}
	if err := variables.Set("missing-separator"); err == nil {
		t.Fatal("expected malformed variable to fail")
	}
}
