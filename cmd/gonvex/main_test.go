package main

import (
	"strings"
	"testing"
)

func TestDevDelegatesToTypeScriptCLI(t *testing.T) {
	err := run([]string{"dev"})
	if err == nil || !strings.Contains(err.Error(), "TypeScript CLI") {
		t.Fatalf("run(dev) = %v", err)
	}
}

func TestUnknownCommandFails(t *testing.T) {
	if err := run([]string{"bundle"}); err == nil {
		t.Fatal("run(bundle) unexpectedly succeeded")
	}
}
