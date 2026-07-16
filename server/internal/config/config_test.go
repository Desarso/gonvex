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
