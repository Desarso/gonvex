package storage

import (
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

func TestObjectKeyNamespacing(t *testing.T) {
	tenant := &Tenant{projectID: "whagons", tenantID: "acme"}
	got := tenant.objectKey("abc123")
	if got != "whagons/acme/abc123" {
		t.Fatalf("objectKey = %q, want whagons/acme/abc123", got)
	}

	// Missing project/tenant should collapse gracefully, never produce a
	// leading slash or empty segments.
	bare := &Tenant{}
	if got := bare.objectKey("abc123"); got != "abc123" {
		t.Fatalf("bare objectKey = %q, want abc123", got)
	}
}

func TestSanitizeSegment(t *testing.T) {
	cases := map[string]string{
		"acme":            "acme",
		"acme corp":       "acme_corp",
		"../etc/passwd":   ".._etc_passwd",
		"weird/../slash":  "weird_.._slash",
		"  trim me  ":     "trim_me",
		"UPPER-lower_1.2": "UPPER-lower_1.2",
		"..":              "",
		".":               "",
	}
	for in, want := range cases {
		if got := sanitizeSegment(in); got != want {
			t.Errorf("sanitizeSegment(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeVisibility(t *testing.T) {
	if got := normalizeVisibility(""); got != gonvex.FileVisibilityPrivate {
		t.Errorf("empty visibility = %q, want private", got)
	}
	if got := normalizeVisibility("bogus"); got != gonvex.FileVisibilityPrivate {
		t.Errorf("bogus visibility = %q, want private", got)
	}
	if got := normalizeVisibility(gonvex.FileVisibilityPublic); got != gonvex.FileVisibilityPublic {
		t.Errorf("public visibility = %q, want public", got)
	}
}

func TestNewFileIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id, err := newFileID()
		if err != nil {
			t.Fatalf("newFileID: %v", err)
		}
		if len(id) != 32 {
			t.Fatalf("file id length = %d, want 32", len(id))
		}
		if seen[id] {
			t.Fatalf("duplicate file id %q", id)
		}
		seen[id] = true
	}
}

// TestFactoryNilWhenUnconfigured verifies the runtime falls back to the
// not-configured path when S3 settings are absent.
func TestFactoryNilWhenUnconfigured(t *testing.T) {
	if f := NewFactory(Config{}); f != nil {
		t.Fatalf("expected nil factory for empty config")
	}
	if f := NewFactory(Config{Endpoint: "http://localhost:9000", Bucket: "b"}); f != nil {
		t.Fatalf("expected nil factory without credentials")
	}
	full := Config{
		Endpoint: "http://localhost:9000", Bucket: "b",
		AccessKeyID: "k", SecretAccessKey: "s",
	}
	if f := NewFactory(full); f == nil {
		t.Fatalf("expected non-nil factory for full config")
	}
}
