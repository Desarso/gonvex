package landlord

import (
	"context"
	"testing"
)

func TestTokenHashIsStableSHA256Hex(t *testing.T) {
	got := tokenHash("secret-token")
	want := "930bbdc51b6aed5c2a5678fd6e28dee7a05e8a4b643cfc0b4427c3efb86c0d94"
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

func TestValidateSessionRequiresToken(t *testing.T) {
	_, err := ValidateSession(context.Background(), "postgres://example", "", "tenant-a")
	if err == nil {
		t.Fatal("expected missing token error")
	}
}

func TestValidateSessionRequiresLandlordDatabase(t *testing.T) {
	_, err := ValidateSession(context.Background(), "", "token", "tenant-a")
	if err == nil {
		t.Fatal("expected missing landlord database error")
	}
}
