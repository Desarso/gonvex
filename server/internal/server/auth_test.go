package server

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestAuthenticateSocketWithoutLandlordUsesDevJWTSubject(t *testing.T) {
	server := New(config.Config{})
	token := devJWT(`{"sub":"firebase-user-123","email":"malek.gabriel33@gmail.com"}`)

	user, _, tenant, err := server.authenticateSocket(context.Background(), "whagons-5", "whagons-5", token, "calaluna")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != "firebase-user-123" {
		t.Fatalf("expected JWT subject user id, got %q", user.ID)
	}
	if user.Email != "malek.gabriel33@gmail.com" {
		t.Fatalf("expected JWT email, got %q", user.Email)
	}
	if tenant != "calaluna" {
		t.Fatalf("expected requested tenant, got %q", tenant)
	}
}

func devJWT(payload string) string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
}
