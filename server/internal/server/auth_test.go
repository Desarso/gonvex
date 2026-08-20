package server

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestAuthenticateSocketWithoutControlPlaneUsesDevJWTSubject(t *testing.T) {
	server := New(config.Config{})
	token := devJWT(`{"sub":"firebase-user-123","email":"malek.gabriel33@gmail.com"}`)

	user, _, project, tenant, err := server.authenticateSocket(context.Background(), "whagons-5", "whagons-5", token, "calaluna")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != "firebase-user-123" {
		t.Fatalf("expected JWT subject user id, got %q", user.ID)
	}
	if user.Email != "malek.gabriel33@gmail.com" {
		t.Fatalf("expected JWT email, got %q", user.Email)
	}
	if project != "whagons-5" {
		t.Fatalf("expected requested project, got %q", project)
	}
	if tenant != "calaluna" {
		t.Fatalf("expected requested tenant, got %q", tenant)
	}
}

func TestAuthenticateSocketRequireAuthRejectsUnsignedDevJWT(t *testing.T) {
	server := New(config.Config{RequireAuth: true})
	token := devJWT(`{"sub":"legacy-user-123","email":"legacy@example.com"}`)

	if _, _, _, _, err := server.authenticateSocket(context.Background(), "whagons-5", "whagons-5", token, "calaluna"); err == nil {
		t.Fatal("required authentication accepted an unsigned legacy token")
	}
}

func devJWT(payload string) string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
}
