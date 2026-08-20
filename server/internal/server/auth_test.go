package server

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestAuthenticateSocketWithoutConfiguredProviderRejectsUnsignedToken(t *testing.T) {
	server := New(config.Config{})
	token := devJWT(`{"sub":"forged-user-123","email":"malek.gabriel33@gmail.com"}`)

	if _, _, _, _, err := server.authenticateSocket(context.Background(), "whagons-5", "whagons-5", token, "calaluna"); err == nil {
		t.Fatal("unconfigured authentication accepted an unsigned token")
	}
}

func TestAuthenticateSocketAlwaysRequiresCanonicalSession(t *testing.T) {
	server := New(config.Config{})
	if _, _, _, _, err := server.authenticateSocket(context.Background(), "project", "", "", "tenant"); err == nil {
		t.Fatal("anonymous tenant entry was accepted")
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
