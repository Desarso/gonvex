package server

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestNormalizeAppRedirectURI(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "https app", value: "https://app.example.com/auth/callback"},
		{name: "localhost http", value: "http://localhost:5173/auth/callback"},
		{name: "loopback http", value: "http://127.0.0.1:4173/auth/callback"},
		{name: "remote http", value: "http://app.example.com/auth/callback", wantErr: true},
		{name: "relative", value: "/auth/callback", wantErr: true},
		{name: "fragment", value: "https://app.example.com/auth/callback#token", wantErr: true},
		{name: "userinfo", value: "https://user@app.example.com/auth/callback", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeAppRedirectURI(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("normalizeAppRedirectURI(%q) error = %v", test.value, err)
			}
		})
	}
}

func TestPKCEChallengeMatchesRFC7636Example(t *testing.T) {
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	if got, want := pkceChallenge(verifier), "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"; got != want {
		t.Fatalf("challenge = %q, want %q", got, want)
	}
}

func TestAppAuthCodeExchangeCreatesProjectScopedSession(t *testing.T) {
	baseURL := tenantRegistryTestPostgresURL(t)
	controlURL := createTenantRegistryTestDatabase(t, baseURL, "gonvex_app_auth_"+tenantRegistryTestSuffix(t))
	runtime := New(config.Config{LandlordURL: controlURL, PostgresURL: baseURL})
	const projectID = "app-auth-project"
	if err := runtime.saveProjectRegistry(context.Background(), projectTarget{
		ID: projectID, Name: "Auth App", Environment: "test", Database: "auth_app",
		DatabaseMode: "single", StorageBucket: "", Status: "test", Description: "auth integration test",
		databaseURL: controlURL, databaseName: "auth_app", syncKey: "project-key",
	}); err != nil {
		t.Fatal(err)
	}
	const redirectURI = "http://localhost:5173/auth/callback"
	if err := runtime.enableGoogleAuth(context.Background(), projectID, redirectURI); err != nil {
		t.Fatal(err)
	}
	redirects, enabled, err := runtime.googleAuthConfiguration(context.Background(), projectID)
	if err != nil || !enabled || len(redirects) != 1 || redirects[0] != redirectURI {
		t.Fatalf("unexpected auth configuration: redirects=%v enabled=%v err=%v", redirects, enabled, err)
	}
	user, err := runtime.upsertAppAuthUser(context.Background(), projectID, googleIdentity{
		Subject: "google-subject", Email: "user@example.test", EmailVerified: true, Name: "Test User",
	})
	if err != nil {
		t.Fatal(err)
	}
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	code, err := runtime.createAppAuthCode(context.Background(), authTransaction{
		ProjectID: projectID, RedirectURI: redirectURI, CodeChallenge: pkceChallenge(verifier),
	}, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	accessToken, _, exchangedUser, err := runtime.exchangeAppAuthCode(context.Background(), projectID, code, verifier, redirectURI)
	if err != nil {
		t.Fatal(err)
	}
	if exchangedUser.ID != user.ID || !strings.HasPrefix(accessToken, "gvx_session_") {
		t.Fatalf("unexpected code exchange: user=%#v token=%q", exchangedUser, accessToken)
	}
	session, tenantID, err := runtime.validateAppSession(context.Background(), projectID, accessToken, "")
	if err != nil {
		t.Fatal(err)
	}
	if session.ProjectID != projectID || session.User.ID != user.ID || tenantID != projectID {
		t.Fatalf("unexpected validated session: %#v tenant=%q", session, tenantID)
	}
	if _, _, err := runtime.validateAppSession(context.Background(), "another-project", accessToken, ""); err == nil {
		t.Fatal("expected a cross-project session to be rejected")
	}
	if _, _, _, err := runtime.exchangeAppAuthCode(context.Background(), projectID, code, verifier, redirectURI); err == nil {
		t.Fatal("expected an authorization code replay to be rejected")
	}
}

func TestVerifyGoogleIDTokenChecksSignatureAudienceAndNonce(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	const keyID = "google-test-key"
	exponent := big.NewInt(int64(privateKey.PublicKey.E)).Bytes()
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Header().Set("cache-control", "public, max-age=60")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA",
			"kid": keyID,
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(privateKey.PublicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(exponent),
		}}})
	}))
	defer jwks.Close()

	runtime := &Server{config: config.Config{
		GoogleClientID: "gonvex-google-client",
		GoogleJWKSURL:  jwks.URL,
	}}
	claims := map[string]any{
		"iss":            "https://accounts.google.com",
		"aud":            "gonvex-google-client",
		"sub":            "google-user-123",
		"email":          "USER@example.com",
		"email_verified": true,
		"name":           "Test User",
		"nonce":          "nonce-123",
		"iat":            time.Now().Add(-time.Minute).Unix(),
		"exp":            time.Now().Add(time.Hour).Unix(),
	}
	token := signTestGoogleJWT(t, privateKey, keyID, claims)
	identity, err := runtime.verifyGoogleIDToken(context.Background(), token, "nonce-123")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Subject != "google-user-123" || identity.Email != "user@example.com" || !identity.EmailVerified {
		t.Fatalf("unexpected identity: %#v", identity)
	}
	if _, err := runtime.verifyGoogleIDToken(context.Background(), token, "different-nonce"); err == nil {
		t.Fatal("expected nonce mismatch to fail")
	}

	claims["aud"] = "another-client"
	wrongAudience := signTestGoogleJWT(t, privateKey, keyID, claims)
	if _, err := runtime.verifyGoogleIDToken(context.Background(), wrongAudience, "nonce-123"); err == nil {
		t.Fatal("expected audience mismatch to fail")
	}
}

func signTestGoogleJWT(t *testing.T, privateKey *rsa.PrivateKey, keyID string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := encodedHeader + "." + encodedPayload
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}
