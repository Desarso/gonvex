package server

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
)

// firebaseTestSigner serves a JWKS for one generated key and mints tokens with
// it, so verification can be exercised without reaching Google.
type firebaseTestSigner struct {
	key   *rsa.PrivateKey
	keyID string
	jwks  *httptest.Server
}

func newFirebaseTestSigner(t *testing.T) *firebaseTestSigner {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer := &firebaseTestSigner{key: key, keyID: "test-key-1"}
	signer.jwks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		exponent := big.NewInt(int64(key.PublicKey.E)).Bytes()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]string{{
				"kty": "RSA",
				"kid": signer.keyID,
				"alg": "RS256",
				"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(exponent),
			}},
		})
	}))
	t.Cleanup(signer.jwks.Close)
	return signer
}

func (f *firebaseTestSigner) token(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": f.keyID, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, f.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func (f *firebaseTestSigner) server(project string) *Server {
	return New(config.Config{FirebaseProjectID: project, FirebaseJWKSURL: f.jwks.URL})
}

func validFirebaseClaims(project string) map[string]any {
	now := time.Now().Unix()
	return map[string]any{
		"iss":   "https://securetoken.google.com/" + project,
		"aud":   project,
		"sub":   "firebase-user-123",
		"email": "malek.gabriel33@gmail.com",
		"iat":   now - 60,
		"exp":   now + 3600,
	}
}

func TestVerifiedFirebaseTokenAuthenticates(t *testing.T) {
	signer := newFirebaseTestSigner(t)
	server := signer.server("whagons-5")

	user, _, project, tenant, err := server.authenticateSocket(
		context.Background(), "whagons-5", "whagons-5", signer.token(t, validFirebaseClaims("whagons-5")), "calaluna")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != "firebase-user-123" {
		t.Fatalf("expected verified subject, got %q", user.ID)
	}
	if user.Email != "malek.gabriel33@gmail.com" {
		t.Fatalf("expected verified email, got %q", user.Email)
	}
	if project != "whagons-5" || tenant != "calaluna" {
		t.Fatalf("expected requested project/tenant, got %q/%q", project, tenant)
	}
}

// Public unified runtimes keep authentication enforcement enabled globally,
// while legacy projects can opt into a trusted external identity provider.
// A verified Firebase token must remain usable in that configuration; it is
// not a Gonvex landlord session and therefore cannot be looked up in sessions.
func TestVerifiedFirebaseTokenAuthenticatesWhenRuntimeRequiresAuth(t *testing.T) {
	signer := newFirebaseTestSigner(t)
	server := New(config.Config{
		RequireAuth:       true,
		FirebaseProjectID: "whagons-5",
		FirebaseJWKSURL:   signer.jwks.URL,
	})

	user, _, project, tenant, err := server.authenticateSocket(
		context.Background(),
		"whagons-5",
		"whagons-5",
		signer.token(t, validFirebaseClaims("whagons-5")),
		"calaluna",
	)
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != "firebase-user-123" || user.Email != "malek.gabriel33@gmail.com" {
		t.Fatalf("expected verified Firebase user, got %#v", user)
	}
	if project != "whagons-5" || tenant != "calaluna" {
		t.Fatalf("expected requested project/tenant, got %q/%q", project, tenant)
	}
}

func TestFirebaseAuthenticationConfigurationIsProjectScoped(t *testing.T) {
	signer := newFirebaseTestSigner(t)
	server := New(config.Config{
		RequireAuth:     true,
		FirebaseJWKSURL: signer.jwks.URL,
	})
	server.projectEnvCache = map[string]projectEnvCacheEntry{
		"whagons-project": {
			values: map[string]string{
				"FIREBASE_SERVICE_ACCOUNT_KEY": `{"project_id":"whagons-5","private_key":"not-used-for-token-verification"}`,
			},
			fetchedAt: time.Now(),
		},
	}
	token := signer.token(t, validFirebaseClaims("whagons-5"))

	user, _, project, _, err := server.authenticateSocket(
		context.Background(), "whagons-project", "whagons-project", token, "calaluna")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != "firebase-user-123" || project != "whagons-project" {
		t.Fatalf("expected project-scoped Firebase identity, got user=%#v project=%q", user, project)
	}

	if _, _, _, _, err := server.authenticateSocket(
		context.Background(), "native-project", "native-project", token, "native-tenant"); err == nil {
		t.Fatal("expected the same Firebase token to be rejected for an unconfigured project")
	}
}

// The whole point of the change: an attacker-minted token that is merely
// well-formed must not authenticate as the user it names.
func TestForgedFirebaseTokenIsRejected(t *testing.T) {
	signer := newFirebaseTestSigner(t)
	server := signer.server("whagons-5")
	forged := devJWT(`{"sub":"firebase-user-123","email":"malek.gabriel33@gmail.com"}`)

	if _, _, _, _, err := server.authenticateSocket(context.Background(), "whagons-5", "whagons-5", forged, "calaluna"); err == nil {
		t.Fatal("expected a forged token to be rejected")
	}
}

func TestFirebaseTokenSignedByAnotherKeyIsRejected(t *testing.T) {
	signer := newFirebaseTestSigner(t)
	attacker := newFirebaseTestSigner(t)
	attacker.keyID = signer.keyID
	server := signer.server("whagons-5")

	token := attacker.token(t, validFirebaseClaims("whagons-5"))
	_, err := server.verifyFirebaseIDToken(context.Background(), "whagons-5", token)
	if err == nil || !strings.Contains(err.Error(), "signature is invalid") {
		t.Fatalf("expected a signature failure, got %v", err)
	}
}

func TestFirebaseTokenClaimsAreEnforced(t *testing.T) {
	signer := newFirebaseTestSigner(t)
	server := signer.server("whagons-5")
	now := time.Now().Unix()

	cases := []struct {
		name    string
		mutate  func(map[string]any)
		message string
	}{
		{"expired", func(c map[string]any) { c["exp"] = now - 1 }, "has expired"},
		{"other project audience", func(c map[string]any) { c["aud"] = "someone-elses-project" }, "audience is invalid"},
		{"other project issuer", func(c map[string]any) {
			c["iss"] = "https://securetoken.google.com/someone-elses-project"
		}, "issuer is invalid"},
		{"no subject", func(c map[string]any) { c["sub"] = "" }, "no subject"},
		{"issued in the future", func(c map[string]any) { c["iat"] = now + 3600 }, "issued in the future"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			claims := validFirebaseClaims("whagons-5")
			testCase.mutate(claims)
			_, err := server.verifyFirebaseIDToken(context.Background(), "whagons-5", signer.token(t, claims))
			if err == nil || !strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("expected %q, got %v", testCase.message, err)
			}
		})
	}
}

// A browser on the sign-in page connects before it has a token. That must fail
// authentication rather than silently becoming the synthetic "dev" user, while
// still leaving unauthenticated public queries to the project's own auth policy.
func TestTokenlessAuthDoesNotBecomeDevUser(t *testing.T) {
	signer := newFirebaseTestSigner(t)
	server := signer.server("whagons-5")

	_, _, _, _, err := server.authenticateSocket(context.Background(), "whagons-5", "whagons-5", "", "calaluna")
	if err == nil {
		t.Fatal("expected a token-less connect to be rejected")
	}
	if !strings.Contains(err.Error(), "authentication is required") {
		t.Fatalf("expected an authentication error, got %v", err)
	}
}

// Without the env var configured nothing changes, so existing local and
// self-hosted deployments keep working until they opt in.
func TestUnconfiguredFirebaseProjectKeepsLegacyBehavior(t *testing.T) {
	server := New(config.Config{})
	token := devJWT(fmt.Sprintf(`{"sub":%q}`, "firebase-user-123"))

	user, _, _, _, err := server.authenticateSocket(context.Background(), "whagons-5", "whagons-5", token, "calaluna")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != "firebase-user-123" {
		t.Fatalf("expected legacy unverified decode, got %q", user.ID)
	}
}
