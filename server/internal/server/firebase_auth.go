package server

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// Firebase ID tokens are RS256 and signed by Google's securetoken service. The
// keys are published as JWKs, so the same shape as the Google OAuth JWKS is
// reused — a separate cache because it is a different endpoint with its own
// rotation schedule.

// firebaseProjectID resolves external authentication per runtime project. The
// process-level setting remains a fallback for older single-project installs.
func (s *Server) firebaseProjectID(ctx context.Context, runtimeProjectID string) string {
	projectEnv := s.projectEnvValues(ctx, runtimeProjectID)
	if configured := strings.TrimSpace(projectEnv["GONVEX_FIREBASE_PROJECT_ID"]); configured != "" {
		return configured
	}
	if configured := firebaseProjectIDFromServiceAccount(projectEnv["FIREBASE_SERVICE_ACCOUNT_KEY"]); configured != "" {
		return configured
	}
	return strings.TrimSpace(s.config.FirebaseProjectID)
}

func firebaseProjectIDFromServiceAccount(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	candidates := [][]byte{[]byte(raw)}
	if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil {
		candidates = append(candidates, decoded)
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(raw); err == nil {
		candidates = append(candidates, decoded)
	}
	for _, candidate := range candidates {
		var account struct {
			ProjectID string `json:"project_id"`
		}
		if json.Unmarshal(candidate, &account) == nil {
			if projectID := strings.TrimSpace(account.ProjectID); projectID != "" {
				return projectID
			}
		}
	}
	return ""
}

// firebaseIdentityFromToken turns a browser-presented Firebase ID token into an
// authenticated user. When the project has no Firebase project configured it
// preserves the legacy local-development behavior of trusting token claims.
func (s *Server) firebaseIdentityFromToken(ctx context.Context, firebaseProjectID string, token string) (*gonvex.User, error) {
	if firebaseProjectID == "" {
		return devUserFromJWT(token), nil
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("authentication is required")
	}
	return s.verifyFirebaseIDToken(ctx, firebaseProjectID, token)
}

func (s *Server) verifyFirebaseIDToken(ctx context.Context, firebaseProjectID string, token string) (*gonvex.User, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("the Firebase ID token is malformed")
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if err := decodeJWTPart(parts[0], &header); err != nil || header.Algorithm != "RS256" || header.KeyID == "" {
		return nil, fmt.Errorf("the Firebase ID token has an invalid header")
	}
	key, err := s.firebasePublicKey(ctx, header.KeyID, false)
	if err != nil {
		return nil, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("the Firebase ID token has an invalid signature")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		// A key can rotate before its cache TTL expires. Refresh once first.
		refreshed, refreshErr := s.firebasePublicKey(ctx, header.KeyID, true)
		if refreshErr != nil || rsa.VerifyPKCS1v15(refreshed, crypto.SHA256, digest[:], signature) != nil {
			return nil, fmt.Errorf("the Firebase ID token signature is invalid")
		}
	}
	var claims struct {
		Issuer    string `json:"iss"`
		Audience  string `json:"aud"`
		Subject   string `json:"sub"`
		Email     string `json:"email"`
		ExpiresAt int64  `json:"exp"`
		IssuedAt  int64  `json:"iat"`
	}
	if err := decodeJWTPart(parts[1], &claims); err != nil {
		return nil, fmt.Errorf("the Firebase ID token claims are malformed")
	}
	project := strings.TrimSpace(firebaseProjectID)
	now := time.Now().Unix()
	if claims.Issuer != "https://securetoken.google.com/"+project {
		return nil, fmt.Errorf("the Firebase ID token issuer is invalid")
	}
	if claims.Audience != project {
		return nil, fmt.Errorf("the Firebase ID token audience is invalid")
	}
	if strings.TrimSpace(claims.Subject) == "" {
		return nil, fmt.Errorf("the Firebase ID token has no subject")
	}
	if claims.ExpiresAt <= now {
		return nil, fmt.Errorf("the Firebase ID token has expired")
	}
	// Small forward tolerance for clock skew between Google and this runtime.
	if claims.IssuedAt > now+300 {
		return nil, fmt.Errorf("the Firebase ID token was issued in the future")
	}
	return &gonvex.User{
		ID:    strings.TrimSpace(claims.Subject),
		Email: strings.ToLower(strings.TrimSpace(claims.Email)),
	}, nil
}

func (s *Server) firebasePublicKey(ctx context.Context, keyID string, forceRefresh bool) (*rsa.PublicKey, error) {
	s.firebaseKeys.mu.Lock()
	defer s.firebaseKeys.mu.Unlock()
	if !forceRefresh && time.Now().Before(s.firebaseKeys.expiresAt) {
		if key := s.firebaseKeys.keys[keyID]; key != nil {
			return key, nil
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.config.FirebaseJWKSURL, nil)
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("the Firebase JWKS returned %d", response.StatusCode)
	}
	var document struct {
		Keys []struct {
			KeyType   string `json:"kty"`
			KeyID     string `json:"kid"`
			Algorithm string `json:"alg"`
			Modulus   string `json:"n"`
			Exponent  string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return nil, err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, item := range document.Keys {
		if item.KeyType != "RSA" || item.KeyID == "" || (item.Algorithm != "" && item.Algorithm != "RS256") {
			continue
		}
		modulus, err := base64.RawURLEncoding.DecodeString(item.Modulus)
		if err != nil {
			continue
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(item.Exponent)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
			continue
		}
		exponent := 0
		for _, value := range exponentBytes {
			exponent = exponent<<8 | int(value)
		}
		if exponent < 3 {
			continue
		}
		keys[item.KeyID] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}
	}
	s.firebaseKeys.keys = keys
	s.firebaseKeys.expiresAt = time.Now().Add(jwksCacheTTL(response.Header.Get("cache-control")))
	key := keys[keyID]
	if key == nil {
		return nil, fmt.Errorf("the Firebase signing key %q was not found", keyID)
	}
	return key, nil
}
