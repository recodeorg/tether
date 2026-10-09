package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const testAudience = "my-app"

type testProvider struct {
	server         *httptest.Server
	key            *rsa.PrivateKey
	discoveryCalls atomic.Int32
	failDiscovery  atomic.Bool
}

func newTestProvider(t *testing.T) *testProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &testProvider{key: key}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		p.discoveryCalls.Add(1)
		if p.failDiscovery.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                p.server.URL,
			"jwks_uri":                              p.server.URL + "/jwks",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key:       &key.PublicKey,
			KeyID:     "test-key",
			Algorithm: "RS256",
			Use:       "sig",
		}}})
	})
	p.server = httptest.NewServer(mux)
	t.Cleanup(p.server.Close)
	return p
}

func (p *testProvider) claims(overrides map[string]any) map[string]any {
	claims := map[string]any{
		"iss": p.server.URL,
		"aud": testAudience,
		"sub": "user_123",
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	for k, v := range overrides {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}
	return claims
}

func signToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: "test-key"}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func (p *testProvider) token(t *testing.T, overrides map[string]any) string {
	t.Helper()
	return signToken(t, p.key, p.claims(overrides))
}

func TestVerifyTokenValid(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience}, WithLeeway(0))

	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	userID, expiresAt, err := a.VerifyToken(context.Background(), nil, p.token(t, map[string]any{"exp": exp.Unix()}))
	if err != nil {
		t.Fatalf("VerifyToken error = %v", err)
	}
	if userID != "user_123" {
		t.Errorf("userID = %q, want user_123", userID)
	}
	if !expiresAt.Equal(exp) {
		t.Errorf("expiresAt = %v, want %v", expiresAt, exp)
	}
}

func TestVerifyTokenBearerPrefix(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience})

	userID, _, err := a.VerifyToken(context.Background(), nil, "Bearer "+p.token(t, nil))
	if err != nil {
		t.Fatalf("VerifyToken error = %v", err)
	}
	if userID != "user_123" {
		t.Errorf("userID = %q, want user_123", userID)
	}
}

func TestVerifyTokenExpiresAtIncludesLeeway(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience}, WithLeeway(30*time.Second))

	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	_, expiresAt, err := a.VerifyToken(context.Background(), nil, p.token(t, map[string]any{"exp": exp.Unix()}))
	if err != nil {
		t.Fatalf("VerifyToken error = %v", err)
	}
	if want := exp.Add(30 * time.Second); !expiresAt.Equal(want) {
		t.Errorf("expiresAt = %v, want %v", expiresAt, want)
	}
}

func TestVerifyTokenRejected(t *testing.T) {
	p := newTestProvider(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"garbage", "not-a-jwt"},
		{"wrong audience", p.token(t, map[string]any{"aud": "other-app"})},
		{"wrong issuer", p.token(t, map[string]any{"iss": "https://evil.example.com"})},
		{"expired", p.token(t, map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})},
		{"missing exp", p.token(t, map[string]any{"exp": nil})},
		{"missing sub", p.token(t, map[string]any{"sub": nil})},
		{"wrong signing key", signToken(t, otherKey, p.claims(nil))},
	}

	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			userID, _, err := a.VerifyToken(context.Background(), nil, tt.token)
			if err == nil {
				t.Errorf("VerifyToken = %q, nil; want error", userID)
			}
		})
	}
}

func TestVerifyTokenExpiredWithinLeeway(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience}, WithLeeway(time.Minute))

	token := p.token(t, map[string]any{"exp": time.Now().Add(-10 * time.Second).Unix()})
	if _, _, err := a.VerifyToken(context.Background(), nil, token); err != nil {
		t.Errorf("VerifyToken error = %v, want token accepted within leeway", err)
	}
}

func TestVerifyTokenAllowAnonymous(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience}, AllowAnonymous(true))

	userID, expiresAt, err := a.VerifyToken(context.Background(), nil, "")
	if err != nil || userID != "" || !expiresAt.IsZero() {
		t.Errorf("VerifyToken = %q, %v, %v; want anonymous", userID, expiresAt, err)
	}
	if n := p.discoveryCalls.Load(); n != 0 {
		t.Errorf("discovery calls = %d, want 0 for anonymous", n)
	}
}

func TestVerifyTokenSkipAudienceCheck(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL}, SkipAudienceCheck(true))

	if _, _, err := a.VerifyToken(context.Background(), nil, p.token(t, map[string]any{"aud": "anything"})); err != nil {
		t.Errorf("VerifyToken error = %v", err)
	}
}

func TestVerifyTokenUserIDClaim(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience}, WithUserIDClaim("oid"))

	userID, _, err := a.VerifyToken(context.Background(), nil, p.token(t, map[string]any{"oid": "object-id"}))
	if err != nil {
		t.Fatalf("VerifyToken error = %v", err)
	}
	if userID != "object-id" {
		t.Errorf("userID = %q, want object-id", userID)
	}

	if _, _, err := a.VerifyToken(context.Background(), nil, p.token(t, nil)); err == nil {
		t.Error("VerifyToken without the configured claim succeeded, want error")
	}
	if _, _, err := a.VerifyToken(context.Background(), nil, p.token(t, map[string]any{"oid": 42})); err == nil {
		t.Error("VerifyToken with a non-string claim succeeded, want error")
	}
}

func TestVerifyTokenDiscoveryRetriedAndCached(t *testing.T) {
	p := newTestProvider(t)
	a := New(Config{IssuerURL: p.server.URL, Audience: testAudience})
	token := p.token(t, nil)

	p.failDiscovery.Store(true)
	if _, _, err := a.VerifyToken(context.Background(), nil, token); err == nil {
		t.Fatal("VerifyToken succeeded while discovery was failing")
	}

	p.failDiscovery.Store(false)
	for range 3 {
		if _, _, err := a.VerifyToken(context.Background(), nil, token); err != nil {
			t.Fatalf("VerifyToken error = %v", err)
		}
	}
	if n := p.discoveryCalls.Load(); n != 2 {
		t.Errorf("discovery calls = %d, want 2 (one failure, then cached)", n)
	}
}

func TestNewPanicsWithoutRequiredConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no issuer", Config{Audience: testAudience}},
		{"no audience", Config{IssuerURL: "https://issuer.example.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("New did not panic")
				}
			}()
			New(tt.cfg)
		})
	}
}
