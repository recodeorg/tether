package clerk

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

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const testIssuer = "https://clerk.example.com"

type testClerk struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	hits   atomic.Int32
}

func newTestClerk(t *testing.T) *testClerk {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	c := &testClerk{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		c.hits.Add(1)
		if r.Header.Get("Authorization") != "Bearer sk_test_123" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key:       &key.PublicKey,
			KeyID:     "test-key",
			Algorithm: "RS256",
			Use:       "sig",
		}}})
	})
	c.server = httptest.NewServer(mux)
	t.Cleanup(c.server.Close)
	return c
}

func (c *testClerk) adapter(t *testing.T, options ...ClerkOption) *ClerkAdapter {
	t.Helper()
	a := New(Config{SecretKey: "sk_test_123"}, options...)
	a.jwksClient.Backend = clerk.NewBackend(&clerk.BackendConfig{
		HTTPClient: c.server.Client(),
		URL:        clerk.String(c.server.URL),
		Key:        clerk.String("sk_test_123"),
	})
	return a
}

func (c *testClerk) token(t *testing.T, overrides map[string]any) string {
	t.Helper()
	claims := map[string]any{
		"iss": testIssuer,
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
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: c.key, KeyID: "test-key"}},
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

func TestVerifyTokenValid(t *testing.T) {
	c := newTestClerk(t)
	a := c.adapter(t, WithLeeway(0))

	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	userID, expiresAt, err := a.VerifyToken(context.Background(), nil, c.token(t, map[string]any{"exp": exp.Unix()}))
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
	c := newTestClerk(t)
	a := c.adapter(t)

	userID, _, err := a.VerifyToken(context.Background(), nil, "Bearer "+c.token(t, nil))
	if err != nil {
		t.Fatalf("VerifyToken error = %v", err)
	}
	if userID != "user_123" {
		t.Errorf("userID = %q, want user_123", userID)
	}
}

func TestVerifyTokenExpiresAtIncludesLeeway(t *testing.T) {
	c := newTestClerk(t)
	a := c.adapter(t, WithLeeway(30*time.Second))

	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	_, expiresAt, err := a.VerifyToken(context.Background(), nil, c.token(t, map[string]any{"exp": exp.Unix()}))
	if err != nil {
		t.Fatalf("VerifyToken error = %v", err)
	}
	if want := exp.Add(30 * time.Second); !expiresAt.Equal(want) {
		t.Errorf("expiresAt = %v, want %v", expiresAt, want)
	}
}

func TestVerifyTokenExpiredWithinLeeway(t *testing.T) {
	c := newTestClerk(t)
	a := c.adapter(t, WithLeeway(time.Minute))

	token := c.token(t, map[string]any{"exp": time.Now().Add(-10 * time.Second).Unix()})
	if _, _, err := a.VerifyToken(context.Background(), nil, token); err != nil {
		t.Errorf("VerifyToken error = %v, want token accepted within leeway", err)
	}
}

func TestVerifyTokenRejected(t *testing.T) {
	c := newTestClerk(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	other := &testClerk{key: otherKey}

	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"bearer empty", "Bearer "},
		{"garbage", "not-a-jwt"},
		{"expired", c.token(t, map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})},
		{"wrong issuer", c.token(t, map[string]any{"iss": "https://evil.example.com"})},
		{"wrong signing key", other.token(t, nil)},
	}
	a := c.adapter(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			userID, _, err := a.VerifyToken(context.Background(), nil, tt.token)
			if err == nil {
				t.Errorf("VerifyToken = %q, nil; want error", userID)
			}
		})
	}
}

func TestVerifyTokenAllowAnonymous(t *testing.T) {
	c := newTestClerk(t)
	a := c.adapter(t, AllowAnonymous(true))

	userID, expiresAt, err := a.VerifyToken(context.Background(), nil, "")
	if err != nil || userID != "" || !expiresAt.IsZero() {
		t.Errorf("VerifyToken = %q, %v, %v; want anonymous", userID, expiresAt, err)
	}
	userID, _, err = a.VerifyToken(context.Background(), nil, "Bearer ")
	if err != nil || userID != "" {
		t.Errorf("Bearer-empty VerifyToken = %q, %v; want anonymous", userID, err)
	}
	if n := c.hits.Load(); n != 0 {
		t.Errorf("jwks calls = %d, want 0 for anonymous", n)
	}

	closed := New(Config{SecretKey: "sk_test_123"})
	if _, _, err := closed.VerifyToken(context.Background(), nil, ""); err == nil {
		t.Error("empty token without AllowAnonymous succeeded, want error")
	}
}

func TestNewPanicsWithoutSecretKey(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New did not panic")
		}
	}()
	New(Config{})
}
