// Package oidc verifies OpenID Connect ID tokens for Tether.
//
// Pass the adapter from [New] to Engine.SetAuth. Discovery runs on the first
// token and is cached; a failed discovery is retried on the next token. The
// user ID defaults to the "sub" claim. The returned expiry includes the
// configured leeway.
package oidc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
)

const discoveryTimeout = 10 * time.Second

type oidcOptions struct {
	AllowAnonymous    bool
	Leeway            time.Duration
	UserIDClaim       string
	SkipAudienceCheck bool
}

// OIDCOption configures an adapter created with [New].
type OIDCOption func(*oidcOptions)

// AllowAnonymous, when true, treats an empty token as an anonymous caller.
// VerifyToken then returns an empty user ID, a zero expiry, and a nil error,
// without contacting the issuer. A token that is present is still verified.
func AllowAnonymous(allowAnonymous bool) OIDCOption {
	return func(opts *oidcOptions) {
		opts.AllowAnonymous = allowAnonymous
	}
}

// WithLeeway sets how much clock skew to allow when checking expiry. The
// default is five seconds. The expiry VerifyToken returns is the token's
// expiry plus this leeway.
func WithLeeway(leeway time.Duration) OIDCOption {
	return func(opts *oidcOptions) {
		opts.Leeway = leeway
	}
}

// WithUserIDClaim sets the token claim used as the user ID. It defaults to
// "sub". The claim must be a non-empty string.
func WithUserIDClaim(claim string) OIDCOption {
	return func(opts *oidcOptions) {
		opts.UserIDClaim = claim
	}
}

// SkipAudienceCheck accepts tokens regardless of their "aud" claim. Only use
// this if the issuer exclusively issues tokens meant for this application.
func SkipAudienceCheck(skip bool) OIDCOption {
	return func(opts *oidcOptions) {
		opts.SkipAudienceCheck = skip
	}
}

// Config identifies the provider whose ID tokens the adapter accepts.
type Config struct {
	// IssuerURL identifies the provider, e.g. "https://accounts.google.com".
	// The discovery document is fetched from
	// IssuerURL + "/.well-known/openid-configuration", and it must match the
	// token's "iss" claim exactly, including any trailing slash.
	IssuerURL string
	// Audience must be contained in the token's "aud" claim. This is usually
	// the client ID for ID tokens or the API identifier for access tokens.
	Audience string
}

// OIDCAdapter checks OpenID Connect ID tokens. It implements Tether's auth
// interface.
type OIDCAdapter struct {
	issuerURL      string
	verifierConfig *gooidc.Config
	allowAnonymous bool
	leeway         time.Duration
	userIDClaim    string

	mu       sync.Mutex
	verifier *gooidc.IDTokenVerifier
}

// New returns an adapter that verifies ID tokens from cfg.IssuerURL. It
// panics if IssuerURL is empty, and if Audience is empty unless
// [SkipAudienceCheck] is set.
//
// An empty token is rejected unless [AllowAnonymous] is set. The user ID is
// the "sub" claim unless [WithUserIDClaim] selects another claim. Expiry
// checks allow five seconds of clock skew unless [WithLeeway] says otherwise.
func New(cfg Config, options ...OIDCOption) *OIDCAdapter {
	if cfg.IssuerURL == "" {
		panic("issuer URL is required")
	}

	opts := &oidcOptions{
		AllowAnonymous: false,
		Leeway:         5 * time.Second,
		UserIDClaim:    "sub",
	}
	for _, option := range options {
		option(opts)
	}

	if cfg.Audience == "" && !opts.SkipAudienceCheck {
		panic("audience is required unless SkipAudienceCheck is set")
	}

	leeway := opts.Leeway
	return &OIDCAdapter{
		issuerURL: cfg.IssuerURL,
		verifierConfig: &gooidc.Config{
			ClientID:          cfg.Audience,
			SkipClientIDCheck: opts.SkipAudienceCheck,
			Now: func() time.Time {
				return time.Now().Add(-leeway)
			},
		},
		allowAnonymous: opts.AllowAnonymous,
		leeway:         leeway,
		userIDClaim:    opts.UserIDClaim,
	}
}

// getVerifier runs discovery on first use. Failures are not cached, so a
// temporarily unreachable issuer is retried on the next token.
func (a *OIDCAdapter) getVerifier(ctx context.Context) (*gooidc.IDTokenVerifier, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.verifier != nil {
		return a.verifier, nil
	}

	discCtx, cancel := context.WithTimeout(context.Background(), discoveryTimeout)
	defer cancel()

	provider, err := gooidc.NewProvider(discCtx, a.issuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}
	a.verifier = provider.Verifier(a.verifierConfig)
	return a.verifier, nil
}

// VerifyToken checks an ID token and returns the configured user-ID claim
// and the token's expiry. A "Bearer " prefix is stripped. The expiry includes
// the adapter's leeway. An empty token is an error unless the adapter was
// created with [AllowAnonymous].
func (a *OIDCAdapter) VerifyToken(ctx context.Context, db *gorm.DB, token string) (string, time.Time, error) {
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimPrefix(token, "Bearer ")
	}

	if token == "" {
		if a.allowAnonymous {
			return "", time.Time{}, nil
		}
		return "", time.Time{}, errors.New("empty token")
	}

	verifier, err := a.getVerifier(ctx)
	if err != nil {
		return "", time.Time{}, err
	}

	idToken, err := verifier.Verify(ctx, token)
	if err != nil {
		return "", time.Time{}, err
	}

	userID := idToken.Subject
	if a.userIDClaim != "sub" {
		var claims map[string]any
		if err := idToken.Claims(&claims); err != nil {
			return "", time.Time{}, err
		}
		userID, _ = claims[a.userIDClaim].(string)
	}
	if userID == "" {
		return "", time.Time{}, fmt.Errorf("token has no %q claim", a.userIDClaim)
	}

	return userID, idToken.Expiry.Add(a.leeway), nil
}
