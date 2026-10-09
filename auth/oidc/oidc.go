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

type OIDCOption func(*oidcOptions)

func AllowAnonymous(allowAnonymous bool) OIDCOption {
	return func(opts *oidcOptions) {
		opts.AllowAnonymous = allowAnonymous
	}
}

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

type OIDCAdapter struct {
	issuerURL      string
	verifierConfig *gooidc.Config
	allowAnonymous bool
	leeway         time.Duration
	userIDClaim    string

	mu       sync.Mutex
	verifier *gooidc.IDTokenVerifier
}

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
