// Package clerk verifies Clerk session tokens for Tether.
//
// Pass the adapter from [New] to Engine.SetAuth. A verified token's user ID
// is its "sub" claim. Expiry includes the configured leeway, so the identity
// stays valid until the token has been expired for that long.
package clerk

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/clerk/clerk-sdk-go/v2"
	"github.com/clerk/clerk-sdk-go/v2/jwks"
	"github.com/clerk/clerk-sdk-go/v2/jwt"
)

type clerkOptions struct {
	AllowAnonymous bool
	Leeway         time.Duration
}

// ClerkOption configures an adapter created with [New].
type ClerkOption func(*clerkOptions)

// AllowAnonymous, when true, treats an empty token as an anonymous caller.
// VerifyToken then returns an empty user ID, a zero expiry, and a nil error.
// A token that is present is still verified.
func AllowAnonymous(allowAnonymous bool) ClerkOption {
	return func(opts *clerkOptions) {
		opts.AllowAnonymous = allowAnonymous
	}
}

// WithLeeway sets how much clock skew to allow when checking expiry. The
// default is five seconds. The expiry VerifyToken returns is the token's
// expiry plus this leeway.
func WithLeeway(leeway time.Duration) ClerkOption {
	return func(opts *clerkOptions) {
		opts.Leeway = leeway
	}
}

// Config identifies the Clerk instance whose session tokens the adapter accepts.
type Config struct {
	// SecretKey is the Clerk secret key, used to fetch the instance's JSON
	// Web Key Set. New panics when it is empty.
	SecretKey string
}

// ClerkAdapter checks Clerk session JWTs. It implements Tether's auth
// interface.
type ClerkAdapter struct {
	jwksClient     *jwks.Client
	allowAnonymous bool
	leeway         time.Duration
}

// New returns an adapter that verifies Clerk session tokens. It panics if
// cfg.SecretKey is empty.
//
// An empty token is rejected unless [AllowAnonymous] is set. Expiry checks
// allow five seconds of clock skew unless [WithLeeway] says otherwise.
func New(cfg Config, options ...ClerkOption) *ClerkAdapter {
	clientConfig := &clerk.ClientConfig{}
	if cfg.SecretKey == "" {
		panic("secret key is required")
	}
	clientConfig.Key = clerk.String(cfg.SecretKey)

	opts := &clerkOptions{
		AllowAnonymous: false,
		Leeway:         5 * time.Second,
	}
	for _, option := range options {
		option(opts)
	}

	return &ClerkAdapter{
		jwksClient:     jwks.NewClient(clientConfig),
		allowAnonymous: opts.AllowAnonymous,
		leeway:         opts.Leeway,
	}
}

// VerifyToken checks a Clerk session token and returns its subject and
// expiry. A "Bearer " prefix is stripped. The expiry includes the adapter's
// leeway. An empty token is an error unless the adapter was created with
// [AllowAnonymous].
func (a *ClerkAdapter) VerifyToken(ctx context.Context, db *gorm.DB, token string) (string, time.Time, error) {
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimPrefix(token, "Bearer ")
	}

	if token == "" {
		if a.allowAnonymous {
			return "", time.Time{}, nil
		}
		return "", time.Time{}, errors.New("empty token")
	}

	unsafeClaims, err := jwt.Decode(ctx, &jwt.DecodeParams{
		Token: token,
	})
	if err != nil {
		return "", time.Time{}, err
	}

	jwk, err := jwt.GetJSONWebKey(ctx, &jwt.GetJSONWebKeyParams{
		KeyID:      unsafeClaims.KeyID,
		JWKSClient: a.jwksClient,
	})
	if err != nil {
		return "", time.Time{}, err
	}

	claims, err := jwt.Verify(ctx, &jwt.VerifyParams{
		Token:  token,
		JWK:    jwk,
		Leeway: a.leeway,
	})
	if err != nil {
		return "", time.Time{}, err
	}

	var expiresAt time.Time
	if claims.Expiry != nil {
		expiresAt = time.Unix(*claims.Expiry, 0).Add(a.leeway)
	}

	return claims.Subject, expiresAt, nil
}
