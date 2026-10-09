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

type ClerkOption func(*clerkOptions)

func AllowAnonymous(allowAnonymous bool) ClerkOption {
	return func(opts *clerkOptions) {
		opts.AllowAnonymous = allowAnonymous
	}
}

func WithLeeway(leeway time.Duration) ClerkOption {
	return func(opts *clerkOptions) {
		opts.Leeway = leeway
	}
}

type Config struct {
	SecretKey string
}

type ClerkAdapter struct {
	jwksClient     *jwks.Client
	allowAnonymous bool
	leeway         time.Duration
}

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
