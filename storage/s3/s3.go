// Package s3 provides a storage adapter for Amazon S3 and S3-compatible
// services such as MinIO, Cloudflare R2 and DigitalOcean Spaces.
package s3

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config configures a [Storage].
type Config struct {
	Region string
	Bucket string
	// Endpoint is the base URL of an S3-compatible service. Leave it empty
	// for Amazon S3. It must use HTTPS.
	Endpoint string
}

// Storage is a storage adapter that stores each file as an object
// in a bucket, keyed by its file ID. Requests use path-style URLs.
type Storage struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
}

// New returns a Storage for cfg. Credentials are loaded the standard AWS
// SDK way, from environment variables, shared config files or an instance
// role. It returns an error if cfg.Endpoint uses plain HTTP.
func New(ctx context.Context, cfg Config) (*Storage, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	if cfg.Endpoint != "" {
		opts = append(opts, config.WithBaseEndpoint(cfg.Endpoint))
	}

	if strings.HasPrefix(cfg.Endpoint, "http://") {
		return nil, errors.New("tether: S3 custom endpoints must use HTTPS to support streaming uploads without memory buffering")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// Some S3-compatible providers (like MinIO or older DO Spaces) prefer Path-style URLs
		o.UsePathStyle = true
	})

	return &Storage{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucket:        cfg.Bucket,
	}, nil
}

// UploadStream streams r.Body to the object fileID without buffering it
// in memory. r.ContentLength is sent as the object size and can be -1
// for a chunked upload; the engine stores that same value as the file size.
func (s *Storage) UploadStream(ctx context.Context, fileID string, contentType string, r *http.Request) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fileID),
		Body:          r.Body,
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(r.ContentLength),
	})
	return err
}

// ServeFile redirects to a presigned URL for the object fileID. The engine
// stores the download token's expiry in the request context under
// "expires_at". The presigned URL lasts until one minute after that time,
// so it covers the token without outliving it for long. When the expiry is
// missing, that same one-minute buffer is applied to a 15-minute lifetime.
func (s *Storage) ServeFile(fileID string, w http.ResponseWriter, r *http.Request) error {
	ttl, ok := r.Context().Value("expires_at").(time.Time)
	presignedReq, err := s.presignClient.PresignGetObject(r.Context(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileID),
	}, s3.WithPresignExpires(downloadPresignTTL(ttl, ok, time.Now())))

	if err != nil {
		return err
	}

	http.Redirect(w, r, presignedReq.URL, http.StatusFound)
	return nil
}

// downloadPresignTTL is how long an S3 redirect stays valid. expiresAt is the
// download token's expiry when ok is true. The extra minute lets the client
// finish the redirect as the token reaches its limit. A missing expiry uses
// 15 minutes from now, plus that same minute.
func downloadPresignTTL(expiresAt time.Time, ok bool, now time.Time) time.Duration {
	if !ok {
		expiresAt = now.Add(15 * time.Minute)
	}
	return expiresAt.Add(time.Minute).Sub(now)
}

// Delete removes the object fileID.
func (s *Storage) Delete(ctx context.Context, fileID string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileID),
	})
	return err
}

// Name returns "tether/storage/s3".
func (s *Storage) Name() string {
	return "tether/storage/s3"
}
