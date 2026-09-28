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
	"github.com/recodeorg/tether/storage"
)

// Config configures an [S3Storage].
type Config struct {
	Region string
	Bucket string
	// Endpoint is the base URL of an S3-compatible service. Leave it empty
	// for Amazon S3. It must use HTTPS.
	Endpoint string
}

// S3Storage is a [storage.StorageAdapter] that stores each file as an object
// in a bucket, keyed by its file ID. Requests use path-style URLs.
type S3Storage struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
	defaults      []storage.UploadOption
}

// NewS3Storage returns an S3Storage for cfg. Credentials are loaded the
// standard AWS SDK way, from environment variables, shared config files or
// an instance role. It returns an error if cfg.Endpoint uses plain HTTP.
//
// defaults are applied to every upload URL from this adapter. Options passed
// to GetUploadURL are applied after them, in order. Nil options are ignored.
func NewS3Storage(ctx context.Context, cfg Config, defaults ...storage.UploadOption) (*S3Storage, error) {
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

	return &S3Storage{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucket:        cfg.Bucket,
		defaults:      append([]storage.UploadOption(nil), defaults...),
	}, nil
}

// UploadStream streams the request body to the object fileID without
// buffering it in memory.
func (s *S3Storage) UploadStream(ctx context.Context, fileID string, contentType string, r *http.Request) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fileID),
		Body:          r.Body,
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(r.ContentLength),
	})
	return err
}

// ServeFile redirects to a presigned URL for the object fileID that is valid
// for 15 minutes.
func (s *S3Storage) ServeFile(fileID string, w http.ResponseWriter, r *http.Request) error {
	// Generate a short-lived AWS URL
	presignedReq, err := s.presignClient.PresignGetObject(r.Context(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileID),
	}, s3.WithPresignExpires(15*time.Minute))

	if err != nil {
		return err
	}

	http.Redirect(w, r, presignedReq.URL, http.StatusFound)
	return nil
}

// Delete removes the object fileID.
func (s *S3Storage) Delete(fileID string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileID),
	})
	return err
}

// Name returns "tether/storage/s3".
func (s *S3Storage) Name() string {
	return "tether/storage/s3"
}

// DefaultUploadOptions returns the defaults passed to [NewS3Storage].
func (s *S3Storage) DefaultUploadOptions() []storage.UploadOption {
	return s.defaults
}
