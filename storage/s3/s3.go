package s3

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	Region   string
	Bucket   string
	Endpoint string
}

type S3Storage struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
}

func NewS3Storage(ctx context.Context, cfg Config) (*S3Storage, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	if cfg.Endpoint != "" {
		opts = append(opts, config.WithBaseEndpoint(cfg.Endpoint))
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
	}, nil
}

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

func (s *S3Storage) Delete(fileID string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileID),
	})
	return err
}
