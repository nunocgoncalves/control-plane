package artifact

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectStore is the byte boundary; callers never receive its credentials or
// endpoint. Implementations must consume the complete stream before Put
// returns and must not overwrite an existing key.
type ObjectStore interface {
	Put(context.Context, string, io.Reader, string) error
	Get(context.Context, string) (io.ReadCloser, error)
	Delete(context.Context, string) error
	Ready(context.Context) error
}

// MinIOConfig configures the dedicated artifact bucket credential.
type MinIOConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Secure    bool
}

type minioStore struct {
	client *minio.Client
	bucket string
}

func NewMinIOStore(cfg MinIOConfig) (ObjectStore, error) {
	if cfg.Endpoint == "" || cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Bucket == "" {
		return nil, fmt.Errorf("artifact minio endpoint, access key, secret key, and bucket are required")
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}
	return &minioStore{client: client, bucket: cfg.Bucket}, nil
}

func (m *minioStore) Ready(ctx context.Context) error {
	exists, err := m.client.BucketExists(ctx, m.bucket)
	if err != nil {
		return fmt.Errorf("check artifact bucket: %w", err)
	}
	if !exists {
		return fmt.Errorf("artifact bucket %q does not exist", m.bucket)
	}
	return nil
}

func (m *minioStore) Put(ctx context.Context, key string, r io.Reader, contentType string) error {
	// Every key contains a fresh artifact UUID. Stat first as a defensive
	// immutability backstop; the service never intentionally reuses a key.
	if _, err := m.client.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{}); err == nil {
		return fmt.Errorf("refusing to overwrite existing artifact object %q", key)
	} else {
		re := minio.ToErrorResponse(err)
		if re.Code != "NoSuchKey" && re.Code != "NoSuchObject" && re.StatusCode != 404 {
			return fmt.Errorf("check artifact object absence: %w", err)
		}
	}
	if _, err := m.client.PutObject(ctx, m.bucket, key, r, -1, minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return fmt.Errorf("put artifact object: %w", err)
	}
	return nil
}

func (m *minioStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := m.client.GetObject(ctx, m.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get artifact object: %w", err)
	}
	// GetObject is lazy; force the first request now so authorization/not-found
	// errors are returned before a response status is committed.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, fmt.Errorf("stat artifact object: %w", err)
	}
	return obj, nil
}

func (m *minioStore) Delete(ctx context.Context, key string) error {
	if err := m.client.RemoveObject(ctx, m.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete artifact object: %w", err)
	}
	return nil
}
