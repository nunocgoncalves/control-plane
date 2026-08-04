package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// MinIOConfig is a ready integration-test bucket.
type MinIOConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
}

func NewMinIO(t *testing.T) MinIOConfig {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping MinIO integration test in short mode")
	}
	ctx := context.Background()
	const access = "artifact-test-access"
	const secret = "artifact-test-secret-key"
	container, err := testcontainers.Run(ctx, "minio/minio:RELEASE.2025-09-07T16-13-09Z",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{"MINIO_ROOT_USER": access, "MINIO_ROOT_PASSWORD": secret}),
		testcontainers.WithCmd("server", "/data"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/minio/health/ready").WithPort("9000/tcp")),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	cfg := MinIOConfig{Endpoint: fmt.Sprintf("%s:%s", host, port.Port()), AccessKey: access, SecretKey: secret, Bucket: "iterabase-artifacts"}
	client, err := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, "")})
	require.NoError(t, err)
	require.NoError(t, client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}))
	return cfg
}
