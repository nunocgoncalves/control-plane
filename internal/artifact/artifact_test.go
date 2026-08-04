package artifact_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	artifactstore "github.com/nunocgoncalves/control-plane/internal/artifact"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

func TestArtifactMinIORoundTripImmutabilityAndDeletion(t *testing.T) {
	pool := testutil.NewPostgresPool(t)
	minioCfg := testutil.NewMinIO(t)
	ctx := context.Background()
	var creator string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO identity.identities (key,kind,source,display_name)
		VALUES ('artifact-user','user','local','Artifact User') RETURNING id::text`).Scan(&creator))
	objects, err := artifactstore.NewMinIOStore(artifactstore.MinIOConfig{
		Endpoint: minioCfg.Endpoint, AccessKey: minioCfg.AccessKey,
		SecretKey: minioCfg.SecretKey, Bucket: minioCfg.Bucket,
	})
	require.NoError(t, err)
	svc := artifactstore.NewService(artifactstore.NewStore(pool), objects, artifactstore.Config{MaxSize: 1024}, nil)
	require.NoError(t, svc.Ready(ctx))

	body := []byte(`{"result":"immutable"}`)
	a, err := svc.Upload(ctx, artifactstore.UploadInput{
		SourceType: artifactstore.SourceUserUpload, CreatedByIdentityID: creator,
		MIMEType: "application/json", ExpectedDigest: "sha256:5ce9fc889d59ef2ae8b395f8f7f4cb012c958591c92893631514743bf9f44bc1",
	}, bytes.NewReader(body))
	require.NoError(t, err)
	ref, err := a.Ref()
	require.NoError(t, err)
	assert.Equal(t, int64(len(body)), ref.SizeBytes)

	_, r, err := svc.Open(ctx, a.ID)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	r.Close()
	assert.Equal(t, body, got)

	// Identical bytes are still a new immutable artifact/reference; no dedupe or
	// overwrite silently rewrites history.
	second, err := svc.Upload(ctx, artifactstore.UploadInput{SourceType: artifactstore.SourceUserUpload, CreatedByIdentityID: creator, MIMEType: "application/json"}, bytes.NewReader(body))
	require.NoError(t, err)
	assert.NotEqual(t, a.ID, second.ID)
	assert.NotEqual(t, a.StorageKey, second.StorageKey)

	deleted, err := svc.Delete(ctx, a.ID, "test")
	require.NoError(t, err)
	assert.Equal(t, artifactstore.StateDeleted, deleted.State)
	_, _, err = svc.Open(ctx, a.ID)
	assert.ErrorIs(t, err, artifactstore.ErrNotAvailable)
	stat, err := svc.Stat(ctx, a.ID)
	require.NoError(t, err)
	assert.NotNil(t, stat.Digest, "metadata tombstone retains digest")
}

func TestArtifactRejectsDigestAndSizeMismatch(t *testing.T) {
	pool := testutil.NewPostgresPool(t)
	minioCfg := testutil.NewMinIO(t)
	ctx := context.Background()
	var creator string
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO identity.identities (key,kind,source) VALUES ('u','user','local') RETURNING id::text`).Scan(&creator))
	objects, err := artifactstore.NewMinIOStore(artifactstore.MinIOConfig{Endpoint: minioCfg.Endpoint, AccessKey: minioCfg.AccessKey, SecretKey: minioCfg.SecretKey, Bucket: minioCfg.Bucket})
	require.NoError(t, err)
	svc := artifactstore.NewService(artifactstore.NewStore(pool), objects, artifactstore.Config{MaxSize: 4}, nil)

	wrongSize := int64(2)
	_, err = svc.Upload(ctx, artifactstore.UploadInput{SourceType: artifactstore.SourceUserUpload, CreatedByIdentityID: creator, MIMEType: "text/plain", ExpectedSize: &wrongSize}, bytes.NewBufferString("one"))
	assert.ErrorIs(t, err, artifactstore.ErrSize)
	_, err = svc.Upload(ctx, artifactstore.UploadInput{SourceType: artifactstore.SourceUserUpload, CreatedByIdentityID: creator, MIMEType: "text/plain", ExpectedDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}, bytes.NewBufferString("one"))
	assert.ErrorIs(t, err, artifactstore.ErrDigest)
	_, err = svc.Upload(ctx, artifactstore.UploadInput{SourceType: artifactstore.SourceUserUpload, CreatedByIdentityID: creator, MIMEType: "text/plain"}, bytes.NewBufferString("oversized"))
	assert.ErrorIs(t, err, artifactstore.ErrTooLarge)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM artifact.artifacts`).Scan(&count))
	assert.Zero(t, count, "failed uploads leave no visible metadata")
}
