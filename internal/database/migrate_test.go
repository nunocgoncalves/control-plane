package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestMigrations applies the schema scaffold against a real pgvector Postgres
// container, asserts the four schemas + vector extension exist, then rolls
// them back. Requires Docker.
func TestMigrations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pgC, err := postgres.Run(ctx, "pgvector/pgvector:pg16",
		postgres.WithDatabase("controlplane"),
		postgres.WithUsername("cp"),
		postgres.WithPassword("cp"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// The default log-based wait can report ready before the server fully
	// accepts external connections, so poll until a real connection succeeds.
	pool := waitForPool(t, ctx, connStr)
	t.Cleanup(pool.Close)

	// Up: schemas + pgvector should exist.
	require.NoError(t, MigrateUp(connStr))

	for _, schema := range []string{"identity", "permissions", "usage", "ai_data"} {
		var exists bool
		err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema,
		).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "schema %q should exist after MigrateUp", schema)
	}

	var extExists bool
	err = pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')",
	).Scan(&extExists)
	require.NoError(t, err)
	assert.True(t, extExists, "pgvector extension should be installed after MigrateUp")

	// Down: schemas should be gone.
	require.NoError(t, MigrateDown(connStr, 0))

	for _, schema := range []string{"identity", "permissions", "usage", "ai_data"} {
		var exists bool
		err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema,
		).Scan(&exists)
		require.NoError(t, err)
		assert.False(t, exists, "schema %q should be dropped after MigrateDown", schema)
	}
}

// waitForPool retries connecting until the database accepts connections, then
// returns a ready pool. Fails the test if it never becomes ready.
func waitForPool(t *testing.T, ctx context.Context, connStr string) *pgxpool.Pool {
	t.Helper()
	var lastErr error
	for range 30 {
		pool, err := pgxpool.New(ctx, connStr)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				return pool
			}
			pool.Close()
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	require.NoError(t, fmt.Errorf("database not ready after 30s: %w", lastErr))
	return nil
}
