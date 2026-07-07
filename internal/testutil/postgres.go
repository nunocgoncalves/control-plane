// Package testutil provides shared test helpers for control-plane integration
// tests (Postgres via testcontainers). It is a non-test package so it can be
// imported by _test packages in internal/identity, internal/controller, and
// internal/server.
package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/nunocgoncalves/control-plane/internal/database"
)

// NewPostgresPool starts a fresh pgvector Postgres container, applies all
// migrations, and returns a ready connection pool. It skips in -short mode.
// Requires Docker.
func NewPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
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

	pool := waitForPool(t, ctx, connStr)
	t.Cleanup(pool.Close)

	require.NoError(t, database.MigrateUp(connStr))
	return pool
}

// waitForPool retries connecting until the database accepts connections.
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
	require.NoError(t, lastErr)
	return nil
}
