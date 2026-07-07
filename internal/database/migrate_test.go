package database_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/nunocgoncalves/control-plane/internal/database"
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
	require.NoError(t, database.MigrateUp(connStr))

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

	// HOR-242: identity tables should exist after MigrateUp.
	for _, table := range []string{"identities", "external_mappings", "local_users", "api_keys"} {
		var exists bool
		err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'identity' AND tablename = $1)", table,
		).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "identity.%s should exist after MigrateUp", table)
	}

	// Down: schemas should be gone.
	require.NoError(t, database.MigrateDown(connStr, 0))

	for _, schema := range []string{"identity", "permissions", "usage", "ai_data"} {
		var exists bool
		err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema,
		).Scan(&exists)
		require.NoError(t, err)
		assert.False(t, exists, "schema %q should be dropped after MigrateDown", schema)
	}
}

// TestIdentityConstraints exercises the identity schema against a real Postgres,
// validating CHECK constraints, unique bindings, and soft-delete behavior that
// the reconciler + identity service rely on. Requires Docker.
func TestIdentityConstraints(t *testing.T) {
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

	// Insert a CR-sourced identity.
	var id string
	err = pool.QueryRow(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ('default/alice', 'user', 'external', 'Alice Wong') RETURNING id`,
	).Scan(&id)
	require.NoError(t, err)

	// Bind two external IDs.
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.external_mappings (identity_id, provider, type, external_id) VALUES
		($1, 'teams', 'user', 'aad:aaaa-1111'),
		($1, 'slack', 'user', 'U012345ABCD')`, id)
	require.NoError(t, err)

	// Duplicate binding (same provider/type/external_id) must be rejected.
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.external_mappings (identity_id, provider, type, external_id)
		VALUES ($1, 'slack', 'user', 'U012345ABCD')`, id)
	assert.Error(t, err, "duplicate external binding should violate UNIQUE constraint")

	// Invalid kind must be rejected.
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.identities (key, kind, source) VALUES ('bad', 'robot', 'local')`)
	assert.Error(t, err, "invalid kind should violate CHECK constraint")

	// Invalid api_key scope must be rejected.
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.api_keys (identity_id, key_hash, prefix, scope)
		VALUES ($1, 'hash', 'cp-xxxx', 'superuser')`, id)
	assert.Error(t, err, "invalid scope should violate CHECK constraint")

	// Soft delete: set deleted_at; identity row persists for usage/history.
	_, err = pool.Exec(ctx, `UPDATE identity.identities SET deleted_at = now() WHERE key = 'default/alice'`)
	require.NoError(t, err)
	var stillThere bool
	err = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM identity.identities WHERE key = 'default/alice')`).Scan(&stillThere)
	require.NoError(t, err)
	assert.True(t, stillThere, "soft-deleted identity row should persist")

	// The updated_at trigger should fire on update.
	var updatedIsNull bool
	err = pool.QueryRow(ctx, `SELECT updated_at IS NULL FROM identity.identities WHERE key = 'default/alice'`).Scan(&updatedIsNull)
	require.NoError(t, err)
	assert.False(t, updatedIsNull, "updated_at should be set by trigger")
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
