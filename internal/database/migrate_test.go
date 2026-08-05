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

	// HOR-243: permissions table + view should exist after MigrateUp.
	var policyTableExists bool
	err = pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'permissions' AND tablename = 'policies')",
	).Scan(&policyTableExists)
	require.NoError(t, err)
	assert.True(t, policyTableExists, "permissions.policies should exist after MigrateUp")

	var viewExists bool
	err = pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'permissions' AND viewname = 'effective_capabilities')",
	).Scan(&viewExists)
	require.NoError(t, err)
	assert.True(t, viewExists, "permissions.effective_capabilities view should exist after MigrateUp")

	// HOR-254 must migrate an existing gateway ledger without requiring an
	// unavailable customer-safe summary backfill. Roll back the three HOR-396
	// migrations plus HOR-397, HOR-399, and HOR-254, seed a pre-existing write
	// descriptor/invocation, and apply all six again.
	require.NoError(t, database.MigrateDown(connStr, 6))
	_, err = pool.Exec(ctx, `
		INSERT INTO toolgateway.tool_versions
		    (name,version,digest,description,input_schema,effect_class,credential_slots,artifact_capabilities,timeout_ms)
		VALUES ('legacy.write','1.0.0','sha256:legacy-write','Legacy write','{}','non_idempotent_write','[]','{}',1000);
		INSERT INTO toolgateway.runner_registrations
		    (runner_id,spiffe_id,namespace,tool_name,tool_version,tool_digest,fencing_generation)
		VALUES ('legacy-runner','spiffe://iterabase.local/tool-runners/legacy/runner','legacy','legacy.write','1.0.0','sha256:legacy-write',1);
		INSERT INTO toolgateway.invocations
		    (attempt_id,caller_scope,caller_scope_id,tool_call_id,tool_name,tool_version_digest,idempotency_key,effect_class)
		VALUES ('legacy-attempt','turn','legacy-turn','legacy-call','legacy.write','sha256:legacy-write','legacy-key','non_idempotent_write')`)
	require.NoError(t, err)
	require.NoError(t, database.MigrateUp(connStr))

	var legacySummaryMissing bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT consequence_summary IS NULL FROM toolgateway.invocations
		WHERE tool_call_id='legacy-call'`).Scan(&legacySummaryMissing))
	assert.True(t, legacySummaryMissing, "historical write invocation should survive without fabricated summary")

	_, err = pool.Exec(ctx, `
		INSERT INTO toolgateway.invocations
		    (attempt_id,caller_scope,caller_scope_id,tool_call_id,tool_name,tool_version_digest,idempotency_key,effect_class)
		VALUES ('new-attempt','turn','new-turn','new-call','legacy.write','sha256:legacy-write','new-key','non_idempotent_write')`)
	assert.Error(t, err, "new write invocations must include a consequence summary")

	var legacyVersionAvailable bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM toolgateway.available_tool_versions WHERE name='legacy.write')`).Scan(&legacyVersionAvailable))
	assert.False(t, legacyVersionAvailable, "legacy write descriptor without a template must not enter new snapshots")

	// HOR-254 graph/work domain exists after migration.
	for _, table := range []string{"work_items", "attempts", "blockers", "feedback", "timeline_events", "value_models", "value_ledger", "artifact_links"} {
		var exists bool
		err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'work' AND tablename = $1)", table,
		).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "work.%s should exist after MigrateUp", table)
	}
	var nodeExecutions bool
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='runtime' AND tablename='node_executions')").Scan(&nodeExecutions))
	assert.True(t, nodeExecutions)
	var artifacts bool
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='artifact' AND tablename='artifacts')").Scan(&artifacts))
	assert.True(t, artifacts)
	for table, column := range map[string]string{"work_items": "source_presentation", "attempts": "presentation_snapshot", "blockers": "response_presentation"} {
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema='work' AND table_name=$1 AND column_name=$2)`, table, column).Scan(&exists))
		assert.True(t, exists, "work.%s.%s should exist after HOR-396 migration", table, column)
	}
	for _, column := range []string{"business_label", "result_presentation"} {
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema='runtime' AND table_name='node_executions' AND column_name=$1)`, column).Scan(&exists))
		assert.True(t, exists, "runtime.node_executions.%s should exist after HOR-396 migration", column)
	}

	// Down: schemas should be gone.
	require.NoError(t, database.MigrateDown(connStr, 0))

	for _, schema := range []string{"identity", "permissions", "usage", "ai_data", "work", "artifact"} {
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
