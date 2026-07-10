package identity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// TestActiveAPIKeysView asserts the identity.active_api_keys view (HOR-334)
// exposes only key_hash + identity_id for non-revoked/non-expired keys, and
// that the `gateway` role (created by the bootstrap PG in prod) is granted
// SELECT on the views. The migration's grant is conditional on the role
// existing; here we create it + run the grant + assert the privilege.
func TestActiveAPIKeysView(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	pool := testutil.NewPostgresPool(t)
	ctx := context.Background()

	var id string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ('default/v', 'user', 'external', 'V') RETURNING id`).Scan(&id))
	_, err := pool.Exec(ctx, `
		INSERT INTO identity.api_keys (identity_id, key_hash, prefix, name, scope)
		VALUES ($1, 'active', 'cp-a', 'a', 'gateway')`, id)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.api_keys (identity_id, key_hash, prefix, name, scope, revoked_at)
		VALUES ($1, 'revoked', 'cp-r', 'r', 'gateway', now())`, id)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.api_keys (identity_id, key_hash, prefix, name, scope, expires_at)
		VALUES ($1, 'expired', 'cp-e', 'e', 'gateway', now() - interval '1 hour')`, id)
	require.NoError(t, err)

	// The view returns only the active key.
	rows, err := pool.Query(ctx, `SELECT key_hash, identity_id FROM identity.active_api_keys`)
	require.NoError(t, err)
	var got [][2]string
	for rows.Next() {
		var k, i string
		require.NoError(t, rows.Scan(&k, &i))
		got = append(got, [2]string{k, i})
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Len(t, got, 1)
	assert.Equal(t, "active", got[0][0])
	assert.Equal(t, id, got[0][1])

	// The `gateway` role (created by the bootstrap PG) is granted SELECT. The
	// migration's grant is conditional on the role existing (it didn't at migrate
	// time); create it + run the grant + assert the privilege.
	_, err = pool.Exec(ctx, `CREATE ROLE gateway`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		GRANT USAGE ON SCHEMA identity, permissions, catalog TO gateway;
		GRANT SELECT ON identity.active_api_keys, permissions.effective_capabilities, permissions.effective_rate_limits, catalog.effective_catalog TO gateway`)
	require.NoError(t, err)

	var hasSelect bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT has_table_privilege('gateway', 'identity.active_api_keys', 'SELECT')`).Scan(&hasSelect))
	assert.True(t, hasSelect, "gateway role must have SELECT on the active_api_keys view")
}
