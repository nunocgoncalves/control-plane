package identity

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// TestAPIKeysChangedNotify asserts the api_keys_changed trigger (HOR-327) fires
// on identity.api_keys insert/update/delete, so the gateway (HOR-247) can LISTEN
// for API-key issue/revoke. Requires Docker.
func TestAPIKeysChangedNotify(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	pool, connStr := testutil.NewPostgres(t)
	ctx := context.Background()

	// A dedicated LISTENing connection (pool conns aren't held for LISTEN).
	listenConn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listenConn.Close(ctx) })
	_, err = listenConn.Exec(ctx, "LISTEN api_keys_changed")
	require.NoError(t, err)

	waitNotify := func() string {
		waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		n, err := listenConn.WaitForNotification(waitCtx)
		require.NoError(t, err)
		return n.Payload
	}

	// Insert an identity + api_key -> notification (payload carries key_hash).
	var id string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ('default/notify', 'user', 'external', 'Notify') RETURNING id`).Scan(&id))
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.api_keys (identity_id, key_hash, prefix, name, scope)
		VALUES ($1, 'hash-1', 'cp-x', 'k1', 'gateway')`, id)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "hash-1")

	// Update -> notification.
	_, err = pool.Exec(ctx, `UPDATE identity.api_keys SET name = 'k1-renamed' WHERE key_hash = 'hash-1'`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "hash-1")

	// Delete -> notification.
	_, err = pool.Exec(ctx, `DELETE FROM identity.api_keys WHERE key_hash = 'hash-1'`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "hash-1")
}
