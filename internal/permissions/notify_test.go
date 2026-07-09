package permissions

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// TestPermissionsChangedNotify asserts the permissions_changed trigger (HOR-243)
// fires on permissions.policies changes AND on identity.identities changes (the
// cross-schema trigger, since effective capabilities read active identities — a
// soft-deleted identity loses its wildcard). Backfilled alongside HOR-327's
// api_keys NOTIFY test for consistent LISTEN/NOTIFY coverage. Requires Docker.
func TestPermissionsChangedNotify(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	pool, connStr := testutil.NewPostgres(t)
	ctx := context.Background()

	listenConn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listenConn.Close(ctx) })
	_, err = listenConn.Exec(ctx, "LISTEN permissions_changed")
	require.NoError(t, err)

	waitNotify := func() string {
		waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		n, err := listenConn.WaitForNotification(waitCtx)
		require.NoError(t, err)
		return n.Payload
	}

	// Policy insert/update/delete -> permissions_changed (payload = policy key).
	_, err = pool.Exec(ctx, `
		INSERT INTO permissions.policies (key, subject_kind, subject_key)
		VALUES ('default/notify-pol', 'user', 'default/x')`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-pol")

	_, err = pool.Exec(ctx, `UPDATE permissions.policies SET subject_key = 'default/y' WHERE key = 'default/notify-pol'`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-pol")

	_, err = pool.Exec(ctx, `DELETE FROM permissions.policies WHERE key = 'default/notify-pol'`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-pol")

	// Cross-schema: an identity.identities change also fires permissions_changed
	// (revocation invalidates capabilities).
	_, err = pool.Exec(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ('default/notify-id', 'user', 'external', 'Notify')`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-id")
}
