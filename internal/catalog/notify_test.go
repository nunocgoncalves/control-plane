package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// TestCatalogChangedNotify asserts the catalog_changed trigger (HOR-306/268)
// fires on catalog.backends and catalog.models changes, so the gateway (HOR-247)
// can LISTEN for catalog updates. Backfilled alongside HOR-327's api_keys NOTIFY
// test for consistent LISTEN/NOTIFY coverage. Requires Docker.
func TestCatalogChangedNotify(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	pool, connStr := testutil.NewPostgres(t)
	ctx := context.Background()

	listenConn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listenConn.Close(ctx) })
	_, err = listenConn.Exec(ctx, "LISTEN catalog_changed")
	require.NoError(t, err)

	waitNotify := func() string {
		waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		n, err := listenConn.WaitForNotification(waitCtx)
		require.NoError(t, err)
		return n.Payload
	}

	// Backend insert/update -> catalog_changed (payload = backend key).
	_, err = pool.Exec(ctx, `
		INSERT INTO catalog.backends (key, name, namespace, kind, service_url)
		VALUES ('default/notify-be', 'notify-be', 'default', 'vLLM', 'http://x')`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-be")

	_, err = pool.Exec(ctx, `UPDATE catalog.backends SET healthy = true WHERE key = 'default/notify-be'`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-be")

	_, err = pool.Exec(ctx, `DELETE FROM catalog.backends WHERE key = 'default/notify-be'`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-be")

	// Model insert/delete -> catalog_changed (payload = model key).
	_, err = pool.Exec(ctx, `
		INSERT INTO catalog.models (key, namespace, model_id, backend_ref)
		VALUES ('default/notify-md', 'default', 'notify-alias', 'notify-be')`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-md")

	_, err = pool.Exec(ctx, `DELETE FROM catalog.models WHERE key = 'default/notify-md'`)
	require.NoError(t, err)
	assert.Contains(t, waitNotify(), "default/notify-md")
}
