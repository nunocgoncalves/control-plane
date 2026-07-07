package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/config"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/logging"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// captureStdout runs fn with os.Stdout replaced by a pipe and returns what was
// written. The bootstrap subcommand prints API keys to stdout exactly once.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() { os.Stdout = old }()

	runErr := fn()
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out), runErr
}

// extractAPIKeys pulls the "cp-..." tokens from bootstrap output.
func extractAPIKeys(out string) []string {
	var keys []string
	for _, line := range strings.Split(out, "\n") {
		if i := strings.Index(line, "cp-"); i >= 0 {
			keys = append(keys, strings.TrimSpace(line[i:]))
		}
	}
	return keys
}

// TestBootstrap creates the admin + a seeded service account, prints their keys
// once, and the printed keys authenticate with the correct scope. --reset
// revokes prior keys. Requires Docker.
func TestBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, connStr := testutil.NewPostgres(t)
	store := identity.NewStore(pool)
	ctx := context.Background()

	logger, _ := logging.New("error", "json") // quiet
	cfg := &config.Config{Database: config.DatabaseConfig{URL: connStr, MaxOpenConns: 5, MaxIdleConns: 2}}

	// First bootstrap: admin + agent-fleet SA.
	out, err := captureStdout(t, func() error {
		return runBootstrap(cfg, logger, []string{"--admin-email", "admin@local", "--service-account", "agent-fleet"})
	})
	require.NoError(t, err)
	keys := extractAPIKeys(out)
	require.Len(t, keys, 2, "expected admin + SA keys, got: %s", out)

	// Both keys authenticate with the right scope.
	adminKey, adminIdent, err := store.ValidateAPIKey(ctx, keys[0])
	require.NoError(t, err)
	assert.Equal(t, identity.ScopeAdmin, adminKey.Scope)
	assert.Equal(t, "admin@local", adminIdent.Key)

	saKey, saIdent, err := store.ValidateAPIKey(ctx, keys[1])
	require.NoError(t, err)
	assert.Equal(t, identity.ScopeToken, saKey.Scope)
	assert.Equal(t, "agent-fleet", saIdent.Key)

	// Re-running without --reset is idempotent (admin/SA already exist) but
	// issues ADDITIONAL keys; the originals remain valid.
	out2, err := captureStdout(t, func() error {
		return runBootstrap(cfg, logger, []string{"--admin-email", "admin@local", "--service-account", "agent-fleet"})
	})
	require.NoError(t, err)
	require.Len(t, extractAPIKeys(out2), 2)
	_, _, err = store.ValidateAPIKey(ctx, keys[0]) // original admin key still valid
	assert.NoError(t, err)

	// --reset revokes the prior keys and issues new ones.
	out3, err := captureStdout(t, func() error {
		return runBootstrap(cfg, logger, []string{"--admin-email", "admin@local", "--service-account", "agent-fleet", "--reset"})
	})
	require.NoError(t, err)
	newKeys := extractAPIKeys(out3)
	require.Len(t, newKeys, 2)

	_, _, err = store.ValidateAPIKey(ctx, keys[0]) // original admin key now revoked
	assert.ErrorIs(t, err, identity.ErrInvalidAPIKey)
	_, _, err = store.ValidateAPIKey(ctx, newKeys[0]) // new admin key valid
	assert.NoError(t, err)
}
