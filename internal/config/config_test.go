package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/config"
)

func TestLoad_EnvOnly(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/cp")

	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "postgres://localhost:5432/cp", cfg.Database.URL)
	assert.Equal(t, ":8080", cfg.API.Addr)
	assert.Equal(t, "info", cfg.Logging.Level)
	assert.Equal(t, "json", cfg.Logging.Format)
	assert.Equal(t, 25, cfg.Database.MaxOpenConns)
	assert.Equal(t, 10, cfg.Database.MaxIdleConns)
	// Defaults for new blocks.
	assert.Equal(t, "15m", cfg.JWT.TTL)
	assert.Equal(t, "enrolled", cfg.Identity.Mode)
}

func TestLoad_YAMLFile(t *testing.T) {
	content := `
api:
  addr: ":9090"
database:
  url: "${DATABASE_URL}"
  max_open_conns: 50
logging:
  level: debug
  format: text
jwt:
  signing_key_path: "/etc/jwt/key.pem"
  key_id: "k1"
  issuer: "cp"
  audience: "inference-gateway"
  ttl: "5m"
identity:
  mode: open
`
	dir := t.TempDir()
	path := filepath.Join(dir, "control-plane.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	t.Setenv("DATABASE_URL", "postgres://localhost:5432/cp")

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, ":9090", cfg.API.Addr)
	assert.Equal(t, "postgres://localhost:5432/cp", cfg.Database.URL)
	assert.Equal(t, 50, cfg.Database.MaxOpenConns)
	assert.Equal(t, "debug", cfg.Logging.Level)
	assert.Equal(t, "text", cfg.Logging.Format)
	assert.Equal(t, "/etc/jwt/key.pem", cfg.JWT.SigningKeyPath)
	assert.Equal(t, "k1", cfg.JWT.KeyID)
	assert.Equal(t, "5m", cfg.JWT.TTL)
	assert.Equal(t, "open", cfg.Identity.Mode)
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	content := `
database:
  url: "postgres://yaml-host:5432/cp"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "control-plane.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	t.Setenv("DATABASE_URL", "postgres://env-host:5432/cp")

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, "postgres://env-host:5432/cp", cfg.Database.URL)
}

func TestLoad_RequiresDatabaseURL(t *testing.T) {
	// No DATABASE_URL, no YAML.
	cfg, err := config.Load("")
	_ = cfg
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database.url")
}

func TestLoad_RejectsInvalidIdentityMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/cp")
	t.Setenv("IDENTITY_MODE", "permissive")

	_, err := config.Load("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity.mode")
}

func TestLoad_RejectsInvalidJWTTTL(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/cp")

	content := `
database:
  url: "postgres://localhost:5432/cp"
jwt:
  ttl: "not-a-duration"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "control-plane.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	_, err := config.Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jwt.ttl")
}

func TestLoad_NormalizesEmptyMode(t *testing.T) {
	content := `
database:
  url: "postgres://localhost:5432/cp"
identity:
  mode: ""
`
	dir := t.TempDir()
	path := filepath.Join(dir, "control-plane.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, "enrolled", cfg.Identity.Mode)
}

// TestLoad_DoesNotRequireServeFields confirms migrate/bootstrap can load config
// without api.addr or jwt.signing_key_path (serve-specific requirements are
// enforced separately by ValidateServe).
func TestLoad_DoesNotRequireServeFields(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/cp")
	t.Setenv("API_ADDR", "")

	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, ":8080", cfg.API.Addr) // default

	// Serve validation should require jwt.signing_key_path (empty here).
	err = config.ValidateServe(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jwt.signing_key_path")
}

func TestValidateServe_OK(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/cp")
	t.Setenv("JWT_SIGNING_KEY_PATH", "/etc/jwt/key.pem")

	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.NoError(t, config.ValidateServe(cfg))
}

func TestDatabaseFromEnv(t *testing.T) {
	t.Run("requires url", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "")
		_, err := config.DatabaseFromEnv()
		require.Error(t, err)
	})
	t.Run("ok", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://localhost:5432/cp")
		cfg, err := config.DatabaseFromEnv()
		require.NoError(t, err)
		assert.Equal(t, "postgres://localhost:5432/cp", cfg.URL)
		assert.Equal(t, 25, cfg.MaxOpenConns)
	})
}
