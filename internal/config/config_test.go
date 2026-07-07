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
