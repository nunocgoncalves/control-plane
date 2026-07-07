// Package config loads control-plane configuration from a YAML file with
// environment-variable overrides. Mirrors the inference-gateway pattern.
//
// Only the api binary loads YAML config today (it owns the database + HTTP
// address). The manager binary is flag-driven (controller-runtime norm);
// both share internal/logging.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the top-level control-plane configuration.
type Config struct {
	API      APIConfig      `yaml:"api"`
	Database DatabaseConfig `yaml:"database"`
	Logging  LoggingConfig  `yaml:"logging"`
}

// APIConfig configures the HTTP API server (cmd/api).
type APIConfig struct {
	Addr string `yaml:"addr"`
}

// DatabaseConfig configures the Postgres connection pool.
type DatabaseConfig struct {
	URL          string `yaml:"url"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

// LoggingConfig configures the shared slog logger.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Load reads configuration from a YAML file (if path is non-empty), expands
// environment variables in the file, applies env overrides, and validates.
// If path is empty, only defaults and env vars are used.
func Load(path string) (*Config, error) {
	cfg := defaults()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading config file: %w", err)
		}
		expanded := os.ExpandEnv(string(data))
		if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
			return nil, fmt.Errorf("parsing config file: %w", err)
		}
	}

	applyEnvOverrides(cfg)

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func defaults() *Config {
	return &Config{
		API:      APIConfig{Addr: ":8080"},
		Database: DatabaseConfig{MaxOpenConns: 25, MaxIdleConns: 10},
		Logging:  LoggingConfig{Level: "info", Format: "json"},
	}
}

// applyEnvOverrides lets critical values be set entirely via environment
// variables, taking precedence over the YAML file.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.Database.URL = v
	}
	if v := os.Getenv("API_ADDR"); v != "" {
		cfg.API.Addr = v
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Logging.Level = v
	}
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		cfg.Logging.Format = v
	}
}

func validate(cfg *Config) error {
	if cfg.Database.URL == "" {
		return fmt.Errorf("database.url (or DATABASE_URL) is required")
	}
	if cfg.API.Addr == "" {
		return fmt.Errorf("api.addr (or API_ADDR) is required")
	}
	return nil
}
