// Package config loads control-plane configuration from a YAML file with
// environment-variable overrides. Mirrors the inference-gateway pattern.
//
// Only the api binary loads YAML config (it owns the database, HTTP address,
// JWT signing key, and identity mode). The manager binary is flag-driven
// (controller-runtime norm) and reads only DATABASE_URL from the environment;
// both share internal/logging.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level control-plane configuration.
type Config struct {
	API      APIConfig      `yaml:"api"`
	Database DatabaseConfig `yaml:"database"`
	Logging  LoggingConfig  `yaml:"logging"`
	JWT      JWTConfig      `yaml:"jwt"`
	Identity IdentityConfig `yaml:"identity"`
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

// JWTConfig configures RS256 JWT issuance + JWKS publishing (cmd/api only).
// The signing key is an RSA private key PEM, mounted from a Kubernetes Secret.
type JWTConfig struct {
	SigningKeyPath string `yaml:"signing_key_path"`
	KeyID          string `yaml:"key_id"`
	Issuer         string `yaml:"issuer"`
	Audience       string `yaml:"audience"`
	TTL            string `yaml:"ttl"` // Go duration string, e.g. "15m"
}

// IdentityConfig configures the identity-resolution mode.
type IdentityConfig struct {
	Mode string `yaml:"mode"` // enrolled (default) | open (deferred, HOR-313)
}

// Load reads configuration from a YAML file (if path is non-empty), expands
// environment variables in the file, applies env overrides, and validates.
// If path is empty, only defaults and env vars are used.
//
// Validation enforces only fields common to every subcommand (database.url +
// field formats). Serve-specific requirements (api.addr, jwt.signing_key_path)
// are checked by the caller (runServe), so migrate/bootstrap need only the DB.
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

// DatabaseFromEnv builds a DatabaseConfig from environment variables for
// binaries that do not load YAML (the manager). DATABASE_URL is required.
func DatabaseFromEnv() (DatabaseConfig, error) {
	cfg := DatabaseConfig{
		URL:          os.Getenv("DATABASE_URL"),
		MaxOpenConns: 25,
		MaxIdleConns: 10,
	}
	if cfg.URL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		API:      APIConfig{Addr: ":8080"},
		Database: DatabaseConfig{MaxOpenConns: 25, MaxIdleConns: 10},
		Logging:  LoggingConfig{Level: "info", Format: "json"},
		JWT:      JWTConfig{TTL: "15m"},
		Identity: IdentityConfig{Mode: "enrolled"},
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
	if v := os.Getenv("JWT_SIGNING_KEY_PATH"); v != "" {
		cfg.JWT.SigningKeyPath = v
	}
	if v := os.Getenv("JWT_KEY_ID"); v != "" {
		cfg.JWT.KeyID = v
	}
	if v := os.Getenv("IDENTITY_MODE"); v != "" {
		cfg.Identity.Mode = v
	}
}

func validate(cfg *Config) error {
	if cfg.Database.URL == "" {
		return fmt.Errorf("database.url (or DATABASE_URL) is required")
	}

	switch cfg.Identity.Mode {
	case "", "enrolled", "open":
		// "" is normalized to enrolled below; open is accepted but not yet
		// wired (HOR-313); the token endpoint returns 501 for open.
	default:
		return fmt.Errorf("identity.mode must be enrolled or open, got %q", cfg.Identity.Mode)
	}
	if cfg.Identity.Mode == "" {
		cfg.Identity.Mode = "enrolled"
	}

	if cfg.JWT.TTL != "" {
		if _, err := time.ParseDuration(cfg.JWT.TTL); err != nil {
			return fmt.Errorf("jwt.ttl is not a valid duration: %w", err)
		}
	}

	return nil
}

// ValidateServe checks serve-specific requirements that config.Load does not
// enforce (so migrate/bootstrap can run without them). It is called by runServe.
func ValidateServe(cfg *Config) error {
	if cfg.API.Addr == "" {
		return fmt.Errorf("api.addr (or API_ADDR) is required for serve")
	}
	if cfg.JWT.SigningKeyPath == "" {
		return fmt.Errorf("jwt.signing_key_path (or JWT_SIGNING_KEY_PATH) is required for serve")
	}
	return nil
}
