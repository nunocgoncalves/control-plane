// Package version exposes build-time version metadata injected via linker
// flags (see Makefile LDFLAGS). Mirrors the forge/inference-gateway pattern.
package version

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// Version returns the semantic version (or "dev" if unset).
func Version() string { return version }

// Commit returns the short git commit hash.
func Commit() string { return commit }

// Date returns the build date (UTC, RFC3339).
func Date() string { return date }
