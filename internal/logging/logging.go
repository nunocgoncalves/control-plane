// Package logging configures the shared slog logger used by both the manager
// and the api. controller-runtime is handed a logr.Logger bridged from the
// same slog.Handler via logr.FromSlogHandler, so the whole process emits
// structured logs through one backend.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Handler builds the shared slog.Handler from a level ("debug"|"info"|"warn"|
// "error") and format ("json"|"text"). Unknown values fall back to info/json.
func Handler(level, format string) slog.Handler {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	if strings.ToLower(format) == "text" {
		return slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.NewJSONHandler(os.Stdout, opts)
}

// New returns a *slog.Logger built from the given level/format, sets it as the
// slog default, and also returns the underlying handler so callers (the
// manager) can bridge it to logr for controller-runtime.
func New(level, format string) (*slog.Logger, slog.Handler) {
	h := Handler(level, format)
	l := slog.New(h)
	slog.SetDefault(l)
	return l, h
}
