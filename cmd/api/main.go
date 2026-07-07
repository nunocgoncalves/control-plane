// Command api runs the control-plane HTTP API (cmd/api). It also owns database
// migrations via the `migrate` subcommand, intended to run as an RBAC-less init
// container before the manager/api start.
//
//	usage:
//	  control-plane-api [--config path] serve
//	  control-plane-api [--config path] migrate up
//	  control-plane-api [--config path] migrate down [steps]
//
// In the skeleton the API exposes /healthz and /readyz only; identity,
// permission, and usage endpoints arrive with their owning tickets.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nunocgoncalves/control-plane/internal/config"
	"github.com/nunocgoncalves/control-plane/internal/database"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/logging"
	"github.com/nunocgoncalves/control-plane/internal/server"
	"github.com/nunocgoncalves/control-plane/internal/version"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to config file (optional; env + defaults used if empty)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading config: %v\n", err)
		os.Exit(1)
	}

	logger, _ := logging.New(cfg.Logging.Level, cfg.Logging.Format)
	logger.Info("starting control-plane api",
		"version", version.Version(), "commit", version.Commit(), "date", version.Date(),
		"command", args[0])

	switch args[0] {
	case "serve":
		if err := runServe(cfg, logger); err != nil {
			logger.Error("api serve failed", "error", err)
			os.Exit(1)
		}
	case "migrate":
		if err := runMigrate(cfg, logger, args[1:]); err != nil {
			logger.Error("api migrate failed", "error", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

func runServe(cfg *config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := database.Connect(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connecting database: %w", err)
	}
	defer pool.Close()

	if err := config.ValidateServe(cfg); err != nil {
		return err
	}

	issuer, err := identity.NewIssuer(cfg.JWT.SigningKeyPath, cfg.JWT.KeyID, cfg.JWT.Issuer, cfg.JWT.Audience, cfg.JWT.TTL)
	if err != nil {
		return fmt.Errorf("loading jwt issuer: %w", err)
	}

	httpSrv := &http.Server{
		Addr:              cfg.API.Addr,
		Handler:           server.New(server.Services{
			Pool:   pool,
			Store:  identity.NewStore(pool),
			Issuer: issuer,
			Mode:   cfg.Identity.Mode,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.ListenAndServe() }()
	logger.Info("listening", "addr", cfg.API.Addr)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}

func runMigrate(cfg *config.Config, logger *slog.Logger, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("migrate requires a direction: up|down [steps]")
	}
	switch args[0] {
	case "up":
		if err := database.MigrateUp(cfg.Database.URL); err != nil {
			return err
		}
		logger.Info("migrations applied")
	case "down":
		steps := 0
		if len(args) >= 2 {
			if _, err := fmt.Sscanf(args[1], "%d", &steps); err != nil {
				return fmt.Errorf("invalid steps %q: %w", args[1], err)
			}
		}
		if err := database.MigrateDown(cfg.Database.URL, steps); err != nil {
			return err
		}
		logger.Info("migrations rolled back", "steps", steps)
	default:
		return fmt.Errorf("unknown migrate direction: %s (use up|down)", args[0])
	}
	return nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: control-plane-api [--config path] <serve | migrate up | migrate down [steps]>")
}
