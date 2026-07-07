// Command api runs the control-plane HTTP API (cmd/api). It also owns database
// migrations and admin bootstrap via subcommands, intended to run as RBAC-less
// init containers before the manager/api start.
//
//	usage:
//	  control-plane-api [--config path] serve
//	  control-plane-api [--config path] migrate up
//	  control-plane-api [--config path] migrate down [steps]
//	  control-plane-api [--config path] bootstrap [--admin-email e] [--service-account name] [--reset]
//
// The API serves JWKS, the delegated-token endpoint (POST /v1/token), and the
// admin endpoints for local users + API keys (HOR-242).
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
	case "bootstrap":
		if err := runBootstrap(cfg, logger, args[1:]); err != nil {
			logger.Error("api bootstrap failed", "error", err)
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
		Addr: cfg.API.Addr,
		Handler: server.New(server.Services{
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
	fmt.Fprintln(os.Stderr, "usage: control-plane-api [--config path] <serve | migrate up | migrate down [steps] | bootstrap [flags]>")
}

// runBootstrap creates (or, with --reset, re-issues credentials for) the admin
// local user and any seeded service accounts, printing each full API key once.
// It is the only way to obtain the first admin credential (no UI; S7 deferred).
func runBootstrap(cfg *config.Config, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	var adminEmail string
	var reset bool
	var serviceAccounts multiString
	fs.StringVar(&adminEmail, "admin-email", "admin@control-plane.local",
		"Email (and identity key) for the bootstrap admin user")
	fs.BoolVar(&reset, "reset", false,
		"Revoke existing keys for the admin and seeded service accounts, then issue new ones")
	fs.Var(&serviceAccounts, "service-account",
		"Name of a service account to seed (repeatable); issues a token-scope key")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := database.Connect(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connecting database: %w", err)
	}
	defer pool.Close()
	store := identity.NewStore(pool)

	// Admin local user + admin-scope key.
	admin, err := store.UpsertLocalUser(ctx, adminEmail, adminEmail, "admin")
	if err != nil {
		return fmt.Errorf("upsert admin: %w", err)
	}
	if reset {
		if err := store.RevokeAPIKeysForIdentity(ctx, admin.ID, identity.ScopeAdmin); err != nil {
			return fmt.Errorf("revoking admin keys: %w", err)
		}
	}
	adminFull, _, err := store.CreateAPIKey(ctx, admin.ID, "bootstrap", identity.ScopeAdmin, nil)
	if err != nil {
		return fmt.Errorf("create admin key: %w", err)
	}

	fmt.Println("Bootstrap complete. Store these securely; they will not be shown again.")
	fmt.Printf("Admin (%s) API key (scope=admin): %s\n", adminEmail, adminFull)

	// Seeded service accounts + token-scope keys.
	for _, name := range serviceAccounts {
		sa, err := store.UpsertServiceAccount(ctx, name, name)
		if err != nil {
			return fmt.Errorf("upsert service account %q: %w", name, err)
		}
		if reset {
			if err := store.RevokeAPIKeysForIdentity(ctx, sa.ID, identity.ScopeToken); err != nil {
				return fmt.Errorf("revoking %s keys: %w", name, err)
			}
		}
		saFull, _, err := store.CreateAPIKey(ctx, sa.ID, "bootstrap", identity.ScopeToken, nil)
		if err != nil {
			return fmt.Errorf("create %s key: %w", name, err)
		}
		fmt.Printf("Service account %q API key (scope=token): %s\n", name, saFull)
	}

	logger.Info("bootstrap completed",
		"admin_email", adminEmail, "service_accounts", []string(serviceAccounts), "reset", reset)
	return nil
}

// multiString is a repeatable string flag value.
type multiString []string

func (m *multiString) String() string     { return fmt.Sprintf("%v", []string(*m)) }
func (m *multiString) Set(v string) error { *m = append(*m, v); return nil }
