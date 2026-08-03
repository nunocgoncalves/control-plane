// Command dispatch runs the control-plane durable-dispatch Work gRPC server
// (HOR-249): the warm-worker bidi stream (iterabase.harness.v1.Harness.Work)
// over native gRPC + mTLS, owning worker fencing, one-credit dispatch, durable
// TurnEvent ACK/dedup, cancellation and worker-loss semantics, and the dispatch
// reconciler that drives pending runs/steps/turns to eligible idle workers.
//
//	usage:
//	  control-plane-dispatch [--config path] serve
//
// Migrations are owned by control-plane-api migrate up (init container) and run
// before the dispatch server starts. The dispatch server shares the
// control-plane image as a fourth entrypoint (manager/api/gateway/dispatch).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"github.com/nunocgoncalves/control-plane/internal/dispatch"
	v1 "github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1"
	"github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1/harnessv1connect"
	"github.com/nunocgoncalves/control-plane/internal/logging"
	"github.com/nunocgoncalves/control-plane/internal/runtime"
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
	logger.Info("starting control-plane dispatch",
		"version", version.Version(), "commit", version.Commit(), "date", version.Date(),
		"command", args[0])

	switch args[0] {
	case "serve":
		if err := runServeCmd(cfg, logger); err != nil {
			logger.Error("dispatch serve failed", "error", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

func runServeCmd(cfg *config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return runServe(ctx, cfg, logger)
}

func runServe(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	if err := config.ValidateDispatchServe(cfg); err != nil {
		return err
	}

	pool, err := database.Connect(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connecting database: %w", err)
	}
	defer pool.Close()

	store := dispatch.NewStore(pool, runtime.NewStore(pool))
	var defaultModel *v1.ModelConfig
	if cfg.Dispatch.DefaultModelID != "" {
		defaultModel = &v1.ModelConfig{Id: cfg.Dispatch.DefaultModelID, Api: cfg.Dispatch.DefaultModelAPI}
	}
	svc := dispatch.NewService(store, dispatch.Config{
		TrustDomain:  cfg.Dispatch.TrustDomain,
		DefaultModel: defaultModel,
	}, logger)

	// Dispatch reconciler: drive pending runs to idle workers before accepting
	// worker traffic, then on a ticker + Ready kick.
	recCtx, cancelRec := context.WithCancel(ctx)
	defer cancelRec()
	svc.StartReconciler(recCtx)
	svc.StartLeaseMonitor(recCtx)

	mux := http.NewServeMux()
	idmw := dispatch.IdentityMiddleware(cfg.Dispatch.TrustDomain)
	path, handler := harnessv1connect.NewHarnessHandler(svc)
	mux.Handle(path, idmw(handler))

	tlsCfg, err := buildTLSConfig(cfg.Dispatch)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              cfg.Dispatch.Addr,
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeTLS("", "") }() // cert/key from tls.Config
	logger.Info("dispatch Work server listening", "addr", cfg.Dispatch.Addr, "trust_domain", cfg.Dispatch.TrustDomain)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}

// buildTLSConfig assembles the mTLS config: server cert + key + client CA pool,
// HTTP/2-only, require+verify client cert (warm worker supervisors).
func buildTLSConfig(d config.DispatchConfig) (*tls.Config, error) {
	serverCert, err := tls.LoadX509KeyPair(d.TLSCertFile, d.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load dispatch server cert: %w", err)
	}
	caPEM, err := os.ReadFile(d.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("load dispatch client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("client CA bundle %s contains no certificates", d.ClientCAFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"}, // HTTP/2 only -- no HTTP/1.1 fallback
	}, nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: control-plane-dispatch [--config path] <serve>")
}

// compile-time assertion that the handler is wired correctly.
var _ harnessv1connect.HarnessHandler = (*dispatch.Service)(nil)
