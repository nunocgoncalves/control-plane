// Command gateway runs the control-plane tool-gateway gRPC server (HOR-392):
// the authorized boundary for customer-system and externally side-effecting
// tool execution. It serves the iterabase.gateway.v1 contract (RunnerService +
// GatewayService) over native gRPC + mTLS, terminating SPIFFE workload
// identities (runners, supervisors, control-plane workflow-step callers).
//
//	usage:
//	  control-plane-gateway [--config path] serve
//
// Migrations are owned by `control-plane-api migrate up` (init container) and
// run before the gateway starts. The gateway shares the control-plane image as
// a third entrypoint (manager/api/gateway).
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

	artifactstore "github.com/nunocgoncalves/control-plane/internal/artifact"
	"github.com/nunocgoncalves/control-plane/internal/config"
	"github.com/nunocgoncalves/control-plane/internal/database"
	"github.com/nunocgoncalves/control-plane/internal/gateway"
	"github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1/gatewayv1connect"
	"github.com/nunocgoncalves/control-plane/internal/logging"
	"github.com/nunocgoncalves/control-plane/internal/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	logger.Info("starting control-plane gateway",
		"version", version.Version(), "commit", version.Commit(), "date", version.Date(),
		"command", args[0])

	switch args[0] {
	case "serve":
		if err := runServeCmd(cfg, logger); err != nil {
			logger.Error("gateway serve failed", "error", err)
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
	if err := config.ValidateGatewayServe(cfg); err != nil {
		return err
	}
	if err := config.ValidateArtifactServe(cfg); err != nil {
		return err
	}

	pool, err := database.Connect(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connecting database: %w", err)
	}
	defer pool.Close()

	// Credential-slot resolution: read K8s Secrets via the in-cluster API,
	// scoped to gateway.kube_namespace (ARCH-008).
	secretResolver, err := newSecretResolver(cfg.Gateway.KubeNamespace, logger)
	if err != nil {
		return err
	}

	var artifacts *artifactstore.Service
	if cfg.Artifact.Enabled {
		artifacts, err = artifactstore.NewConfiguredService(cfg, pool, logger)
		if err != nil {
			return fmt.Errorf("configure artifact service: %w", err)
		}
		if err := artifacts.Ready(ctx); err != nil {
			return fmt.Errorf("artifact service not ready: %w", err)
		}
		artifacts.StartSweeper(ctx)
	}

	svc := gateway.NewService(
		gateway.NewStore(pool),
		secretResolver,
		gateway.NewHTTPOAuthAcquirer(nil),
		gateway.Config{
			TrustDomain: cfg.Gateway.TrustDomain,
			InlineLimit: cfg.Gateway.InlineLimit,
		},
		logger,
	)
	if artifacts != nil {
		svc.SetArtifactService(artifacts)
	}

	// Crash-recovery reconciliation (SCN-008/ARCH-014): classify orphaned
	// in-flight invocations before accepting traffic, then on a ticker.
	recCtx, cancelRec := context.WithCancel(ctx)
	defer cancelRec()
	svc.StartReconciler(recCtx)

	mux := http.NewServeMux()
	idmw := gateway.IdentityMiddleware(cfg.Gateway.TrustDomain)
	runnerPath, runnerHandler := gatewayv1connect.NewRunnerServiceHandler(svc)
	gwPath, gwHandler := gatewayv1connect.NewGatewayServiceHandler(svc)
	artifactPath, artifactHandler := gatewayv1connect.NewArtifactServiceHandler(svc)
	mux.Handle(runnerPath, idmw(runnerHandler))
	mux.Handle(gwPath, idmw(gwHandler))
	mux.Handle(artifactPath, idmw(artifactHandler))

	tlsCfg, err := buildTLSConfig(cfg.Gateway)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              cfg.Gateway.Addr,
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeTLS("", "") }() // cert/key from tls.Config
	logger.Info("tool gateway listening", "addr", cfg.Gateway.Addr, "trust_domain", cfg.Gateway.TrustDomain)

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
// HTTP/2-only, require+verify client cert.
func buildTLSConfig(g config.GatewayConfig) (*tls.Config, error) {
	serverCert, err := tls.LoadX509KeyPair(g.TLSCertFile, g.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load gateway server cert: %w", err)
	}
	caPEM, err := os.ReadFile(g.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("load gateway client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("client CA bundle %s contains no certificates", g.ClientCAFile)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	}
	return cfg, nil
}

// newSecretResolver builds the K8s Secret reader. It prefers in-cluster config;
// if that is unavailable (dev), it returns an error (the gateway cannot resolve
// credentials outside a cluster — tests inject a fake resolver directly).
func newSecretResolver(namespace string, logger *slog.Logger) (gateway.SecretResolver, error) {
	kcfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster k8s config (gateway must run in-cluster for credential resolution): %w", err)
	}
	client, err := kubernetes.NewForConfig(kcfg)
	if err != nil {
		return nil, fmt.Errorf("build k8s client: %w", err)
	}
	logger.Info("credential resolution via in-cluster K8s API", "namespace", namespace)
	return gateway.NewK8sSecretResolver(client, namespace), nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: control-plane-gateway [--config path] <serve>")
}

// compile-time assertion that the handlers are wired correctly.
var (
	_ gatewayv1connect.RunnerServiceHandler   = (*gateway.Service)(nil)
	_ gatewayv1connect.GatewayServiceHandler  = (*gateway.Service)(nil)
	_ gatewayv1connect.ArtifactServiceHandler = (*gateway.Service)(nil)
)
