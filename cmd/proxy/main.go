// Command proxy runs the per-sandbox egress proxy (HOR-244) — a credentialed
// reverse proxy that is the single egress point for an AgentSandbox pod's model
// + tool traffic. The harness (HOR-351) + overlay tools address it on localhost
// (TLS, sidecar); the proxy injects the real credential per route (model =
// shared agent-egress SA gateway key; tools = bearer or OAuth2
// client-credentials) and forwards. The proxy is DB-less; its route table is
// baked at provisioning (HOR-245 calls internal/egress.Resolve) and
// live-updated by hot-reload of the mounted ConfigMap.
//
// One image, one entrypoint. Separate from the control-plane image (manager/api)
// because it runs in sandbox pods (less-trusted) and is self-contained.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nunocgoncalves/control-plane/internal/logging"
	"github.com/nunocgoncalves/control-plane/internal/proxy"
	"github.com/nunocgoncalves/control-plane/internal/version"
)

func main() {
	os.Exit(run())
}

// run loads the config, runs the proxy until SIGINT/SIGTERM, and returns a
// process exit code (so deferred cleanup runs before os.Exit).
func run() int {
	var cfgPath, logLevel, logFormat string
	flag.StringVar(&cfgPath, "config", getenv("PROXY_CONFIG", "/etc/proxy/config.yaml"),
		"Path to the ProxyConfig YAML (ConfigMap-mounted by HOR-245).")
	flag.StringVar(&logLevel, "log-level", getenv("LOG_LEVEL", "info"), "Log level (debug|info|warn|error).")
	flag.StringVar(&logFormat, "log-format", getenv("LOG_FORMAT", "json"), "Log format (json|text).")
	flag.Parse()

	logger, _ := logging.New(logLevel, logFormat)
	slog.SetDefault(logger)

	logger.Info("starting egress proxy",
		"version", version.Version(), "commit", version.Commit(), "date", version.Date(),
		"config", cfgPath)

	cfg, err := proxy.LoadConfig(cfgPath)
	if err != nil {
		logger.Error("failed to load config", "err", err)
		return 1
	}

	srv, err := proxy.NewServer(cfg, cfgPath)
	if err != nil {
		logger.Error("failed to build server", "err", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		logger.Error("proxy exited with error", "err", err)
		return 1
	}
	logger.Info("egress proxy stopped")
	return 0
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
