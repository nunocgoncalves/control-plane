package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/config"
	"github.com/nunocgoncalves/control-plane/internal/logging"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// TestServe_PlainHTTP confirms the api serves plain HTTP (backward-compat)
// when no TLS cert/key are configured. Requires Docker (Postgres).
func TestServe_PlainHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := baseServeConfig(t) // no TLS env -> plain HTTP

	ctx, cancel := context.WithCancel(context.Background())
	var done <-chan error
	defer func() {
		cancel()
		waitDone(t, done)
	}()
	logger, _ := logging.New("error", "json") // quiet
	done = runServeAsync(t, ctx, cfg, logger)

	url := fmt.Sprintf("http://%s/healthz", cfg.API.Addr)
	assert.Equal(t, "ok", waitForReady(t, url, http.DefaultClient))
}

// TestServe_TLS confirms the api serves HTTPS when TLS_CERT_FILE + TLS_KEY_FILE
// are set: a client trusting the self-signed cert gets /healthz 200, and a
// plain-HTTP attempt is rejected (the server speaks TLS only). Requires Docker.
func TestServe_TLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	certPath, keyPath := writeSelfSignedCert(t)
	cfg := baseServeConfig(t)
	cfg.API.TLSCertFile = certPath
	cfg.API.TLSKeyFile = keyPath

	ctx, cancel := context.WithCancel(context.Background())
	var done <-chan error
	defer func() {
		cancel()
		waitDone(t, done)
	}()
	logger, _ := logging.New("error", "json") // quiet
	done = runServeAsync(t, ctx, cfg, logger)

	client := httpsClientTrusting(t, certPath)
	url := fmt.Sprintf("https://localhost:%s/healthz", portOf(cfg.API.Addr))
	assert.Equal(t, "ok", waitForReady(t, url, client))

	// A plain-HTTP client must be rejected: Go's TLS server returns 400
	// "Client sent an HTTP request to an HTTPS server" rather than a 200.
	plainURL := fmt.Sprintf("http://%s/healthz", cfg.API.Addr)
	plainClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := plainClient.Get(plainURL)
	require.NoError(t, err, "plain HTTP gets a 400 response, not a transport error")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NotEqual(t, http.StatusOK, resp.StatusCode, "plain HTTP must not be served over the HTTPS port")
}

// baseServeConfig builds a serve-ready config against a fresh migrated Postgres
// on an OS-assigned free port, with a temp JWT signing key (ValidateServe +
// identity.NewIssuer both require it).
func baseServeConfig(t *testing.T) *config.Config {
	t.Helper()
	_, connStr := testutil.NewPostgres(t)
	return &config.Config{
		API: config.APIConfig{Addr: freePortAddr(t)},
		Database: config.DatabaseConfig{
			URL:          connStr,
			MaxOpenConns: 5,
			MaxIdleConns: 2,
		},
		JWT: config.JWTConfig{
			SigningKeyPath: writeJWTKey(t),
			KeyID:          "test-kid",
			Issuer:         "control-plane",
			Audience:       "inference-gateway",
			TTL:            "15m",
		},
		Identity: config.IdentityConfig{Mode: "enrolled"},
	}
}

// runServeAsync starts runServe in a goroutine, failing the test if it errors
// before the test cancels the context.
func runServeAsync(t *testing.T, ctx context.Context, cfg *config.Config, logger *slog.Logger) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runServe(ctx, cfg, logger) }()
	// Surface an immediate startup error (e.g. bad cert path) without blocking.
	select {
	case err := <-done:
		require.NoError(t, err, "runServe exited unexpectedly at startup")
	case <-time.After(100 * time.Millisecond):
		// running; the test proceeds to poll.
	}
	return done
}

// waitDone waits briefly for runServe to return after the context is canceled.
func waitDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Log("runServe did not shut down within 3s; leaking goroutine")
	}
}

// waitForReady polls url with client until it returns 200 "ok" or times out.
func waitForReady(t *testing.T, url string, client *http.Client) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				return string(body)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not become ready at %s", url)
	return ""
}

// freePortAddr returns "127.0.0.1:<port>" for an OS-assigned free port.
func freePortAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// portOf extracts the port from a "host:port" address.
func portOf(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return port
}

// writeJWTKey generates an RSA private key to a temp file and returns its path.
func writeJWTKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})
	path := t.TempDir() + "/jwt.pem"
	require.NoError(t, os.WriteFile(path, pemBytes, 0o600))
	return path
}

// writeSelfSignedCert generates a self-signed cert (SAN: localhost,
// 127.0.0.1) + its key to temp files and returns (certPath, keyPath).
func writeSelfSignedCert(t *testing.T) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "control-plane-api-test"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath := dir + "/tls.crt"
	keyPath := dir + "/tls.key"
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	return certPath, keyPath
}

// httpsClientTrusting returns an http.Client that trusts the cert at certPath
// (self-signed -> installed as a root CA) and skips system roots.
func httpsClientTrusting(t *testing.T, certPath string) *http.Client {
	t.Helper()
	pemBytes, err := os.ReadFile(certPath)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes), "failed to load test cert")
	return &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool}, //nolint:gosec // test cert
		},
	}
}
