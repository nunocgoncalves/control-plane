package proxy_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/proxy"
)

// writeSecret writes a secret value to <dir>/<name>/<key> and returns the ref.
func writeSecret(t *testing.T, dir, name, key, val string) proxy.SecretRef {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, name), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name, key), []byte(val), 0o600))
	return proxy.SecretRef{Name: name, Key: key}
}

// newProxy builds a proxy.Server from an in-memory Config (SecretDir = a temp
// dir) and fronts it with an httptest.Server. The reload loop is NOT started
// (ServeHTTP is exercised directly).
func newProxy(t *testing.T, cfg *proxy.Config) (*proxy.Server, *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	cfg.SecretDir = dir
	// Apply the same defaults LoadConfig would.
	if cfg.Listen == "" {
		cfg.Listen = ":0"
	}
	if cfg.HealthAddr == "" {
		cfg.HealthAddr = ":0"
	}
	srv, err := proxy.NewServer(cfg, "")
	require.NoError(t, err)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts
}

// upstreamCapture is a fake upstream that records the last request's path +
// Authorization header and returns 200 with a body.
func upstreamCapture(t *testing.T, body string) (*httptest.Server, *string, *string) {
	t.Helper()
	var gotPath, gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return ts, &gotPath, &gotAuth
}

func TestModelRouteInjectsGatewayKey(t *testing.T) {
	gw, gotPath, gotAuth := upstreamCapture(t, "model-ok")
	cfg := &proxy.Config{
		Model: proxy.ModelRoute{
			Upstream: gw.URL,
			Auth:     proxy.BearerAuth{SecretRef: proxy.SecretRef{Name: "gw", Key: "token"}},
		},
	}
	_, ts := newProxy(t, cfg)
	writeSecret(t, cfg.SecretDir, "gw", "token", "agent-egress-key")

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer placeholder") // harness placeholder — must be stripped
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Equal(t, "/v1/chat/completions", *gotPath, "model route forwards path as-is")
	assert.Equal(t, "Bearer agent-egress-key", *gotAuth, "model route injects the SA gateway key")
}

func TestBearerToolRouteRewritesAndInjects(t *testing.T) {
	up, gotPath, gotAuth := upstreamCapture(t, "data")
	cfg := &proxy.Config{
		Model: proxy.ModelRoute{Upstream: "http://127.0.0.1:1", Auth: proxy.BearerAuth{SecretRef: proxy.SecretRef{Name: "gw", Key: "token"}}},
		Routes: []proxy.Route{{
			ID:       "legacy",
			Upstream: proxy.Upstream{BaseURL: up.URL},
			Auth:     proxy.Auth{Scheme: "bearer", SecretRef: &proxy.SecretRef{Name: "legacy-key", Key: "token"}},
		}},
	}
	_, ts := newProxy(t, cfg)
	writeSecret(t, cfg.SecretDir, "gw", "token", "gw-key")
	writeSecret(t, cfg.SecretDir, "legacy-key", "token", "legacy-secret")

	resp, err := ts.Client().Get(ts.URL + "/upstreams/legacy/v1/items/42?q=1")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, "/v1/items/42", *gotPath, "tool route strips /upstreams/<id>")
	assert.Equal(t, "Bearer legacy-secret", *gotAuth)
}

func TestUnknownRoute404(t *testing.T) {
	cfg := &proxy.Config{
		Model: proxy.ModelRoute{Upstream: "http://127.0.0.1:1", Auth: proxy.BearerAuth{SecretRef: proxy.SecretRef{Name: "gw", Key: "token"}}},
	}
	_, ts := newProxy(t, cfg)
	writeSecret(t, cfg.SecretDir, "gw", "token", "gw-key")

	resp, err := ts.Client().Get(ts.URL + "/upstreams/missing/v1")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOAuthAcquireInjectAndCache(t *testing.T) {
	var tokenHits atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"tok-123","expires_in":3600}`))
	}))
	t.Cleanup(tokenSrv.Close)

	up, gotPath, gotAuth := upstreamCapture(t, "graph-data")
	cfg := &proxy.Config{
		Model: proxy.ModelRoute{Upstream: "http://127.0.0.1:1", Auth: proxy.BearerAuth{SecretRef: proxy.SecretRef{Name: "gw", Key: "token"}}},
		Routes: []proxy.Route{{
			ID:       "graph",
			Upstream: proxy.Upstream{BaseURL: up.URL},
			Auth: proxy.Auth{
				Scheme:          "oauthClientCredentials",
				TokenURL:        tokenSrv.URL,
				ClientID:        "app-id",
				ClientSecretRef: &proxy.SecretRef{Name: "graph-oauth", Key: "client_secret"},
				Scope:           "https://graph.microsoft.com/.default",
			},
		}},
	}
	_, ts := newProxy(t, cfg)
	writeSecret(t, cfg.SecretDir, "gw", "token", "gw-key")
	writeSecret(t, cfg.SecretDir, "graph-oauth", "client_secret", "super-secret")

	// Two requests: the OAuth token is acquired once (expires_in=3600 > margin)
	// and cached for the second.
	for range 2 {
		resp, err := ts.Client().Get(ts.URL + "/upstreams/graph/v1.0/me")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}
	assert.Equal(t, "/v1.0/me", *gotPath)
	assert.Equal(t, "Bearer tok-123", *gotAuth, "injected the acquired OAuth token")
	assert.Equal(t, int32(1), tokenHits.Load(), "token acquired once + cached")
}

func TestOAuthRefreshesWhenNearExpiry(t *testing.T) {
	var tokenHits atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenHits.Add(1)
		// expires_in=1s < refreshMargin(60s) -> the manager re-acquires each call.
		_, _ = w.Write([]byte(`{"access_token":"short-lived","expires_in":1}`))
	}))
	t.Cleanup(tokenSrv.Close)

	up, _, gotAuth := upstreamCapture(t, "")
	cfg := &proxy.Config{
		Model: proxy.ModelRoute{Upstream: "http://127.0.0.1:1", Auth: proxy.BearerAuth{SecretRef: proxy.SecretRef{Name: "gw", Key: "token"}}},
		Routes: []proxy.Route{{
			ID:       "graph",
			Upstream: proxy.Upstream{BaseURL: up.URL},
			Auth: proxy.Auth{Scheme: "oauthClientCredentials", TokenURL: tokenSrv.URL, ClientID: "id",
				ClientSecretRef: &proxy.SecretRef{Name: "graph-oauth", Key: "client_secret"}},
		}},
	}
	_, ts := newProxy(t, cfg)
	writeSecret(t, cfg.SecretDir, "gw", "token", "gw-key")
	writeSecret(t, cfg.SecretDir, "graph-oauth", "client_secret", "s")

	for range 3 {
		resp, err := ts.Client().Get(ts.URL + "/upstreams/graph/v1.0/me")
		require.NoError(t, err)
		resp.Body.Close()
	}
	assert.Equal(t, "Bearer short-lived", *gotAuth)
	assert.Equal(t, int32(3), tokenHits.Load(), "near-expiry token re-acquired each call")
}

func TestOAuthSingleFlight(t *testing.T) {
	var tokenHits atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenHits.Add(1)
		time.Sleep(50 * time.Millisecond) // slow acquire to widen the single-flight window
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	t.Cleanup(tokenSrv.Close)

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.Close)
	cfg := &proxy.Config{
		Model: proxy.ModelRoute{Upstream: "http://127.0.0.1:1", Auth: proxy.BearerAuth{SecretRef: proxy.SecretRef{Name: "gw", Key: "token"}}},
		Routes: []proxy.Route{{
			ID:       "graph",
			Upstream: proxy.Upstream{BaseURL: up.URL},
			Auth: proxy.Auth{Scheme: "oauthClientCredentials", TokenURL: tokenSrv.URL, ClientID: "id",
				ClientSecretRef: &proxy.SecretRef{Name: "graph-oauth", Key: "client_secret"}},
		}},
	}
	_, ts := newProxy(t, cfg)
	writeSecret(t, cfg.SecretDir, "gw", "token", "gw-key")
	writeSecret(t, cfg.SecretDir, "graph-oauth", "client_secret", "s")

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := ts.Client().Get(ts.URL + "/upstreams/graph/v1.0/me")
			require.NoError(t, err)
			resp.Body.Close()
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), tokenHits.Load(), "concurrent requests share a single acquire")
}

func TestLoadConfigValidate(t *testing.T) {
	dir := t.TempDir()
	writeSecret(t, dir, "gw", "token", "k")
	writeSecret(t, dir, "graph-oauth", "client_secret", "s")

	valid := "listen: \":8444\"\n" +
		"model:\n  upstream: \"http://gateway\"\n  auth:\n    secretRef: {name: gw, key: token}\n" +
		"routes:\n  - id: graph\n    upstream: {baseURL: \"https://graph.microsoft.com\"}\n" +
		"    auth:\n      scheme: oauthClientCredentials\n      tokenURL: \"https://login.microsoftonline.com/t/oauth2/v2.0/token\"\n" +
		"      clientID: app-id\n      clientSecretRef: {name: graph-oauth, key: client_secret}\n" +
		"      scope: \"https://graph.microsoft.com/.default\"\n"
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(valid), 0o600))

	cfg, err := proxy.LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "/secrets", cfg.SecretDir, "default secretDir")
	assert.Equal(t, ":8444", cfg.Listen)
	require.Len(t, cfg.Routes, 1)
	assert.Equal(t, "graph", cfg.Routes[0].ID)

	// Invalid: model missing secretRef.
	bad := "model:\n  upstream: \"http://gateway\"\n  auth: {secretRef: {name: \"\", key: token}}\n"
	badPath := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(badPath, []byte(bad), 0o600))
	_, err = proxy.LoadConfig(badPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secretRef")
}

// (sync.WaitGroup is used by TestOAuthSingleFlight above.)
