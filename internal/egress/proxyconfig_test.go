package egress_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/egress"
	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/proxy"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

// captureUpstream is a fake upstream that records the last path + Authorization.
func captureUpstream(t *testing.T) (*httptest.Server, *string, *string) {
	t.Helper()
	var p, a string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p = r.URL.Path
		a = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)
	return ts, &p, &a
}

// TestResolveToProxyConfigContract is the HOR-244 -> HOR-245 config-shape drift
// guard: it exercises the full bridge the AgentSandbox operator will run —
// egress.Resolve -> egress.BuildProxyConfig -> proxy.NewServer -> a request
// flows with the injected credential. If ResolveResult's shape drifts from
// proxy.Config's, this fails. Uses fake upstreams + a fake OAuth token endpoint.
func TestResolveToProxyConfigContract(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	pool := testutil.NewPostgresPool(t)
	egStore, idStore := egress.NewStore(pool), identity.NewStore(pool)
	ctx := context.Background()

	alice, err := idStore.UpsertIdentity(ctx, "default/alice", "user", identity.SourceExternal, "Alice")
	require.NoError(t, err)

	// Fake upstreams + a fake OAuth token endpoint.
	gwUp, gwPath, gwAuth := captureUpstream(t)
	graphUp, graphPath, graphAuth := captureUpstream(t)
	legacyUp, legacyPath, legacyAuth := captureUpstream(t)
	var tokenHits atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenHits.Add(1)
		_, _ = w.Write([]byte(`{"access_token":"graph-tok","expires_in":3600}`))
	}))
	t.Cleanup(tokenSrv.Close)

	// Seed EgressRoutes pointing at the fake upstreams (graph oauth + legacy bearer).
	_, err = egStore.UpsertRoute(ctx, "default/graph", "graph", "default", graphUp.URL,
		egress.Auth{Scheme: "oauthClientCredentials", TokenURL: tokenSrv.URL, ClientID: "app-id",
			ClientSecretRef: &egress.SecretRef{Name: "graph-oauth", Key: "client_secret"},
			Scope:           "https://graph.microsoft.com/.default"}, nil)
	require.NoError(t, err)
	_, err = egStore.UpsertRoute(ctx, "default/legacy", "legacy", "default", legacyUp.URL,
		egress.Auth{Scheme: "bearer", SecretRef: &egress.SecretRef{Name: "legacy", Key: "token"}}, nil)
	require.NoError(t, err)

	// Resolve (broad-default -> both routes) + build the proxy config.
	res, err := egStore.Resolve(ctx, alice.ID)
	require.NoError(t, err)
	require.Len(t, res.Routes, 2)

	cfg := egress.BuildProxyConfig(
		egress.ModelRouteConfig{Upstream: gwUp.URL, AuthSecretRef: egress.SecretRef{Name: "gw", Key: "token"}},
		res,
	)
	cfg.SecretDir = t.TempDir() // override the /secrets default for the test
	// Write the referenced secrets into the proxy's secret dir.
	writeFile(t, filepath.Join(cfg.SecretDir, "gw", "token"), "gw-key")
	writeFile(t, filepath.Join(cfg.SecretDir, "graph-oauth", "client_secret"), "super-secret")
	writeFile(t, filepath.Join(cfg.SecretDir, "legacy", "token"), "legacy-key")

	srv, err := proxy.NewServer(cfg, "")
	require.NoError(t, err)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	cli := ts.Client()

	// Model route: /v1/<rest> -> gateway as-is, SA key injected, placeholder stripped.
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer placeholder")
	resp, err := cli.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/v1/chat/completions", *gwPath)
	assert.Equal(t, "Bearer gw-key", *gwAuth)

	// Tool route (oauth): /upstreams/graph/<rest> -> graph, OAuth token injected.
	resp, err = cli.Get(ts.URL + "/upstreams/graph/v1.0/me")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/v1.0/me", *graphPath)
	assert.Equal(t, "Bearer graph-tok", *graphAuth)
	assert.Equal(t, int32(1), tokenHits.Load(), "OAuth token acquired once + cached")

	// Tool route (bearer): /upstreams/legacy/<rest> -> legacy, static bearer injected.
	resp, err = cli.Get(ts.URL + "/upstreams/legacy/v1/items")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/v1/items", *legacyPath)
	assert.Equal(t, "Bearer legacy-key", *legacyAuth)
}

func writeFile(t *testing.T, path, val string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(val), 0o600))
}
