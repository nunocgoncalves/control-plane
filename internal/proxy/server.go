package proxy

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// reloadInterval is how often the proxy polls its config file for hot-reload.
// The mounted ConfigMap is kubelet-refreshed (~1m); this matches that cadence.
const reloadInterval = 60 * time.Second

// hopByHopHeaders are stripped from upstream responses (reverse-proxy hygiene).
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailers", "Transfer-Encoding", "Upgrade",
}

// RouteTable is the immutable, hot-swappable set of routes the proxy serves.
// Swapped atomically on hot-reload; in-flight requests keep using the table
// they matched against.
type RouteTable struct {
	model *modelRoute
	tools map[string]*toolRoute
}

type modelRoute struct {
	upstream *url.URL
	injector credInjector
}

type toolRoute struct {
	upstream *url.URL
	injector credInjector
}

// match resolves a request path to its upstream URL, the path to forward, and
// the credential injector. Returns ok=false for unknown paths (404).
func (t *RouteTable) match(p string) (upstream *url.URL, forwardPath string, inj credInjector, ok bool) {
	// Model route: /v1/<rest> -> <gateway>/v1/<rest> (no prefix strip).
	if t.model != nil && (p == "/v1" || strings.HasPrefix(p, "/v1/")) {
		return t.model.upstream, p, t.model.injector, true
	}
	// Tool route: /upstreams/<id>/<rest> -> <baseURL>/<rest> (strip /upstreams/<id>).
	if strings.HasPrefix(p, "/upstreams/") {
		rest := strings.TrimPrefix(p, "/upstreams/")
		id := rest
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			id, rest = rest[:i], rest[i:]
		} else {
			rest = "/"
		}
		if tr, found := t.tools[id]; found {
			return tr.upstream, path.Join(tr.upstream.Path, rest), tr.injector, true
		}
	}
	return nil, "", nil, false
}

// Server is the per-sandbox egress proxy: a credentialed reverse proxy over a
// hot-reloadable RouteTable, with plain-HTTP health probes + graceful drain.
type Server struct {
	cfgPath    string
	listen     string
	healthAddr string
	tls        TLSConfig
	table      atomic.Pointer[RouteTable]
	client     *http.Client
	mainSrv    *http.Server
	healthSrv  *http.Server
}

// NewServer builds the initial table from cfg and prepares the servers.
func NewServer(cfg *Config, cfgPath string) (*Server, error) {
	tab, err := buildTable(cfg)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfgPath:    cfgPath,
		listen:     cfg.Listen,
		healthAddr: cfg.HealthAddr,
		tls:        cfg.TLS,
		client:     &http.Client{Timeout: 5 * time.Minute},
	}
	s.table.Store(tab)
	s.mainSrv = &http.Server{Addr: cfg.Listen, Handler: s, ReadHeaderTimeout: 10 * time.Second}
	s.healthSrv = &http.Server{Addr: cfg.HealthAddr, Handler: http.HandlerFunc(healthHandler), ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

// buildTable assembles a RouteTable from a Config, wiring the cred injectors
// (bearer file reader / OAuth manager) to the mounted Secret paths.
func buildTable(cfg *Config) (*RouteTable, error) {
	tab := &RouteTable{tools: make(map[string]*toolRoute, len(cfg.Routes))}

	mUp, err := url.Parse(cfg.Model.Upstream)
	if err != nil {
		return nil, err
	}
	tab.model = &modelRoute{
		upstream: mUp,
		injector: &bearerInjector{path: secretPath(cfg.SecretDir, cfg.Model.Auth.SecretRef)},
	}

	for _, r := range cfg.Routes {
		up, err := url.Parse(r.Upstream.BaseURL)
		if err != nil {
			return nil, err
		}
		var inj credInjector
		switch r.Auth.Scheme {
		case "bearer":
			inj = &bearerInjector{path: secretPath(cfg.SecretDir, *r.Auth.SecretRef)}
		case "oauthClientCredentials":
			inj = &oauthInjector{mgr: NewOAuthManager(
				r.Auth.TokenURL, r.Auth.ClientID,
				secretPath(cfg.SecretDir, *r.Auth.ClientSecretRef), r.Auth.Scope)}
		}
		tab.tools[r.ID] = &toolRoute{upstream: up, injector: inj}
	}
	return tab, nil
}

func secretPath(dir string, ref SecretRef) string {
	return filepath.Join(dir, ref.Name, ref.Key)
}

// ServeHTTP routes, strips inbound auth, injects the route's real credential,
// and forwards to the upstream. Bodies are opaque (pass-through).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tab := s.table.Load()
	upstream, forwardPath, inj, ok := tab.match(r.URL.Path)
	if !ok {
		http.Error(w, "no egress route for "+r.URL.Path, http.StatusNotFound)
		return
	}

	outURL := &url.URL{
		Scheme:   upstream.Scheme,
		Host:     upstream.Host,
		Path:     forwardPath,
		RawQuery: r.URL.RawQuery,
	}
	outReq := r.Clone(r.Context())
	outReq.URL = outURL
	outReq.RequestURI = ""
	outReq.Host = upstream.Host

	// Strip any inbound placeholder auth (the harness/tool sends a placeholder
	// or none); the proxy owns the real credential.
	outReq.Header.Del("Authorization")
	outReq.Header.Del("Api-Key")
	outReq.Header.Del("Apikey")

	if err := inj.inject(r.Context(), outReq); err != nil {
		http.Error(w, "credential injection failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	resp, err := s.client.Do(outReq) //nolint:gosec // forwarding to operator-configured upstreams is the proxy's purpose
	if err != nil {
		http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for _, h := range hopByHopHeaders {
		resp.Header.Del(h)
	}
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// healthHandler serves /healthz + /readyz (loopback, plain HTTP).
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// Run starts the main (TLS or plain) + health servers, the hot-reload loop,
// and blocks until ctx is canceled (SIGTERM), then drains gracefully.
func (s *Server) Run(ctx context.Context) error {
	go s.reloadLoop(ctx)

	errCh := make(chan error, 2)
	go func() { errCh <- s.healthSrv.ListenAndServe() }()
	go func() {
		if s.tls.CertFile != "" && s.tls.KeyFile != "" {
			errCh <- s.mainSrv.ListenAndServeTLS(s.tls.CertFile, s.tls.KeyFile)
		} else {
			errCh <- s.mainSrv.ListenAndServe()
		}
	}()

	select {
	case <-ctx.Done():
		return s.shutdown()
	case err := <-errCh:
		_ = s.shutdown()
		return err
	}
}

// reloadLoop polls the config file every reloadInterval and atomically swaps
// the RouteTable when it changes. Secret rotation is handled by the injectors
// (bearer cache TTL + OAuth re-read), so a config-byte change here means the
// route set itself changed (new route, upstream, or auth params).
func (s *Server) reloadLoop(ctx context.Context) {
	ticker := time.NewTicker(reloadInterval)
	defer ticker.Stop()
	last := s.configHash()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg, err := LoadConfig(s.cfgPath)
			if err != nil {
				continue // keep serving the current table on a bad config
			}
			h := s.hashConfig(cfg)
			if h == last {
				continue
			}
			tab, err := buildTable(cfg)
			if err != nil {
				continue
			}
			s.table.Store(tab)
			last = h
		}
	}
}

// configHash returns the hash of the current table's source config. The initial
// table was built from the startup config; we hash that same config so the
// first reload only swaps on a real change.
func (s *Server) configHash() uint64 {
	cfg, err := LoadConfig(s.cfgPath)
	if err != nil {
		return 0
	}
	return s.hashConfig(cfg)
}

func (s *Server) hashConfig(cfg *Config) uint64 {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return 0
	}
	h := fnv.New64a()
	h.Write(raw)
	return binary.BigEndian.Uint64(h.Sum(nil))
}

func (s *Server) shutdown() error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var first error
	if err := s.mainSrv.Shutdown(shutdownCtx); err != nil {
		first = err
	}
	_ = s.healthSrv.Shutdown(shutdownCtx)
	return first
}

// CurrentTable returns the active route table (for tests/inspection).
func (s *Server) CurrentTable() *RouteTable { return s.table.Load() }
