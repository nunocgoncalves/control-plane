// Package proxy implements the per-sandbox egress proxy (HOR-244): a
// credentialed reverse proxy that is the single egress point for an
// AgentSandbox pod's model + tool traffic. The harness (HOR-351) and overlay
// tools address it on localhost; the proxy terminates the inbound leg (TLS),
// strips any placeholder auth, injects the real credential for the route
// (static bearer from a mounted Secret, or an OAuth2 client-credentials token
// it acquires + refreshes), and forwards to the real upstream.
//
// The proxy is DB-less and identity-agnostic at runtime: its route table is
// baked at provisioning by the AgentSandbox operator (HOR-245, calling
// internal/egress.Resolve) and live-updated by hot-reload of the mounted
// ConfigMap. The agent never holds credentials.
package proxy

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the ProxyConfig the proxy reads at startup (ConfigMap-mounted by
// HOR-245). Model route = built-in /v1 -> inference-gateway (internal-trusted,
// shared agent-egress SA key). Tool routes = from egress.Resolve (EgressRoute
// CRs). Secrets are referenced by name+key and read from
// <SecretDir>/<name>/<key> (SecretDir defaults to /secrets; the operator mounts
// each referenced K8s Secret to /secrets/<name>/).
type Config struct {
	Listen     string     `yaml:"listen"`     // TLS listen addr, e.g. ":8444"
	HealthAddr string     `yaml:"healthAddr"` // plain-HTTP probe addr, e.g. ":8081"
	SecretDir  string     `yaml:"secretDir"`  // default /secrets
	TLS        TLSConfig  `yaml:"tls"`
	Model      ModelRoute `yaml:"model"`  // built-in /v1 route
	Routes     []Route    `yaml:"routes"` // tool routes (/upstreams/<id>/...)
}

// TLSConfig configures the localhost-leg server certificate (cert-manager,
// SAN=localhost, mounted by HOR-245). If empty, the proxy listens plain HTTP
// (tests / non-TLS). No client-cert validation in v1 (localhost is pod-private).
type TLSConfig struct {
	CertFile string `yaml:"certFile"`
	KeyFile  string `yaml:"keyFile"`
}

// ModelRoute is the built-in /v1 route to the inference-gateway. The proxy
// forwards /v1/<rest> -> <Upstream>/v1/<rest> (no prefix strip) and injects the
// agent-egress SA gateway key (a static bearer). Agent model egress is
// internal-trusted; the gateway enforces per-identity only for direct callers.
type ModelRoute struct {
	Upstream string     `yaml:"upstream"` // gateway origin, e.g. http://inference-gateway.iterabase.svc:80
	Auth     BearerAuth `yaml:"auth"`
}

// BearerAuth injects a static bearer token read from a mounted Secret file.
type BearerAuth struct {
	SecretRef SecretRef `yaml:"secretRef"`
}

// Route is a tool route (/upstreams/<id>/<rest>). The proxy strips
// /upstreams/<id> and forwards <rest> to Upstream.BaseURL/<rest>.
type Route struct {
	ID       string   `yaml:"id"`
	Upstream Upstream `yaml:"upstream"`
	Auth     Auth     `yaml:"auth"`
}

// Upstream names the real target origin.
type Upstream struct {
	BaseURL string `yaml:"baseURL"` // e.g. https://graph.microsoft.com
}

// Auth configures credential injection. Scheme selects the fields.
type Auth struct {
	Scheme          string     `yaml:"scheme"`                    // bearer | oauthClientCredentials
	SecretRef       *SecretRef `yaml:"secretRef,omitempty"`       // bearer
	TokenURL        string     `yaml:"tokenURL,omitempty"`        // oauthClientCredentials
	ClientID        string     `yaml:"clientID,omitempty"`        // oauthClientCredentials
	ClientSecretRef *SecretRef `yaml:"clientSecretRef,omitempty"` // oauthClientCredentials
	Scope           string     `yaml:"scope,omitempty"`           // oauthClientCredentials
}

// SecretRef names a key within a K8s Secret; the proxy reads
// <SecretDir>/<name>/<key>.
type SecretRef struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

// LoadConfig reads + validates a ProxyConfig from a YAML file.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8444"
	}
	if c.HealthAddr == "" {
		c.HealthAddr = ":8081"
	}
	if c.SecretDir == "" {
		c.SecretDir = "/secrets"
	}
}

func (c *Config) validate() error {
	if c.Model.Upstream == "" {
		return fmt.Errorf("model.upstream is required")
	}
	if c.Model.Auth.SecretRef.Name == "" || c.Model.Auth.SecretRef.Key == "" {
		return fmt.Errorf("model.auth.secretRef.{name,key} are required")
	}
	ids := make(map[string]struct{}, len(c.Routes))
	for i, r := range c.Routes {
		if err := validateRoute(i, r, ids); err != nil {
			return err
		}
	}
	return nil
}

// validateRoute checks one tool route's required fields + unique id.
func validateRoute(i int, r Route, ids map[string]struct{}) error {
	if r.ID == "" {
		return fmt.Errorf("routes[%d].id is required", i)
	}
	if _, dup := ids[r.ID]; dup {
		return fmt.Errorf("routes[%d].id %q is duplicate", i, r.ID)
	}
	ids[r.ID] = struct{}{}
	if r.Upstream.BaseURL == "" {
		return fmt.Errorf("routes[%d].upstream.baseURL is required", i)
	}
	return validateAuth(i, r.Auth)
}

// validateAuth checks the auth block matches its scheme.
func validateAuth(i int, a Auth) error {
	switch a.Scheme {
	case "bearer":
		if a.SecretRef == nil || a.SecretRef.Name == "" || a.SecretRef.Key == "" {
			return fmt.Errorf("routes[%d] bearer auth requires secretRef.{name,key}", i)
		}
	case "oauthClientCredentials":
		if a.TokenURL == "" || a.ClientID == "" ||
			a.ClientSecretRef == nil || a.ClientSecretRef.Name == "" || a.ClientSecretRef.Key == "" {
			return fmt.Errorf("routes[%d] oauthClientCredentials requires tokenURL, clientID, clientSecretRef.{name,key}", i)
		}
	default:
		return fmt.Errorf("routes[%d] unknown auth scheme %q", i, a.Scheme)
	}
	return nil
}
