package egress

import (
	"github.com/nunocgoncalves/control-plane/internal/proxy"
)

// ModelRouteConfig is the platform-wide model route (the inference-gateway
// origin + the agent-egress SA gateway key Secret ref) the AgentSandbox
// operator (HOR-245) adds from platform config. It is NOT identity-scoped, so
// it does not come from egress.Resolve; the operator supplies it alongside.
type ModelRouteConfig struct {
	Upstream      string    // gateway origin, e.g. http://inference-gateway.iterabase.svc:80
	AuthSecretRef SecretRef // the agent-egress SA gateway key (Path 1)
}

// BuildProxyConfig assembles a proxy.Config from a resolved route set plus the
// platform model route. This is the HOR-245 operator's bridge:
//
//	egress.Resolve(scopeIdentityID) -> ResolveResult
//	egress.BuildProxyConfig(modelRoute, resolveResult) -> *proxy.Config
//	marshal to YAML -> ConfigMap -> proxy (mounted) + Secrets (mounted)
//
// Tool routes come from the ResolveResult (identity-scoped, from EgressRoute
// CRs); the model route is platform-wide. The returned config carries the
// proxy's default listen/health/secret-dir addresses; the operator may override
// before marshalling. This is the single place that knows both the resolved
// route shape and the proxy's config shape, so a shape drift between them is
// caught by the accompanying contract test.
func BuildProxyConfig(model ModelRouteConfig, resolved ResolveResult) *proxy.Config {
	cfg := &proxy.Config{
		Listen:     ":8444",
		HealthAddr: ":8081",
		SecretDir:  "/secrets",
		Model: proxy.ModelRoute{
			Upstream: model.Upstream,
			Auth:     proxy.BearerAuth{SecretRef: proxy.SecretRef{Name: model.AuthSecretRef.Name, Key: model.AuthSecretRef.Key}},
		},
		Routes: make([]proxy.Route, 0, len(resolved.Routes)),
	}
	for _, r := range resolved.Routes {
		rt := proxy.Route{
			ID:       r.RouteID,
			Upstream: proxy.Upstream{BaseURL: r.UpstreamBaseURL},
		}
		switch r.Auth.Scheme {
		case "bearer":
			if r.Auth.SecretRef != nil {
				rt.Auth = proxy.Auth{
					Scheme:    "bearer",
					SecretRef: &proxy.SecretRef{Name: r.Auth.SecretRef.Name, Key: r.Auth.SecretRef.Key},
				}
			}
		case "oauthClientCredentials":
			var ref *proxy.SecretRef
			if r.Auth.ClientSecretRef != nil {
				ref = &proxy.SecretRef{Name: r.Auth.ClientSecretRef.Name, Key: r.Auth.ClientSecretRef.Key}
			}
			rt.Auth = proxy.Auth{
				Scheme:          "oauthClientCredentials",
				TokenURL:        r.Auth.TokenURL,
				ClientID:        r.Auth.ClientID,
				ClientSecretRef: ref,
				Scope:           r.Auth.Scope,
			}
		}
		cfg.Routes = append(cfg.Routes, rt)
	}
	return cfg
}
