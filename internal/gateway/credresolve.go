package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	v1 "github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1"
)

// SecretResolver resolves a K8s SecretRef to its value. The production
// implementation reads Secrets via the in-cluster Kubernetes API (Secret-read
// RBAC scoped to the platform namespace); tests inject a fake. Values live only
// in gateway memory + the runner's per-invocation context (ARCH-008).
type SecretResolver interface {
	Resolve(ctx context.Context, ref SecretRef) (string, error)
}

// OAuthAcquirer performs an OAuth2 client-credentials grant. The gateway
// centralizes OAuth acquisition/refresh (ARCH-008): the runner receives a
// short-lived bearer token, never the client secret. Tests inject a fake.
type OAuthAcquirer interface {
	Acquire(ctx context.Context, tokenURL, clientID, clientSecret, scope string) (string, error)
}

// BearerSpec is the secret_ref JSONB spec for scheme=bearer.
type BearerSpec struct {
	ValueRef SecretRef `json:"value_ref"`
}

// OAuthSpec is the secret_ref JSONB spec for scheme=oauth_client_credentials.
type OAuthSpec struct {
	ClientID        string    `json:"client_id"`
	ClientSecretRef SecretRef `json:"client_secret_ref"`
	TokenURL        string    `json:"token_url"`
	Scope           string    `json:"scope"`
}

// resolveCredential builds one proto Credential for a binding, resolving secret
// values via the resolvers. The returned Credential is handed to the trusted
// runner over mTLS (ARCH-008).
func resolveCredential(ctx context.Context, b CredentialBinding, secrets SecretResolver, oauth OAuthAcquirer) (*v1.Credential, error) {
	switch b.Scheme {
	case CredBearer:
		var spec BearerSpec
		if err := unmarshalJSON(b.SecretRef, &spec); err != nil {
			return nil, fmt.Errorf("parse bearer spec: %w", err)
		}
		val, err := secrets.Resolve(ctx, spec.ValueRef)
		if err != nil {
			return nil, fmt.Errorf("resolve bearer secret: %w", err)
		}
		return &v1.Credential{
			Scheme:                  v1.CredentialScheme_CREDENTIAL_SCHEME_BEARER,
			BearerValue:             val,
			ResourceConstraintsJson: b.ResourceConstraints,
		}, nil
	case CredOAuthClientCredentials:
		var spec OAuthSpec
		if err := unmarshalJSON(b.SecretRef, &spec); err != nil {
			return nil, fmt.Errorf("parse oauth spec: %w", err)
		}
		secret, err := secrets.Resolve(ctx, spec.ClientSecretRef)
		if err != nil {
			return nil, fmt.Errorf("resolve oauth client secret: %w", err)
		}
		token, err := oauth.Acquire(ctx, spec.TokenURL, spec.ClientID, secret, spec.Scope)
		if err != nil {
			return nil, fmt.Errorf("oauth acquire: %w", err)
		}
		return &v1.Credential{
			Scheme:                  v1.CredentialScheme_CREDENTIAL_SCHEME_OAUTH_CLIENT_CREDENTIALS,
			BearerValue:             token, // the acquired access token; runner uses it as a bearer
			ResourceConstraintsJson: b.ResourceConstraints,
			OauthClientId:           spec.ClientID,
			OauthTokenUrl:           spec.TokenURL,
			OauthScope:              spec.Scope,
		}, nil
	}
	return nil, fmt.Errorf("unknown credential scheme %q", b.Scheme)
}

// resolveCredentialContext resolves all slot bindings for a pool+tool into a
// CredentialContext keyed by slot name.
func resolveCredentialContext(ctx context.Context, poolID, toolName string, store *Store, secrets SecretResolver, oauth OAuthAcquirer) (*v1.CredentialContext, error) {
	bindings, err := store.ResolveCredentialBindings(ctx, poolID, toolName)
	if err != nil {
		return nil, err
	}
	slots := make(map[string]*v1.Credential, len(bindings))
	for _, b := range bindings {
		cred, err := resolveCredential(ctx, b, secrets, oauth)
		if err != nil {
			return nil, fmt.Errorf("slot %q: %w", b.SlotName, err)
		}
		slots[b.SlotName] = cred
	}
	return &v1.CredentialContext{Slots: slots}, nil
}

// --- production implementations ---

// FakeSecretResolver is an in-memory SecretResolver for tests. (Production
// uses k8sSecretResolver in cmd/gateway.)
type FakeSecretResolver struct {
	mu    sync.Mutex
	store map[string]string // key: name+"/"+key
}

// NewFakeSecretResolver returns a SecretResolver backed by an in-memory map.
// Tests seed values with Set.
func NewFakeSecretResolver() *FakeSecretResolver {
	return &FakeSecretResolver{store: make(map[string]string)}
}

// Set seeds a secret value (test helper).
func (f *FakeSecretResolver) Set(name, key, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store[name+"/"+key] = value
}

func (f *FakeSecretResolver) Resolve(_ context.Context, ref SecretRef) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.store[ref.Name+"/"+ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s not found", ref.Name, ref.Key)
	}
	return v, nil
}

// httpOAuthAcquirer performs a real OAuth2 client-credentials grant over HTTP.
type httpOAuthAcquirer struct{ client *http.Client }

// NewHTTPOAuthAcquirer returns an OAuthAcquirer that performs real grants.
func NewHTTPOAuthAcquirer(client *http.Client) OAuthAcquirer {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &httpOAuthAcquirer{client: client}
}

func (h *httpOAuthAcquirer) Acquire(ctx context.Context, tokenURL, clientID, clientSecret, scope string) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	if scope != "" {
		form.Set("scope", scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := h.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oauth token endpoint %s: %s: %s", tokenURL, resp.Status, body)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", fmt.Errorf("parse token response: %w", err)
	}
	if tok.AccessToken == "" {
		return "", fmt.Errorf("token response missing access_token")
	}
	return tok.AccessToken, nil
}

// fakeOAuthAcquirer returns a canned token for tests.
type fakeOAuthAcquirer struct{ token string }

// NewFakeOAuthAcquirer returns an OAuthAcquirer that returns the given token.
func NewFakeOAuthAcquirer(token string) OAuthAcquirer { return &fakeOAuthAcquirer{token: token} }

func (f *fakeOAuthAcquirer) Acquire(_ context.Context, _, _, _, _ string) (string, error) {
	return f.token, nil
}
