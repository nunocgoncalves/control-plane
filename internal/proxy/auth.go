package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// refreshMargin is how long before expiry the OAuth manager refreshes a token.
const refreshMargin = 60 * time.Second

// bearerCacheTTL is how long a bearer injector caches a secret file's value
// before re-reading (picks up K8s Secret rotation without per-request I/O).
const bearerCacheTTL = 60 * time.Second

// credInjector injects the real credential into an outbound request. The proxy
// strips any inbound auth before calling this.
type credInjector interface {
	inject(ctx context.Context, req *http.Request) error
}

// bearerInjector reads a static token from a mounted Secret file (cached for
// bearerCacheTTL) and sets Authorization: Bearer <token>. Used for the model
// route (agent-egress SA gateway key) and static-key tool routes.
type bearerInjector struct {
	path string

	mu     sync.Mutex
	val    string
	readAt time.Time
}

func (b *bearerInjector) inject(_ context.Context, req *http.Request) error {
	tok, err := b.token()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

func (b *bearerInjector) token() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.val != "" && time.Since(b.readAt) < bearerCacheTTL {
		return b.val, nil
	}
	raw, err := os.ReadFile(b.path) //nolint:gosec // path is operator-provisioned (mounted Secret), not user input
	if err != nil {
		return "", fmt.Errorf("read bearer secret %s: %w", b.path, err)
	}
	b.val = strings.TrimSpace(string(raw))
	b.readAt = time.Now()
	if b.val == "" {
		return "", fmt.Errorf("bearer secret %s is empty", b.path)
	}
	return b.val, nil
}

// oauthInjector acquires (and refreshes) an OAuth2 client-credentials token and
// sets Authorization: Bearer <token>.
type oauthInjector struct {
	mgr *OAuthManager
}

func (o *oauthInjector) inject(ctx context.Context, req *http.Request) error {
	tok, err := o.mgr.Token(ctx)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

// OAuthManager acquires, caches, and refreshes an OAuth2 client-credentials
// token. Concurrent requests share a single refresh (single-flight via the
// mutex); the client secret is re-read from its file on each acquire so K8s
// Secret rotation is picked up within the refresh cycle.
type OAuthManager struct {
	tokenURL   string
	clientID   string
	secretPath string
	scope      string
	client     *http.Client

	mu     sync.Mutex
	cached *oauthToken
}

type oauthToken struct {
	token     string
	expiresAt time.Time
}

// NewOAuthManager builds a manager for the given OAuth2 client-credentials
// config. The client secret is read from secretPath on each acquire.
func NewOAuthManager(tokenURL, clientID, secretPath, scope string) *OAuthManager {
	return &OAuthManager{
		tokenURL:   tokenURL,
		clientID:   clientID,
		secretPath: secretPath,
		scope:      scope,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// Token returns a valid access token, acquiring or refreshing as needed.
func (m *OAuthManager) Token(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cached != nil && time.Until(m.cached.expiresAt) > refreshMargin {
		return m.cached.token, nil
	}
	tok, err := m.acquire(ctx)
	if err != nil {
		// Keep the stale token if we still have one (better than failing); a
		// truly expired token will surface as a 401 from the upstream.
		if m.cached != nil {
			return m.cached.token, nil
		}
		return "", err
	}
	m.cached = tok
	return tok.token, nil
}

// acquire performs the client-credentials grant. Caller holds m.mu.
func (m *OAuthManager) acquire(ctx context.Context) (*oauthToken, error) {
	secret, err := os.ReadFile(m.secretPath) //nolint:gosec // operator-provisioned client-secret path
	if err != nil {
		return nil, fmt.Errorf("read oauth client secret %s: %w", m.secretPath, err)
	}
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {m.clientID},
		"client_secret": {strings.TrimSpace(string(secret))},
	}
	if m.scope != "" {
		form.Set("scope", m.scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.tokenURL, strings.NewReader(form.Encode())) //nolint:gosec // tokenURL is operator-provisioned (EgressRoute auth)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.client.Do(req) //nolint:gosec // acquiring an OAuth token from the configured endpoint
	if err != nil {
		return nil, fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token endpoint %s: %s: %s", m.tokenURL, resp.Status, strings.TrimSpace(string(body)))
	}
	var tr struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, fmt.Errorf("parse token response: %w", err)
	}
	if tr.AccessToken == "" {
		return nil, fmt.Errorf("token response missing access_token")
	}
	expiresIn := time.Duration(tr.ExpiresIn) * time.Second
	if expiresIn <= 0 {
		expiresIn = 5 * time.Minute // fallback if the server omits expires_in
	}
	return &oauthToken{token: tr.AccessToken, expiresAt: time.Now().Add(expiresIn)}, nil
}
