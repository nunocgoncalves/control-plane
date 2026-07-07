package server_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/permissions"
	"github.com/nunocgoncalves/control-plane/internal/server"
	"github.com/nunocgoncalves/control-plane/internal/testutil"
)

func TestHealthz(t *testing.T) {
	router := server.New(server.Services{})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestReadyz_NoDatabase(t *testing.T) {
	router := server.New(server.Services{})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

func TestJWKS_NoIssuer(t *testing.T) {
	router := server.New(server.Services{})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/jwks.json", nil)
	router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

// testIssuer generates an RSA key, writes it to a temp file, and returns an
// Issuer ready for tests.
func testIssuer(t *testing.T) *identity.Issuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})
	path := t.TempDir() + "/key.pem"
	require.NoError(t, os.WriteFile(path, pemBytes, 0o600))

	iss, err := identity.NewIssuer(path, "test-kid", "control-plane", "inference-gateway", "15m")
	require.NoError(t, err)
	return iss
}

// TestAPI exercises the identity HTTP API end-to-end against a real Postgres:
// JWKS, delegated token issuance (linked + unlinked), and admin CRUD for users
// + API keys, including scope enforcement. Requires Docker.
func TestAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := testutil.NewPostgresPool(t)
	store := identity.NewStore(pool)
	issuer := testIssuer(t)
	ctx := context.Background()

	router := server.New(server.Services{Pool: pool, Store: store, Permissions: permissions.NewStore(pool), Issuer: issuer, Mode: identity.ModeEnrolled})

	// Seed: an admin local user + admin key, an agent-fleet SA + token key, and
	// a CR-style linked identity (alice) with a teams binding.
	admin, err := store.UpsertLocalUser(ctx, "admin@local", "admin@local", "admin")
	require.NoError(t, err)
	adminKey, _, err := store.CreateAPIKey(ctx, admin.ID, "admin", identity.ScopeAdmin, nil)
	require.NoError(t, err)

	sa, err := store.UpsertServiceAccount(ctx, "agent-fleet", "Agent Fleet")
	require.NoError(t, err)
	saKey, _, err := store.CreateAPIKey(ctx, sa.ID, "agent-fleet", identity.ScopeToken, nil)
	require.NoError(t, err)

	alice, err := store.UpsertIdentity(ctx, "default/alice", "user", identity.SourceExternal, "Alice Wong")
	require.NoError(t, err)
	require.NoError(t, store.ReplaceExternalMappings(ctx, alice.ID, []identity.Binding{
		{Provider: "teams", Type: "user", ExternalID: "aad:alice"},
	}))

	authHeader := func(key string) string { return "Bearer " + key }

	t.Run("jwks", func(t *testing.T) {
		rr := do(router, http.MethodGet, "/.well-known/jwks.json", "", nil)
		assert.Equal(t, http.StatusOK, rr.Code)
		var jwks struct {
			Keys []map[string]string `json:"keys"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &jwks))
		require.Len(t, jwks.Keys, 1)
		assert.Equal(t, "test-kid", jwks.Keys[0]["kid"])
	})

	t.Run("token requires auth", func(t *testing.T) {
		body := `{"provider":"teams","type":"user","externalID":"aad:alice"}`
		rr := do(router, http.MethodPost, "/v1/token", "", strings.NewReader(body))
		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	})

	t.Run("token wrong scope (admin key)", func(t *testing.T) {
		body := `{"provider":"teams","type":"user","externalID":"aad:alice"}`
		rr := do(router, http.MethodPost, "/v1/token", authHeader(adminKey), strings.NewReader(body))
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("token linked user issues JWT", func(t *testing.T) {
		body := `{"provider":"teams","type":"user","externalID":"aad:alice"}`
		rr := do(router, http.MethodPost, "/v1/token", authHeader(saKey), strings.NewReader(body))
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		tok, ok := resp["access_token"].(string)
		require.True(t, ok)
		parts := strings.Split(tok, ".")
		require.Len(t, parts, 3)
		// The JWT subject is alice's identity id.
		payload, err := base64urlDecode(parts[1])
		require.NoError(t, err)
		assert.Contains(t, string(payload), alice.ID)
	})

	t.Run("token unlinked user denied", func(t *testing.T) {
		body := `{"provider":"teams","type":"user","externalID":"aad:nobody"}`
		rr := do(router, http.MethodPost, "/v1/token", authHeader(saKey), strings.NewReader(body))
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("admin CRUD with wrong scope rejected", func(t *testing.T) {
		rr := do(router, http.MethodGet, "/v1/users", authHeader(saKey), nil)
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("create + list + get user", func(t *testing.T) {
		body := `{"email":"bob@local","role":"user"}`
		rr := do(router, http.MethodPost, "/v1/users", authHeader(adminKey), strings.NewReader(body))
		require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
		var u map[string]any
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &u))
		bobID, _ := u["id"].(string)
		require.NotEmpty(t, bobID)

		rr = do(router, http.MethodGet, "/v1/users", authHeader(adminKey), nil)
		require.Equal(t, http.StatusOK, rr.Code)
		var list []map[string]any
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &list))
		assert.GreaterOrEqual(t, len(list), 2) // admin + bob

		rr = do(router, http.MethodGet, "/v1/users/"+bobID, authHeader(adminKey), nil)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("create + list + revoke api key", func(t *testing.T) {
		body := fmt.Sprintf(`{"identityID":%q,"name":"gw","scope":"gateway"}`, alice.ID)
		rr := do(router, http.MethodPost, "/v1/api-keys", authHeader(adminKey), strings.NewReader(body))
		require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		full, _ := resp["fullKey"].(string)
		require.NotEmpty(t, full)
		keyID, _ := resp["id"].(string)
		require.NotEmpty(t, keyID)

		rr = do(router, http.MethodGet, "/v1/api-keys", authHeader(adminKey), nil)
		require.Equal(t, http.StatusOK, rr.Code)

		rr = do(router, http.MethodDelete, "/v1/api-keys/"+keyID, authHeader(adminKey), nil)
		assert.Equal(t, http.StatusNoContent, rr.Code)

		// The revoked key no longer authenticates (via gateway scope is not
		// enforced by control-plane, but revocation makes ValidateAPIKey fail).
		_, _, err := store.ValidateAPIKey(ctx, full)
		assert.ErrorIs(t, err, identity.ErrInvalidAPIKey)
	})

	t.Run("permissions debug: linked identity has wildcard", func(t *testing.T) {
		rr := do(router, http.MethodGet, "/v1/permissions/identities/"+alice.ID, authHeader(adminKey), nil)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		var caps []map[string]string
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &caps))
		require.Len(t, caps, 1)
		assert.Equal(t, "*", caps[0]["resource"])
		assert.Equal(t, "*", caps[0]["action"])
	})

	t.Run("permissions debug: unknown identity 404", func(t *testing.T) {
		rr := do(router, http.MethodGet, "/v1/permissions/identities/00000000-0000-0000-0000-000000000000", authHeader(adminKey), nil)
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("permissions debug: wrong scope rejected", func(t *testing.T) {
		rr := do(router, http.MethodGet, "/v1/permissions/identities/"+alice.ID, authHeader(saKey), nil)
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})
}

// do is a small request helper.
func do(handler http.Handler, method, path, auth string, body io.Reader) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, body)
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, r)
	return rr
}

func base64urlDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
