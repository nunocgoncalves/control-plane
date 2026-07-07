// Package server exposes the control-plane HTTP API (cmd/api). It serves
// health/readiness, JWKS, the delegated-token endpoint (Path 2), and the admin
// endpoints for local users + API keys.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nunocgoncalves/control-plane/internal/identity"
	"github.com/nunocgoncalves/control-plane/internal/permissions"
)

// Services are the dependencies injected into the HTTP API.
type Services struct {
	Pool        *pgxpool.Pool
	Store       *identity.Store
	Permissions *permissions.Store
	Issuer      *identity.Issuer
	Mode        string // enrolled | open
}

type contextKey string

const (
	ctxIdentity contextKey = "identity"
	ctxAPIKey   contextKey = "apikey"
)

// Handler holds the identity service dependencies used by the API handlers.
type Handler struct {
	store    *identity.Store
	perms    *permissions.Store
	issuer   *identity.Issuer
	resolver *identity.Resolver
}

// New builds the HTTP API router.
//
//	/healthz                       - liveness (always 200)
//	/readyz                        - readiness (DB reachable -> 200, else 503)
//	/.well-known/jwks.json         - JWKS (public)
//	POST /v1/token                 - delegated JWT (scope=token)
//	POST /v1/users                 - create a local user (scope=admin)
//	GET  /v1/users                 - list local users (scope=admin)
//	GET  /v1/users/{id}            - get a local user (scope=admin)
//	POST /v1/api-keys              - issue an API key (scope=admin)
//	GET  /v1/api-keys              - list API keys (scope=admin)
//	DELETE /v1/api-keys/{id}       - revoke an API key (scope=admin)
//	GET  /v1/permissions/identities/{id} - effective capabilities (scope=admin)
func New(svc Services) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", healthz)
	r.Get("/readyz", readyz(svc.Pool))

	h := &Handler{
		store:    svc.Store,
		perms:    svc.Permissions,
		issuer:   svc.Issuer,
		resolver: identity.NewResolver(svc.Store, svc.Mode),
	}

	r.Get("/.well-known/jwks.json", h.jwks)

	// Delegated token endpoint: service accounts (scope=token) authenticate and
	// mint a JWT for a resolved surface user.
	r.With(h.auth(identity.ScopeToken)).Post("/v1/token", h.token)

	// Admin endpoints (scope=admin).
	r.Route("/v1", func(r chi.Router) {
		r.Use(h.auth(identity.ScopeAdmin))
		r.Get("/users", h.listUsers)
		r.Post("/users", h.createUser)
		r.Get("/users/{id}", h.getUser)
		r.Get("/api-keys", h.listAPIKeys)
		r.Post("/api-keys", h.createAPIKey)
		r.Delete("/api-keys/{id}", h.deleteAPIKey)
		r.Get("/permissions/identities/{id}", h.getCapabilities)
	})

	return r
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func readyz(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool == nil {
			http.Error(w, "database not ready", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "database not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	}
}

// jwks serves the public signing key set.
func (h *Handler) jwks(w http.ResponseWriter, _ *http.Request) {
	if h.issuer == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "jwt issuer not configured"})
		return
	}
	out, err := h.issuer.JWKS()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "building jwks"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

type tokenRequest struct {
	Provider   string `json:"provider"`
	Type       string `json:"type"`
	ExternalID string `json:"externalID"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// token resolves a surface user to an identity and issues a delegated JWT
// (Path 2). The calling service account is authenticated by the auth middleware.
func (h *Handler) token(w http.ResponseWriter, r *http.Request) {
	if h.issuer == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "jwt issuer not configured"})
		return
	}

	var req tokenRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	if req.Provider == "" || req.Type == "" || req.ExternalID == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "provider, type, and externalID are required"})
		return
	}

	ident, err := h.resolver.Resolve(r.Context(), req.Provider, req.Type, req.ExternalID)
	if errors.Is(err, identity.ErrOpenModeNotImplemented) {
		writeJSON(w, http.StatusNotImplemented, errorBody{Error: err.Error()})
		return
	}
	if errors.Is(err, identity.ErrNotFound) {
		// Enrolled mode: unlinked surface user is denied.
		writeJSON(w, http.StatusForbidden, errorBody{Error: "surface user is not linked to an identity"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "resolving identity"})
		return
	}

	tok, err := h.issuer.Issue(ident.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "issuing token"})
		return
	}

	writeJSON(w, http.StatusOK, tokenResponse{
		AccessToken: tok,
		TokenType:   "Bearer",
		ExpiresIn:   int(h.issuer.TTL().Seconds()),
	})
}

type createUserRequest struct {
	Email string `json:"email"`
	Role  string `json:"role"` // admin | user; defaults to user
}

func (h *Handler) createUser(w http.ResponseWriter, r *http.Request) {
	var req createUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	if req.Email == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "email is required"})
		return
	}
	role := req.Role
	if role == "" {
		role = "user"
	}
	if role != "admin" && role != "user" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "role must be admin or user"})
		return
	}

	u, err := h.store.UpsertLocalUser(r.Context(), req.Email, req.Email, role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "creating user"})
		return
	}
	writeJSON(w, http.StatusCreated, toUserResponse(u))
}

func (h *Handler) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.store.ListLocalUsers(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "listing users"})
		return
	}
	out := make([]userResponse, 0, len(users))
	for _, u := range users {
		out = append(out, toUserResponse(u))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) getUser(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	u, err := h.store.GetLocalUser(r.Context(), id)
	if errors.Is(err, identity.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, errorBody{Error: "user not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "getting user"})
		return
	}
	writeJSON(w, http.StatusOK, toUserResponse(u))
}

type createAPIKeyRequest struct {
	IdentityID string     `json:"identityID"`
	Name       string     `json:"name"`
	Scope      string     `json:"scope"`
	ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
}

type apiKeyResponse struct {
	ID         string     `json:"id"`
	IdentityID string     `json:"identityID"`
	Prefix     string     `json:"prefix"`
	Name       string     `json:"name"`
	Scope      string     `json:"scope"`
	ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
	RevokedAt  *time.Time `json:"revokedAt,omitempty"`
	CreatedAt  time.Time  `json:"createdAt"`
}

type createAPIKeyResponse struct {
	apiKeyResponse
	FullKey string `json:"fullKey"` // shown once; not stored
}

func (h *Handler) createAPIKey(w http.ResponseWriter, r *http.Request) {
	var req createAPIKeyRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	if req.IdentityID == "" || !identity.ValidScope(req.Scope) {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "identityID and a valid scope are required"})
		return
	}

	full, key, err := h.store.CreateAPIKey(r.Context(), req.IdentityID, req.Name, req.Scope, req.ExpiresAt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "creating api key"})
		return
	}
	writeJSON(w, http.StatusCreated, createAPIKeyResponse{apiKeyResponse: toAPIKeyResponse(key), FullKey: full})
}

func (h *Handler) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := h.store.ListAPIKeys(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "listing api keys"})
		return
	}
	out := make([]apiKeyResponse, 0, len(keys))
	for _, k := range keys {
		out = append(out, toAPIKeyResponse(k))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) deleteAPIKey(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.store.RevokeAPIKey(r.Context(), id); err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorBody{Error: "api key not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "revoking api key"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type capabilityResponse struct {
	Resource string `json:"resource"`
	Action   string `json:"action"`
}

// getCapabilities returns the effective capability rows for an identity (admin
// debug). It reads the permissions.effective_capabilities view — the same
// contract the gateway (HOR-247) and agent-fleet consume directly. No rows
// means the identity is unknown/inactive (denied) -> 404.
func (h *Handler) getCapabilities(w http.ResponseWriter, r *http.Request) {
	if h.perms == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "permissions store not configured"})
		return
	}
	id := chi.URLParam(r, "id")
	caps, err := h.perms.EffectiveCapabilities(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "evaluating capabilities"})
		return
	}
	if len(caps) == 0 {
		writeJSON(w, http.StatusNotFound, errorBody{Error: "no capabilities for identity"})
		return
	}
	out := make([]capabilityResponse, 0, len(caps))
	for _, c := range caps {
		out = append(out, capabilityResponse{Resource: c.Resource, Action: c.Action})
	}
	writeJSON(w, http.StatusOK, out)
}

// auth validates the Bearer API key and requires the given scope, then stores
// the identity + key in the request context.
func (h *Handler) auth(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if h.store == nil {
				writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "identity store not configured"})
				return
			}
			token, ok := identity.ParseBearer(r.Header.Get("Authorization"))
			if !ok {
				writeJSON(w, http.StatusUnauthorized, errorBody{Error: "missing or invalid authorization"})
				return
			}
			key, ident, err := h.store.ValidateAPIKey(r.Context(), token)
			if err != nil {
				writeJSON(w, http.StatusUnauthorized, errorBody{Error: "invalid api key"})
				return
			}
			if key.Scope != scope {
				writeJSON(w, http.StatusForbidden, errorBody{Error: "insufficient scope"})
				return
			}
			ctx := context.WithValue(r.Context(), ctxIdentity, ident)
			ctx = context.WithValue(ctx, ctxAPIKey, key)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// --- helpers ---

type errorBody struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

type userResponse struct {
	ID          string `json:"id"`
	Key         string `json:"key"`
	Email       string `json:"email"`
	Role        string `json:"role"`
	DisplayName string `json:"displayName"`
}

func toUserResponse(u identity.LocalUser) userResponse {
	return userResponse{ID: u.ID, Key: u.Key, Email: u.Email, Role: u.Role, DisplayName: u.DisplayName}
}

func toAPIKeyResponse(k identity.APIKey) apiKeyResponse {
	return apiKeyResponse{
		ID: k.ID, IdentityID: k.IdentityID, Prefix: k.Prefix, Name: k.Name,
		Scope: k.Scope, ExpiresAt: k.ExpiresAt, RevokedAt: k.RevokedAt, CreatedAt: k.CreatedAt,
	}
}
