// Package egress implements the control-plane egress credential source: the
// Postgres store for EgressRoute CRs (the data-scope plane) and the
// identity-keyed resolution the AgentSandbox operator (HOR-245) calls at
// provisioning to bake a ProxyConfig into the per-sandbox egress proxy.
//
// The proxy itself (cmd/proxy, HOR-244) is DB-less and location-bound — it
// reads a resolved config from mounted files. This package is the
// control-plane side: the reconciler materializes EgressRoute CRs into
// egress.credentials (Git -> DB bridge), and Resolve reads the
// egress.effective_routes view for a scope identity. Broad-default (v1): every
// active identity gets every route; the view ignores `subject` (stored, not
// enforced). Group-scoped narrowing is additive later (SSO/HOR-314).
//
// The credential VALUE never lives here — only the K8s Secret reference
// (name+key); the value stays in a Secret mounted into the proxy by the
// AgentSandbox operator.
package egress

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors.
var (
	// ErrNotFound is returned when no route matches.
	ErrNotFound = errors.New("egress: not found")
)

// Auth is the materialized credential-injection config for a route (stored as
// JSONB in egress.credentials.auth). The credential VALUE is never here — only
// the K8s Secret reference. Scheme selects which fields apply.
type Auth struct {
	Scheme          string     `json:"scheme"`                    // bearer | oauthClientCredentials
	SecretRef       *SecretRef `json:"secretRef,omitempty"`       // bearer
	TokenURL        string     `json:"tokenURL,omitempty"`        // oauthClientCredentials
	ClientID        string     `json:"clientID,omitempty"`        // oauthClientCredentials
	ClientSecretRef *SecretRef `json:"clientSecretRef,omitempty"` // oauthClientCredentials
	Scope           string     `json:"scope,omitempty"`           // oauthClientCredentials
}

// SecretRef names a key within a K8s Secret. The proxy reads the value from a
// mounted volume at /secrets/<name>/<key>.
type SecretRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// Subject scopes a route to an identity/group. Stored but NOT enforced in v1
// (broad-default); group-scoped narrowing is additive later.
type Subject struct {
	Kind string `json:"kind"` // user | group
	Key  string `json:"key"`  // identity.key
}

// Route is a row from egress.credentials.
type Route struct {
	ID              string
	Key             string // CR "<ns>/<name>"
	Name            string // route-id (EgressRoute name)
	Namespace       string
	UpstreamBaseURL string
	Auth            Auth
	Subject         *Subject
}

// ResolvedRoute is one effective route for a scope identity (from
// egress.effective_routes).
type ResolvedRoute struct {
	RouteID         string
	UpstreamBaseURL string
	Auth            Auth
}

// ResolveResult is what Resolve returns: the effective tool routes for a scope
// identity plus the distinct K8s Secret references the AgentSandbox operator
// must mount into the proxy.
type ResolveResult struct {
	Routes     []ResolvedRoute
	SecretRefs []SecretRef
}

// Store reads and writes the egress schema via a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool for egress operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// UpsertRoute inserts a route keyed by `key`, or — if the row exists
// (including a soft-deleted one) — revives it and updates name/namespace/
// upstream/auth/subject. This is the reconciler's primary write on
// add/update and the foundation of revive-on-recreate.
func (s *Store) UpsertRoute(ctx context.Context, key, name, namespace, upstreamBaseURL string, auth Auth, subject *Subject) (Route, error) {
	authJSON, err := marshalJSON(auth)
	if err != nil {
		return Route{}, fmt.Errorf("marshal auth: %w", err)
	}
	var subjJSON []byte
	if subject != nil {
		if subjJSON, err = marshalJSON(subject); err != nil {
			return Route{}, fmt.Errorf("marshal subject: %w", err)
		}
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO egress.credentials (key, name, namespace, upstream_base_url, auth, subject)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (key) DO UPDATE
			SET name               = EXCLUDED.name,
			    namespace          = EXCLUDED.namespace,
			    upstream_base_url  = EXCLUDED.upstream_base_url,
			    auth               = EXCLUDED.auth,
			    subject            = EXCLUDED.subject,
			    deleted_at         = NULL,
			    updated_at         = now()
		RETURNING id, key, name, namespace, upstream_base_url, auth, subject`,
		key, name, namespace, upstreamBaseURL, authJSON, subjJSON)

	return scanRoute(row)
}

// SoftDeleteRouteByKey marks the route inactive (deleted_at). Used by the
// reconciler on CR deletion: the route drops out of effective_routes (access
// revoked) while the row is kept for history.
func (s *Store) SoftDeleteRouteByKey(ctx context.Context, key string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE egress.credentials SET deleted_at = now()
		WHERE key = $1 AND deleted_at IS NULL`, key)
	if err != nil {
		return fmt.Errorf("soft delete: %w", err)
	}
	_ = ct // may be zero if already deleted or never existed; both are fine
	return nil
}

// GetRouteByKey fetches an active route by its natural key.
func (s *Store) GetRouteByKey(ctx context.Context, key string) (Route, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, name, namespace, upstream_base_url, auth, subject
		FROM egress.credentials WHERE key = $1 AND deleted_at IS NULL`, key)
	r, err := scanRoute(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Route{}, ErrNotFound
	}
	return r, err
}

// scanRoute scans a 7-column route row (auth + subject are JSONB).
func scanRoute(row pgx.Row) (Route, error) {
	var r Route
	var authJSON, subjJSON []byte
	if err := row.Scan(&r.ID, &r.Key, &r.Name, &r.Namespace, &r.UpstreamBaseURL, &authJSON, &subjJSON); err != nil {
		return Route{}, err
	}
	if err := unmarshalJSON(authJSON, &r.Auth); err != nil {
		return Route{}, fmt.Errorf("unmarshal auth: %w", err)
	}
	if len(subjJSON) > 0 {
		var subj Subject
		if err := unmarshalJSON(subjJSON, &subj); err != nil {
			return Route{}, fmt.Errorf("unmarshal subject: %w", err)
		}
		r.Subject = &subj
	}
	return r, nil
}
