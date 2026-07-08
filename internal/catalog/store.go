// Package catalog implements the control-plane catalog store: the Postgres
// materialization of ModelBackend CRs (catalog.backends) and, later, Model CRs
// + the effective_catalog view (HOR-268). The view is the contract consumed by
// the gateway (HOR-247) and agent-fleet; they read it directly and own their
// cache. Mirrors the identity (HOR-242) and permissions (HOR-243) stores.
package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors.
var (
	// ErrNotFound is returned when no active backend matches.
	ErrNotFound = errors.New("catalog: not found")
)

// Backend is a row from catalog.backends (materialized from a ModelBackend CR).
type Backend struct {
	ID         string
	Key        string // stable natural key: "<namespace>/<name>"
	Name       string // ModelBackend name (Service name)
	Namespace  string
	Kind       string // vLLM | SGLang | external
	Model      string // HuggingFace id (vLLM/SGLang --model)
	ServiceURL string // in-cluster Service URL / external baseURL
	Image      string
	Deployed   bool
	Healthy    bool
}

// Store reads and writes the catalog schema via a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool for catalog operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// UpsertBackend inserts a backend keyed by `key`, or — if the row exists
// (including a soft-deleted one) — revives it and updates its fields. This is
// the reconciler's primary write on add/update and the foundation of
// revive-on-recreate.
func (s *Store) UpsertBackend(ctx context.Context, b Backend) (Backend, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO catalog.backends (key, name, namespace, kind, model, service_url, image, deployed, healthy)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (key) DO UPDATE
			SET name        = EXCLUDED.name,
			    namespace   = EXCLUDED.namespace,
			    kind        = EXCLUDED.kind,
			    model       = EXCLUDED.model,
			    service_url = EXCLUDED.service_url,
			    image       = EXCLUDED.image,
			    deployed    = EXCLUDED.deployed,
			    healthy     = EXCLUDED.healthy,
			    deleted_at  = NULL,
			    updated_at  = now()
		RETURNING id, key, name, namespace, kind, model, service_url, image, deployed, healthy`,
		b.Key, b.Name, b.Namespace, b.Kind, b.Model, b.ServiceURL, b.Image, b.Deployed, b.Healthy)
	return scanBackend(row)
}

// SoftDeleteBackendByKey marks the backend inactive (deleted_at) and unhealthy.
// Used by the reconciler on CR deletion; the row is retained for history.
func (s *Store) SoftDeleteBackendByKey(ctx context.Context, key string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE catalog.backends SET deleted_at = now(), healthy = false
		WHERE key = $1 AND deleted_at IS NULL`, key)
	if err != nil {
		return fmt.Errorf("soft delete backend: %w", err)
	}
	return nil
}

// GetBackendByKey fetches an active backend by its natural key.
func (s *Store) GetBackendByKey(ctx context.Context, key string) (Backend, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, name, namespace, kind, model, service_url, image, deployed, healthy
		FROM catalog.backends WHERE key = $1 AND deleted_at IS NULL`, key)
	b, err := scanBackend(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Backend{}, ErrNotFound
	}
	return b, err
}

// scanBackend scans a 10-column backend row.
func scanBackend(row pgx.Row) (Backend, error) {
	var b Backend
	err := row.Scan(&b.ID, &b.Key, &b.Name, &b.Namespace, &b.Kind, &b.Model, &b.ServiceURL, &b.Image, &b.Deployed, &b.Healthy)
	return b, err
}
