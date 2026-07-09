package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Model is a row from catalog.models (materialized from a Model CR). The
// per-alias config fields are raw JSON (JSONB columns); producers marshal and
// consumers (gateway HOR-247) unmarshal into their own types.
type Model struct {
	ID              string
	Key             string
	Namespace       string
	ModelID         string
	DisplayName     string
	ContextLength   int
	Capabilities    []byte // JSONB array of strings
	BackendRef      string
	DefaultParams   []byte // JSONB
	ReasoningConfig []byte // JSONB
	Transforms      []byte // JSONB
	RateLimits      []byte // JSONB
	Available       bool
}

// CatalogEntry is a row of catalog.effective_catalog — the contract the gateway
// routes on: alias -> backend_url (+ backend_model_id for the model-field
// rewrite) + per-alias config, filtered to available rows.
type CatalogEntry struct {
	ModelKey        string
	Namespace       string
	ModelID         string
	DisplayName     string
	ContextLength   int
	Capabilities    []byte
	BackendRef      string
	BackendKind     string
	BackendModelID  string
	BackendURL      string
	DefaultParams   []byte
	ReasoningConfig []byte
	Transforms      []byte
	RateLimits      []byte
	Available       bool
}

// UpsertModel inserts a model keyed by `key`, or revives+updates it. The caller
// must pass valid JSON for the JSONB fields (marshaled from the CRD structs).
func (s *Store) UpsertModel(ctx context.Context, m Model) (Model, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO catalog.models (key, namespace, model_id, display_name, context_length, capabilities, backend_ref, default_params, reasoning_config, transforms, rate_limits, available)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (key) DO UPDATE
			SET namespace        = EXCLUDED.namespace,
			    model_id         = EXCLUDED.model_id,
			    display_name     = EXCLUDED.display_name,
			    context_length   = EXCLUDED.context_length,
			    capabilities     = EXCLUDED.capabilities,
			    backend_ref      = EXCLUDED.backend_ref,
			    default_params   = EXCLUDED.default_params,
			    reasoning_config = EXCLUDED.reasoning_config,
			    transforms       = EXCLUDED.transforms,
			    rate_limits      = EXCLUDED.rate_limits,
			    available        = EXCLUDED.available,
			    deleted_at       = NULL,
			    updated_at       = now()
		RETURNING id, key, namespace, model_id, display_name, context_length, capabilities, backend_ref, default_params, reasoning_config, transforms, rate_limits, available`,
		m.Key, m.Namespace, m.ModelID, m.DisplayName, m.ContextLength,
		jsonb(m.Capabilities), m.BackendRef, jsonb(m.DefaultParams), jsonb(m.ReasoningConfig), jsonb(m.Transforms), jsonb(m.RateLimits),
		m.Available)
	return scanModel(row)
}

// SoftDeleteModelByKey marks the model inactive (deleted_at) and unavailable.
func (s *Store) SoftDeleteModelByKey(ctx context.Context, key string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE catalog.models SET deleted_at = now(), available = false
		WHERE key = $1 AND deleted_at IS NULL`, key)
	if err != nil {
		return fmt.Errorf("soft delete model: %w", err)
	}
	return nil
}

// GetModelByKey fetches an active model by its natural key.
func (s *Store) GetModelByKey(ctx context.Context, key string) (Model, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, namespace, model_id, display_name, context_length, capabilities, backend_ref, default_params, reasoning_config, transforms, rate_limits, available
		FROM catalog.models WHERE key = $1 AND deleted_at IS NULL`, key)
	m, err := scanModel(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Model{}, ErrNotFound
	}
	return m, err
}

// EffectiveCatalog reads catalog.effective_catalog — the gateway's contract.
func (s *Store) EffectiveCatalog(ctx context.Context) ([]CatalogEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT model_key, namespace, model_id, display_name, context_length, capabilities, backend_ref, backend_kind, backend_model_id, backend_url, default_params, reasoning_config, transforms, rate_limits, available
		FROM catalog.effective_catalog ORDER BY namespace, model_id`)
	if err != nil {
		return nil, fmt.Errorf("query effective_catalog: %w", err)
	}
	defer rows.Close()

	var out []CatalogEntry
	for rows.Next() {
		var e CatalogEntry
		if err := rows.Scan(&e.ModelKey, &e.Namespace, &e.ModelID, &e.DisplayName, &e.ContextLength, &e.Capabilities, &e.BackendRef, &e.BackendKind, &e.BackendModelID, &e.BackendURL, &e.DefaultParams, &e.ReasoningConfig, &e.Transforms, &e.RateLimits, &e.Available); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// jsonb returns the JSON text for a JSONB column, or NULL when empty (the column
// default applies on insert). Callers pass already-marshaled JSON.
func jsonb(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func scanModel(row pgx.Row) (Model, error) {
	var m Model
	err := row.Scan(&m.ID, &m.Key, &m.Namespace, &m.ModelID, &m.DisplayName, &m.ContextLength, &m.Capabilities, &m.BackendRef, &m.DefaultParams, &m.ReasoningConfig, &m.Transforms, &m.RateLimits, &m.Available)
	return m, err
}
