// Package permissions implements the control-plane permission store: the
// Postgres materialization of PermissionPolicy CRs and the effective-capabilities
// + effective-rate-limits views that are the permission/rate-limit engine. It is
// shared by the manager (reconciler) and the api (admin debug endpoint). The
// views are the contract consumed by the gateway (HOR-247) and agent-fleet; they
// read them directly and own their cache.
package permissions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors.
var (
	// ErrNotFound is returned when no active policy matches.
	ErrNotFound = errors.New("permissions: not found")
)

// Policy is a row from permissions.policies.
type Policy struct {
	ID          string
	Key         string
	SubjectKind string // user | group | service_account | workflow
	SubjectKey  string
	RateLimits  *RateLimits // nil = unlimited
}

// RateLimits is a per-identity throughput limit (gateway-enforced).
type RateLimits struct {
	RPM int
	TPM int
}

// Capability is a row from permissions.effective_capabilities: an identity is
// granted (resource, action). A wildcard '*' matches any resource/action.
type Capability struct {
	IdentityID string
	Resource   string
	Action     string
}

// Store reads and writes the permissions schema via a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool for permission operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// UpsertPolicy inserts a policy keyed by `key`, or — if the row exists
// (including a soft-deleted one) — revives it and updates its subject + rate
// limits. This is the reconciler's primary write on add/update and the
// foundation of revive-on-recreate. A nil rateLimits clears the limit.
func (s *Store) UpsertPolicy(ctx context.Context, key, subjectKind, subjectKey string, rateLimits *RateLimits) (Policy, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO permissions.policies (key, subject_kind, subject_key, rate_limits)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (key) DO UPDATE
			SET subject_kind = EXCLUDED.subject_kind,
			    subject_key  = EXCLUDED.subject_key,
			    rate_limits  = EXCLUDED.rate_limits,
			    deleted_at   = NULL,
			    updated_at   = now()
		RETURNING id, key, subject_kind, subject_key, rate_limits`,
		key, subjectKind, subjectKey, rateLimitsJSONB(rateLimits))
	return scanPolicy(row)
}

// SoftDeletePolicyByKey marks the policy inactive (deleted_at). Used by the
// reconciler on CR deletion; the row is retained for history.
func (s *Store) SoftDeletePolicyByKey(ctx context.Context, key string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE permissions.policies SET deleted_at = now()
		WHERE key = $1 AND deleted_at IS NULL`, key)
	if err != nil {
		return fmt.Errorf("soft delete policy: %w", err)
	}
	return nil
}

// GetPolicyByKey fetches an active policy by its natural key.
func (s *Store) GetPolicyByKey(ctx context.Context, key string) (Policy, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, subject_kind, subject_key, rate_limits
		FROM permissions.policies WHERE key = $1 AND deleted_at IS NULL`, key)
	p, err := scanPolicy(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Policy{}, ErrNotFound
	}
	return p, err
}

// EffectiveCapabilities returns the capability rows the view grants to an
// identity. An empty result means the identity is unknown/inactive (denied) —
// the caller (debug endpoint) treats empty as not found.
func (s *Store) EffectiveCapabilities(ctx context.Context, identityID string) ([]Capability, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT identity_id, resource, action
		FROM permissions.effective_capabilities WHERE identity_id = $1`, identityID)
	if err != nil {
		return nil, fmt.Errorf("query effective capabilities: %w", err)
	}
	defer rows.Close()

	var out []Capability
	for rows.Next() {
		var c Capability
		if err := rows.Scan(&c.IdentityID, &c.Resource, &c.Action); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// EffectiveRateLimits returns the per-identity throughput limit from the
// effective_rate_limits view, or ErrNotFound if the identity has no rate-limit
// policy (caller treats not-found as unlimited).
func (s *Store) EffectiveRateLimits(ctx context.Context, identityID string) (RateLimits, error) {
	var rl RateLimits
	err := s.pool.QueryRow(ctx, `
		SELECT rpm, tpm FROM permissions.effective_rate_limits WHERE identity_id = $1`,
		identityID).Scan(&rl.RPM, &rl.TPM)
	if errors.Is(err, pgx.ErrNoRows) {
		return RateLimits{}, ErrNotFound
	}
	return rl, err
}

// rateLimitsJSONB marshals rate limits for the jsonb column, or returns nil
// (→ NULL) when unlimited.
func rateLimitsJSONB(rl *RateLimits) any {
	if rl == nil {
		return nil
	}
	b, err := json.Marshal(map[string]int{"rpm": rl.RPM, "tpm": rl.TPM})
	if err != nil {
		return nil
	}
	return string(b)
}

// scanPolicy scans a 5-column policy row (rate_limits is nullable jsonb).
func scanPolicy(row pgx.Row) (Policy, error) {
	var p Policy
	var rlBytes []byte
	if err := row.Scan(&p.ID, &p.Key, &p.SubjectKind, &p.SubjectKey, &rlBytes); err != nil {
		return Policy{}, err
	}
	if len(rlBytes) > 0 {
		var raw struct {
			RPM int `json:"rpm"`
			TPM int `json:"tpm"`
		}
		if err := json.Unmarshal(rlBytes, &raw); err == nil {
			p.RateLimits = &RateLimits{RPM: raw.RPM, TPM: raw.TPM}
		}
	}
	return p, nil
}
