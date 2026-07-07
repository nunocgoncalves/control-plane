// Package identity implements the control-plane identity service: the Postgres
// store for identities and their credentials, JWT/JWKS issuance, and surface-to-
// identity resolution. It is shared by the manager (reconciler) and the api
// (HTTP endpoints + bootstrap).
package identity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors.
var (
	// ErrNotFound is returned when no identity/binding matches.
	ErrNotFound = errors.New("identity: not found")
	// ErrInvalidAPIKey is returned when an API key is absent, revoked, expired,
	// or bound to a soft-deleted identity.
	ErrInvalidAPIKey = errors.New("identity: invalid api key")
)

// Identity is a row from identity.identities.
type Identity struct {
	ID          string
	Key         string
	Kind        string // user | group | service_account | workflow
	Source      string // local | external | external-auto
	DisplayName string
}

// LocalUser is an identity with a local_users satellite row.
type LocalUser struct {
	Identity
	Email string
	Role  string // admin | user
}

// APIKey is a row from identity.api_keys (never includes the full key).
type APIKey struct {
	ID         string
	IdentityID string
	Prefix     string
	Name       string
	Scope      string
	ExpiresAt  *time.Time
	LastUsedAt *time.Time
	RevokedAt  *time.Time
	CreatedAt  time.Time
}

// Binding pairs an identity to an external provider ID.
type Binding struct {
	Provider   string // slack | teams
	Type       string // user | group
	ExternalID string
}

// Store reads and writes the identity schema via a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool for identity operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// UpsertIdentity inserts an identity keyed by `key`, or — if the row exists
// (including a soft-deleted one) — revives it and updates kind/source/display.
// This is the reconciler's primary write on add/update and the foundation of
// revive-on-recreate (HOR-242 Q9).
func (s *Store) UpsertIdentity(ctx context.Context, key, kind, source, displayName string) (Identity, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (key) DO UPDATE
			SET kind         = EXCLUDED.kind,
			    source       = EXCLUDED.source,
			    display_name = EXCLUDED.display_name,
			    deleted_at   = NULL,
			    updated_at   = now()
		RETURNING id, key, kind, source, display_name`,
		key, kind, source, displayName)

	return scanIdentity(row)
}

// SoftDeleteIdentityByKey marks the identity inactive (deleted_at) and removes
// its external mappings, in one transaction. Used by the reconciler on CR
// deletion: access is revoked (no bindings -> not resolvable) while the row is
// kept for usage/history (HOR-242 Q9).
func (s *Store) SoftDeleteIdentityByKey(ctx context.Context, key string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		DELETE FROM identity.external_mappings
		WHERE identity_id = (SELECT id FROM identity.identities WHERE key = $1)`, key); err != nil {
		return fmt.Errorf("delete mappings: %w", err)
	}

	ct, err := tx.Exec(ctx, `
		UPDATE identity.identities SET deleted_at = now()
		WHERE key = $1 AND deleted_at IS NULL`, key)
	if err != nil {
		return fmt.Errorf("soft delete: %w", err)
	}
	_ = ct // may be zero if already deleted or never existed; both are fine

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// ReplaceExternalMappings atomically replaces the identity's external bindings
// with the given set (deduped). A binding claimed by another identity violates
// the UNIQUE(provider,type,external_id) constraint and is returned as an error
// so the reconciler can surface it in status.
func (s *Store) ReplaceExternalMappings(ctx context.Context, identityID string, bindings []Binding) error {
	deduped := dedupeBindings(bindings)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM identity.external_mappings WHERE identity_id = $1`, identityID); err != nil {
		return fmt.Errorf("clear mappings: %w", err)
	}

	for _, b := range deduped {
		if _, err := tx.Exec(ctx, `
			INSERT INTO identity.external_mappings (identity_id, provider, type, external_id)
			VALUES ($1, $2, $3, $4)`, identityID, b.Provider, b.Type, b.ExternalID); err != nil {
			return fmt.Errorf("insert binding %s/%s/%s: %w", b.Provider, b.Type, b.ExternalID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// ResolveByExternalID resolves an external binding to an active identity. Used
// by the token endpoint (Path 2) and (in open mode, deferred) auto-provisioning.
func (s *Store) ResolveByExternalID(ctx context.Context, provider, bindingType, externalID string) (Identity, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT i.id, i.key, i.kind, i.source, i.display_name
		FROM identity.external_mappings m
		JOIN identity.identities i ON i.id = m.identity_id
		WHERE m.provider = $1 AND m.type = $2 AND m.external_id = $3
		  AND i.deleted_at IS NULL`,
		provider, bindingType, externalID)

	id, err := scanIdentity(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Identity{}, ErrNotFound
	}
	return id, err
}

// GetIdentityByID fetches an active identity by its UUID.
func (s *Store) GetIdentityByID(ctx context.Context, id string) (Identity, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, key, kind, source, display_name
		FROM identity.identities WHERE id = $1 AND deleted_at IS NULL`, id)

	ident, err := scanIdentity(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Identity{}, ErrNotFound
	}
	return ident, err
}

// UpsertLocalUser creates or revives a local-user identity keyed by `key`
// (email), and upserts its local_users satellite row. Idempotent for bootstrap.
func (s *Store) UpsertLocalUser(ctx context.Context, key, email, role string) (LocalUser, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LocalUser{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var identID string
	err = tx.QueryRow(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ($1, 'user', 'local', $2)
		ON CONFLICT (key) DO UPDATE
			SET deleted_at   = NULL,
			    display_name = EXCLUDED.display_name,
			    updated_at   = now()
		RETURNING id`, key, email).Scan(&identID)
	if err != nil {
		return LocalUser{}, fmt.Errorf("upsert identity: %w", err)
	}

	var lu LocalUser
	err = tx.QueryRow(ctx, `
		INSERT INTO identity.local_users (identity_id, email, role)
		VALUES ($1, $2, $3)
		ON CONFLICT (identity_id) DO UPDATE
			SET email = EXCLUDED.email, role = EXCLUDED.role, updated_at = now()
		RETURNING identity_id, email, role`,
		identID, email, role).Scan(&lu.ID, &lu.Email, &lu.Role)
	if err != nil {
		return LocalUser{}, fmt.Errorf("upsert local_user: %w", err)
	}

	// Backfill the rest of the identity fields for a complete LocalUser.
	ident, err := s.identityInTx(ctx, tx, identID)
	if err != nil {
		return LocalUser{}, err
	}
	lu.Identity = ident

	if err := tx.Commit(ctx); err != nil {
		return LocalUser{}, fmt.Errorf("commit: %w", err)
	}
	return lu, nil
}

// UpsertServiceAccount creates or revives a service-account identity keyed by
// `key` (the SA name). Service accounts have api_keys but no satellite table.
func (s *Store) UpsertServiceAccount(ctx context.Context, key, displayName string) (Identity, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO identity.identities (key, kind, source, display_name)
		VALUES ($1, 'service_account', 'local', $2)
		ON CONFLICT (key) DO UPDATE
			SET deleted_at   = NULL,
			    display_name = EXCLUDED.display_name,
			    updated_at   = now()
		RETURNING id, key, kind, source, display_name`, key, displayName)
	return scanIdentity(row)
}

// CreateAPIKey generates a new API key for an identity and stores its hash.
// The full key is returned once; only the hash + prefix are persisted.
func (s *Store) CreateAPIKey(ctx context.Context, identityID, name, scope string, expiresAt *time.Time) (string, APIKey, error) {
	if !ValidScope(scope) {
		return "", APIKey{}, fmt.Errorf("invalid scope %q", scope)
	}
	full, prefix, hash, err := GenerateAPIKey()
	if err != nil {
		return "", APIKey{}, err
	}

	var k APIKey
	err = s.pool.QueryRow(ctx, `
		INSERT INTO identity.api_keys (identity_id, key_hash, prefix, name, scope, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, identity_id, prefix, name, scope, expires_at, last_used_at, revoked_at, created_at`,
		identityID, hash, prefix, name, scope, expiresAt).
		Scan(&k.ID, &k.IdentityID, &k.Prefix, &k.Name, &k.Scope, &k.ExpiresAt, &k.LastUsedAt, &k.RevokedAt, &k.CreatedAt)
	if err != nil {
		return "", APIKey{}, fmt.Errorf("insert api_key: %w", err)
	}
	return full, k, nil
}

// RevokeAPIKeysForIdentity revokes all non-revoked keys of the given scope for
// an identity. Used by bootstrap --reset.
func (s *Store) RevokeAPIKeysForIdentity(ctx context.Context, identityID, scope string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE identity.api_keys SET revoked_at = now()
		WHERE identity_id = $1 AND scope = $2 AND revoked_at IS NULL`, identityID, scope)
	if err != nil {
		return fmt.Errorf("revoke api_keys: %w", err)
	}
	return nil
}

// ValidateAPIKey looks up a key by its hash, enforcing revocation/expiry and
// that the bound identity is active, and updates last_used_at. Returns the key
// and its identity.
func (s *Store) ValidateAPIKey(ctx context.Context, fullKey string) (APIKey, Identity, error) {
	hash := HashAPIKey(fullKey)

	var k APIKey
	var ident Identity
	err := s.pool.QueryRow(ctx, `
		SELECT k.id, k.identity_id, k.prefix, k.name, k.scope, k.expires_at, k.last_used_at, k.revoked_at, k.created_at,
		       i.id, i.key, i.kind, i.source, i.display_name
		FROM identity.api_keys k
		JOIN identity.identities i ON i.id = k.identity_id
		WHERE k.key_hash = $1
		  AND k.revoked_at IS NULL
		  AND (k.expires_at IS NULL OR k.expires_at > now())
		  AND i.deleted_at IS NULL`, hash).
		Scan(&k.ID, &k.IdentityID, &k.Prefix, &k.Name, &k.Scope, &k.ExpiresAt, &k.LastUsedAt, &k.RevokedAt, &k.CreatedAt,
			&ident.ID, &ident.Key, &ident.Kind, &ident.Source, &ident.DisplayName)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIKey{}, Identity{}, ErrInvalidAPIKey
	}
	if err != nil {
		return APIKey{}, Identity{}, fmt.Errorf("validate api_key: %w", err)
	}

	// Best-effort last-used stamp; do not fail auth on a write error.
	_, _ = s.pool.Exec(ctx, `UPDATE identity.api_keys SET last_used_at = now() WHERE id = $1`, k.ID)
	return k, ident, nil
}

// ListAPIKeys returns all API keys (newest first), for the admin endpoint.
func (s *Store) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, identity_id, prefix, name, scope, expires_at, last_used_at, revoked_at, created_at
		FROM identity.api_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list api_keys: %w", err)
	}
	defer rows.Close()

	var out []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.IdentityID, &k.Prefix, &k.Name, &k.Scope, &k.ExpiresAt, &k.LastUsedAt, &k.RevokedAt, &k.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RevokeAPIKey soft-revokes a key by ID.
func (s *Store) RevokeAPIKey(ctx context.Context, id string) error {
	ct, err := s.pool.Exec(ctx, `UPDATE identity.api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("revoke api_key: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListLocalUsers returns all local users (newest first).
func (s *Store) ListLocalUsers(ctx context.Context) ([]LocalUser, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT i.id, i.key, i.kind, i.source, i.display_name, lu.email, lu.role
		FROM identity.local_users lu
		JOIN identity.identities i ON i.id = lu.identity_id
		WHERE i.deleted_at IS NULL
		ORDER BY lu.created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list local_users: %w", err)
	}
	defer rows.Close()

	var out []LocalUser
	for rows.Next() {
		var lu LocalUser
		if err := rows.Scan(&lu.ID, &lu.Key, &lu.Kind, &lu.Source, &lu.DisplayName, &lu.Email, &lu.Role); err != nil {
			return nil, err
		}
		out = append(out, lu)
	}
	return out, rows.Err()
}

// GetLocalUser returns the local user for an identity ID.
func (s *Store) GetLocalUser(ctx context.Context, identityID string) (LocalUser, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT i.id, i.key, i.kind, i.source, i.display_name, lu.email, lu.role
		FROM identity.local_users lu
		JOIN identity.identities i ON i.id = lu.identity_id
		WHERE lu.identity_id = $1 AND i.deleted_at IS NULL`, identityID)

	var lu LocalUser
	err := row.Scan(&lu.ID, &lu.Key, &lu.Kind, &lu.Source, &lu.DisplayName, &lu.Email, &lu.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return LocalUser{}, ErrNotFound
	}
	return lu, err
}

// scanIdentity scans a 5-column identity row.
func scanIdentity(row pgx.Row) (Identity, error) {
	var i Identity
	err := row.Scan(&i.ID, &i.Key, &i.Kind, &i.Source, &i.DisplayName)
	return i, err
}

// identityInTx fetches an identity within an existing transaction.
func (s *Store) identityInTx(ctx context.Context, tx pgx.Tx, id string) (Identity, error) {
	return scanIdentity(tx.QueryRow(ctx, `
		SELECT id, key, kind, source, display_name
		FROM identity.identities WHERE id = $1`, id))
}

// dedupeBindings removes duplicate bindings (same provider/type/external_id)
// preserving order, so a CR with repeated entries does not violate the UNIQUE
// constraint on insert.
func dedupeBindings(in []Binding) []Binding {
	seen := make(map[Binding]struct{}, len(in))
	out := make([]Binding, 0, len(in))
	for _, b := range in {
		if _, ok := seen[b]; ok {
			continue
		}
		seen[b] = struct{}{}
		out = append(out, b)
	}
	return out
}
