package artifact

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store owns artifact metadata and its immutable work links.
type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) CreatePending(ctx context.Context, id, key string, in UploadInput) (Artifact, error) {
	var a Artifact
	var sourceRef any
	if in.SourceRef != "" {
		sourceRef = in.SourceRef
	}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO artifact.artifacts
			(id,storage_key,source_type,source_ref,created_by_identity_id,mime_type,retention_until)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		RETURNING id::text,storage_key,source_type,source_ref,created_by_identity_id::text,mime_type,
		          size_bytes,digest,state,retention_until,deletion_reason,created_at,available_at,deleted_at`,
		id, key, in.SourceType, sourceRef, in.CreatedByIdentityID, in.MIMEType, in.RetentionUntil,
	).Scan(&a.ID, &a.StorageKey, &a.SourceType, &a.SourceRef, &a.CreatedByIdentityID, &a.MIMEType,
		&a.SizeBytes, &a.Digest, &a.State, &a.RetentionUntil, &a.DeletionReason, &a.CreatedAt, &a.AvailableAt, &a.DeletedAt)
	if err != nil {
		return Artifact{}, fmt.Errorf("create pending artifact: %w", err)
	}
	return a, nil
}

// Finalize makes bytes visible and atomically creates the work relationship.
func (s *Store) Finalize(ctx context.Context, id string, size int64, digest string, scope *Scope) (Artifact, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Artifact{}, fmt.Errorf("finalize begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if scope != nil {
		if err := validateScopeTx(ctx, tx, *scope); err != nil {
			return Artifact{}, err
		}
		var node any
		if scope.NodeExecutionID != "" {
			node = scope.NodeExecutionID
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO work.artifact_links
				(artifact_id,work_item_id,attempt_id,node_execution_id,role,metadata)
			VALUES ($1,$2,$3,$4,$5,'{}'::jsonb)`, id, scope.WorkItemID, scope.AttemptID, node, scope.Role); err != nil {
			return Artifact{}, fmt.Errorf("link artifact: %w", err)
		}
	}

	var a Artifact
	err = tx.QueryRow(ctx, `
		UPDATE artifact.artifacts
		SET size_bytes=$2,digest=$3,state='available',available_at=now()
		WHERE id=$1 AND state='pending'
		RETURNING id::text,storage_key,source_type,source_ref,created_by_identity_id::text,mime_type,
		          size_bytes,digest,state,retention_until,deletion_reason,created_at,available_at,deleted_at`,
		id, size, digest,
	).Scan(&a.ID, &a.StorageKey, &a.SourceType, &a.SourceRef, &a.CreatedByIdentityID, &a.MIMEType,
		&a.SizeBytes, &a.Digest, &a.State, &a.RetentionUntil, &a.DeletionReason, &a.CreatedAt, &a.AvailableAt, &a.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Artifact{}, ErrNotFound
	}
	if err != nil {
		return Artifact{}, fmt.Errorf("finalize artifact: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Artifact{}, fmt.Errorf("finalize commit: %w", err)
	}
	return a, nil
}

func validateScopeTx(ctx context.Context, tx pgx.Tx, scope Scope) error {
	if scope.WorkItemID == "" || scope.AttemptID == "" || scope.Role == "" {
		return fmt.Errorf("%w: incomplete work scope", ErrInvalidInput)
	}
	var ok bool
	var node any
	if scope.NodeExecutionID != "" {
		node = scope.NodeExecutionID
	}
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM work.attempts a
			WHERE a.id=$1 AND a.work_item_id=$2
			  AND ($3::uuid IS NULL OR EXISTS(
				SELECT 1 FROM runtime.node_executions n
				WHERE n.id=$3::uuid AND n.attempt_id=a.id))
		)`, scope.AttemptID, scope.WorkItemID, node).Scan(&ok); err != nil {
		return fmt.Errorf("validate artifact scope: %w", err)
	}
	if !ok {
		return ErrUnauthorized
	}
	return nil
}

func (s *Store) Get(ctx context.Context, id string) (Artifact, error) {
	return scanArtifact(s.pool.QueryRow(ctx, `
		SELECT id::text,storage_key,source_type,source_ref,created_by_identity_id::text,mime_type,
		       size_bytes,digest,state,retention_until,deletion_reason,created_at,available_at,deleted_at
		FROM artifact.artifacts WHERE id=$1`, id))
}

func (s *Store) GetAvailable(ctx context.Context, id string) (Artifact, error) {
	a, err := s.Get(ctx, id)
	if err != nil {
		return Artifact{}, err
	}
	if a.State != StateAvailable {
		return Artifact{}, ErrNotAvailable
	}
	return a, nil
}

// LinkedToAttempt is the workload read boundary. If nodeID is non-empty an
// exact node link or an attempt-wide link is accepted.
func (s *Store) LinkedToAttempt(ctx context.Context, artifactID, attemptID, nodeID string) (bool, error) {
	var ok bool
	var node any
	if nodeID != "" {
		node = nodeID
	}
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM work.artifact_links l
			JOIN artifact.artifacts a ON a.id=l.artifact_id
			WHERE l.artifact_id=$1 AND l.attempt_id=$2 AND a.state='available'
			  AND ($3::uuid IS NULL OR l.node_execution_id IS NULL OR l.node_execution_id=$3::uuid)
		)`, artifactID, attemptID, node).Scan(&ok)
	return ok, err
}

// ExecutionScope derives work scope and creator from durable execution state.
func (s *Store) ExecutionScope(ctx context.Context, attemptID, callerScope, callerScopeID string) (Scope, string, error) {
	var scope Scope
	var creator string
	var node *string
	var row pgx.Row
	switch callerScope {
	case "turn":
		row = s.pool.QueryRow(ctx, `
			SELECT a.work_item_id::text,a.id::text,t.node_execution_id::text,wi.scope_identity_id::text
			FROM work.attempts a
			JOIN work.work_items wi ON wi.id=a.work_item_id
			JOIN runtime.turns t ON t.run_id=a.id
			WHERE a.id=$1 AND t.id=$2 AND t.state='running'`, attemptID, callerScopeID)
	case "workflow_step":
		row = s.pool.QueryRow(ctx, `
			SELECT a.work_item_id::text,a.id::text,NULL::text,wi.scope_identity_id::text
			FROM work.attempts a
			JOIN work.work_items wi ON wi.id=a.work_item_id
			JOIN runtime.run_steps rs ON rs.run_id=a.id
			WHERE a.id=$1 AND rs.id=$2 AND rs.state='running'`, attemptID, callerScopeID)
	default:
		return Scope{}, "", ErrUnauthorized
	}
	if err := row.Scan(&scope.WorkItemID, &scope.AttemptID, &node, &creator); errors.Is(err, pgx.ErrNoRows) {
		return Scope{}, "", ErrUnauthorized
	} else if err != nil {
		return Scope{}, "", fmt.Errorf("resolve execution scope: %w", err)
	}
	if node != nil {
		scope.NodeExecutionID = *node
	}
	return scope, creator, nil
}

func (s *Store) BeginDelete(ctx context.Context, id, reason string) (Artifact, error) {
	var a Artifact
	err := s.pool.QueryRow(ctx, `
		UPDATE artifact.artifacts
		SET state='deleting',deletion_reason=$2,deletion_started_at=COALESCE(deletion_started_at,now()),deletion_error=NULL
		WHERE id=$1 AND state IN ('available','deleting')
		RETURNING id::text,storage_key,source_type,source_ref,created_by_identity_id::text,mime_type,
		          size_bytes,digest,state,retention_until,deletion_reason,created_at,available_at,deleted_at`, id, reason,
	).Scan(&a.ID, &a.StorageKey, &a.SourceType, &a.SourceRef, &a.CreatedByIdentityID, &a.MIMEType,
		&a.SizeBytes, &a.Digest, &a.State, &a.RetentionUntil, &a.DeletionReason, &a.CreatedAt, &a.AvailableAt, &a.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := s.Get(ctx, id)
		if getErr == nil && existing.State == StateDeleted {
			return existing, nil
		}
		return Artifact{}, ErrNotFound
	}
	if err != nil {
		return Artifact{}, fmt.Errorf("begin artifact deletion: %w", err)
	}
	return a, nil
}

func (s *Store) FinishDelete(ctx context.Context, id string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE artifact.artifacts SET state='deleted',deleted_at=now(),deletion_error=NULL
		WHERE id=$1 AND state='deleting'`, id)
	if err != nil {
		return fmt.Errorf("finish artifact deletion: %w", err)
	}
	if ct.RowsAffected() == 0 {
		var state string
		if scanErr := s.pool.QueryRow(ctx, `SELECT state FROM artifact.artifacts WHERE id=$1`, id).Scan(&state); scanErr == nil && state == StateDeleted {
			return nil
		}
		return ErrNotFound
	}
	return nil
}

func (s *Store) RecordDeleteError(ctx context.Context, id string, cause error) {
	_, _ = s.pool.Exec(ctx, `UPDATE artifact.artifacts SET deletion_error=$2 WHERE id=$1 AND state='deleting'`, id, cause.Error())
}

func (s *Store) DeletePending(ctx context.Context, id string) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM artifact.artifacts WHERE id=$1 AND state='pending'`, id)
}

func (s *Store) StalePending(ctx context.Context, before time.Time, limit int) ([]Artifact, error) {
	return s.listLifecycle(ctx, `state='pending' AND created_at < $1`, before, limit)
}

func (s *Store) Expired(ctx context.Context, now time.Time, limit int) ([]Artifact, error) {
	return s.listLifecycle(ctx, `state='available' AND retention_until IS NOT NULL AND retention_until <= $1`, now, limit)
}

func (s *Store) Deleting(ctx context.Context, before time.Time, limit int) ([]Artifact, error) {
	return s.listLifecycle(ctx, `state='deleting' AND deletion_started_at <= $1`, before, limit)
}

func (s *Store) listLifecycle(ctx context.Context, predicate string, arg time.Time, limit int) ([]Artifact, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id::text,storage_key,source_type,source_ref,created_by_identity_id::text,mime_type,
		       size_bytes,digest,state,retention_until,deletion_reason,created_at,available_at,deleted_at
		FROM artifact.artifacts WHERE `+predicate+` ORDER BY created_at LIMIT $2`, arg, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Artifact
	for rows.Next() {
		a, err := scanArtifact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func scanArtifact(row pgx.Row) (Artifact, error) {
	var a Artifact
	err := row.Scan(&a.ID, &a.StorageKey, &a.SourceType, &a.SourceRef, &a.CreatedByIdentityID, &a.MIMEType,
		&a.SizeBytes, &a.Digest, &a.State, &a.RetentionUntil, &a.DeletionReason, &a.CreatedAt, &a.AvailableAt, &a.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Artifact{}, ErrNotFound
	}
	if err != nil {
		return Artifact{}, err
	}
	return a, nil
}
