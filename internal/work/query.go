package work

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (s *Store) GetBlocker(ctx context.Context, id string) (Blocker, error) {
	b, err := scanBlocker(s.pool.QueryRow(ctx, `
		SELECT id,work_item_id,attempt_id,node_execution_id,kind,title,description,response_schema,
		       allowed_outcomes,required_consequences,state,response_outcome,response,created_at,resolved_at
		FROM work.blockers WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Blocker{}, ErrNotFound
	}
	return b, err
}

func (s *Store) OpenBlockerForItem(ctx context.Context, itemID string) (Blocker, error) {
	b, err := scanBlocker(s.pool.QueryRow(ctx, `
		SELECT id,work_item_id,attempt_id,node_execution_id,kind,title,description,response_schema,
		       allowed_outcomes,required_consequences,state,response_outcome,response,created_at,resolved_at
		FROM work.blockers WHERE work_item_id=$1 AND state='open'`, itemID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Blocker{}, ErrNotFound
	}
	return b, err
}

// ListFeedback returns the durable, customer-safe feedback and revision
// association for one work item in capture order.
func (s *Store) ListFeedback(ctx context.Context, itemID string) ([]Feedback, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM work.work_items WHERE id=$1)`, itemID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `
		SELECT f.id,f.work_item_id,f.attempt_id,f.category,f.explanation,f.corrected_result,
		       f.created_by,f.created_at,a.id
		FROM work.feedback f
		LEFT JOIN work.attempts a ON a.revision_feedback_id=f.id
		WHERE f.work_item_id=$1 ORDER BY f.created_at,f.id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Feedback, 0)
	for rows.Next() {
		f, err := scanFeedback(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// GetFeedback returns one item-scoped feedback record, preventing an opaque
// feedback ID from crossing the customer work-item boundary.
func (s *Store) GetFeedback(ctx context.Context, itemID, feedbackID string) (Feedback, error) {
	f, err := scanFeedback(s.pool.QueryRow(ctx, `
		SELECT f.id,f.work_item_id,f.attempt_id,f.category,f.explanation,f.corrected_result,
		       f.created_by,f.created_at,a.id
		FROM work.feedback f
		LEFT JOIN work.attempts a ON a.revision_feedback_id=f.id
		WHERE f.work_item_id=$1 AND f.id=$2`, itemID, feedbackID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Feedback{}, ErrNotFound
	}
	return f, err
}

func scanFeedback(row pgx.Row) (Feedback, error) {
	var f Feedback
	err := row.Scan(&f.ID, &f.WorkItemID, &f.AttemptID, &f.Category, &f.Explanation,
		&f.CorrectedResult, &f.CreatedBy, &f.CreatedAt, &f.RevisedAttemptID)
	return f, err
}
