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
