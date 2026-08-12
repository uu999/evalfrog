package postgres

import (
	"context"
	"time"

	"github.com/uu999/evalfrog/internal/recovery"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
)

func (store *Store) ListExpiredAttempts(ctx context.Context, grace time.Duration, batch int) ([]attempt.MarkLostCommand, error) {
	rows, err := store.pool.Query(ctx, `
		SELECT a.project_id::text, a.run_id::text, a.attempt_id::text, a.attempt_seq
		FROM node_attempts a
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		WHERE a.state='running' AND n.current_attempt_id=a.attempt_id
		  AND a.lease_expires_at + ($1 * interval '1 millisecond') <= clock_timestamp()
		ORDER BY a.lease_expires_at, a.attempt_id
		LIMIT $2`, grace.Milliseconds(), batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]attempt.MarkLostCommand, 0, batch)
	for rows.Next() {
		var value attempt.MarkLostCommand
		if err = rows.Scan(&value.ProjectID, &value.RunID, &value.AttemptID, &value.AttemptSequence); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

var _ recovery.Repository = (*Store)(nil)
