package data

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type RandomizeTasksResult struct {
	Updated    int64 `json:"updated"`
	Requested  int   `json:"requested"`
	DurationMS int64 `json:"durationMs"`
}

func RandomizeTaskStatusPriority(ctx context.Context, databaseURL string, count int) (RandomizeTasksResult, error) {
	if databaseURL == "" {
		return RandomizeTasksResult{}, fmt.Errorf("database URL is not configured")
	}
	if count <= 0 {
		count = 3000
	}
	if count > 3000 {
		count = 3000
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return RandomizeTasksResult{}, err
	}
	defer db.Close()

	started := time.Now()
	result, err := db.ExecContext(ctx, `
WITH picked AS MATERIALIZED (
  SELECT id
  FROM tasks
  ORDER BY random()
  LIMIT $1
)
UPDATE tasks t
SET
  status = (ARRAY['new', 'in_progress', 'waiting', 'review', 'done'])[1 + floor(random() * 5)::int],
  priority = (ARRAY['low', 'medium', 'high', 'urgent'])[1 + floor(random() * 4)::int],
  updated_at = now()
FROM picked
WHERE t.id = picked.id
`, count)
	if err != nil {
		return RandomizeTasksResult{}, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return RandomizeTasksResult{}, err
	}
	return RandomizeTasksResult{Updated: updated, Requested: count, DurationMS: time.Since(started).Milliseconds()}, nil
}
