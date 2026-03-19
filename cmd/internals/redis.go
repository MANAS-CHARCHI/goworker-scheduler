package internals

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func BuildRedisSortedSet(ctx context.Context, db *sql.DB, rdb *redis.Client, table string) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
	SELECT id, next_executed_at FROM %s
	WHERE status = 'ACTIVE'
	AND COALESCE(next_executed_at, execution_time) > NOW()
	ORDER BY next_executed_at ASC;`, table))
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()
	var members []redis.Z
	for rows.Next() {
		var id string
		var runAt time.Time
		if err := rows.Scan(&id, &runAt); err != nil {
			continue
		}
		members = append(members, redis.Z{
			Score:  float64(runAt.UnixMilli()),
			Member: id,
		})
	}
	if len(members) == 0 {
		return nil
	}

	return rdb.ZAddNX(ctx, RedisKey, members...).Err()
}
