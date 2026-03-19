package internals

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func Listener(ctx context.Context, cfg Config, db *sql.DB, rdb *redis.Client, wakeup chan<- string) {
	for {
		if err := runListener(ctx, cfg, db, rdb, wakeup); err != nil {
			log.Printf("listener error: %v — reconnecting in 2s", err)
			time.Sleep(RetryDBRedisListenerTick)
		}
	}
}

func pushJobToRedis(ctx context.Context, db *sql.DB, rdb *redis.Client, jobID string, table string) error {
	var runAt time.Time
	err := db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COALESCE(next_executed_at, execution_time) FROM %s WHERE id = $1 AND status = 'ACTIVE';
	`, table), jobID).Scan(&runAt)
	if err == sql.ErrNoRows {
		return nil // job cancelled or already done
	}
	if err != nil {
		return err
	}

	return rdb.ZAddNX(ctx, RedisKey, redis.Z{
		Score:  float64(runAt.UnixMilli()),
		Member: jobID,
	}).Err()
}

func runListener(ctx context.Context, cfg Config, db *sql.DB, rdb *redis.Client, wakeup chan<- string) error {
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("pq listener event %d: %v", ev, err)
		}
	}
	l := pq.NewListener(cfg.Postgres, 1*time.Second, 30*time.Second, reportProblem)
	defer l.Close()
	if err := l.Listen(PgChannel); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	log.Println("listener: listening on channel", PgChannel)
	keepalive := time.NewTicker(KeepaliveTick)
	defer keepalive.Stop()

	// fallback poll ticker
	fallback := time.NewTicker(FallbackPollTick)
	defer fallback.Stop()
	for {
		select {

		case n, ok := <-l.Notify:
			if !ok {
				return fmt.Errorf("notify channel closed")
			}
			if n == nil {
				// keepalive ping from pq, ignore
				continue
			}
			jobID := strings.TrimSpace(n.Extra)
			log.Printf("listener: notified for job %s", jobID)
			// Push this job into Redis sorted set
			if err := pushJobToRedis(ctx, db, rdb, jobID, cfg.TableName); err != nil {
				log.Printf("listener: pushJobToRedis error: %v", err)
				continue
			}
			select {
			case wakeup <- jobID:
			default:
				// channel full, executor will pick it up on next fallback poll
			}
		case <-keepalive.C:
			// ping postgres to keep connection alive
			if err := l.Ping(); err != nil {
				return fmt.Errorf("keepalive ping: %w", err)
			}
		case <-fallback.C:
			// safety net: rebuild Redis from Postgres in case we missed anything
			log.Println("listener: fallback poll — rebuilding redis sorted set")
			if err := BuildRedisSortedSet(ctx, db, rdb, cfg.TableName); err != nil {
				log.Printf("listener: fallback build error: %v", err)
			}
			// wake up executor in case new jobs appeared
			select {
			case wakeup <- "":
			default:
			}
		case <-ctx.Done():
			return nil
		}
	}
}
