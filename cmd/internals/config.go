package internals

import (
	"os"
	"time"
)

type Config struct {
	Postgres  string
	RedisAddr string
	TableName string
}

func loadConfig() Config {
	return Config{
		Postgres:  os.Getenv("DATABASE_URL"),
		RedisAddr: os.Getenv("REDIS_SERVER"),
		TableName: os.Getenv("TABLE_NAME"),
	}
}

const (
	RedisKey                 = "scheduler:jobs"
	PgChannel                = "scheduler_notify"
	MaxRetries               = 3
	KeepaliveTick            = 30 * time.Second
	FallbackPollTick         = 60 * time.Second
	RetryDBRedisListenerTick = 2 * time.Second
)
