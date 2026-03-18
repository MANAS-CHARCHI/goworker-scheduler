package main

import (
	"context"
	"database/sql"
	"fmt"
	"goworker/cmd/internals"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Environment file not found")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_SERVER"),
		Password: "",
		DB:       0,
		Protocol: 3,
	})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Could not connect to redis: %v", err)
	}

	fmt.Printf("----------Connected with Redis on %s----------\n", os.Getenv("REDIS_SERVER"))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("Database URL not found")
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("Could not connect to database: %v", err)
	}
	fmt.Printf("----------Connected with database on %s----------\n", dbURL)
	tableName := os.Getenv("TABLE_NAME")
	if tableName == "" {
		log.Fatal("TABLE_NAME not found in environment")
	}
	var exists bool
	err = db.QueryRowContext(ctx, `
	SELECT EXISTS (
		SELECT 1 FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_name = $1
	)
	`, tableName).Scan(&exists)
	if err != nil {
		log.Fatalf("Could not check table existence: %v", err)
	}
	if !exists {
		log.Fatalf("Table '%s' does not exist in the database", tableName)
	}

	fmt.Printf("----------Table '%s' found----------\n", tableName)
	// Ensure trigger exists on the table
	if err := internals.EnsureTrigger(db, tableName); err != nil {
		log.Fatalf("Could not create trigger: %v", err)
	}
	fmt.Println("----------Trigger ready----------")
	// 3. Build redis sorted set from all active pending jobs
	if err := internals.BuildRedisSortedSet(ctx, db, client, tableName); err != nil {
		log.Fatalf("Could not build redis sorted set: %v", err)
	}
	fmt.Println("----------Redis sorted set ready----------")

	// 4. Wire up config and signal channel
	cfg := internals.Config{
		Postgres:  dbURL,
		RedisAddr: os.Getenv("REDIS_SERVER"),
		TableName: tableName,
	}
	wakeup := make(chan string, 100)

	// 5. Start listener and executor goroutines
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		internals.Listener(ctx, cfg, db, client, wakeup)
	}()

	go func() {
		defer wg.Done()
		internals.Executor(ctx, db, client, tableName, wakeup)
	}()

	fmt.Println("----------Worker started----------")
	wg.Wait()
}
