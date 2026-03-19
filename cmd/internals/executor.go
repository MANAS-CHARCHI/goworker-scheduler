package internals

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID             int
	Name           string
	Payload        map[string]interface{}
	NextExecutedAt time.Time
	ScheduleType   string
	RepeatOn       []int64
	MaxRetry       int
	LastExecutedAt *time.Time

	// from scheduler_webhook_data join
	WebhookURL            string
	WebhookHeaders        map[string]string
	WebhookTimeoutSeconds int

	// from scheduler_callback_data join (nullable)
	CallbackURL            *string
	CallbackHeaders        map[string]string
	CallbackTimeoutSeconds *int
}

func Executor(ctx context.Context, db *sql.DB, rdb *redis.Client, table string, wakeup <-chan string) {
	for {
		ExecLoop(ctx, db, rdb, table, wakeup)
	}
}

func ExecLoop(ctx context.Context, db *sql.DB, rdb *redis.Client, table string, wakeup <-chan string) {
	results, err := rdb.ZRangeWithScores(ctx, RedisKey, 0, 0).Result()
	if err != nil || len(results) == 0 {
		log.Println("executor: no jobs in queue, waiting...")
		select {
		case <-wakeup:
		case <-time.After(FallbackPollTick):
		case <-ctx.Done():
		}
		return
	}
	nearest := results[0]
	runAt := time.UnixMilli(int64(nearest.Score))
	sleepDur := time.Until(runAt)
	if sleepDur > 0 {
		log.Printf("executor: sleeping %v until next job", sleepDur.Round(time.Millisecond))
		select {
		case <-time.After(sleepDur):
		case <-wakeup:
			log.Println("executor: woken up early, recalculating")
			return
		case <-ctx.Done():
			return
		}
	}
	nowMs := float64(time.Now().UnixMilli())
	jobIDs, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     RedisKey,
		Start:   "-inf",
		Stop:    strconv.FormatFloat(nowMs, 'f', 0, 64),
		ByScore: true,
	}).Result()
	if err != nil || len(jobIDs) == 0 {
		return
	}
	rdb.ZRemRangeByScore(ctx, RedisKey, "-inf", strconv.FormatFloat(nowMs, 'f', 0, 64))
	log.Printf("executor: executing %d jobs", len(jobIDs))
	// Fetch full details for all due jobs in one query
	jobs, err := FetchJobs(ctx, db, table, jobIDs)
	if err != nil {
		log.Printf("executor: fetchJobs error: %v", err)
		return
	}
	// Fire all webhooks in parallel
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(j Job) {
			defer wg.Done()
			ExecuteJob(ctx, db, rdb, table, j)
		}(job)
	}
	wg.Wait()
}

func FetchJobs(ctx context.Context, db *sql.DB, table string, ids []string) ([]Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf(`
		SELECT
			s.id,
			s.name,
			s.payload,
			s.next_executed_at,
			s.schedule_type,
			s.repeat_on,
			s.max_retry,
			s.last_executed_at,
			w.webhook_url,
			w.webhook_headers,
			w.webhook_timeout_seconds,
			c.callback_url,
			c.callback_headers,
			c.callback_timeout_seconds
		FROM %s s
		JOIN scheduler_webhook_data w ON w.id = s.webhook_id
		LEFT JOIN scheduler_callback_data c ON c.id = s.callback_id
		WHERE s.id IN (%s)
		AND s.status = 'ACTIVE'
	`, table, strings.Join(placeholders, ","))

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		var payloadJSON []byte
		var webhookHeadersJSON []byte
		var callbackHeadersJSON []byte
		var repeatOn pq.Int64Array
		var callbackURL sql.NullString
		var callbackTimeoutSeconds sql.NullInt32
		var lastExecutedAt sql.NullTime
		var nextExecutedAt sql.NullTime
		err := rows.Scan(
			&j.ID,
			&j.Name,
			&payloadJSON,
			&nextExecutedAt,
			&j.ScheduleType,
			&repeatOn,
			&j.MaxRetry,
			&lastExecutedAt,
			&j.WebhookURL,
			&webhookHeadersJSON,
			&j.WebhookTimeoutSeconds,
			&callbackURL,
			&callbackHeadersJSON,
			&callbackTimeoutSeconds,
		)
		if err != nil {
			log.Printf("fetchJobs scan error: %v", err)
			continue
		}
		if lastExecutedAt.Valid {
			j.LastExecutedAt = &lastExecutedAt.Time
		}
		if callbackURL.Valid {
			j.CallbackURL = &callbackURL.String
		}
		if callbackTimeoutSeconds.Valid {
			v := int(callbackTimeoutSeconds.Int32)
			j.CallbackTimeoutSeconds = &v
		}
		json.Unmarshal(payloadJSON, &j.Payload)
		json.Unmarshal(webhookHeadersJSON, &j.WebhookHeaders)
		if callbackHeadersJSON != nil {
			json.Unmarshal(callbackHeadersJSON, &j.CallbackHeaders)
		}
		j.RepeatOn = []int64(repeatOn)
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func ExecuteJob(ctx context.Context, db *sql.DB, rdb *redis.Client, table string, job Job) {
	var (
		lastErr    error
		statusCode int
	)

	maxRetry := job.MaxRetry
	if maxRetry == 0 {
		maxRetry = MaxRetries
	}
	var errMsg = ""
	for attempt := 1; attempt <= maxRetry; attempt++ {
		log.Printf("job %d: attempt %d/%d", job.ID, attempt, maxRetry)
		start := time.Now()
		statusCode, lastErr = CallWebhook(ctx, job)
		responseTimeMs := int(time.Since(start).Milliseconds())
		if lastErr == nil && statusCode >= 200 && statusCode < 300 {
			// Success
			log.Printf("job %d: success (%d) in %dms", job.ID, statusCode, responseTimeMs)

			LogExecution(ctx, db, job.ID, attempt, statusCode, "success", responseTimeMs, "")
			UpdateAfterSuccess(ctx, db, rdb, table, job)

			if job.CallbackURL != nil && *job.CallbackURL != "" {
				go FireCallback(job, "success", statusCode, "")
			}
			return
		}
		errMsg := ""
		if lastErr != nil {
			errMsg = lastErr.Error()
		} else {
			errMsg = fmt.Sprintf("non-2xx status: %d", statusCode)
		}

		log.Printf("job %d: attempt %d failed: %s", job.ID, attempt, errMsg)
		LogExecution(ctx, db, job.ID, attempt, statusCode, "failed", responseTimeMs, errMsg)

		if attempt < maxRetry {
			// exponential backoff before next retry
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		errMsg = ""
		if lastErr != nil {
			errMsg = lastErr.Error()
		}
	}
	log.Printf("job %d: all retries exhausted: %s", job.ID, errMsg)

	UpdateAfterFailure(ctx, db, table, job.ID)

	if job.CallbackURL != nil && *job.CallbackURL != "" {
		go FireCallback(job, "failed", statusCode, errMsg)
	}
}

func CallWebhook(ctx context.Context, job Job) (int, error) {
	payloadBytes, err := json.Marshal(job.Payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}

	timeout := time.Duration(job.WebhookTimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 90 * time.Second
	}

	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.WebhookURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range job.WebhookHeaders {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return resp.StatusCode, nil
}
func LogExecution(ctx context.Context, db *sql.DB, jobID int, attempt int, statusCode int, status string, responseTimeMs int, errMsg string) {
	_, err := db.ExecContext(ctx, `
		INSERT INTO schedule_task_executions
			(scheduler_id, status, attempt, response_code, executed_at, response_time_ms, error_message, created_at, updated_at)
		VALUES ($1, $2, $3, $4, NOW(), $5, $6, NOW(), NOW())
	`, jobID, status, attempt, statusCode, responseTimeMs, errMsg)
	if err != nil {
		log.Printf("logExecution error for job %d: %v", jobID, err)
	}
}

func UpdateAfterSuccess(ctx context.Context, db *sql.DB, rdb *redis.Client, table string, job Job) {
	now := time.Now().UTC()
	next := CalculateNextExecution(job)

	if next == nil {
		// ONCE type — mark completed
		_, err := db.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s SET
				status           = 'completed',
				last_executed_at = $1,
				updated_at       = NOW()
			WHERE id = $2
		`, table), now, job.ID)
		if err != nil {
			log.Printf("updateAfterSuccess (completed) error for job %d: %v", job.ID, err)
		}
		return
	}

	// Recurring — update times and push next execution to Redis
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s SET
			last_executed_at = $1,
			next_executed_at = $2,
			updated_at       = NOW()
		WHERE id = $3
	`, table), now, next, job.ID)
	if err != nil {
		log.Printf("updateAfterSuccess (recurring) error for job %d: %v", job.ID, err)
		return
	}

	// Push next execution back into Redis sorted set
	err = rdb.ZAdd(ctx, RedisKey, redis.Z{
		Score:  float64(next.UnixMilli()),
		Member: strconv.Itoa(job.ID),
	}).Err()
	if err != nil {
		log.Printf("updateAfterSuccess redis push error for job %d: %v", job.ID, err)
	}

	log.Printf("job %d: next execution scheduled at %v", job.ID, next)
}

func UpdateAfterFailure(ctx context.Context, db *sql.DB, table string, jobID int) {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s SET
			status           = 'failed',
			last_executed_at = NOW(),
			updated_at       = NOW()
		WHERE id = $1
	`, table), jobID)
	if err != nil {
		log.Printf("updateAfterFailure error for job %d: %v", jobID, err)
	}
}

func CalculateNextExecution(job Job) *time.Time {
	now := time.Now().UTC()

	switch job.ScheduleType {
	case "once":
		return nil

	case "daily":
		next := job.NextExecutedAt.Add(24 * time.Hour)
		return &next

	case "weekly":
		// repeat_on = [1,3,5] = Mon, Wed, Fri (Go weekday: Sun=0 Mon=1 ... Sat=6)
		for i := 1; i <= 7; i++ {
			candidate := now.AddDate(0, 0, i)
			for _, day := range job.RepeatOn {
				if int64(candidate.Weekday()) == day {
					next := time.Date(
						candidate.Year(), candidate.Month(), candidate.Day(),
						job.NextExecutedAt.Hour(), job.NextExecutedAt.Minute(), 0, 0, time.UTC,
					)
					return &next
				}
			}
		}

	case "monthly":
		// repeat_on = [1,15] = 1st and 15th of each month
		for i := 1; i <= 31; i++ {
			candidate := now.AddDate(0, 0, i)
			for _, day := range job.RepeatOn {
				if int64(candidate.Day()) == day {
					next := time.Date(
						candidate.Year(), candidate.Month(), candidate.Day(),
						job.NextExecutedAt.Hour(), job.NextExecutedAt.Minute(), 0, 0, time.UTC,
					)
					return &next
				}
			}
		}
	}

	return nil
}

func FireCallback(job Job, status string, statusCode int, errMsg string) {
	if job.CallbackURL == nil || *job.CallbackURL == "" {
		return
	}

	payload := map[string]interface{}{
		"job_id":      job.ID,
		"job_name":    job.Name,
		"status":      status,
		"status_code": statusCode,
		"error":       errMsg,
		"timestamp":   time.Now().UTC(),
	}

	body, _ := json.Marshal(payload)

	timeout := 90 * time.Second
	if job.CallbackTimeoutSeconds != nil {
		timeout = time.Duration(*job.CallbackTimeoutSeconds) * time.Second
	}

	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodPost, *job.CallbackURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("fireCallback request error for job %d: %v", job.ID, err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range job.CallbackHeaders {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("fireCallback failed for job %d: %v", job.ID, err)
		return
	}
	defer resp.Body.Close()

	log.Printf("callback fired for job %d → %s (%d)", job.ID, *job.CallbackURL, resp.StatusCode)
}
