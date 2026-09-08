package skein_test

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein"
)

type mailParams struct {
	To string `json:"to"`
}

// Example shows a host process embedding the engine: migrate at release time,
// register executors, declare definitions, start, submit, and drain on SIGTERM.
func Example() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// Explicit and serial: run it from the release step, not on every start.
	if err := skein.Migrate(ctx, pool, "skein"); err != nil {
		log.Fatal(err)
	}

	engine, err := skein.New(pool, skein.Config{Concurrency: 8})
	if err != nil {
		log.Fatal(err)
	}
	// Typed executor: params are decoded into mailParams; a decode failure is permanent.
	skein.Register(engine, "send_mail", func(ctx context.Context, req *skein.Request, p mailParams) (skein.RawJSON, error) {
		// Use req.IdempotencyKey to deduplicate: delivery is at-least-once.
		return skein.RawJSON(`{"sent":true}`), nil
	})

	if err := engine.Jobs().Declare(ctx, skein.JobSpec{Name: "welcome_mail", ExecutorType: "send_mail"}); err != nil {
		log.Fatal(err)
	}
	if err := engine.Schedules().Put(ctx, skein.ScheduleSpec{Name: "digest", Job: "welcome_mail", Cron: "0 9 * * 1-5", Timezone: "Asia/Shanghai"}); err != nil {
		log.Fatal(err)
	}

	if err := engine.Start(ctx); err != nil {
		log.Fatal(err)
	}
	// Submit from request handlers; a dedup key makes retries return the same run.
	if _, err := engine.Jobs().Trigger(ctx, "welcome_mail", skein.RawJSON(`{"to":"a@example.com"}`), skein.DedupKey("user:42")); err != nil {
		log.Println("trigger:", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	<-stop
	// Shutdown stops claiming, lets executors finish within ShutdownGrace, then
	// releases what is left for other instances to pick up.
	if err := engine.Shutdown(ctx); err != nil {
		log.Println("shutdown:", err)
	}
}

// ExampleSnooze simulates a provider that becomes ready after one second. Replace
// the timestamp simulation with idempotent Submit/Poll calls, not a sleeping worker.
func ExampleSnooze() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := skein.Migrate(ctx, pool, "skein"); err != nil {
		log.Fatal(err)
	}
	engine, err := skein.New(pool, skein.Config{})
	if err != nil {
		log.Fatal(err)
	}
	type operation struct {
		TaskId   string    `json:"task_id"`
		ReadyAt  time.Time `json:"ready_at"` // simulated provider state, not needed for real Poll
		Deadline time.Time `json:"deadline"`
	}
	done := make(chan struct{}, 1)
	engine.Register("video.submit", func(ctx context.Context, req *skein.Request) (skein.RawJSON, error) {
		// A stable database timestamp keeps the budget fixed even if Submit is retried.
		run, err := engine.Workflows().GetRun(ctx, *req.WorkflowRunId)
		if err != nil {
			return nil, err
		}
		return json.Marshal(operation{
			TaskId: req.IdempotencyKey, ReadyAt: run.CreatedAt.Add(time.Second),
			Deadline: run.CreatedAt.Add(24 * time.Hour),
		})
	})
	engine.Register("video.poll", func(ctx context.Context, req *skein.Request) (skein.RawJSON, error) {
		var op operation
		if err := json.Unmarshal(req.Deps["video.submit"], &op); err != nil {
			return nil, skein.Permanent(err)
		}
		var now time.Time
		if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return nil, err
		}
		remaining := op.Deadline.Sub(now)
		if remaining <= 0 {
			return nil, skein.Permanent(errors.New("video generation deadline exceeded"))
		}
		if now.Before(op.ReadyAt) {
			return nil, skein.Snooze(min(100*time.Millisecond, remaining))
		}
		select {
		case done <- struct{}{}:
		default:
		}
		return skein.RawJSON(`{"video_url":"https://example.com/video.mp4"}`), nil
	})
	for _, name := range []string{"video.submit", "video.poll"} {
		if err := engine.Jobs().Declare(ctx, skein.JobSpec{
			Name: name, ExecutorType: name, Timeout: 30 * time.Second,
		}); err != nil {
			log.Fatal(err)
		}
	}
	if err := engine.Workflows().Declare(ctx, skein.WorkflowSpec{
		Name: "video", Nodes: []skein.Node{
			{Job: "video.submit"}, {Job: "video.poll", Deps: []string{"video.submit"}},
		},
	}); err != nil {
		log.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		log.Fatal(err)
	}
	id, err := engine.Workflows().Trigger(ctx, "video", nil)
	if err != nil {
		log.Println("trigger:", err)
	} else {
		select {
		case <-done:
			log.Println("video workflow:", id)
		case <-ctx.Done():
			log.Println("wait:", ctx.Err())
		}
	}
	stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := engine.Shutdown(stopCtx); err != nil {
		log.Println("shutdown:", err)
	}
}
