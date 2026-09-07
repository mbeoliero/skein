package skein_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

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
