package skein

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
)

// Wakeup acceptance checks (design §2.7): every check runs with a 5s PollInterval so that only a wake
// source (NOTIFY, a local event, or the next-due timer) can explain a fast result.
func slowPoll(schema string) Config {
	cfg := fastConfig(schema)
	cfg.PollInterval = 5 * time.Second
	return cfg
}

// tight is the acceptance bound for a wake and its database round trips on the
// test machine; it is not a production SLA (README.md validation entry).
const tight = 300 * time.Millisecond

// entryClock is an executor that reads the database clock on entry, so that entry −
// run_at is arithmetic on one clock: a local time.Now() here would compare the host's
// clock with the database's. The channel carries one reading per execution.
func entryClock(pool *pgxpool.Pool) (Executor, <-chan time.Time) {
	ch := make(chan time.Time, 8)
	return func(ctx context.Context, req *Request) (RawJSON, error) {
		var now time.Time
		if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return nil, err
		}
		ch <- now
		return nil, nil
	}, ch
}

func runAt(t *testing.T, pool *pgxpool.Pool, schema string, id int64) time.Time {
	t.Helper()
	var at time.Time
	if err := pool.QueryRow(t.Context(), "SELECT run_at FROM "+qualified(schema, "job_run")+" WHERE id = $1", id).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

func expectEntry(t *testing.T, what string, entries <-chan time.Time, pool *pgxpool.Pool, schema string, id int64) {
	t.Helper()
	select {
	case at := <-entries:
		if late := at.Sub(runAt(t, pool, schema, id)); late < 0 || late > tight {
			t.Fatalf("%s: executor entered %s after run_at, want within [0, %s]", what, late, tight)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: run %d not executed within 2s (poll is 5s)", what, id)
	}
}

func apiOnly(schema string) Config {
	cfg := slowPoll(schema)
	cfg.DisableWorker, cfg.DisableScheduler = true, true
	return cfg
}

// A run triggered on one instance starts on another within a round trip: the API
// instance (DisableWorker) has no claim loop, so only NOTIFY can carry it.
func TestRemoteTriggerStartsAtOnce(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	exec, entries := entryClock(pool)
	worker := startEngine(t, pool, slowPoll(schema), func(e *Engine) { e.Register("echo", exec) })
	api := startEngine(t, pool, apiOnly(schema), nil)
	declare(t, worker, JobSpec{Name: "j", ExecutorType: "echo"})

	id := trigger(t, api, "j", "{}")
	expectEntry(t, "Trigger", entries, pool, schema, id)

	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTx(t, tx)
	id, err = api.Jobs().TriggerTx(t.Context(), tx, "j", RawJSON("{}"))
	if err != nil {
		t.Fatalf("TriggerTx: %v", err)
	}
	select {
	case <-entries:
		t.Fatalf("run %d started before the caller committed", id)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	expectEntry(t, "TriggerTx after commit", entries, pool, schema, id)
}

// A delayed run starts at its run_at, not at the next poll: the claimer read the
// nearest pending run_at and slept until then.
func TestDelayedRunStartsOnTime(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	exec, entries := entryClock(pool)
	worker := startEngine(t, pool, slowPoll(schema), func(e *Engine) { e.Register("echo", exec) })
	api := startEngine(t, pool, apiOnly(schema), nil)
	declare(t, worker, JobSpec{Name: "j", ExecutorType: "echo"})

	now, err := worker.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id := trigger(t, api, "j", "{}", At(now.Add(700*time.Millisecond)))
	expectEntry(t, "At(now+700ms)", entries, pool, schema, id)
}

// A retry with backoff is a delayed run created by a settle: the same timer covers it.
func TestRetryBackoffFiresOnTime(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := slowPoll(schema)
	cfg.BackoffBase, cfg.BackoffMax = 500*time.Millisecond, 500*time.Millisecond
	entries := make(chan time.Time, 2)
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("flaky", func(ctx context.Context, req *Request) (RawJSON, error) {
			if req.Attempt == 1 {
				return nil, context.DeadlineExceeded
			}
			var now time.Time
			if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
				return nil, err
			}
			entries <- now
			return nil, nil
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "flaky", Retry: RetryPolicy{MaxAttempts: 2}})
	id := trigger(t, e, "j", "{}")
	expectEntry(t, "second attempt", entries, pool, schema, id) // run_at is the retry's, set by the settle
}

// A cron beat fires when next_run_at arrives: the scheduler slept until the nearest
// next_run_at instead of waiting for the poll. The beat is placed 700ms ahead through
// the fixture, then the loop is nudged so it reads the new time.
func TestCronBeatFiresOnTime(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	exec, entries := entryClock(pool)
	e := startEngine(t, pool, slowPoll(schema), func(e *Engine) { e.Register("echo", exec) })
	declare(t, e, JobSpec{Name: "j", ExecutorType: "echo"})
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Job: "j", Cron: yearly}); err != nil {
		t.Fatal(err)
	}
	now, err := e.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	beat := now.Add(700 * time.Millisecond)
	dueAt(t, pool, schema, "s", beat)
	e.wakeScheduler()
	select {
	case at := <-entries:
		if late := at.Sub(beat); late < 0 || late > tight {
			t.Fatalf("executor entered %s after the beat, want within [0, %s]", late, tight)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("beat not executed within 2s (poll is 5s)")
	}
	var lag float64
	if err := pool.QueryRow(t.Context(), "SELECT extract(epoch FROM created_at - scheduled_at) FROM "+qualified(schema, "job_run")+" WHERE schedule_name = 's'").Scan(&lag); err != nil {
		t.Fatal(err)
	}
	if lag < 0 || lag > tight.Seconds() {
		t.Fatalf("run created %.3fs after the beat, want within %s", lag, tight)
	}
}

// Put on one instance wakes the scheduler of another through the schedule trigger:
// the scanning instance had nothing to sleep for and would otherwise poll in 5s.
func TestRemotePutWakesScheduler(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	exec, entries := entryClock(pool)
	scanner := startEngine(t, pool, slowPoll(schema), func(e *Engine) { e.Register("echo", exec) })
	api := startEngine(t, pool, apiOnly(schema), nil)
	declare(t, scanner, JobSpec{Name: "j", ExecutorType: "echo"})
	spec := ScheduleSpec{Name: "s", Job: "j", Cron: yearly}
	if err := api.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	// The beat is moved into the past through the fixture (no notification), then the
	// same spec is Put again: cron and timezone unchanged, so next_run_at is kept and
	// only the trigger's "schedule" notification reaches the scanning instance.
	dueAt(t, pool, schema, "s", time.Now().Add(-time.Second))
	if err := api.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	select {
	case <-entries:
		if took := time.Since(start); took > tight {
			t.Fatalf("beat executed %s after the remote Put, want within %s", took, tight)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("beat not executed within 2s of a remote Put (poll is 5s)")
	}
}

// Shutdown releases a run; another instance picks it up at once through NOTIFY
// (settle released leaves a pending row) instead of at its next poll.
func TestReleasedRunIsReclaimedAtOnce(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	entered := make(chan struct{}, 1)
	cfgA := slowPoll(schema)
	cfgA.ShutdownGrace = 50 * time.Millisecond
	a, err := New(pool, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	a.Register("slow", func(ctx context.Context, req *Request) (RawJSON, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err := a.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	declare(t, a, JobSpec{Name: "j", ExecutorType: "slow"})
	id := trigger(t, a, "j", "{}")
	<-entered // running in the database is not enough: the executor must be admitted, or Shutdown releases it unrun

	exec, entries := entryClock(pool)
	startEngine(t, pool, slowPoll(schema), func(e *Engine) { e.Register("slow", exec) })
	if err := a.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown a: %v", err)
	}
	expectEntry(t, "released run on b", entries, pool, schema, id) // run_at is the release time
}

// A due row the claim statement skipped (locked by another instance's claim that
// then rolled back, or one that came due right after the claim) is found by the
// next-due read and reclaimed after wakeFloor, not at the next poll.
func TestDueRowLeftBehindIsClaimedAtOnce(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	exec, entries := entryClock(pool)
	worker := startEngine(t, pool, slowPoll(schema), func(e *Engine) { e.Register("echo", exec) })
	declare(t, worker, JobSpec{Name: "j", ExecutorType: "echo"})
	now, err := worker.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	due := now.Add(400 * time.Millisecond)
	id := trigger(t, worker, "j", "{}", At(due))
	// another instance's claim holds the row across its due moment, then rolls back
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTx(t, tx)
	execSql(t, tx, "SELECT 1 FROM "+qualified(schema, "job_run")+" WHERE id = "+itoa(id)+" FOR UPDATE")
	waitFor(t, "the row to be due", func(ctx context.Context) bool {
		n, err := worker.st.Now(ctx)
		return err == nil && n.After(due.Add(100*time.Millisecond))
	})
	select {
	case <-entries:
		t.Fatal("executed while another transaction held the row")
	default:
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	released := time.Now()
	select {
	case <-entries:
		if took := time.Since(released); took > tight {
			t.Fatalf("claimed %s after the lock was released, want within %s", took, tight)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("due row left behind was not claimed within 2s (poll is 5s)")
	}
}

// With one slot and a 5s poll, five runs must finish back to back because
// freeing a slot wakes the claimer.
func TestSlotReleaseClaimsAgainAtOnce(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := slowPoll(schema)
	cfg.Concurrency = 1
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("echo", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "echo"})
	var ids []int64
	for range 5 {
		ids = append(ids, trigger(t, e, "j", "{}"))
	}
	start := time.Now()
	for _, id := range ids {
		waitRun(t, e, id, StateSucceeded)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("5 runs on 1 slot took %s, want well under one poll", took)
	}
}

// The listener's backend is killed: the next trigger still arrives within the poll,
// the reconnect is counted, and once reconnected delivery is immediate again.
func TestListenerReconnects(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &metricsRec{}
	cfg := slowPoll(schema)
	cfg.Metrics = rec
	cfg.PollInterval = time.Second // the reconnect wait is a jittered poll; keep the test short
	exec, entries := entryClock(pool)
	worker := startEngine(t, pool, cfg, func(e *Engine) { e.Register("echo", exec) })
	api := startEngine(t, pool, apiOnly(schema), nil)
	declare(t, worker, JobSpec{Name: "j", ExecutorType: "echo"})

	// the worker's listener is the backend whose last statement is LISTEN on this schema
	listenStmt := "LISTEN " + pgx.Identifier{schema}.Sanitize()
	listeners := func(ctx context.Context) int {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE query = $1", listenStmt).Scan(&n); err != nil {
			return -1
		}
		return n
	}
	waitFor(t, "the listener backend", func(ctx context.Context) bool { return listeners(ctx) >= 1 })
	var killed int
	if err := pool.QueryRow(t.Context(), "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE query = $1", listenStmt).Scan(&killed); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the reconnect to be counted", func(ctx context.Context) bool { return rec.get("listener_reconnect_total") >= 1 })
	id := trigger(t, api, "j", "{}")
	select {
	case <-entries:
	case <-time.After(3 * time.Second):
		t.Fatalf("run %d not started within the poll after the listener was killed", id)
	}
	waitFor(t, "the listener to be back", func(ctx context.Context) bool { return listeners(ctx) >= 1 })
	id = trigger(t, api, "j", "{}")
	expectEntry(t, "after reconnect", entries, pool, schema, id)
}

// The wake trigger's firing rules (§1.3 / §2.7), observed on a raw LISTEN connection:
// pending rows notify once per executor_type per transaction, heartbeat and claim
// updates do not, a schedule Put and Delete do, an Advance does not, and a type too
// long for a payload degrades to the broadcast wake instead of failing the insert.
func TestWakeTriggerRules(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	l, err := store.Open(pool, schema).Listen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	jobRun, schedule := qualified(schema, "job_run"), qualified(schema, "schedule")
	execSql(t, pool, "INSERT INTO "+qualified(schema, "job")+" (name, executor_type, params, timeout, retry_policy) VALUES ('j', 'x', '{}', 60, '{\"max_attempts\":3}')")

	collect := func(what string, want ...string) {
		t.Helper()
		var got []string
		for {
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			p, err := l.Wait(ctx)
			cancel()
			if err != nil {
				break
			}
			got = append(got, p)
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: notifications %q, want %q", what, got, want)
		}
	}
	execSql(t, pool, "INSERT INTO "+jobRun+" (job_name, executor_type, params, timeout, retry_policy, state) VALUES ('j','x','{}',60,'{}','pending'), ('j','x','{}',60,'{}','pending'), ('j','y','{}',60,'{}','pending')")
	collect("insert pending", "run:x", "run:y")
	execSql(t, pool, "UPDATE "+jobRun+" SET state = 'running', lease_token = gen_random_uuid(), extra = extra || jsonb_build_object('lease_owner', 'me'), lease_expires_at = now() + interval '1 minute', started_at = now()")
	collect("claim")
	execSql(t, pool, "UPDATE "+jobRun+" SET lease_expires_at = now() + interval '2 minutes'")
	collect("heartbeat")
	execSql(t, pool, "UPDATE "+jobRun+" SET state = 'pending', run_at = now() + interval '10 seconds', lease_token = NULL, extra = extra - 'lease_owner', lease_expires_at = NULL WHERE executor_type = 'y'")
	collect("retry with backoff", "run:y")
	execSql(t, pool, "INSERT INTO "+jobRun+" (job_name, executor_type, params, timeout, retry_policy, state) VALUES ('j', repeat('t', 7996), '{}', 60, '{}', 'pending')")
	collect("type too long for a payload", "")
	execSql(t, pool, "INSERT INTO "+schedule+" (name, job_name, cron, timezone, overlap, enabled, next_run_at) VALUES ('s', 'j', '"+yearly+"', 'UTC', 'skip', true, now())")
	collect("put", "schedule")
	execSql(t, pool, "UPDATE "+schedule+" SET next_run_at = now() + interval '1 minute' WHERE name = 's'")
	collect("advance")
	execSql(t, pool, "DELETE FROM "+schedule+" WHERE name = 's'")
	collect("delete", "schedule")
}

// The next-due read is one idx_job_run_claim probe per type (§2.2), never a scan of
// the pending backlog.
func TestNextDueUsesClaimIndex(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	path := pgx.Identifier{schema}.Sanitize()
	execSql(t, pool, "SET LOCAL search_path = "+path+`;
		INSERT INTO job_run (job_name, executor_type, params, timeout, retry_policy, state, run_at)
		SELECT 'j', 'x', '{}', 60, '{"max_attempts":3}', 'pending', now() + (g || ' seconds')::interval FROM generate_series(1, 2000) g;
		ANALYZE job_run;`)

	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	execSql(t, tx, "SET LOCAL search_path = "+path+"; SET LOCAL enable_seqscan = off")
	rows, err := tx.Query(t.Context(), "EXPLAIN "+store.NextPendingAt, []string{"x", "y"})
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if p := strings.Join(lines, "\n"); !strings.Contains(p, "idx_job_run_claim") || !strings.Contains(p, "Limit") || strings.Contains(p, "Seq Scan") {
		t.Errorf("next due plan:\n%s", p)
	}
}

// untilDue: the wait is next − db_now less what the local clock says has already
// passed since the statement returned; a due row is wakeFloor; nothing ahead is the
// jittered poll.
func TestUntilDueSubtractsElapsedTime(t *testing.T) {
	t.Parallel()
	e := &Engine{cfg: Config{PollInterval: 5 * time.Second}}
	dbNow := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	next := dbNow.Add(500 * time.Millisecond)
	if got := e.untilDue(store.Due{Next: &next, DbNow: dbNow, Sampled: time.Now().Add(-200 * time.Millisecond)}); got < 250*time.Millisecond || got > 310*time.Millisecond {
		t.Errorf("wait %s, want about 300ms", got)
	}
	past := dbNow.Add(-time.Second)
	if got := e.untilDue(store.Due{Next: &past, DbNow: dbNow, Sampled: time.Now()}); got != wakeFloor {
		t.Errorf("due row: wait %s, want %s", got, wakeFloor)
	}
	if got := e.untilDue(store.Due{DbNow: dbNow, Sampled: time.Now()}); got < 4*time.Second || got > 6*time.Second {
		t.Errorf("nothing ahead: wait %s, want a jittered 5s", got)
	}
	far := dbNow.Add(time.Hour)
	if got := e.untilDue(store.Due{Next: &far, DbNow: dbNow, Sampled: time.Now()}); got > 6*time.Second {
		t.Errorf("far ahead: wait %s, want capped by the poll", got)
	}
}
