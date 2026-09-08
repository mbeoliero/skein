package skein

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
)

// M2: multi-instance reliability (design §9). Fixtures that would need a paused
// process or a clock are raw SQL on the test schema.

func submitOnly(schema string) Config {
	cfg := fastConfig(schema)
	cfg.DisableWorker = true
	return cfg
}

func qualified(schema, table string) string { return pgx.Identifier{schema, table}.Sanitize() }

// stealLease is what ClaimExpired does once a lease has expired, in one statement so
// the holder cannot renew between expiring and reclaiming: a new token and owner, one
// interrupted attempt.
func stealLease(t *testing.T, pool *pgxpool.Pool, schema string, id int64, owner string) {
	t.Helper()
	execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET lease_token = gen_random_uuid(), lease_owner = '"+owner+"', lease_expires_at = now() + interval '1 minute', attempt = attempt + 1 WHERE id = "+itoa(id))
}

func leaseToken(t *testing.T, pool *pgxpool.Pool, schema string, id int64) uuid.UUID {
	t.Helper()
	var token uuid.UUID
	if err := pool.QueryRow(t.Context(), "SELECT lease_token FROM "+qualified(schema, "job_run")+" WHERE id = $1", id).Scan(&token); err != nil {
		t.Fatalf("lease token: %v", err)
	}
	return token
}

// kill -9 the holder; another instance takes over after the lease expires, attempt +1.
func TestKillTakeover(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	sub := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, sub, JobSpec{Name: "j", ExecutorType: "crash", Retry: RetryPolicy{MaxAttempts: 3}})
	id := trigger(t, sub, "j", "")

	child := startHelper(t, schema)
	waitRun(t, sub, id, StateRunning)
	_ = child.Process.Kill()
	_ = child.Wait()
	killed := time.Now()

	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("crash", func(ctx context.Context, req *Request) (RawJSON, error) { return RawJSON(`"took over"`), nil })
	})
	run := waitRun(t, b, id, StateSucceeded)
	took := time.Since(killed)
	es := errorsOf(t, run)
	if run.Attempt != 1 || len(es) != 1 || es[0].Kind != "interrupted" || es[0].Attempt != 1 {
		t.Fatalf("attempt %d errors %+v", run.Attempt, es)
	}
	if took > 5*time.Second {
		t.Fatalf("takeover took %s", took)
	}
}

func TestSnoozeSurvivesProcessExit(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	sub := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, sub, JobSpec{Name: "j", ExecutorType: "snooze", Retry: RetryPolicy{MaxAttempts: 1}})
	id := trigger(t, sub, "j", "")
	child := startHelper(t, schema, "SKEIN_HELPER_MODE=snooze")
	var saved *JobRun
	waitFor(t, "committed snooze", func() bool {
		var err error
		saved, err = sub.Runs().Get(t.Context(), id)
		return err == nil && saved.State == StatePending && saved.StartedAt != nil
	})
	if saved.LeaseExpiresAt != nil || saved.LeaseOwner != "" || saved.Attempt != 0 || len(errorsOf(t, saved)) != 0 {
		t.Fatalf("snoozed run: %+v", saved)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("snooze", func(ctx context.Context, req *Request) (RawJSON, error) {
			return RawJSON(`"resumed"`), nil
		})
	})
	run, err := b.Runs().Get(t.Context(), id)
	if err != nil || run.State != StatePending || !run.RunAt.Equal(saved.RunAt) {
		t.Fatalf("after restart: %+v %v", run, err)
	}
	execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET run_at = now() WHERE id = "+itoa(id))
	run = waitRun(t, b, id, StateSucceeded)
	if run.Id != id || run.Attempt != 0 || len(errorsOf(t, run)) != 0 || string(run.Output) != `"resumed"` {
		t.Fatalf("after wake: %+v", run)
	}
}

func TestSnoozePendingCanBeCancelled(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	s := settlementFor(c, store.Snoozed)
	s.Delay = 24 * time.Hour
	if res, err := e.st.Settle(t.Context(), s); err != nil || res.State != "pending" {
		t.Fatalf("snooze: %+v %v", res, err)
	}
	if err := e.Runs().Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	run := waitRun(t, e, id, StateCancelled)
	if run.Attempt != 0 || len(errorsOf(t, run)) != 0 || run.Output != nil {
		t.Fatalf("cancelled snooze: %+v", run)
	}
}

func TestSnoozeFailedSettlementReclaimed(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	jr, reject := qualified(schema, "job_run"), qualified(schema, "reject_snooze")
	execSql(t, pool, "CREATE FUNCTION "+reject+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced snooze rollback' USING ERRCODE = '40001'; END $$;
		CREATE TRIGGER reject_snooze BEFORE UPDATE ON `+jr+`
		FOR EACH ROW WHEN (OLD.state = 'running' AND NEW.state = 'pending') EXECUTE FUNCTION `+reject+"()")

	e.settleResult(
		e.log,
		c,
		parseRetry(c.RetryPolicy),
		nil,
		Snooze(time.Hour),
		nil,
		time.Millisecond,
	)
	run, err := e.Runs().Get(t.Context(), id)
	if err != nil || run.State != StateRunning || run.Attempt != 0 || len(errorsOf(t, run)) != 0 {
		t.Fatalf("failed settlement changed run: %+v %v", run, err)
	}
	if leaseToken(t, pool, schema, id) != c.LeaseToken {
		t.Fatal("failed settlement cleared its lease")
	}
	execSql(t, pool, "DROP TRIGGER reject_snooze ON "+jr)
	execSql(t, pool, "UPDATE "+jr+" SET lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(id))
	got, err := e.st.ClaimExpired(t.Context(), []string{"x"}, 1, "B", time.Minute)
	if err != nil || len(got) != 1 || got[0].Attempt != 1 {
		t.Fatalf("reclaim: %+v %v", got, err)
	}
	if _, err := e.st.Settle(t.Context(), settlementFor(got[0], store.Succeeded)); err != nil {
		t.Fatal(err)
	}
	run = waitRun(t, e, id, StateSucceeded)
	es := errorsOf(t, run)
	if run.Attempt != 1 || len(es) != 1 || es[0].Kind != "interrupted" {
		t.Fatalf("reclaimed failure history: %+v", run)
	}
}

// The fake provider has its own durable idempotency record, independent of job_run.
func fakeVideoSubmit(ctx context.Context, pool *pgxpool.Pool, schema, key string) (RawJSON, error) {
	var out []byte
	err := pool.QueryRow(ctx,
		"INSERT INTO "+qualified(schema, "video_task")+" (request_key) VALUES ($1) "+
			"ON CONFLICT (request_key) DO UPDATE SET request_key = EXCLUDED.request_key "+
			"RETURNING jsonb_build_object('task_id', task_id, 'deadline', deadline)", key,
	).Scan(&out)
	return out, err
}

func TestSnoozeWorkflowSubmitCrash(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	provider := qualified(schema, "video_task")
	execSql(t, pool, "CREATE TABLE "+provider+` (
		request_key text PRIMARY KEY,
		task_id uuid NOT NULL DEFAULT gen_random_uuid(),
		deadline timestamptz NOT NULL DEFAULT now() + interval '24 hours'
	)`)

	sub := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, sub, JobSpec{Name: "submit", ExecutorType: "submit-crash"})
	declare(t, sub, JobSpec{Name: "poll", ExecutorType: "poll", Retry: RetryPolicy{MaxAttempts: 1}})
	declareWorkflow(t, sub, WorkflowSpec{Name: "video", Nodes: []Node{
		{Job: "submit"}, {Job: "poll", Deps: []string{"submit"}},
	}})
	id := triggerWorkflow(t, sub, "video", "")
	child := startHelper(t, schema, "SKEIN_HELPER_MODE=snooze")
	var saved []byte
	waitFor(t, "provider submission before crash", func() bool {
		return pool.QueryRow(
			t.Context(),
			"SELECT jsonb_build_object('task_id', task_id, 'deadline', deadline) FROM "+provider,
		).Scan(&saved) == nil
	})
	before, err := sub.Workflows().GetRun(t.Context(), id)
	if err != nil || nodeOf(t, before, "submit").State != StateRunning || nodeOf(t, before, "submit").Output != nil {
		t.Fatalf("submit must not have settled: %+v %v", before, err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	var ready atomic.Bool
	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("submit-crash", func(ctx context.Context, req *Request) (RawJSON, error) {
			return fakeVideoSubmit(ctx, pool, schema, req.IdempotencyKey)
		})
		e.Register("poll", func(ctx context.Context, req *Request) (RawJSON, error) {
			if string(req.Deps["submit"]) != string(saved) {
				return nil, Permanent(errors.New("task id or deadline changed after reclaim"))
			}
			if !ready.Load() {
				return nil, Snooze(24 * time.Hour)
			}
			return RawJSON(`{"video":"done"}`), nil
		})
	})
	var waiting *WorkflowRun
	waitFor(t, "poll snooze after submit recovery", func() bool {
		var err error
		waiting, err = b.Workflows().GetRun(t.Context(), id)
		if err != nil {
			return false
		}
		poll := nodeOf(t, waiting, "poll")
		return poll.State == StatePending && poll.StartedAt != nil
	})
	submitted := nodeOf(t, waiting, "submit")
	if submitted.State != StateSucceeded || submitted.Attempt != 1 || string(submitted.Output) != string(saved) {
		t.Fatalf("recovered submit: %+v", submitted)
	}
	var effects int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM "+provider).Scan(&effects); err != nil || effects != 1 {
		t.Fatalf("provider effects %d: %v", effects, err)
	}
	ready.Store(true)
	execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+
		" SET run_at = now() WHERE id = "+itoa(nodeOf(t, waiting, "poll").Id))

	finished := waitWorkflow(t, b, id, WorkflowSucceeded)
	if !nodeOf(t, finished, "submit").StartedAt.Equal(*submitted.StartedAt) || nodeOf(t, finished, "poll").Attempt != 0 {
		t.Fatalf("workflow after poll: %+v", finished)
	}
}

// A lease that moved to another holder: the old holder's heartbeat and settle write nothing,
// and its executor is cancelled with the lease-lost cause and its result dropped.
func TestStaleTokenCannotWrite(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cause := make(chan error, 1)
	a := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("hold", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return RawJSON(`"late"`), nil
		})
	})
	declare(t, a, JobSpec{Name: "j", ExecutorType: "hold"})
	id := trigger(t, a, "j", "")
	waitRun(t, a, id, StateRunning)
	old := leaseToken(t, pool, schema, id)

	// B takes over as if A had paused past its lease
	stealLease(t, pool, schema, id, "B")
	st := store.Open(pool, schema)
	if c := <-cause; !errors.Is(c, errLeaseLost) {
		t.Fatalf("executor cancelled with %v, want lease lost", c)
	}
	rows, err := st.Heartbeat(t.Context(), []int64{id}, []uuid.UUID{old}, time.Minute, time.Second)
	if err != nil || len(rows) != 0 {
		t.Fatalf("stale heartbeat renewed: %+v %v", rows, err)
	}
	for _, tc := range []struct {
		name    string
		outcome store.Outcome
	}{{name: "succeeded", outcome: store.Succeeded}, {name: "snoozed", outcome: store.Snoozed}} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.Settle(t.Context(), store.Settlement{
				Id: id, Token: old, Outcome: tc.outcome, Output: []byte(`1`), Delay: time.Hour,
			})
			if !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("stale settle: %v", err)
			}
		})
	}
	run, _ := a.Runs().Get(t.Context(), id)
	if run.State != StateRunning || run.LeaseOwner != "B" || run.Attempt != 1 {
		t.Fatalf("row after stale writes: %s owner %q attempt %d", run.State, run.LeaseOwner, run.Attempt)
	}
}

// The max_attempts-th reclaim settles failed without running the executor.
func TestMaxAttemptsReclaimFails(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	sub := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, sub, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 2}})
	id := trigger(t, sub, "j", "")

	st := store.Open(pool, schema)
	if got, err := st.ClaimPending(t.Context(), []string{"x"}, 1, "A", time.Minute); err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	// one earlier interruption already counted, then A dies too
	execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET attempt = 1, lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(id))

	calls := 0
	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) { calls++; return nil, nil })
	})
	run := waitRun(t, b, id, StateFailed)
	es := errorsOf(t, run)
	if calls != 0 || run.Attempt != 2 || len(es) != 1 || es[0].Kind != "interrupted" || es[0].Attempt != 2 {
		t.Fatalf("calls %d attempt %d errors %+v", calls, run.Attempt, es)
	}
}

// A heartbeat statement waiting on a row lock does not hold Shutdown past its ctx:
// step 6 cancels it, nothing is left to renew by then. The executor's release settle
// waits on the same lock, hence ErrNotDrained; it lands once the lock is free.
func TestShutdownCancelsBlockedHeartbeat(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := fastConfig(schema)
	cfg.HeartbeatInterval, cfg.LeaseTTL = 2*time.Second, 5*time.Second
	e := startEngine(t, namedPool(t, "hb-blocked"), cfg, func(e *Engine) {
		e.Register("wait", func(ctx context.Context, req *Request) (RawJSON, error) { <-ctx.Done(); return nil, ctx.Err() })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "wait"})
	id := trigger(t, e, "j", "")
	waitRun(t, e, id, StateRunning)
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	execSql(t, tx, "SELECT id FROM "+qualified(schema, "job_run")+" WHERE id = "+itoa(id)+" FOR UPDATE")
	waitBlocked(t, pool, "hb-blocked")

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = e.Shutdown(ctx)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Shutdown waited %s for the blocked heartbeat", took)
	}
	if !errors.Is(err, ErrNotDrained) {
		t.Fatalf("shutdown: %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitRun(t, e, id, StatePending)
}

// A holder that dies at the attempt cap: the reclaim keeps attempt at 32767 instead
// of overflowing the smallint, which failed the whole ClaimExpired batch and left every
// expired row in it unclaimable. The capped row settles failed, the healthy one runs.
func TestReclaimAtAttemptCapConverges(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	sub := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, sub, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 32767}})
	capped, healthy := trigger(t, sub, "j", ""), trigger(t, sub, "j", "")
	st := store.Open(pool, schema)
	if got, err := st.ClaimPending(t.Context(), []string{"x"}, 2, "A", time.Minute); err != nil || len(got) != 2 {
		t.Fatalf("claim: %v %v", got, err)
	}
	jr := qualified(schema, "job_run")
	execSql(t, pool, "UPDATE "+jr+" SET attempt = 32767, lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(capped))
	execSql(t, pool, "UPDATE "+jr+" SET lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(healthy))

	var calls atomic.Int32
	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) { calls.Add(1); return nil, nil })
	})
	run := waitRun(t, b, capped, StateFailed)
	es := errorsOf(t, run)
	if run.Attempt != 32767 || len(es) != 1 || es[0].Kind != "interrupted" || es[0].Attempt != 32767 {
		t.Fatalf("capped: attempt %d errors %+v", run.Attempt, es)
	}
	waitRun(t, b, healthy, StateSucceeded)
	if calls.Load() != 1 {
		t.Fatalf("executor ran %d times; the capped row must not run", calls.Load())
	}
}

// An executor that ignores ctx: Shutdown returns ErrNotDrained, and once the lease
// expires another instance reclaims the run; the late result is fenced off.
func TestShutdownNotDrainedThenReclaimed(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	gate, entered := make(chan struct{}), make(chan struct{}, 1)
	cfg := fastConfig(schema)
	cfg.ShutdownGrace, cfg.CancelTimeout = 100*time.Millisecond, 100*time.Millisecond
	a := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("stubborn", func(ctx context.Context, req *Request) (RawJSON, error) {
			entered <- struct{}{}
			<-gate
			return RawJSON(`"late"`), nil
		})
	})
	declare(t, a, JobSpec{Name: "j", ExecutorType: "stubborn"})
	id := trigger(t, a, "j", "")
	<-entered // running in the database is not admission: Shutdown before it would release the run unrun

	if err := a.Shutdown(context.Background()); !errors.Is(err, ErrNotDrained) {
		t.Fatalf("shutdown: %v, want ErrNotDrained", err)
	}
	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("stubborn", func(ctx context.Context, req *Request) (RawJSON, error) { return RawJSON(`"b"`), nil })
	})
	run := waitRun(t, b, id, StateSucceeded)
	if run.Attempt != 1 || string(run.Output) != `"b"` {
		t.Fatalf("attempt %d output %s", run.Attempt, run.Output)
	}
	close(gate) // A's executor returns now; its settle is rejected by the fence
	a.execs.Wait()
	if r, _ := b.Runs().Get(t.Context(), id); r.State != StateSucceeded || string(r.Output) != `"b"` {
		t.Fatalf("A's late result was written over B's: %s %s", r.State, r.Output)
	}
}

// An executor that ignores cancellation, whose lease then expires and is reclaimed by
// this same process: when the new attempt finishes, Shutdown must still report the old
// goroutine, so active is keyed by lease token, not by run id.
func TestShutdownReportsExecutorOfEarlierAttempt(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	gate := make(chan struct{})
	var calls atomic.Int32
	cfg := fastConfig(schema)
	cfg.ShutdownGrace = 100 * time.Millisecond
	a := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("stubborn", func(ctx context.Context, req *Request) (RawJSON, error) {
			if calls.Add(1) == 1 {
				<-gate // ignores ctx
				return RawJSON(`"late"`), nil
			}
			return RawJSON(`"second"`), nil
		})
	})
	t.Cleanup(func() { close(gate) })
	declare(t, a, JobSpec{Name: "j", ExecutorType: "stubborn", Timeout: time.Second})
	id := trigger(t, a, "j", "")
	// the first attempt times out, stops being renewed after CancelTimeout, its lease
	// expires, and the claim loop of this process reclaims the run
	run := waitRun(t, a, id, StateSucceeded)
	if run.Attempt != 1 || string(run.Output) != `"second"` || calls.Load() != 2 {
		t.Fatalf("attempt %d output %s calls %d", run.Attempt, run.Output, calls.Load())
	}
	if err := a.Shutdown(context.Background()); !errors.Is(err, ErrNotDrained) {
		t.Fatalf("shutdown: %v, want ErrNotDrained for the first attempt's executor", err)
	}
}

// A claimed run whose post-claim checks straddle Shutdown: run must not start an
// executor that nothing would cancel; the row is settled released (§6.8 step 1).
func TestRunAfterShutdownReleases(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	calls := 0
	e := startEngine(t, pool, submitOnly(schema), func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) { calls++; return nil, nil })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.run(c)
	run, _ := e.Runs().Get(t.Context(), id)
	if es := errorsOf(t, run); calls != 0 || run.State != StatePending || run.Attempt != 0 || len(es) != 1 || es[0].Kind != "released" {
		t.Fatalf("calls %d run %s attempt %d errors %s", calls, run.State, run.Attempt, run.Errors)
	}
}

// Heartbeats failing for LeaseTTL − HeartbeatInterval: executors are cancelled and
// their results dropped; the row is left for reclaim.
func TestHeartbeatFailureDropsExecutors(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	own, err := pgxpool.New(context.Background(), testDsn())
	if err != nil {
		t.Fatal(err)
	}
	cause := make(chan error, 1)
	a := startEngine(t, own, fastConfig(schema), func(e *Engine) {
		e.Register("hold", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return RawJSON(`"late"`), nil
		})
	})
	declare(t, a, JobSpec{Name: "j", ExecutorType: "hold"})
	id := trigger(t, a, "j", "")
	waitRun(t, a, id, StateRunning)

	own.Close() // every heartbeat fails from here on
	select {
	case c := <-cause:
		if !errors.Is(c, errLeaseLost) {
			t.Fatalf("cancelled with %v, want lease lost", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor was not cancelled after heartbeats failed")
	}
	sub := startEngine(t, pool, submitOnly(schema), nil)
	if run, _ := sub.Runs().Get(t.Context(), id); run.State != StateRunning || run.LeaseOwner != a.owner {
		t.Fatalf("row was written by a holder that lost its heartbeat: %s %q", run.State, run.LeaseOwner)
	}
}

func TestCancelPendingRun(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "", At(time.Now().Add(time.Hour)))

	if err := e.Runs().Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	run := waitRun(t, e, id, StateCancelled)
	if run.FinishedAt == nil || run.Attempt != 0 {
		t.Fatalf("%+v", run)
	}
	if err := e.Runs().Cancel(t.Context(), id); err != nil {
		t.Fatalf("second cancel must be a no-op: %v", err)
	}
	if err := e.Runs().Cancel(t.Context(), id+1000); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
}

// Cancel must recheck the new pending tuple after a retry or snooze commits (§6.6).
func TestCancelSeesRunReleasedMeanwhile(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		attempt int
	}{{name: "retry", attempt: 1}, {name: "snooze", attempt: 0}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			named := namedPool(t, schema)
			e := startEngine(t, named, submitOnly(schema), nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			id := trigger(t, e, "j", "")
			claimAsDeadHolder(t, pool, schema, "x")
			tx, err := pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			jr := qualified(schema, "job_run")
			execSql(t, tx, "SELECT id FROM "+jr+" WHERE id = "+itoa(id)+" FOR UPDATE")
			cancelled := make(chan error, 1)
			ctx := t.Context()
			go func() { cancelled <- e.Runs().Cancel(ctx, id) }()
			waitBlocked(t, pool, schema)
			execSql(t, tx, "UPDATE "+jr+" SET state = 'pending', attempt = "+itoa(int64(tc.attempt))+
				", run_at = now() + interval '1 hour', lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL "+
				"WHERE id = "+itoa(id))

			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-cancelled; err != nil {
				t.Fatal(err)
			}
			if run, _ := e.Runs().Get(ctx, id); run.State != StateCancelled || run.Attempt != tc.attempt {
				t.Fatalf("after cancel: %+v", run)
			}
		})
	}
}

// Cancelling a running run reaches the holder through the heartbeat; whatever the
// executor returns after that, the run is cancelled.
func TestCancelRunningRun(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cause := make(chan error, 1)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("hold", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return RawJSON(`"finished anyway"`), nil
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "hold"})
	id := trigger(t, e, "j", "")
	waitRun(t, e, id, StateRunning)

	if err := e.Runs().Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	run := waitRun(t, e, id, StateCancelled)
	if c := <-cause; !errors.Is(c, errCancelRequested) {
		t.Fatalf("cause %v", c)
	}
	if run.Output != nil || run.Attempt != 0 || string(run.Errors) != "[]" {
		t.Fatalf("%+v", run)
	}
}

// Metrics follow the persisted state even if cancellation wins only inside settle.
func TestExecDurationLabelFollowsSettledState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "retry", err: errors.New("boom")},
		{name: "snooze", err: Snooze(time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			rec := &metricsRec{}
			cfg := submitOnly(schema)
			cfg.Metrics = rec
			e := startEngine(t, pool, cfg, nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			id := trigger(t, e, "j", "")
			c := claimAsDeadHolder(t, pool, schema, "x")
			if err := e.Runs().Cancel(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			e.settleResult(
				e.log,
				c,
				parseRetry(c.RetryPolicy),
				nil,
				tc.err,
				nil,
				time.Millisecond,
			)
			if run, _ := e.Runs().Get(t.Context(), id); run.State != StateCancelled {
				t.Fatalf("run %s", run.State)
			}
			for _, outcome := range []string{"cancelled", "failed", "snoozed"} {
				want := 0
				if outcome == "cancelled" {
					want = 1
				}
				if got := rec.get("exec_duration", "executor_type", "x", "outcome", outcome); got != want {
					t.Fatalf("%s observations: %d, want %d", outcome, got, want)
				}
			}
		})
	}
}

// The cancel-hit CASE also applies to Snooze, without consuming an attempt.
func TestSettleCancelHit(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	st := store.Open(pool, schema)
	entry := encodeErr(errEntry{Attempt: 1, Kind: "business", Message: "x"})

	for _, tc := range []struct {
		name    string
		outcome store.Outcome
	}{
		{name: "retry", outcome: store.Retry}, {name: "released", outcome: store.Released},
		{name: "failed", outcome: store.Failed}, {name: "snoozed", outcome: store.Snoozed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := trigger(t, e, "j", "")
			got, err := st.ClaimPending(t.Context(), []string{"x"}, 1, "A", time.Minute)
			if err != nil || len(got) != 1 {
				t.Fatalf("claim: %v %v", got, err)
			}
			if err := e.Runs().Cancel(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			res, err := st.Settle(t.Context(), store.Settlement{
				Id: id, Token: got[0].LeaseToken, Outcome: tc.outcome,
				Err: entry, Backoff: time.Second, Delay: time.Hour,
			})
			if err != nil || res.State != "cancelled" {
				t.Fatalf("settled as %q: %v", res.State, err)
			}
			run, err := e.Runs().Get(t.Context(), id)
			if err != nil || run.FinishedAt == nil || run.State != StateCancelled {
				t.Fatalf("row %+v: %v", run, err)
			}
			if tc.outcome == store.Snoozed && (run.Attempt != 0 || len(errorsOf(t, run)) != 0) {
				t.Fatalf("snooze changed failure history: %+v", run)
			}
		})
	}
}

// Shutdown runs once, but a later caller must not queue on the lifecycle mutex
// behind it: the background Shutdown that a cancelled Start ctx begins has no
// budget, and the host's own call with a short one must return on that ctx.
func TestShutdownLaterCallerHonoursItsCtx(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	gate, entered := make(chan struct{}), make(chan struct{}, 1)
	openGate := sync.OnceFunc(func() { close(gate) }) // the test and its cleanup both open it
	cfg := fastConfig(schema)
	cfg.ShutdownGrace, cfg.CancelTimeout = 2*time.Second, 2*time.Second
	e, err := New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Register("stubborn", func(ctx context.Context, req *Request) (RawJSON, error) {
		entered <- struct{}{}
		<-gate
		return nil, nil
	})
	if err := e.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { openGate(); _ = e.Shutdown(context.Background()) })
	declare(t, e, JobSpec{Name: "j", ExecutorType: "stubborn"})
	trigger(t, e, "j", "")
	<-entered

	first := make(chan error, 1)
	go func() { first <- e.Shutdown(context.Background()) }()
	waitFor(t, "the first Shutdown to start", func() bool { return e.shutting.Load() })
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := e.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Shutdown: %v, want its own ctx error", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("second Shutdown waited %s on the first one", took)
	}
	openGate()
	if err := <-first; err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after completion must return the first result: %v", err)
	}
}

func TestShutdownCancelsStart(t *testing.T) {
	t.Parallel()
	_, schema := freshSchema(t)
	poolCfg, err := pgxpool.ParseConfig(testDsn())
	if err != nil {
		t.Fatal(err)
	}
	poolCfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(t.Context(), poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	held, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(held.Release)
	e, err := New(pool, fastConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	startCtx, cancelStart := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancelStart()
		held.Release()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	})
	started := make(chan error, 1)
	go func() { started <- e.Start(startCtx) }()
	waitFor(t, "Start to begin its blocked schema check", func() bool { return e.started.Load() })
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- e.Shutdown(ctx) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("shutdown before execution: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown waited on Start's blocked database acquisition")
	}
	// The connection is still held: only the stop signal can end this Start.
	select {
	case err := <-started:
		if err == nil {
			t.Fatal("Start succeeded after Shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not cancel the startup query")
	}
	if err := e.Start(t.Context()); err == nil {
		t.Fatal("Start after Shutdown succeeded")
	}
}

// §6.1: a winner can finish between INSERT and lookup. Exhausting all three
// rounds permits (0, ErrDuplicate); this bounded trigger loop retries on its next round.
func TestDedupWhileWinnersFinish(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("quick", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "quick"})
	rounds := 300
	if testing.Short() {
		rounds = 50
	}
	var created, dups, ended atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range rounds {
				id, err := e.Jobs().Trigger(t.Context(), "j", nil, DedupKey("k"))
				switch {
				case err == nil && id > 0:
					created.Add(1)
				case errors.Is(err, ErrDuplicate) && id > 0:
					dups.Add(1)
				case errors.Is(err, ErrDuplicate) && id == 0:
					ended.Add(1)
				default:
					t.Errorf("trigger: id %d err %v", id, err)
					return
				}
			}
		})
	}
	wg.Wait()
	t.Logf("created %d, duplicates %d, ended winners %d", created.Load(), dups.Load(), ended.Load())
	if created.Load() == 0 || dups.Load() == 0 {
		t.Errorf("both outcomes must occur for the test to mean anything: created %d duplicates %d", created.Load(), dups.Load())
	}
}
