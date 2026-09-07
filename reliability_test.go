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
	execSQL(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET lease_token = gen_random_uuid(), lease_owner = '"+owner+"', lease_expires_at = now() + interval '1 minute', attempt = attempt + 1 WHERE id = "+itoa(id))
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
	if _, err := st.Settle(t.Context(), store.Settlement{Id: id, Token: old, Outcome: store.Succeeded, Output: []byte(`1`)}); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale settle: %v", err)
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
	execSQL(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET attempt = 1, lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(id))

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
	execSQL(t, tx, "SELECT id FROM "+qualified(schema, "job_run")+" WHERE id = "+itoa(id)+" FOR UPDATE")
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
	execSQL(t, pool, "UPDATE "+jr+" SET attempt = 32767, lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(capped))
	execSQL(t, pool, "UPDATE "+jr+" SET lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(healthy))

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
	own, err := pgxpool.New(context.Background(), testDSN())
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

// Cancel reaches a running row just as its holder settles it back to pending (a
// retry): the cancel applies to the row's new version. Two statements, one per state,
// would both match zero rows and report success with the run queued and unflagged,
// the fourth zero-row meaning §7.2 forbids (§6.6).
func TestCancelSeesRunReleasedMeanwhile(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	named := namedPool(t, schema)
	e := startEngine(t, named, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	claimAsDeadHolder(t, pool, schema, "x")

	// the holder's retry settle in progress: the row is locked until it commits
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	jr := qualified(schema, "job_run")
	execSQL(t, tx, "SELECT id FROM "+jr+" WHERE id = "+itoa(id)+" FOR UPDATE")
	cancelled := make(chan error, 1)
	go func() { cancelled <- e.Runs().Cancel(context.Background(), id) }()
	waitBlocked(t, pool, schema)
	execSQL(t, tx, "UPDATE "+jr+" SET state = 'pending', attempt = attempt + 1, run_at = now() + interval '1 hour', lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL WHERE id = "+itoa(id))
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-cancelled; err != nil {
		t.Fatal(err)
	}
	if run, _ := e.Runs().Get(t.Context(), id); run.State != StateCancelled {
		t.Fatalf("after cancel: %s, cancel_requested %v", run.State, run.CancelRequested)
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

// exec_duration's outcome label is the state the row took: a retry that the cancel-hit
// CASE turned into cancelled is reported as cancelled, not failed.
func TestExecDurationLabelFollowsSettledState(t *testing.T) {
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
	e.settleResult(e.log, c, parseRetry(c.RetryPolicy), nil, errors.New("boom"), nil, time.Millisecond)
	if run, _ := e.Runs().Get(t.Context(), id); run.State != StateCancelled {
		t.Fatalf("run %s", run.State)
	}
	if rec.get("exec_duration", "executor_type", "x", "outcome", "cancelled") != 1 || rec.get("exec_duration", "executor_type", "x", "outcome", "failed") != 0 {
		t.Fatalf("labels %v", rec.counts)
	}
}

// The cancel-hit CASE in settle: a retry or a release that lands after Cancel ends
// the run instead of putting it back in the queue.
func TestSettleCancelHit(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	st := store.Open(pool, schema)
	entry := encodeErr(errEntry{Attempt: 1, Kind: "business", Message: "x"})

	for _, outcome := range []store.Outcome{store.Retry, store.Released, store.Failed} {
		id := trigger(t, e, "j", "")
		got, err := st.ClaimPending(t.Context(), []string{"x"}, 1, "A", time.Minute)
		if err != nil || len(got) != 1 {
			t.Fatalf("claim: %v %v", got, err)
		}
		if err := e.Runs().Cancel(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		res, err := st.Settle(t.Context(), store.Settlement{Id: id, Token: got[0].LeaseToken, Outcome: outcome, Err: entry, Backoff: time.Second})
		if err != nil || res.State != "cancelled" {
			t.Fatalf("outcome %d settled as %q: %v", outcome, res.State, err)
		}
		if run, _ := e.Runs().Get(t.Context(), id); run.FinishedAt == nil || run.State != StateCancelled {
			t.Fatalf("outcome %d row %+v", outcome, run)
		}
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

// ErrDuplicate always carries the in-flight run's id (§6.1). The run holding the key
// can finish between the INSERT that hit the key and the lookup; then the key is free
// and the INSERT is repeated, so a caller never sees (0, ErrDuplicate) from that
// window. Triggers race against completions for a few hundred rounds; each result is
// either a new run or a duplicate with a real id.
func TestDedupNeverReturnsIdZero(t *testing.T) {
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
	var created, dups atomic.Int32
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
				default:
					t.Errorf("trigger: id %d err %v", id, err)
					return
				}
			}
		})
	}
	wg.Wait()
	t.Logf("created %d, duplicates %d", created.Load(), dups.Load())
	if created.Load() == 0 || dups.Load() == 0 {
		t.Errorf("both outcomes must occur for the test to mean anything: created %d duplicates %d", created.Load(), dups.Load())
	}
}
