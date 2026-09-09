package skein

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"log/slog"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tests run against a real PostgreSQL: SKEIN_TEST_DSN, default the Homebrew socket.
// Each test gets its own schema, so tests run in parallel without sharing state.

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return namedPool(t, "")
}

// namedPool is testPool with application_name set, so a test can find that pool's
// backends in pg_stat_activity (waitBlocked). Without a reachable database the test
// skips, unless SKEIN_TEST_REQUIRE_DB is set (CI): then an all-green run cannot mean
// that every database test was skipped.
func namedPool(t *testing.T, appName string) *pgxpool.Pool {
	t.Helper()
	dsn := testDsn()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if appName != "" {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["application_name"] = appName
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(t.Context()); err != nil {
		pool.Close()
		if os.Getenv("SKEIN_TEST_REQUIRE_DB") != "" {
			t.Fatalf("SKEIN_TEST_REQUIRE_DB is set and there is no test database at %s: %v", dsn, err)
		}
		t.Skipf("no test database at %s: %v", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// waitBlocked waits until a backend of the pool named appName waits on a lock: the
// fixture for "that transaction has reached the row this test holds".
func waitBlocked(t *testing.T, pool *pgxpool.Pool, appName string) {
	t.Helper()
	waitFor(t, appName+" to block on a lock", func() bool {
		var n int
		err := pool.QueryRow(t.Context(), "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'", appName).Scan(&n)
		return err == nil && n > 0
	})
}

func freshSchema(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool := testPool(t)
	var b [4]byte
	_, _ = rand.Read(b[:])
	schema := "skein_test_" + hex.EncodeToString(b[:])
	if err := Migrate(t.Context(), pool, schema); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop schema %q: %v", schema, err)
		}
	})
	return pool, schema
}

func fastConfig(schema string) Config {
	return Config{
		Schema:            schema,
		PollInterval:      50 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		LeaseTTL:          400 * time.Millisecond,
		ShutdownGrace:     time.Second,
		CancelTimeout:     200 * time.Millisecond,
		BackoffBase:       50 * time.Millisecond,
		BackoffMax:        200 * time.Millisecond,
		Logger:            slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
}

// startEngine registers executors via setup, starts, and shuts down at cleanup.
func startEngine(t *testing.T, pool *pgxpool.Pool, cfg Config, setup func(e *Engine)) *Engine {
	t.Helper()
	e, err := New(pool, cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if setup != nil {
		setup(e)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	})
	return e
}

func waitRun(t *testing.T, e *Engine, id int64, state RunState) *JobRun {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		run, err := e.Runs().Get(t.Context(), id)
		if err != nil {
			t.Fatalf("get run %d: %v", id, err)
		}
		if run.State == state {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %d is %s, want %s (attempt %d, errors %s)", id, run.State, state, run.Attempt, run.Errors)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func declare(t *testing.T, e *Engine, spec JobSpec) {
	t.Helper()
	if err := e.Jobs().Declare(t.Context(), spec); err != nil {
		t.Fatalf("declare %s: %v", spec.Name, err)
	}
}

func trigger(t *testing.T, e *Engine, name string, params string, opts ...TriggerOption) int64 {
	t.Helper()
	id, err := e.Jobs().Trigger(t.Context(), name, RawJSON(params), opts...)
	if err != nil {
		t.Fatalf("trigger %s: %v", name, err)
	}
	return id
}

func errorsOf(t *testing.T, run *JobRun) []errEntry {
	t.Helper()
	var es []errEntry
	if err := json.Unmarshal(run.Errors, &es); err != nil {
		t.Fatalf("errors %s: %v", run.Errors, err)
	}
	return es
}

func TestTriggerRunsToSuccess(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	var got *Request
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("echo", func(ctx context.Context, req *Request) (RawJSON, error) {
			got = req
			return req.Params, nil
		})
	})
	declare(t, e, JobSpec{Name: "echo", ExecutorType: "echo", Params: RawJSON(`{"a":1,"b":1}`)})
	id := trigger(t, e, "echo", `{"b":2}`)
	run := waitRun(t, e, id, StateSucceeded)

	if string(run.Output) != `{"a": 1, "b": 2}` {
		t.Errorf("output %s: override must win the merge", run.Output)
	}
	if run.Attempt != 0 || run.StartedAt == nil || run.FinishedAt == nil || run.LeaseOwner != "" || run.LeaseExpiresAt != nil {
		t.Errorf("settled row %+v", run)
	}
	if got.Attempt != 1 || got.IdempotencyKey != "run:"+itoa(id) || got.JobName != "echo" {
		t.Errorf("request %+v", got)
	}
}

func TestDuplicateReturnsExistingId(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	release := make(chan struct{})
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("block", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-release
			return nil, nil
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "block"})
	id := trigger(t, e, "j", "", DedupKey("k"))
	waitRun(t, e, id, StateRunning)

	// a lost response is retried with the same key: same id, ErrDuplicate
	again, err := e.Jobs().Trigger(t.Context(), "j", nil, DedupKey("k"))
	if !errors.Is(err, ErrDuplicate) || again != id {
		t.Fatalf("second trigger: id %d err %v, want %d ErrDuplicate", again, err, id)
	}
	if _, err := e.Jobs().Trigger(t.Context(), "missing", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown job: %v", err)
	}

	close(release)
	waitRun(t, e, id, StateSucceeded)
	// a terminal row keeps the key for the retention window (§2.1)
	if again, err := e.Jobs().Trigger(t.Context(), "j", nil, DedupKey("k")); !errors.Is(err, ErrDuplicate) || again != id {
		t.Fatalf("after success: id %d err %v, want %d ErrDuplicate", again, err, id)
	}
}

// Concurrent Triggers with the same key: exactly one creates the run, every other
// caller gets that id with ErrDuplicate (ON CONFLICT waits for the winner to commit,
// and FindDedupRun reads a fresh snapshot).
func TestConcurrentDedupCreatesOne(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	release := make(chan struct{})
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("block", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-release
			return nil, nil
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "block"})
	const n = 16
	ids := make([]int64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { ids[i], errs[i] = e.Jobs().Trigger(t.Context(), "j", nil, DedupKey("k")) })
	}
	wg.Wait()
	winner := expectOneWinner(t, ids, errs)
	close(release)
	waitRun(t, e, winner, StateSucceeded)
}

// expectOneWinner: exactly one of n concurrent dedup-keyed triggers created a run;
// every other one got ErrDuplicate carrying that run's id.
func expectOneWinner(t *testing.T, ids []int64, errs []error) (winner int64) {
	t.Helper()
	created := 0
	for i := range ids {
		if errs[i] == nil {
			created, winner = created+1, ids[i]
		}
	}
	if created != 1 {
		t.Fatalf("%d of %d concurrent triggers created a run", created, len(ids))
	}
	for i := range ids {
		if errs[i] != nil && (!errors.Is(errs[i], ErrDuplicate) || ids[i] != winner) {
			t.Errorf("trigger %d: id %d err %v, want %d ErrDuplicate", i, ids[i], errs[i], winner)
		}
	}
	return winner
}

// An output the database cannot store is a permanent failure carrying the reason,
// not a lease left to expire: "not json" is caught in Go; a NUL escape and a number
// past numeric only by jsonb, which used to leave the row running until it was
// reclaimed with nothing but interrupted entries.
func TestInvalidOutputFailsPermanently(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	outputs := map[string]string{"syntax": `not json`, "nul": `"\u0000"`, "huge": `1e1000000`}
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("bad", func(ctx context.Context, req *Request) (RawJSON, error) { return RawJSON(outputs[req.JobName]), nil })
	})
	want := map[string]string{"syntax": "output is not valid JSON", "nul": "rejected by the database", "huge": "rejected by the database"}
	for name := range outputs {
		declare(t, e, JobSpec{Name: name, ExecutorType: "bad", Retry: RetryPolicy{MaxAttempts: 3}})
		run := waitRun(t, e, trigger(t, e, name, ""), StateFailed)
		es := errorsOf(t, run)
		if run.Attempt != 1 || len(es) != 1 || es[0].Kind != "business" || !strings.HasPrefix(es[0].Message, want[name]) {
			t.Errorf("%s: attempt %d errors %+v", name, run.Attempt, es)
		}
	}
}

func TestEmptyOutputSucceeds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		out  RawJSON
	}{{name: "nil"}, {name: "empty", out: RawJSON{}}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
				e.Register("empty", func(context.Context, *Request) (RawJSON, error) { return tc.out, nil })
			})
			declare(t, e, JobSpec{Name: "j", ExecutorType: "empty"})
			id := trigger(t, e, "j", "")
			var run *JobRun
			waitFor(t, "empty output to settle", func() bool {
				var err error
				run, err = e.Runs().Get(t.Context(), id)
				return err == nil && run.State.Terminal()
			})
			if run.State != StateSucceeded || run.Attempt != 0 || run.Output != nil || len(errorsOf(t, run)) != 0 {
				t.Fatalf("empty output: state %s attempt %d output %q errors %s", run.State, run.Attempt, run.Output, run.Errors)
			}
			declare(t, e, JobSpec{Name: "next", ExecutorType: "empty"})
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}, {Job: "next", Deps: []string{"j"}}}})
			wf := waitWorkflow(t, e, triggerWorkflow(t, e, "w", ""), WorkflowSucceeded)
			for _, node := range wf.Nodes {
				if node.Attempt != 0 || node.Output != nil || len(errorsOf(t, &node)) != 0 {
					t.Fatalf("empty node output: %+v", node)
				}
			}
		})
	}
}

func TestPanicCountsOneFailure(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("flaky", func(ctx context.Context, req *Request) (RawJSON, error) {
			if req.Attempt == 1 {
				panic("boom")
			}
			return RawJSON(`"ok"`), nil
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "flaky", Retry: RetryPolicy{MaxAttempts: 3}})
	run := waitRun(t, e, trigger(t, e, "j", ""), StateSucceeded)

	es := errorsOf(t, run)
	if run.Attempt != 1 || len(es) != 1 || es[0].Kind != "panic" || es[0].Attempt != 1 {
		t.Fatalf("attempt %d errors %+v", run.Attempt, es)
	}
}

func TestPermanentIsTerminal(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	calls := 0
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("bad", func(ctx context.Context, req *Request) (RawJSON, error) {
			calls++
			return nil, Permanent(errors.New("no way"))
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "bad", Retry: RetryPolicy{MaxAttempts: 3}})
	run := waitRun(t, e, trigger(t, e, "j", ""), StateFailed)

	es := errorsOf(t, run)
	if calls != 1 || run.Attempt != 1 || len(es) != 1 || es[0].Kind != "business" || es[0].Message != "permanent: no way" {
		t.Fatalf("calls %d attempt %d errors %+v", calls, run.Attempt, es)
	}
}

func TestRetryUntilMaxAttempts(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	calls := 0
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("bad", func(ctx context.Context, req *Request) (RawJSON, error) {
			calls++
			return nil, errors.New("again")
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "bad", Retry: RetryPolicy{MaxAttempts: 2}})
	run := waitRun(t, e, trigger(t, e, "j", ""), StateFailed)

	es := errorsOf(t, run)
	if calls != 2 || run.Attempt != 2 || len(es) != 2 || es[1].Attempt != 2 {
		t.Fatalf("calls %d attempt %d errors %+v", calls, run.Attempt, es)
	}
}

// The message of a permanent error is the executor's text: invalid UTF-8 (json/v2
// refuses to encode it) and NUL (jsonb refuses to store it) must still end as one
// recorded failed attempt, not as a settle that cannot commit, a lease that expires
// and a third execution that leaves only an interrupted entry.
func TestPermanentErrorWithBadTextIsRecorded(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	messages := map[string]string{"nul": "nul \x00 here", "utf8": "bad \xff utf8"}
	var calls atomic.Int32
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("bad", func(ctx context.Context, req *Request) (RawJSON, error) {
			calls.Add(1)
			return nil, Permanent(errors.New(messages[req.JobName]))
		})
	})
	for name := range messages {
		declare(t, e, JobSpec{Name: name, ExecutorType: "bad", Retry: RetryPolicy{MaxAttempts: 3}})
		calls.Store(0)
		run := waitRun(t, e, trigger(t, e, name, ""), StateFailed)
		es := errorsOf(t, run)
		if run.Attempt != 1 || calls.Load() != 1 || len(es) != 1 || es[0].Kind != "business" || !strings.Contains(es[0].Message, "\uFFFD") {
			t.Fatalf("%s: attempt %d calls %d errors %+v", name, run.Attempt, calls.Load(), es)
		}
	}
}

// cleanMessage is what makes the above hold for every message the executor can
// produce: replacements, a length cap on a rune boundary, and a document the
// database accepts.
func TestCleanMessage(t *testing.T) {
	t.Parallel()
	if got := cleanMessage("a\x00b\xffc"); got != "a\uFFFDb\uFFFDc" {
		t.Errorf("got %q", got)
	}
	long := strings.Repeat("字", errMessageMax) // 3 bytes each: the cut lands inside a rune
	if got := cleanMessage(long); len(got) > errMessageMax || !utf8.ValidString(got) {
		t.Errorf("len %d valid %v", len(got), utf8.ValidString(got))
	}
	if b := encodeErr(errEntry{Message: "x\xff"}); !RawJSON(b).IsValid() {
		t.Errorf("encodeErr produced %q", b)
	}
}

// A partly filled retry policy is validated as written: only the untouched zero
// value takes the default, so {BaseSec: -1} cannot slip in by omitting MaxAttempts.
// Names are bounded too: they travel into dedup keys, errors entries and the NOTIFY
// payload.
func TestDeclareRejectsPartialRetryAndLongNames(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), nil)
	for _, r := range []RetryPolicy{{BaseSec: -1}, {Jitter: math.NaN()}, {MaxSec: -1}} {
		if err := e.Jobs().Declare(t.Context(), JobSpec{Name: "j", ExecutorType: "x", Retry: r}); err == nil {
			t.Errorf("Declare accepted retry %+v", r)
		}
	}
	long := strings.Repeat("n", maxNameLen+1)
	if err := e.Jobs().Declare(t.Context(), JobSpec{Name: long, ExecutorType: "x"}); err == nil {
		t.Error("Declare accepted a job name over the limit")
	}
	if err := e.Jobs().Declare(t.Context(), JobSpec{Name: "j", ExecutorType: long}); err == nil {
		t.Error("Declare accepted an executor type over the limit")
	}
	if err := e.Workflows().Declare(t.Context(), WorkflowSpec{Name: long}); err == nil {
		t.Error("Workflows.Declare accepted a name over the limit")
	}
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: long, Job: "j", Cron: yearly}); err == nil {
		t.Error("Schedules.Put accepted a name over the limit")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Register accepted an executor type over the limit")
			}
		}()
		e2, _ := New(pool, fastConfig(schema))
		e2.Register(long, func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	}()
}

func TestRegisterSerializesLifecycle(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, submitOnly(schema))
	if err != nil {
		t.Fatal(err)
	}
	e.lifecycle.Lock()
	unlock := sync.OnceFunc(e.lifecycle.Unlock)
	defer unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	}()
	// Observe the actual mutex wait, not a scheduling delay before Register starts.
	waitFor(t, "Register to wait on lifecycle", func() bool {
		select {
		case <-done:
			t.Fatal("Register completed while lifecycle was locked")
		default:
		}
		buf := make([]byte, 1<<20)
		for _, stack := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if strings.Contains(stack, "TestRegisterSerializesLifecycle.func") &&
				strings.Contains(stack, "(*Engine).Register(") && strings.Contains(stack, "[sync.Mutex.Lock]") {
				return true
			}
		}
		return false
	})
	unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Register did not finish after lifecycle was unlocked")
	}
	if e.executors["x"] == nil {
		t.Fatal("Register did not bind the executor")
	}
}

func TestRegisterRejectsStartedOrShutdown(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"started", "shutdown before start"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e, err := New(pool, submitOnly(schema))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Shutdown(context.Background()) })
			if state == "started" {
				err = e.Start(t.Context())
			} else {
				err = e.Shutdown(t.Context())
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if recover() == nil {
					t.Error("Register accepted an executor after " + state)
				}
				if len(e.executors) != 0 {
					t.Error("rejected registration mutated executors")
				}
			}()
			e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
		})
	}
}

// The typed Register must reject nil before wrapping it: the wrapper is never nil,
// so the plain Register's check cannot see it and the panic would wait for a run.
func TestTypedRegisterRejectsNil(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, fastConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Register[P] accepted a nil function")
		}
	}()
	Register[struct{}](e, "typed", nil)
}

func TestTypedRegisterDecodeFailureIsPermanent(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	type params struct {
		N int `json:"n"`
	}
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		Register(e, "typed", func(ctx context.Context, req *Request, p params) (RawJSON, error) {
			return RawJSON(itoa(int64(p.N * 2))), nil
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "typed"})
	if run := waitRun(t, e, trigger(t, e, "j", `{"n":21}`), StateSucceeded); string(run.Output) != "42" {
		t.Fatalf("output %s", run.Output)
	}
	run := waitRun(t, e, trigger(t, e, "j", `{"n":"x"}`), StateFailed)
	if run.Attempt != 1 {
		t.Fatalf("decode failure retried: attempt %d errors %s", run.Attempt, run.Errors)
	}
}

func TestTriggerTxVisibleAfterCommit(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("ok", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "ok"})

	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var before string
	_ = tx.QueryRow(t.Context(), "SELECT current_setting('search_path')").Scan(&before)
	id, err := e.Jobs().TriggerTx(t.Context(), tx, "j", nil)
	if err != nil {
		t.Fatalf("trigger tx: %v", err)
	}
	var after string
	_ = tx.QueryRow(t.Context(), "SELECT current_setting('search_path')").Scan(&after)
	if after != before {
		t.Fatalf("search_path leaked into the caller's transaction: %q -> %q", before, after)
	}
	if _, err := e.Runs().Get(t.Context(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("run visible before commit: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitRun(t, e, id, StateSucceeded)
}

// A temp table named job_run in the caller's session must not take the run: a
// temporary schema that search_path does not name is searched before every schema it
// does, so the library lists pg_temp after its own schema.
func TestTriggerTxIgnoresTempTable(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	execSql(t, tx, "CREATE TEMP TABLE job_run (LIKE "+qualified(schema, "job_run")+" INCLUDING ALL) ON COMMIT DROP")
	id, err := e.Jobs().TriggerTx(t.Context(), tx, "j", nil)
	if err != nil {
		t.Fatal(err)
	}
	var inTemp int
	if err := tx.QueryRow(t.Context(), "SELECT count(*) FROM pg_temp.job_run").Scan(&inTemp); err != nil || inTemp != 0 {
		t.Fatalf("the temp table took the run: %d %v", inTemp, err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Runs().Get(t.Context(), id); err != nil {
		t.Fatalf("run is not in the real table: %v", err)
	}
}

func TestShutdownReleasesRunning(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := fastConfig(schema)
	cfg.ShutdownGrace = 100 * time.Millisecond
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("wait", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "wait"})
	id := trigger(t, e, "j", "")
	waitRun(t, e, id, StateRunning)

	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	run := waitRun(t, e, id, StatePending)
	es := errorsOf(t, run)
	if run.Attempt != 0 || len(es) != 1 || es[0].Kind != "released" || run.LeaseOwner != "" {
		t.Fatalf("released row: attempt %d errors %+v owner %q", run.Attempt, es, run.LeaseOwner)
	}
}

func TestTimeoutIsRetried(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := fastConfig(schema)
	// This test is about the ctx deadline, not the lease: under -race load the 400ms
	// lease of fastConfig can expire before the heartbeat renews it, which turns a
	// timeout attempt into an interrupted one.
	cfg.HeartbeatInterval, cfg.LeaseTTL = 500*time.Millisecond, 2*time.Second
	cfg.CancelTimeout = 2 * time.Second // the heartbeat caps the lease at timeout + CancelTimeout; 200ms would race the settle
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("slow", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "slow", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2}})
	run := waitRun(t, e, trigger(t, e, "j", ""), StateFailed)
	es := errorsOf(t, run)
	if run.Attempt != 2 || len(es) != 2 || es[0].Kind != "timeout" || es[1].Kind != "timeout" {
		t.Fatalf("attempt %d errors %+v", run.Attempt, es)
	}
}

// §1.3: first migrations racing on the same schema name all succeed. The advisory
// lock has to cover CREATE SCHEMA: IF NOT EXISTS is not atomic on its own, and the
// loser of that race would fail on the pg_namespace unique index.
func TestConcurrentFirstMigrate(t *testing.T) {
	t.Parallel()
	pool := testPool(t)
	var b [4]byte
	_, _ = rand.Read(b[:])
	schema := "skein_test_" + hex.EncodeToString(b[:])
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { errs[i] = Migrate(t.Context(), pool, schema) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("migrate %d: %v", i, err)
		}
	}
	startEngine(t, pool, submitOnly(schema), nil) // Start proves the recorded version
}

// §3.3: Stats before Start may overlap with Register; it reads the published snapshot,
// never the map being written. Meaningful under -race.
func TestRegisterAndStatsConcurrentlyBeforeStart(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, submitOnly(schema))
	if err != nil {
		t.Fatal(err)
	}
	const n = 16
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			e.Register("t"+itoa(int64(i)), func(context.Context, *Request) (RawJSON, error) { return nil, nil })
		})
		wg.Go(func() {
			if _, err := e.Stats(t.Context()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got := e.registeredTypes(); len(got) != n || !slices.IsSorted(got) {
		t.Fatalf("snapshot %v", got)
	}
}

// §3.1: every input document is size-checked, the absent one included. MaxPayload 1
// cannot hold the empty object, so a nil template or override is rejected like `{}`.
func TestEmptyInputIsSizeChecked(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := submitOnly(schema)
	cfg.MaxPayload = 1
	tiny := startEngine(t, pool, cfg, nil)
	if err := tiny.Jobs().Declare(t.Context(), JobSpec{Name: "j", ExecutorType: "x"}); err == nil {
		t.Fatal("nil params template accepted under MaxPayload 1")
	}
	cfg.MaxPayload = 2
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	trigger(t, e, "j", "")
	if _, err := e.Jobs().Trigger(t.Context(), "j", RawJSON(`{"a":1}`)); err == nil {
		t.Fatal("7-byte params accepted under MaxPayload 2")
	}
}

// New rejects what would fail later: a negative period panics in time.NewTicker on a
// loop goroutine, a retry policy attempt (smallint) cannot hold overflows on reclaim,
// and an Engine that was shut down cannot be started again.
func TestConfigValidation(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	bad := []Config{
		{Schema: schema, HeartbeatInterval: -time.Second},
		{Schema: schema, MaintenanceInterval: -time.Second},
		{Schema: schema, PollInterval: -time.Millisecond},
		{Schema: schema, CancelTimeout: -time.Second},
		{Schema: schema, MaxPayload: -1},
		{Schema: schema, DefaultRetry: RetryPolicy{MaxAttempts: 40000}},
		{Schema: schema, DefaultRetry: RetryPolicy{MaxAttempts: 3, BaseSec: 1 << 40}},
		{Schema: schema, DefaultRetry: RetryPolicy{MaxAttempts: 3, Jitter: math.NaN()}},
		{Schema: schema, DefaultRetry: RetryPolicy{BaseSec: -1}}, // a partial policy is validated, not replaced by the default
		{Schema: schema, DefaultRetry: RetryPolicy{Jitter: math.NaN()}},
		{Schema: schema, DefaultTimeout: (math.MaxInt32 + 1) * time.Second}, // int32 seconds in the column: 4294967297s would wrap to 1s
	}
	for _, cfg := range bad {
		if _, err := New(pool, cfg); err == nil {
			t.Errorf("accepted %+v", cfg)
		}
	}
	e := startEngine(t, pool, submitOnly(schema), nil)
	for _, retry := range []RetryPolicy{
		{MaxAttempts: 32768},
		{MaxAttempts: 3, MaxSec: maxRetrySeconds + 1},
		{MaxAttempts: 3, Jitter: 1},
		{MaxAttempts: 3, Jitter: math.NaN()},
	} {
		if err := e.Jobs().Declare(t.Context(), JobSpec{Name: "j", ExecutorType: "x", Retry: retry}); err == nil {
			t.Errorf("declared retry policy %+v", retry)
		}
	}
	if err := e.Jobs().Declare(t.Context(), JobSpec{Name: "j", ExecutorType: "x", Timeout: (math.MaxInt32 + 1) * time.Second}); err == nil {
		t.Error("declared a timeout past int32 seconds")
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err == nil {
		t.Error("Start after Shutdown succeeded")
	}
}

func TestStartRefusesUnmigratedSchema(t *testing.T) {
	t.Parallel()
	pool := testPool(t)
	e, err := New(pool, fastConfig("skein_test_never_migrated"))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err == nil {
		t.Fatal("Start accepted a schema without migrations")
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
