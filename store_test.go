package skein

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
)

// cancelAfterTx cancels the operation ctx once the n-th QueryRow has been scanned:
// the fixture for "the caller's ctx ended between the last statement and the restore".
type cancelAfterTx struct {
	pgx.Tx
	cancel context.CancelFunc
	after  int
	n      int
}

type rowThen struct {
	pgx.Row
	then func()
}

func (r rowThen) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	r.then()
	return err
}

func (c *cancelAfterTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	c.n++
	row := c.Tx.QueryRow(ctx, sql, args...)
	if c.n == c.after {
		return rowThen{Row: row, then: c.cancel}
	}
	return row
}

type queryThenTx struct {
	pgx.Tx
	then func(string)
}

func (tx *queryThenTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return rowThen{Row: tx.Tx.QueryRow(ctx, sql, args...), then: func() { tx.then(sql) }}
}

// §2.1: a user key is held by terminal runs too, so the only way its holder vanishes
// between the INSERT and the lookup is retention deleting the row. Each deleted winner
// costs one more round; three rounds exhausted is (0, ErrDuplicate) and a plain retry works.
func TestDedupRoundsWhenWinnersVanish(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"job", "workflow"} {
		for _, ends := range []int{1, 3} {
			t.Run(kind+"/"+itoa(int64(ends)), func(t *testing.T) {
				t.Parallel()
				pool, schema := freshSchema(t)
				e := startEngine(t, pool, submitOnly(schema), nil)
				declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
				create := func() (int64, error) { return e.Jobs().Trigger(t.Context(), "j", nil, DedupKey("k")) }
				table := qualified(schema, "job_run")
				triggerTx := func(tx pgx.Tx) (int64, error) {
					return e.Jobs().TriggerTx(t.Context(), tx, "j", nil, DedupKey("k"))
				}
				if kind == "workflow" {
					if err := e.Workflows().Declare(t.Context(), WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}}); err != nil {
						t.Fatal(err)
					}
					create = func() (int64, error) { return e.Workflows().Trigger(t.Context(), "w", nil, DedupKey("k")) }
					table = qualified(schema, "workflow_run")
					triggerTx = func(tx pgx.Tx) (int64, error) {
						return e.Workflows().TriggerTx(t.Context(), tx, "w", nil, DedupKey("k"))
					}
				}
				vanish := func(id int64) error { // what retention does to a terminal holder (§2.8)
					_, err := pool.Exec(t.Context(), "DELETE FROM "+table+" WHERE id = $1", id)
					return err
				}
				winner, err := create()
				if err != nil {
					t.Fatal(err)
				}
				tx, err := pool.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer rollbackTx(t, tx)
				inserts, lookups := 0, 0
				wrapped := &queryThenTx{Tx: tx, then: func(sql string) {
					switch sql {
					case store.TriggerJob, store.TriggerWorkflow:
						inserts++
						if inserts <= ends {
							if err := vanish(winner); err != nil {
								t.Fatal(err)
							}
						}
					case store.FindDedupRun, store.FindDedupWorkflowRun:
						lookups++
						if lookups < ends {
							winner, err = create()
							if err != nil {
								t.Fatal(err)
							}
						}
					}
				}}
				id, err := triggerTx(wrapped)
				if inserts != min(ends+1, 3) || lookups != ends {
					t.Fatalf("inserts %d lookups %d after %d vanished winners", inserts, lookups, ends)
				}
				if ends == 3 {
					if id != 0 || !errors.Is(err, ErrDuplicate) {
						t.Fatalf("exhausted rounds: id %d err %v", id, err)
					}
				} else if err != nil || id == 0 || id == winner {
					t.Fatalf("freed key: id %d err %v, old winner %d", id, err, winner)
				}
				if err := tx.Commit(t.Context()); err != nil {
					t.Fatal(err)
				}
				if ends == 3 {
					if id, err := create(); err != nil || id == 0 {
						t.Fatalf("retry after exhausted rounds: id %d err %v", id, err)
					}
				}
			})
		}
	}
}

// commitTracer records when each COMMIT was sent: the fixture for "the local sample
// was taken before the transaction committed" (§2.7).
type commitTracer struct {
	mu      sync.Mutex
	commits []time.Time
}

func (c *commitTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.EqualFold(data.SQL, "commit") {
		c.mu.Lock()
		c.commits = append(c.commits, time.Now())
		c.mu.Unlock()
	}
	return ctx
}

func (c *commitTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (c *commitTracer) last() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commits[len(c.commits)-1]
}

// §2.7: the local clock behind a due wait is sampled when the statement returns, inside
// the transaction, so the COMMIT round trip counts as elapsed time instead of being
// added to the wait.
func TestDueSampledBeforeCommit(t *testing.T) {
	t.Parallel()
	plain, schema := freshSchema(t)
	e := startEngine(t, plain, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	trigger(t, e, "j", "", At(time.Now().Add(time.Hour)))
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Job: "j", Cron: yearly}); err != nil {
		t.Fatal(err)
	}
	tr := &commitTracer{}
	cfg, err := pgxpool.ParseConfig(testDsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = tr
	traced, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(traced.Close)
	st := store.Open(traced, schema)

	before := time.Now()
	due, err := st.NextPendingAt(t.Context(), []string{"x"})
	if err != nil || due.Next == nil {
		t.Fatalf("next pending: %+v %v", due, err)
	}
	if due.Sampled.Before(before) || !due.Sampled.Before(tr.last()) {
		t.Errorf("pending sample %s not between statement return and commit %s", due.Sampled.Format(time.RFC3339Nano), tr.last().Format(time.RFC3339Nano))
	}
	before = time.Now()
	sc, err := st.ScanDue(t.Context(), nextRun)
	if err != nil || sc.Next == nil || len(sc.Fired) != 0 {
		t.Fatalf("scan: %+v %v", sc, err)
	}
	if sc.Sampled.Before(before) || !sc.Sampled.Before(tr.last()) {
		t.Errorf("schedule sample %s not between statement return and commit %s", sc.Sampled.Format(time.RFC3339Nano), tr.last().Format(time.RFC3339Nano))
	}
}

// TriggerTx restores the caller's search_path on a ctx of its own: with the caller's
// ctx cancelled after the duplicate lookup, pgx would not send the restore at all and
// the caller's transaction would carry on inside the library's schema.
func TestCallerTxRestoresSearchPathAfterCancel(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	trigger(t, e, "j", "", DedupKey("k"))

	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var before string
	if err := tx.QueryRow(t.Context(), "SELECT current_setting('search_path')").Scan(&before); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// QueryRow order: CurrentSearchPath, TriggerJob (conflict), JobExists, FindDedupRun; then the restore
	k := "k"
	wtx := &cancelAfterTx{Tx: tx, cancel: cancel, after: 4}
	id, err := store.Open(pool, schema).TriggerJob(ctx, wtx, store.TriggerJobParams{JobName: "j", Params: []byte("{}"), DedupKey: &k})
	if !errors.Is(err, store.ErrDuplicate) || id == 0 || wtx.n != 4 {
		t.Fatalf("trigger: id %d err %v after %d QueryRows", id, err, wtx.n)
	}
	var after string
	if err := tx.QueryRow(t.Context(), "SELECT current_setting('search_path')").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("search_path left as %q, was %q", after, before)
	}
}

// M1 check: the two claim branches each use their own partial index. Seq scans are
// disabled so a predicate the partial index cannot serve shows up as a seq scan
// with a huge cost instead of a plan the planner merely preferred on a small table.
func TestClaimUsesPartialIndexes(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	path := pgx.Identifier{schema}.Sanitize()
	execSql(t, pool, "SET LOCAL search_path = "+path+`;
		INSERT INTO job_run (job_name, executor_type, params, timeout, retry_policy, state, run_at)
		SELECT 'j', 'x', '{}', 60, '{"max_attempts":3}', 'pending', now() - (g || ' seconds')::interval FROM generate_series(1, 2000) g;
		INSERT INTO job_run (job_name, executor_type, params, timeout, retry_policy, state, run_at,
		                     lease_token, lease_owner, lease_expires_at, started_at)
		SELECT 'j', 'x', '{}', 60, '{"max_attempts":3}', 'running', now(), gen_random_uuid(), 'old', now() - interval '1 minute', now() FROM generate_series(1, 5);
		ANALYZE job_run;`)

	plan := func(sql string) string {
		tx, err := pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		execSql(t, tx, "SET LOCAL search_path = "+path+"; SET LOCAL enable_seqscan = off")
		rows, err := tx.Query(t.Context(), "EXPLAIN "+sql, "me", time.Minute, []string{"x"}, int32(16))
		if err != nil {
			t.Fatal(err)
		}
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(lines, "\n")
	}
	if p := plan(store.ClaimPending); !strings.Contains(p, "idx_job_run_claim") || strings.Contains(p, "Seq Scan") {
		t.Errorf("branch one plan:\n%s", p)
	}
	if p := plan(store.ClaimExpired); !strings.Contains(p, "idx_job_run_running") || strings.Contains(p, "Seq Scan") {
		t.Errorf("branch two plan:\n%s", p)
	}
}

// M1 check: heartbeat updates are HOT (no index touched, new version on the same page).
// Not parallel on purpose: HOT pruning needs the previous version to be older than
// every live snapshot, and sibling tests hammering the same database hold snapshots
// open. Sequential tests run while the parallel ones are paused, so this measures a
// quiet database, which is what a heartbeat every 15 s sees in production.
func TestHeartbeatIsHot(t *testing.T) {
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), nil) // no executors: nothing claims
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Params: RawJSON(`{"payload":"` + strings.Repeat("x", 600) + `"}`)})
	trigger(t, e, "j", "")

	st := store.Open(pool, schema)
	claimed, err := st.ClaimPending(t.Context(), []string{"x"}, 1, "me", time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	// Production heartbeats are seconds apart, so the previous version is always
	// older than every live snapshot and page pruning reclaims it. Back-to-back
	// updates would defeat pruning and measure the test, not the storage settings.
	const beats = 200
	for range beats {
		time.Sleep(2 * time.Millisecond)
		rows, err := st.Heartbeat(t.Context(), []int64{claimed[0].Id}, []uuid.UUID{claimed[0].LeaseToken}, time.Minute, time.Second)
		if err != nil || len(rows) != 1 {
			t.Fatalf("heartbeat: %v %v", rows, err)
		}
	}
	var upd, hot int64
	waitFor(t, "table statistics", func() bool {
		err := pool.QueryRow(t.Context(),
			"SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'job_run'", schema).Scan(&upd, &hot)
		return err == nil && upd >= beats+1
	})
	if ratio := float64(hot) / float64(upd); ratio < 0.95 {
		t.Fatalf("hot ratio %.3f (%d/%d): storage parameters are not taking effect", ratio, hot, upd)
	}
}

func rollbackTx(t *testing.T, tx pgx.Tx) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("rollback: %v", err)
	}
}

func execSql(t *testing.T, db interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, sql string) {
	t.Helper()
	if _, err := db.Exec(t.Context(), sql); err != nil {
		t.Fatalf("exec: %v\n%s", err, sql)
	}
}
