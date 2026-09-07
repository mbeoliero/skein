package skein

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// M5 performance baseline (design §9): three worker processes, 100k existing rows,
// short and long tasks with 1 KB and 256 KB payloads. No SLA; it prints a markdown
// report to compare against later. Gated because it takes a few minutes:
//
//	SKEIN_BENCH=1 go test -run TestPerformanceBaseline -v -timeout 20m
//
// Knobs: SKEIN_BENCH_POLL (worker PollInterval, default 1s), SKEIN_BENCH_CONCURRENCY
// (per worker, default 16), SKEIN_BENCH_OUT (append the report to this file).

func benchWorkerConfig(schema string) Config {
	poll, _ := time.ParseDuration(cmp.Or(os.Getenv("SKEIN_BENCH_POLL"), "1s"))
	conc, _ := strconv.Atoi(cmp.Or(os.Getenv("SKEIN_BENCH_CONCURRENCY"), "16"))
	return Config{
		Schema:            schema,
		PollInterval:      poll,
		Concurrency:       conc,
		HeartbeatInterval: time.Second, // long tasks outlive a few heartbeats so the HOT ratio means something
		LeaseTTL:          4 * time.Second,
		Logger:            slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
}

func registerBenchExecutors(e *Engine) {
	e.Register("short", func(ctx context.Context, req *Request) (RawJSON, error) {
		time.Sleep(5 * time.Millisecond)
		return RawJSON(`{"ok":true}`), nil
	})
	e.Register("long", func(ctx context.Context, req *Request) (RawJSON, error) {
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
		}
		return RawJSON(`{"ok":true}`), nil
	})
}

type benchGroup struct {
	job      string
	executor string
	payload  int // bytes of params
	count    int
}

func percentiles(ds []time.Duration) (p50, p95, p99 time.Duration) {
	if len(ds) == 0 {
		return
	}
	slices.Sort(ds)
	at := func(p float64) time.Duration { return ds[min(len(ds)-1, int(math.Ceil(p*float64(len(ds))))-1)] }
	return at(0.50), at(0.95), at(0.99)
}

func TestPerformanceBaseline(t *testing.T) {
	if os.Getenv("SKEIN_BENCH") == "" {
		t.Skip("set SKEIN_BENCH=1 to run the M5 baseline")
	}
	pool, schema := freshSchema(t)
	ctx := t.Context()
	jr, wf := qualified(schema, "job_run"), qualified(schema, "workflow_run")
	_ = wf

	// 100k existing terminal rows with ~1 KB params, finished over the last 20 days
	execSQL(t, pool, `INSERT INTO `+jr+` (job_name, executor_type, params, timeout, retry_policy, state, run_at, started_at, finished_at, created_at)
		SELECT 'seed', 'short', jsonb_build_object('blob', repeat(md5(g::text), 32)), 60, '{"max_attempts":3}',
		       CASE WHEN g % 2 = 0 THEN 'succeeded' ELSE 'failed' END,
		       now() - make_interval(secs => random() * 20 * 86400), now() - make_interval(secs => random() * 20 * 86400),
		       now() - make_interval(secs => random() * 20 * 86400), now() - make_interval(secs => random() * 20 * 86400)
		  FROM generate_series(1, 100000) g;
		ANALYZE `+jr+`;`)

	sub := startEngine(t, pool, Config{Schema: schema, DisableWorker: true, DisableScheduler: true,
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))}, nil)
	groups := []benchGroup{
		{"short_1k", "short", 1 << 10, 4000},
		{"short_256k", "short", 250 << 10, 400},
		{"long_1k", "long", 1 << 10, 240},
	}
	for _, g := range groups {
		declare(t, sub, JobSpec{Name: g.job, ExecutorType: g.executor})
	}
	declare(t, sub, JobSpec{Name: "probe", ExecutorType: "short"})

	const workers = 3
	env := []string{"SKEIN_HELPER_MODE=bench"}
	for range workers {
		startHelper(t, schema, env...)
	}
	waitRun(t, sub, trigger(t, sub, "probe", ""), StateSucceeded) // all three reported ready; the probe checks the claim path end to end

	sizeOf := func(rel string) int64 {
		var n int64
		if err := pool.QueryRow(ctx, "SELECT pg_relation_size($1)", schema+"."+rel).Scan(&n); err != nil {
			t.Fatalf("size of %s: %v", rel, err)
		}
		return n
	}
	tableStats := func() (upd, hot, dead int64) {
		if err := pool.QueryRow(ctx, "SELECT n_tup_upd, n_tup_hot_upd, n_dead_tup FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'job_run'", schema).Scan(&upd, &hot, &dead); err != nil {
			t.Fatalf("table stats: %v", err)
		}
		return
	}
	// settledStats waits until the counters stop moving: a worker backend that went idle
	// with statistics pending flushes them only when its idle timeout fires (10 s), so a
	// snapshot taken right after the last run misses part of the updates. Wait past that
	// timeout first, then for three quiet seconds.
	settledStats := func() (upd, hot, dead int64) {
		time.Sleep(11 * time.Second)
		upd, hot, dead = tableStats()
		for stable, i := 0, 0; stable < 3 && i < 40; i++ {
			time.Sleep(time.Second)
			u, h, d := tableStats()
			if u == upd && h == hot {
				stable++
			} else {
				stable = 0
			}
			upd, hot, dead = u, h, d
		}
		return
	}
	activeMs := func() float64 {
		var ms float64
		if err := pool.QueryRow(ctx, "SELECT active_time FROM pg_stat_database WHERE datname = current_database()").Scan(&ms); err != nil {
			t.Fatalf("active time: %v", err)
		}
		return ms
	}
	claimIdxBefore, runningIdxBefore := sizeOf("idx_job_run_claim"), sizeOf("idx_job_run_running")
	updBefore, hotBefore, _ := settledStats()
	activeBefore := activeMs()

	// lock-wait sampler
	sampleCtx, stopSampling := context.WithCancel(ctx)
	var lockSamples []int
	var sampleMu sync.Mutex
	go func() {
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			var n int
			if err := pool.QueryRow(sampleCtx, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'").Scan(&n); err != nil {
				if sampleCtx.Err() == nil {
					t.Error(err)
				}
				return
			}
			sampleMu.Lock()
			lockSamples = append(lockSamples, n)
			sampleMu.Unlock()
		}
	}()

	// submit phase: 8 goroutines per group, 24 in all; trigger latency per call
	var mu sync.Mutex
	triggerLat := map[string][]time.Duration{}
	total := 0
	for _, g := range groups {
		total += g.count
	}
	start := time.Now()
	var wg sync.WaitGroup
	for _, g := range groups {
		payload := RawJSON(`{"blob":"` + strings.Repeat("x", g.payload) + `"}`)
		per := g.count / 8
		for w := range 8 {
			n := per
			if w == 7 {
				n = g.count - per*7
			}
			wg.Go(func() {
				var lats []time.Duration
				for range n {
					t0 := time.Now()
					if _, err := sub.Jobs().Trigger(ctx, g.job, payload); err != nil {
						t.Errorf("trigger %s: %v", g.job, err)
					}
					lats = append(lats, time.Since(t0))
				}
				mu.Lock()
				triggerLat[g.job] = append(triggerLat[g.job], lats...)
				mu.Unlock()
			})
		}
	}
	wg.Wait()
	submitted := time.Since(start)

	// drain phase
	var claimPeak int64
	for {
		var left int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+jr+" WHERE state IN ('pending','running') AND job_name <> 'seed'").Scan(&left); err != nil {
			t.Fatal(err)
		}
		claimPeak = max(claimPeak, sizeOf("idx_job_run_claim"))
		if left == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	wall := time.Since(start)
	stopSampling()
	activeAfter := activeMs()
	updAfter, hotAfter, deadAfter := settledStats()

	// per-group queue and execution latency from the rows themselves; attempt + 1 is
	// the number of starts, each one claim and one settle
	type row struct {
		job          string
		queued, exec time.Duration
		failed       bool
		attempt      int
	}
	rows, err := pool.Query(ctx, `SELECT job_name, extract(epoch FROM started_at - run_at), extract(epoch FROM finished_at - started_at), state <> 'succeeded', attempt
		FROM `+jr+` WHERE job_name = ANY($1)`, []string{"short_1k", "short_256k", "long_1k"})
	if err != nil {
		t.Fatal(err)
	}
	queued, execd := map[string][]time.Duration{}, map[string][]time.Duration{}
	failed, starts := 0, int64(0)
	for rows.Next() {
		var r row
		var q, x float64
		if err := rows.Scan(&r.job, &q, &x, &r.failed, &r.attempt); err != nil {
			t.Fatal(err)
		}
		queued[r.job] = append(queued[r.job], time.Duration(q*float64(time.Second)))
		execd[r.job] = append(execd[r.job], time.Duration(x*float64(time.Second)))
		starts += int64(r.attempt) + 1
		if r.failed {
			failed++
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	// what the claim index looks like once vacuum has run (pending rows are gone)
	execSQL(t, pool, "VACUUM "+jr)
	claimIdxAfter := sizeOf("idx_job_run_claim")
	_, _, deadAfterVacuum := tableStats()

	sampleMu.Lock()
	maxLock, sumLock := 0, 0
	for _, n := range lockSamples {
		maxLock, sumLock = max(maxLock, n), sumLock+n
	}
	nSamples := len(lockSamples)
	sampleMu.Unlock()

	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }
	w("## Baseline %s", time.Now().Format("2006-01-02 15:04"))
	w("")
	w("Workers %d processes × Concurrency %d, PollInterval %s, HeartbeatInterval 1s, LeaseTTL 4s; 100k seeded rows; PostgreSQL %s.",
		workers, benchWorkerConfig(schema).Concurrency, benchWorkerConfig(schema).PollInterval, pgVersion(t, pool))
	w("")
	w("| group | runs | payload | trigger p50 / p95 / p99 | queue p50 / p95 / p99 | exec p50 / p95 / p99 |")
	w("|---|---|---|---|---|---|")
	for _, g := range groups {
		t50, t95, t99 := percentiles(triggerLat[g.job])
		q50, q95, q99 := percentiles(queued[g.job])
		x50, x95, x99 := percentiles(execd[g.job])
		w("| %s | %d | %d KB | %s / %s / %s | %s / %s / %s | %s / %s / %s |", g.job, g.count, g.payload>>10,
			t50.Round(time.Millisecond), t95.Round(time.Millisecond), t99.Round(time.Millisecond),
			q50.Round(time.Millisecond), q95.Round(time.Millisecond), q99.Round(time.Millisecond),
			x50.Round(time.Millisecond), x95.Round(time.Millisecond), x99.Round(time.Millisecond))
	}
	w("")
	w("| measure | value |")
	w("|---|---|")
	w("| submitted | %d runs in %s (%.0f triggers/s) |", total, submitted.Round(time.Millisecond), float64(total)/submitted.Seconds())
	w("| completed | %d runs in %s (%.0f runs/s end to end), %d failed |", total, wall.Round(time.Millisecond), float64(total)/wall.Seconds(), failed)
	// claims and settles change indexed columns and are never HOT, so every HOT update
	// is a heartbeat and the heartbeats are what is left after claims and settles
	upd, hot := updAfter-updBefore, hotAfter-hotBefore
	heartbeats := upd - 2*starts
	w("| job_run updates | %d total = %d claims + %d settles + %d heartbeats; %d HOT = %.0f%% of heartbeats |",
		upd, starts, starts, heartbeats, hot, 100*float64(hot)/float64(max(heartbeats, 1)))
	w("| idx_job_run_claim | %d KB before, %d KB peak, %d KB after VACUUM; dead tuples %d before / %d after VACUUM |",
		claimIdxBefore>>10, claimPeak>>10, claimIdxAfter>>10, deadAfter, deadAfterVacuum)
	w("| idx_job_run_running | %d KB before, %d KB after |", runningIdxBefore>>10, sizeOf("idx_job_run_running")>>10)
	w("| DB active time | %.2f s per wall second (pg_stat_database.active_time) |", (activeAfter-activeBefore)/1000/wall.Seconds())
	w("| lock waits | max %d, mean %.2f backends waiting on a lock over %d samples every 100 ms |", maxLock, float64(sumLock)/float64(max(nSamples, 1)), nSamples)
	report := b.String()
	fmt.Println(report)
	if out := os.Getenv("SKEIN_BENCH_OUT"); out != "" {
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		fmt.Fprintln(f, report)
	}
}

func pgVersion(t *testing.T, pool *pgxpool.Pool) string {
	var v string
	_ = pool.QueryRow(t.Context(), "SHOW server_version").Scan(&v)
	return v
}
