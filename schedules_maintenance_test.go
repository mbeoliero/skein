package skein

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
)

// Schedules, retention, Stats and listing acceptance checks.

type metricsRec struct {
	mu     sync.Mutex
	counts map[string]int
}

func (m *metricsRec) key(name string, labels []string) string {
	for i := 0; i+1 < len(labels); i += 2 {
		name += "{" + labels[i] + "=" + labels[i+1] + "}"
	}
	return name
}

func (m *metricsRec) Count(name string, n int, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]int{}
	}
	m.counts[m.key(name, labels)] += n
}
func (m *metricsRec) Gauge(string, float64, ...string) {}

// Observe counts observations per label set; tests check labels, not values.
func (m *metricsRec) Observe(name string, _ float64, labels ...string) { m.Count(name, 1, labels...) }

func (m *metricsRec) get(name string, labels ...string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[m.key(name, labels)]
}

// yearly cannot fire on its own during a test, so every beat comes from dueAt: a
// per-minute cron would add a genuine beat whenever the wall clock crosses :00.
const yearly = "0 0 1 1 *"

func nextNewYear() time.Time {
	return time.Date(time.Now().UTC().Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
}

// dueAt moves a schedule's next fire time, the fixture for "the clock reached it".
func dueAt(t *testing.T, pool *pgxpool.Pool, schema, name string, at time.Time) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), "UPDATE "+qualified(schema, "schedule")+" SET next_run_at = $1 WHERE name = $2", at, name); err != nil {
		t.Fatal(err)
	}
}

// maintain runs one §2.8 pass, retrying while another test's pass holds the advisory
// lock, which is one per database, not per schema.
func maintain(t *testing.T, e *Engine) {
	t.Helper()
	waitFor(t, "the maintenance lock", func() bool { return !e.maintainOnce(t.Context()) })
}

func scheduledRuns(t *testing.T, e *Engine, schedule string) []JobRun {
	t.Helper()
	page, _, err := e.Runs().List(t.Context(), RunFilter{Limit: 500}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []JobRun
	for _, r := range page {
		if r.ScheduleName == schedule {
			out = append(out, r)
		}
	}
	return out
}

func TestNextRunDST(t *testing.T) {
	t.Parallel()
	ny, _ := time.LoadLocation("America/New_York")
	// 2026-03-08: 02:00 EST jumps to 03:00 EDT, so 02:30 does not exist that day
	next, err := nextRun("30 2 * * *", "America/New_York", time.Date(2026, 3, 8, 0, 0, 0, 0, ny))
	if err != nil || !next.Equal(time.Date(2026, 3, 9, 2, 30, 0, 0, ny)) {
		t.Errorf("spring forward: %v %v", next, err)
	}
	// 2026-11-01: 01:00-02:00 EDT is followed by 01:00-02:00 EST; 01:30 happens twice
	first, _ := nextRun("30 1 * * *", "America/New_York", time.Date(2026, 11, 1, 0, 0, 0, 0, ny))
	second, _ := nextRun("30 1 * * *", "America/New_York", first)
	if !first.Equal(time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)) || !second.Equal(time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC)) {
		t.Errorf("fall back: first %v second %v", first.UTC(), second.UTC())
	}
	// strictly after: a due time on the boundary moves to the next beat
	after, _ := nextRun("* * * * *", "UTC", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if !after.Equal(time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)) {
		t.Errorf("strictly after: %v", after)
	}
}

func TestSchedulePutValidationAndIdempotence(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	bad := []ScheduleSpec{
		{Name: "s", Job: "j", Cron: "not a cron"},
		{Name: "s", Job: "j", Cron: "0 0 31 2 *"}, // parses, never fires
		{Name: "s", Job: "j", Cron: "* * * * *", Timezone: "Mars/Olympus"},
		{Name: "s", Cron: "* * * * *"},
		{Name: "s", Job: "j", Workflow: "w", Cron: "* * * * *"},
		{Name: "s", Job: "j", Cron: "* * * * *", Overlap: "maybe"},
		{Name: "s", Job: "j", Cron: "TZ=UTC"},                                           // robfig panics on the prefix without a space
		{Name: "s", Job: "j", Cron: "CRON_TZ=UTC 0 9 * * *", Timezone: "Asia/Shanghai"}, // the prefix would silently win over Timezone
		{Name: "s", Job: "j", Cron: "@every 1h"},                                        // relative to the scan, not a grid
		{Name: "s", Job: "j", Cron: "* * * * *", Timezone: "Local"},                     // whichever host scans
	}
	for _, spec := range bad {
		if err := e.Schedules().Put(t.Context(), spec); err == nil {
			t.Errorf("accepted %+v", spec)
		}
	}
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Job: "nope", Cron: "* * * * *"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown job: %v", err)
	}

	st := store.Open(pool, schema)
	spec := ScheduleSpec{Name: "s", Job: "j", Cron: "0 3 * * *", Timezone: "Asia/Shanghai"}
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	row, _ := st.GetSchedule(t.Context(), "s")
	if !row.NextRunAt.After(time.Now()) || row.Overlap != "skip" || !row.Enabled {
		t.Fatalf("%+v", row)
	}
	first := row.NextRunAt
	// a rolling deploy re-Puts the same rule: next_run_at must not move
	dueAt(t, pool, schema, "s", first.Add(-time.Hour))
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if row, _ = st.GetSchedule(t.Context(), "s"); !row.NextRunAt.Equal(first.Add(-time.Hour)) {
		t.Fatalf("same config moved next_run_at to %v", row.NextRunAt)
	}
	// a changed rule recomputes
	spec.Cron = "0 4 * * *"
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if row, _ = st.GetSchedule(t.Context(), "s"); !row.NextRunAt.After(time.Now()) || row.Cron != "0 4 * * *" {
		t.Fatalf("changed cron: %+v", row)
	}
	// disabled keeps its stale time, enabling recomputes it
	spec.Disabled = true
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	dueAt(t, pool, schema, "s", first.Add(-time.Hour))
	spec.Disabled = false
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if row, _ = st.GetSchedule(t.Context(), "s"); !row.NextRunAt.After(time.Now()) || !row.Enabled {
		t.Fatalf("re-enabled: %+v", row)
	}
	if err := e.Jobs().Delete(t.Context(), "j"); !errors.Is(err, ErrReferenced) {
		t.Errorf("delete scheduled job: %v", err)
	}
	if err := e.Schedules().Delete(t.Context(), "s"); err != nil {
		t.Fatal(err)
	}
	if err := e.Schedules().Delete(t.Context(), "s"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete: %v", err)
	}
}

// A retention batch waiting on a row lock does not hold Shutdown past its ctx: the
// batch is cancelled with the loop, its dedicated connection closed, and once the
// lock holder ends the session lock is free for the next pass.
func TestShutdownInterruptsMaintenance(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := submitOnly(schema)
	cfg.MaintenanceInterval = 50 * time.Millisecond
	cfg.RetentionSucceeded, cfg.RetentionFailed = time.Second, time.Second
	e := startEngine(t, namedPool(t, "maint-shutdown"), cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	jr := qualified(schema, "job_run")
	execSql(t, pool, "UPDATE "+jr+" SET state = 'succeeded', finished_at = now() - interval '1 hour' WHERE id = "+itoa(id))
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	execSql(t, tx, "SELECT id FROM "+jr+" WHERE id = "+itoa(id)+" FOR UPDATE")
	waitBlocked(t, pool, "maint-shutdown")

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := e.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Shutdown waited %s for the blocked retention batch", took)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	// the orphaned backend gets the row lock, fails to reply and exits, releasing the
	// session lock; maintain retries until then and the next pass deletes the row
	maintain(t, startEngine(t, pool, cfg, nil))
	if _, err := e.Runs().Get(t.Context(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old run after retention: %v", err)
	}
}

// Two instances scan the same due schedule: one run per beat, scheduled_at is the beat.
func TestTwoInstancesFireOnce(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	ok := func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil }
	a := startEngine(t, pool, fastConfig(schema), func(e *Engine) { e.Register("x", ok) })
	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) { e.Register("x", ok) })
	declare(t, a, JobSpec{Name: "j", ExecutorType: "x"})
	if err := a.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Job: "j", Cron: yearly}); err != nil {
		t.Fatal(err)
	}
	st := store.Open(pool, schema)
	for beat := 1; beat <= 5; beat++ {
		due := time.Now().Add(-time.Duration(beat) * time.Hour).Truncate(time.Second)
		dueAt(t, pool, schema, "s", due)
		waitFor(t, "beat to fire", func() bool { return len(scheduledRuns(t, a, "s")) >= beat })
		for _, engine := range []*Engine{a, b} {
			if _, err := engine.st.ScanDue(t.Context(), nextRun); err != nil {
				t.Fatal(err)
			}
		}
		runs := scheduledRuns(t, a, "s")
		if len(runs) != beat {
			t.Fatalf("beat %d: %d runs", beat, len(runs))
		}
		if got := runs[0].ScheduledAt; got == nil || !got.Equal(due) {
			t.Fatalf("beat %d scheduled_at %v, want %v", beat, got, due)
		}
		row, _ := st.GetSchedule(t.Context(), "s")
		if !row.NextRunAt.Equal(nextNewYear()) {
			t.Fatalf("beat %d next_run_at %v, want %v", beat, row.NextRunAt, nextNewYear())
		}
		waitRun(t, a, runs[0].Id, StateSucceeded)
	}
}

// Down for three periods: one catch-up beat, then back on the rule.
func TestCatchUpOneBeat(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Job: "j", Cron: yearly, Overlap: OverlapAllow}); err != nil {
		t.Fatal(err)
	}
	missed := time.Now().AddDate(-3, 0, 0).Truncate(time.Second) // three periods behind
	dueAt(t, pool, schema, "s", missed)
	waitFor(t, "catch-up beat", func() bool { return len(scheduledRuns(t, e, "s")) >= 1 })
	if _, err := e.st.ScanDue(t.Context(), nextRun); err != nil {
		t.Fatal(err)
	}
	runs := scheduledRuns(t, e, "s")
	if len(runs) != 1 || !runs[0].ScheduledAt.Equal(missed) {
		t.Fatalf("runs %d scheduled_at %v want %v", len(runs), runs[0].ScheduledAt, missed)
	}
	row, _ := store.Open(pool, schema).GetSchedule(t.Context(), "s")
	if !row.NextRunAt.Equal(nextNewYear()) {
		t.Fatalf("next_run_at %v, want %v", row.NextRunAt, nextNewYear())
	}
}

// overlap = skip: while the previous beat is in flight the next one is skipped and
// counted; overlap = allow runs them side by side.
func TestOverlapSkip(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	gate := make(chan struct{})
	rec := &metricsRec{}
	cfg := fastConfig(schema)
	cfg.Metrics = rec
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) { <-gate; return nil, nil })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	spec := ScheduleSpec{Name: "s", Job: "j", Cron: yearly}
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	dueAt(t, pool, schema, "s", time.Now().Add(-2*time.Hour))
	waitFor(t, "first beat", func() bool { return len(scheduledRuns(t, e, "s")) == 1 })
	dueAt(t, pool, schema, "s", time.Now().Add(-time.Hour))
	waitFor(t, "second beat skipped", func() bool { return rec.get("schedule_skipped_total", "reason", "overlap") == 1 })
	if runs := scheduledRuns(t, e, "s"); len(runs) != 1 || runs[0].DedupKey != "sched:s" {
		t.Fatalf("runs %+v", runs)
	}
	spec.Overlap = OverlapAllow
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	dueAt(t, pool, schema, "s", time.Now().Add(-30*time.Minute))
	waitFor(t, "allowed beat", func() bool { return len(scheduledRuns(t, e, "s")) == 2 })
	close(gate)
	for _, r := range scheduledRuns(t, e, "s") {
		waitRun(t, e, r.Id, StateSucceeded)
	}
}

func TestScheduledWorkflow(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("ok", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "a", ExecutorType: "ok"})
	declare(t, e, JobSpec{Name: "b", ExecutorType: "ok"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}, {Job: "b", Deps: []string{"a"}}}})
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Workflow: "w", Cron: yearly}); err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(-time.Hour).Truncate(time.Second)
	dueAt(t, pool, schema, "s", due)
	var run *WorkflowRun
	waitFor(t, "scheduled workflow", func() bool {
		page, _, err := e.Workflows().ListRuns(t.Context(), WorkflowRunFilter{WorkflowName: "w"}, 0)
		if err != nil || len(page) == 0 {
			return false
		}
		run = &page[0]
		return true
	})
	if run.ScheduleName != "s" || run.ScheduledAt == nil || !run.ScheduledAt.Equal(due) || run.DedupKey != "sched:s" {
		t.Fatalf("%+v", run)
	}
	waitWorkflow(t, e, run.Id, WorkflowSucceeded)
	if err := e.Workflows().Delete(t.Context(), "w"); !errors.Is(err, ErrReferenced) {
		t.Errorf("delete scheduled workflow: %v", err)
	}
}

// Retention deletes finished plain runs and finished workflow runs by their windows,
// and never a node of a workflow that is still in flight.
func TestRetentionKeepsNodesOfLiveWorkflows(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &metricsRec{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	cfg.Metrics = rec
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	declare(t, e, JobSpec{Name: "a", ExecutorType: "x"})
	declare(t, e, JobSpec{Name: "b", ExecutorType: "x"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}, {Job: "b", Deps: []string{"a"}}}})

	finish := func(id int64, state string, age time.Duration) {
		execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET state = '"+state+"', finished_at = now() - interval '"+age.String()+"', lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL WHERE id = "+itoa(id))
	}
	oldOK := trigger(t, e, "j", "")
	finish(oldOK, "succeeded", 8*24*time.Hour)
	recentOK := trigger(t, e, "j", "")
	finish(recentOK, "succeeded", 24*time.Hour)
	oldFailed := trigger(t, e, "j", "")
	finish(oldFailed, "failed", 8*24*time.Hour)
	ancientFailed := trigger(t, e, "j", "")
	finish(ancientFailed, "failed", 31*24*time.Hour)

	live := triggerWorkflow(t, e, "w", "")
	liveRun, _ := e.Workflows().GetRun(t.Context(), live)
	finish(nodeOf(t, liveRun, "a").Id, "succeeded", 8*24*time.Hour) // a finished long ago, b never started
	done := triggerWorkflow(t, e, "w", "")
	doneRun, _ := e.Workflows().GetRun(t.Context(), done)
	for _, n := range doneRun.Nodes {
		finish(n.Id, "succeeded", 8*24*time.Hour)
	}
	execSql(t, pool, "UPDATE "+qualified(schema, "workflow_run")+" SET state = 'succeeded', finished_at = now() - interval '8 days' WHERE id = "+itoa(done))

	maintain(t, e)

	gone := func(id int64) bool { _, err := e.Runs().Get(t.Context(), id); return errors.Is(err, ErrNotFound) }
	if !gone(oldOK) || gone(recentOK) || gone(oldFailed) || !gone(ancientFailed) {
		t.Errorf("plain runs: oldOK gone %v recentOK gone %v oldFailed gone %v ancientFailed gone %v", gone(oldOK), gone(recentOK), gone(oldFailed), gone(ancientFailed))
	}
	if gone(nodeOf(t, liveRun, "a").Id) {
		t.Errorf("node of a live workflow was deleted")
	}
	if _, err := e.Workflows().GetRun(t.Context(), done); !errors.Is(err, ErrNotFound) {
		t.Errorf("finished workflow kept: %v", err)
	}
	if gone(nodeOf(t, doneRun, "a").Id) != true {
		t.Errorf("nodes did not cascade with their workflow")
	}
	if rec.get("retention_deleted_total", "table", "job_run_succeeded") != 1 || rec.get("retention_deleted_total", "table", "job_run_failed") != 1 || rec.get("retention_deleted_total", "table", "workflow_run_succeeded") != 1 {
		t.Errorf("counts %v", rec.counts)
	}
	// a second holder finds the lock free again
	maintain(t, e)
}

// Retention races Resume on a workflow_run past the failed window: the DELETE picks the
// row from its snapshot, waits for Resume's lock, and must re-check the row it then sees
// (running again) instead of deleting it with all its nodes (§2.8, §2.9).
func TestRetentionSkipsResumedWorkflow(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	named := namedPool(t, schema)
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	e := startEngine(t, named, cfg, nil)
	declare(t, e, JobSpec{Name: "a", ExecutorType: "x"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}}})
	id := triggerWorkflow(t, e, "w", "")
	run, _ := e.Workflows().GetRun(t.Context(), id)
	jr, wr := qualified(schema, "job_run"), qualified(schema, "workflow_run")
	execSql(t, pool, "UPDATE "+jr+" SET state = 'failed', finished_at = now() - interval '31 days' WHERE id = "+itoa(nodeOf(t, run, "a").Id))
	execSql(t, pool, "UPDATE "+wr+" SET state = 'failed', finished_at = now() - interval '31 days' WHERE id = "+itoa(id))

	// Resume's first statement, held while retention runs (§2.5)
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	execSql(t, tx, "SELECT id FROM "+wr+" WHERE id = "+itoa(id)+" FOR UPDATE")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e.maintainOnce(t.Context()) { // another test's pass may hold the lock for a moment
			time.Sleep(20 * time.Millisecond)
		}
	}()
	waitBlocked(t, pool, schema)
	// the rest of Resume, then commit: the workflow is running again
	execSql(t, tx, "UPDATE "+jr+" SET state = 'pending', attempt = 0, run_at = now(), started_at = NULL, finished_at = NULL, output = NULL WHERE workflow_run_id = "+itoa(id))
	execSql(t, tx, "UPDATE "+wr+" SET state = 'running', finished_at = NULL WHERE id = "+itoa(id))
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-done

	got, err := e.Workflows().GetRun(t.Context(), id)
	if err != nil || got.State != WorkflowRunning || nodeOf(t, got, "a").State != StatePending {
		t.Fatalf("resumed workflow after retention: %v, %+v", err, got)
	}
}

// A rule that stops producing fire times (robfig looks five years ahead) is disabled
// by the scan instead of being due on every tick with a zero next_run_at (§2.1).
func TestScanDisablesScheduleWithoutNextFireTime(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &metricsRec{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	cfg.Metrics = rec
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Job: "j", Cron: yearly}); err != nil {
		t.Fatal(err)
	}
	// the rule turns bad behind Put's back and the beat is due
	execSql(t, pool, "UPDATE "+qualified(schema, "schedule")+" SET cron = '0 0 31 2 *', next_run_at = now() - interval '1 minute' WHERE name = 's'")
	e.scanOnce()
	row, err := store.Open(pool, schema).GetSchedule(t.Context(), "s")
	if err != nil || row.Enabled {
		t.Fatalf("schedule after the scan: err %v enabled %v", err, row.Enabled)
	}
	if n := len(scheduledRuns(t, e, "s")); n != 0 || rec.get("schedule_skipped_total", "reason", "no_next") != 1 {
		t.Fatalf("runs %d, counts %v", n, rec.counts)
	}
	e.scanOnce() // disabled: not picked up again
	if rec.get("schedule_skipped_total", "reason", "no_next") != 1 {
		t.Fatalf("counts %v", rec.counts)
	}
}

// A process that registered nothing must still report the backlog it cannot serve:
// a nil slice would reach the database as NULL, and NOT x = ANY(NULL) is never true.
func TestStatsWithoutExecutors(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "nobody"})
	trigger(t, e, "j", "")
	s, err := e.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.PendingDue != 1 || s.UnregisteredDue != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestStatsAndList(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := submitOnly(schema)
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "x1", ExecutorType: "x"})
	declare(t, e, JobSpec{Name: "y1", ExecutorType: "y"})
	ids := []int64{trigger(t, e, "x1", ""), trigger(t, e, "x1", ""), trigger(t, e, "y1", ""), trigger(t, e, "x1", "", At(time.Now().Add(time.Hour)))}
	ids = append(ids, trigger(t, e, "x1", ""))
	claimAsDeadHolder(t, pool, schema, "x")
	s, err := e.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.PendingDue != 3 || s.Running != 1 || s.UnregisteredDue != 1 || s.OldestPendingAge <= 0 {
		t.Fatalf("%+v", s)
	}

	var seen []int64
	var cursor int64
	pages := 0
	for {
		page, next, err := e.Runs().List(t.Context(), RunFilter{Limit: 2}, cursor)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, r := range page {
			seen = append(seen, r.Id)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if pages != 3 || len(seen) != 5 || seen[0] != ids[4] || seen[4] != ids[0] {
		t.Fatalf("pages %d seen %v ids %v", pages, seen, ids)
	}
	if page, next, _ := e.Runs().List(t.Context(), RunFilter{Limit: 5}, 0); len(page) != 5 || next != 0 {
		t.Errorf("an exactly full last page must end the listing: %d rows, next %d", len(page), next)
	}
	if page, _, _ := e.Runs().List(t.Context(), RunFilter{JobName: "y1"}, 0); len(page) != 1 || page[0].Id != ids[2] {
		t.Errorf("filter by job: %v", page)
	}
	if page, _, _ := e.Runs().List(t.Context(), RunFilter{State: StateRunning}, 0); len(page) != 1 {
		t.Errorf("filter by state: %v", page)
	}
}
