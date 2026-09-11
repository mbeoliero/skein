package skein

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type scenarioHarness struct {
	t                   *testing.T
	ctx                 context.Context
	pool                *pgxpool.Pool
	observer            *pgxpool.Pool
	stopObs             context.CancelFunc
	obs                 sync.WaitGroup
	obsOps              atomic.Int64
	e                   *Engine
	schema              string
	dir                 string
	manifest            *os.File
	wants               map[int64]scenarioWant
	stops               []func()
	latencies           map[string][]time.Duration
	started             time.Time
	loadStart           string
	rolledBack          []int64
	workflowWants       map[int64]WorkflowState
	keptStarts          map[int64]time.Time
	actualStates        map[RunState]int
	workerRows          []scenarioWorkerRow
	callCount           int
	effectCount         int
	missingReturns      int
	missingObservations int
	verified            bool
	mixedDuration       time.Duration
}

func newScenarioHarness(t *testing.T) *scenarioHarness {
	t.Helper()
	deadline := time.Now().Add(14 * time.Minute)
	if end, ok := t.Deadline(); ok && end.Add(-time.Minute).Before(deadline) {
		deadline = end.Add(-time.Minute)
	}
	if !deadline.After(time.Now()) {
		t.Fatal("scenario needs at least one minute reserved for cleanup")
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	t.Cleanup(cancel)
	pool, schema := freshSchema(t)
	h := &scenarioHarness{t: t, ctx: ctx, pool: pool, schema: schema, dir: t.ArtifactDir(), loadStart: scenarioCommand(ctx, "uptime"),
		wants: map[int64]scenarioWant{}, latencies: map[string][]time.Duration{}, started: time.Now(),
		workflowWants: map[int64]WorkflowState{}, keptStarts: map[int64]time.Time{}}
	var err error
	h.manifest, err = os.Create(filepath.Join(h.dir, "manifest.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := sync.OnceFunc(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		h.stopWorkers()
		if h.stopObs != nil {
			h.stopObs()
			h.obs.Wait()
		}
		if err := h.manifest.Close(); err != nil {
			t.Error(err)
		}
		h.finish(cleanupCtx)
		if h.observer != nil {
			h.observer.Close()
		}
		if h.e != nil {
			ctx, cancel := context.WithTimeout(cleanupCtx, 5*time.Second)
			if err := h.e.Shutdown(ctx); err != nil {
				t.Error(err)
			}
			cancel()
		}
	})
	t.Cleanup(cleanup)
	inv, effect := qualified(schema, "scenario_invocation"), qualified(schema, "scenario_effect")
	_, err = pool.Exec(ctx, `CREATE TABLE `+inv+` (
		id uuid PRIMARY KEY, run_id bigint NOT NULL, attempt integer NOT NULL, owner text NOT NULL,
		token uuid NOT NULL, params_hash text NOT NULL, digest text NOT NULL, key text NOT NULL,
		run_at timestamptz NOT NULL, started_at timestamptz NOT NULL, scheduled_at timestamptz, lease_start timestamptz NOT NULL, lease_peak timestamptz NOT NULL, entered_at timestamptz NOT NULL,
		returned_at timestamptz, observed_at timestamptz, exec_ns bigint NOT NULL DEFAULT 0, error text NOT NULL DEFAULT '',
		snooze_ns bigint NOT NULL DEFAULT 0, next_at timestamptz, pending_clean boolean NOT NULL DEFAULT false
	);
	CREATE INDEX ON `+inv+` (run_id);
	CREATE TABLE `+effect+` (key text PRIMARY KEY, run_id bigint NOT NULL, digest text NOT NULL, invocation uuid NOT NULL REFERENCES `+inv+`(id));
	CREATE TABLE `+qualified(schema, "scenario_control")+` (key text PRIMARY KEY, enabled boolean NOT NULL);
	CREATE TABLE `+qualified(schema, "scenario_worker")+` (owner text PRIMARY KEY, pid integer NOT NULL, stopped_at timestamptz, stats jsonb NOT NULL DEFAULT '{}');`)
	if err != nil {
		t.Fatal(err)
	}
	var maxConnections int
	if err := pool.QueryRow(ctx, "SELECT current_setting('max_connections')::int").Scan(&maxConnections); err != nil {
		t.Fatal(err)
	}
	if budget := 3*(8+1+2) + int(pool.Config().MaxConns) + 2; budget > maxConnections-5 {
		t.Fatalf("scenario connection budget %d leaves fewer than five server connections free (%d total)", budget, maxConnections)
	}
	cfg := scenarioConfig(schema)
	cfg.DisableWorker, cfg.DisableScheduler = true, true
	h.e, err = New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	h.observer, err = scenarioPool(ctx, "skein_scenario_observer", 2)
	if err != nil {
		t.Fatal(err)
	}
	obsCtx, stopObs := context.WithCancel(ctx)
	h.stopObs = stopObs
	h.obs.Go(func() {
		for obsCtx.Err() == nil {
			callCtx, cancel := context.WithTimeout(obsCtx, time.Second)
			err := h.observe(callCtx)
			cancel()
			if err != nil && obsCtx.Err() == nil {
				t.Errorf("scenario observer: %v", err)
				return
			}
			if scenarioSleep(obsCtx, 20*time.Millisecond) != nil {
				return
			}
		}
	})
	for range 3 {
		cmd := startHelperContext(t, ctx, schema, "SKEIN_HELPER_MODE=scenario", "SKEIN_SCENARIO_ARTIFACT_DIR="+h.dir)
		stop := sync.OnceFunc(func() { stopScenarioWorker(t, cmd) })
		h.stops = append(h.stops, stop)
		// Run the shared, concurrent cleanup before helper fallbacks, including a later startup failure.
		t.Cleanup(cleanup)
	}
	t.Logf("scenario artifacts: %s", h.dir)
	return h
}

func stopScenarioWorker(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Errorf("stop scenario worker: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("scenario worker exited: %v", err)
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scenario worker could not be reaped")
		}
		t.Error("scenario worker did not stop within 15s")
	}
}

func (h *scenarioHarness) stopWorkers() {
	var wg sync.WaitGroup
	for _, stop := range h.stops {
		wg.Go(stop)
	}
	wg.Wait()
}

func (h *scenarioHarness) record[T any](v T) {
	h.t.Helper()
	if _, err := h.manifest.Write(append(scenarioJSON(v), '\n')); err != nil {
		h.t.Fatal(err)
	}
}

func (h *scenarioHarness) wait(what string, ready func(context.Context) (bool, error)) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Minute)
	defer cancel()
	for {
		if h.t.Failed() {
			h.t.Fatalf("%s: stopped after an earlier scenario failure", what)
		}
		ok, err := ready(ctx)
		if err != nil {
			h.t.Fatalf("%s: %v", what, err)
		}
		if ok {
			return
		}
		if err := scenarioSleep(ctx, 20*time.Millisecond); err != nil {
			h.t.Fatalf("%s: stage or scenario budget exhausted: %v", what, err)
		}
	}
}

func (h *scenarioHarness) observe(ctx context.Context) error {
	h.obsOps.Add(2)
	inv, runs := qualified(h.schema, "scenario_invocation"), qualified(h.schema, "job_run")
	_, err := h.observer.Exec(ctx, `UPDATE `+inv+` a SET lease_peak=r.lease_expires_at FROM `+runs+` r
		WHERE a.run_id=r.id AND a.token=r.lease_token AND r.lease_expires_at>a.lease_peak;
		UPDATE `+inv+` a SET observed_at=clock_timestamp(),
			next_at=CASE WHEN a.snooze_ns>0 THEN r.run_at END,
			pending_clean=a.snooze_ns>0 AND r.state='pending' AND r.attempt=a.attempt-1 AND r.errors='[]'::jsonb
				AND r.output IS NULL AND r.finished_at IS NULL AND r.lease_token IS NULL AND NOT (r.extra ? 'lease_owner') AND r.lease_expires_at IS NULL
				AND (r.workflow_run_id IS NULL OR EXISTS (
					SELECT 1 FROM `+qualified(h.schema, "workflow_run")+` w WHERE w.id=r.workflow_run_id AND w.state='running'
					AND NOT EXISTS (SELECT 1 FROM `+runs+` child WHERE child.workflow_run_id=w.id
						AND (w.dag->child.job_name) ? r.job_name AND child.state<>'blocked')))
		FROM `+runs+` r
		WHERE a.run_id=r.id AND a.returned_at IS NOT NULL AND a.observed_at IS NULL AND r.lease_token IS DISTINCT FROM a.token
			AND (a.snooze_ns=0 OR (r.state='pending' AND r.started_at=a.started_at))`)
	return err
}

func (h *scenarioHarness) drain(ids []int64) {
	h.t.Helper()
	h.wait("drain", func(ctx context.Context) (bool, error) {
		var left int
		err := h.pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+qualified(h.schema, "job_run")+` WHERE id=ANY($1::bigint[]) AND state IN ('blocked','pending','running')) +
			(SELECT count(*) FROM `+qualified(h.schema, "scenario_invocation")+` WHERE run_id=ANY($1::bigint[]) AND observed_at IS NULL)`, ids).Scan(&left)
		return left == 0, err
	})
}

func (h *scenarioHarness) now() time.Time {
	h.t.Helper()
	var now time.Time
	if err := h.pool.QueryRow(h.ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		h.t.Fatal(err)
	}
	return now
}

func (h *scenarioHarness) declare(name string, p scenarioParams) {
	h.t.Helper()
	timeout := time.Minute
	retry := RetryPolicy{MaxAttempts: 3}
	if p.Kind == "timeout" {
		timeout, retry.MaxAttempts = time.Second, 2
	} else if p.Kind == "snooze" {
		retry.MaxAttempts = 1
	}
	if err := h.e.Jobs().Declare(h.ctx, JobSpec{Name: name, ExecutorType: "scenario", Params: scenarioJSON(p), Timeout: timeout, Retry: retry}); err != nil {
		h.t.Fatal(err)
	}
}

func scenarioExpected(id int64, phase string, p scenarioParams) scenarioWant {
	w := scenarioWant{Id: id, Case: p.Case, Phase: phase, Kind: p.Kind, State: StateSucceeded,
		Digest: scenarioDigest(p, "", nil), ParamsHash: scenarioHash(scenarioJSON(p)), Key: fmt.Sprintf("run:%d", id),
		PayloadBytes: len(scenarioJSON(p)), Invocations: []int{1}, Precision: strings.HasPrefix(phase, "precision_"), CheckRenewal: p.Kind == "long" && p.WorkMs > 2000}
	switch p.Kind {
	case "snooze":
		w.Invocations = slices.Repeat([]int{1}, p.Snoozes+1)
		w.SnoozeNs, w.Precision = int64(time.Duration(p.SnoozeMs)*time.Millisecond), true
	case "retry":
		w.Attempt, w.Invocations, w.Errors = 2, []int{1, 2, 3}, []string{"business", "business"}
	case "permanent":
		w.State, w.Attempt, w.Errors = StateFailed, 1, []string{"business"}
	case "timeout":
		w.State, w.Attempt, w.Invocations, w.Errors = StateFailed, 2, []int{1, 2}, []string{"timeout", "timeout"}
	case "cancel":
		w.State = StateCancelled
	}
	return w
}

func (h *scenarioHarness) trigger(name, phase string, p scenarioParams, at *time.Time, opts ...TriggerOption) int64 {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	if at != nil {
		opts = append(opts, At(*at))
	}
	started := time.Now()
	id, err := h.e.Jobs().Trigger(ctx, name, scenarioJSON(p), opts...)
	elapsed := time.Since(started)
	h.record(map[string]any{"operation": "trigger", "case": p.Case, "phase": phase, "id": id, "error": fmt.Sprint(err), "trigger_ns": elapsed.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	if _, exists := h.wants[id]; exists {
		h.t.Fatalf("new submission %s unexpectedly reused id %d", p.Case, id)
	}
	w := scenarioExpected(id, phase, p)
	w.TriggerNs, w.ExpectedAt = elapsed.Nanoseconds(), at
	h.wants[id] = w
	h.record(w)
	key := fmt.Sprintf("%s/%s/%dB/trigger", phase, p.Kind, w.PayloadBytes)
	h.latencies[key] = append(h.latencies[key], elapsed)
	return id
}

func scenarioRows[T any](ctx context.Context, pool *pgxpool.Pool, table string) ([]T, error) {
	rows, err := pool.Query(ctx, "SELECT * FROM "+table)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[T])
}

type scenarioWorkerRow struct {
	Owner     string
	Pid       int
	StoppedAt *time.Time
	Stats     []byte
}

func (h *scenarioHarness) verify() {
	h.t.Helper()
	runs := map[int64]JobRun{}
	for cursor := int64(0); ; {
		page, next, err := h.e.Runs().List(h.ctx, RunFilter{Limit: 500}, cursor)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, run := range page {
			runs[run.Id] = run
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(runs) != len(h.wants) {
		h.t.Errorf("database has %d job_run rows, manifest has %d", len(runs), len(h.wants))
	}
	h.actualStates = map[RunState]int{}
	for _, run := range runs {
		h.actualStates[run.State]++
	}
	phaseCounts, mixedKinds, sizes := map[string]int{}, map[string]int{}, map[int]int{}
	for _, want := range h.wants {
		phaseCounts[want.Phase]++
		if want.Phase == "mixed" {
			mixedKinds[want.Kind]++
			sizes[want.PayloadBytes]++
		}
	}
	for phase, count := range map[string]int{"mixed": 1000, "workflow": 40, "precision_immediate": 100, "precision_tx": 100, "precision_delayed": 100, "precision_cron": 100, "precision_retry": 100, "transaction": 1, "dedup": 2, "snooze_plain": 48, "snooze_slots": 48, "snooze_workflow": 3} {
		if phaseCounts[phase] != count {
			h.t.Errorf("%s has %d runs in the manifest, want %d", phase, phaseCounts[phase], count)
		}
	}
	if !maps.Equal(mixedKinds, map[string]int{"short": 600, "long": 150, "retry": 100, "permanent": 50, "timeout": 50, "cancel": 50}) {
		h.t.Errorf("wrong mixed workload distribution: %v", mixedKinds)
	}
	if !maps.Equal(sizes, map[int]int{1024: 900, 32 << 10: 90, 250 << 10: 10}) {
		h.t.Errorf("wrong encoded payload distribution: %v", sizes)
	}
	if len(h.workflowWants) != 11 || len(h.rolledBack) != 1 {
		h.t.Errorf("manifest has %d workflows and %d rollbacks, want 11 and 1", len(h.workflowWants), len(h.rolledBack))
	}
	calls, err := scenarioRows[scenarioInvocation](h.ctx, h.pool, qualified(h.schema, "scenario_invocation"))
	if err != nil {
		h.t.Fatal(err)
	}
	effects, err := scenarioRows[scenarioEffect](h.ctx, h.pool, qualified(h.schema, "scenario_effect"))
	if err != nil {
		h.t.Fatal(err)
	}
	workers, err := scenarioRows[scenarioWorkerRow](h.ctx, h.pool, qualified(h.schema, "scenario_worker"))
	if err != nil {
		h.t.Fatal(err)
	}
	h.workerRows, h.callCount, h.effectCount = workers, len(calls), len(effects)
	byRun, byOwner := map[int64][]scenarioInvocation{}, map[string]int{}
	for _, c := range calls {
		if c.ReturnedAt == nil {
			h.missingReturns++
		}
		if c.ObservedAt == nil {
			h.missingObservations++
		}
		byRun[c.RunId] = append(byRun[c.RunId], c)
		byOwner[c.Owner]++
		if _, ok := h.wants[c.RunId]; !ok {
			h.t.Errorf("invocation for unrecorded run %d", c.RunId)
		}
	}
	byEffect := map[int64][]scenarioEffect{}
	for _, effect := range effects {
		byEffect[effect.RunId] = append(byEffect[effect.RunId], effect)
		if _, ok := h.wants[effect.RunId]; !ok {
			h.t.Errorf("effect for unrecorded run %d", effect.RunId)
		}
	}
	for id, want := range h.wants {
		slices.SortFunc(byRun[id], func(a, b scenarioInvocation) int { return a.EnteredAt.Compare(b.EnteredAt) })
		run, ok := runs[id]
		if !ok {
			h.t.Errorf("missing run %d", id)
			continue
		}
		if err := checkScenarioRun(want, &run, byRun[id], byEffect[id]); err != nil {
			h.t.Error(err)
		}
		prefix := fmt.Sprintf("%s/%s/%dB", want.Phase, want.Kind, want.PayloadBytes)
		for i, c := range byRun[id] {
			if i > 0 && want.SnoozeNs > 0 {
				h.latencies[prefix+"/snooze_reentry"] = append(h.latencies[prefix+"/snooze_reentry"], c.EnteredAt.Sub(c.RunAt))
			}
			h.latencies[prefix+"/entry"] = append(h.latencies[prefix+"/entry"], c.EnteredAt.Sub(scenarioDue(want, c, i == 0)))
			h.latencies[prefix+"/exec"] = append(h.latencies[prefix+"/exec"], time.Duration(c.ExecNs))
			if c.ReturnedAt != nil && c.ObservedAt != nil {
				h.latencies[prefix+"/settle_confirm"] = append(h.latencies[prefix+"/settle_confirm"], c.ObservedAt.Sub(*c.ReturnedAt))
			}
			for _, dep := range want.Deps {
				parent := runs[dep]
				if parent.State != StateSucceeded || parent.FinishedAt == nil || c.EnteredAt.Before(*parent.FinishedAt) {
					h.t.Errorf("run %d entered before dependency %d succeeded", id, dep)
				}
			}
		}
	}
	if len(workers) != 3 {
		h.t.Errorf("recorded %d workers, want 3", len(workers))
	}
	for _, worker := range workers {
		var stats map[string]int64
		if err := json.Unmarshal(worker.Stats, &stats); err != nil {
			h.t.Fatal(err)
		}
		if worker.StoppedAt == nil || stats["unexpected"] != 0 || stats["active"] != 0 || stats["peak"] > 16 || stats["starts"] != int64(byOwner[worker.Owner]) {
			h.t.Errorf("worker %s: stopped=%v stats=%v audit_calls=%d", worker.Owner, worker.StoppedAt, stats, byOwner[worker.Owner])
		}
		delete(byOwner, worker.Owner)
	}
	if len(byOwner) > 0 {
		h.t.Errorf("unregistered audit owners: %v", byOwner)
	}
	for _, id := range h.rolledBack {
		if _, ok := runs[id]; ok {
			h.t.Errorf("rolled-back run %d exists", id)
		}
	}
	for id, state := range h.workflowWants {
		run, err := h.e.Workflows().GetRun(h.ctx, id)
		if err != nil || run.State != state {
			h.t.Errorf("workflow %d: want %s, got %+v, err=%v", id, state, run, err)
		}
	}
	for id, at := range h.keptStarts {
		run := runs[id]
		if run.StartedAt == nil || !run.StartedAt.Equal(at) {
			h.t.Errorf("Resume changed successful node %d started_at", id)
		}
	}
	h.verified = true
}
