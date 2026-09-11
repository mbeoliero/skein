package skein

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func scenarioPayload(rng *rand.Rand, p scenarioParams, size int) scenarioParams {
	p.Blob = ""
	blob := make([]byte, size-len(scenarioJSON(p)))
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	for i := range blob {
		blob[i] = alphabet[rng.IntN(len(alphabet))]
	}
	p.Blob = string(blob)
	return p
}

func (h *scenarioHarness) triggerTx(phase string, p scenarioParams, hold time.Duration, rollback bool) int64 {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var opts []TriggerOption
	var due time.Time
	if strings.HasPrefix(phase, "precision_") {
		due = h.now().Add(700 * time.Millisecond)
		opts = append(opts, At(due))
	}
	started := time.Now()
	id, err := h.e.Jobs().TriggerTx(ctx, tx, "m7_short", scenarioJSON(p), opts...)
	elapsed := time.Since(started)
	h.record(map[string]any{"operation": "trigger_tx", "phase": phase, "id": id, "case": p.Case, "error": fmt.Sprint(err), "trigger_ns": elapsed.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	if hold > 0 {
		// This is simulated caller work inside the transaction, not a scheduler synchronization sleep.
		if err := scenarioSleep(ctx, hold); err != nil {
			h.t.Fatal(err)
		}
	}
	if _, err := h.e.Runs().Get(ctx, id); !errors.Is(err, ErrNotFound) {
		h.t.Fatalf("uncommitted run %d visible: %v", id, err)
	}
	var entries int
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM `+qualified(h.schema, "scenario_invocation")+` WHERE run_id=$1`, id).Scan(&entries); err != nil || entries != 0 {
		h.t.Fatalf("uncommitted run %d entered executor: entries=%d err=%v", id, entries, err)
	}
	if rollback {
		err := tx.Rollback(ctx)
		h.record(map[string]any{"operation": "rollback", "id": id, "error": fmt.Sprint(err)})
		if err != nil {
			h.t.Fatal(err)
		}
		h.rolledBack = append(h.rolledBack, id)
		return id
	}
	commit := time.Now()
	err = tx.Commit(ctx)
	commitDuration := time.Since(commit)
	h.record(map[string]any{"operation": "commit", "id": id, "error": fmt.Sprint(err), "commit_ns": commitDuration.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	w := scenarioExpected(id, phase, p)
	w.TriggerNs, w.CommitNs = elapsed.Nanoseconds(), commitDuration.Nanoseconds()
	if !due.IsZero() {
		w.ExpectedAt = new(due)
	}
	h.wants[id] = w
	h.record(w)
	prefix := fmt.Sprintf("%s/%s/%dB", phase, p.Kind, w.PayloadBytes)
	h.latencies[prefix+"/trigger"] = append(h.latencies[prefix+"/trigger"], elapsed)
	h.latencies[prefix+"/commit"] = append(h.latencies[prefix+"/commit"], commitDuration)
	if !due.IsZero() && !h.now().Before(due) {
		h.t.Fatal("TriggerTx precision precondition failed: commit was not confirmed before At")
	}
	return id
}

func (h *scenarioHarness) precision(rng *rand.Rand) {
	h.t.Helper()
	for _, mode := range []string{"immediate", "tx", "delayed", "cron", "retry"} {
		phase := "precision_" + mode
		for batch := range 10 {
			var ids []int64
			cron := map[string]scenarioParams{}
			cronDue := map[string]time.Time{}
			for n := range 10 {
				p := scenarioParams{Case: fmt.Sprintf("%s_%03d", phase, batch*10+n), Kind: "short", WorkMs: 5}
				if mode == "retry" {
					p.Kind = "retry"
				}
				p = scenarioPayload(rng, p, 1024)
				switch mode {
				case "tx":
					ids = append(ids, h.triggerTx(phase, p, 0, false))
				case "cron":
					h.declare(p.Case, p)
					if err := h.e.Schedules().Put(h.ctx, ScheduleSpec{Name: p.Case, Job: p.Case, Cron: "@yearly", Timezone: "UTC"}); err != nil {
						h.t.Fatal(err)
					}
					due := h.now().Add(700 * time.Millisecond)
					if _, err := h.pool.Exec(h.ctx, `UPDATE `+qualified(h.schema, "schedule")+` SET next_run_at=$1 WHERE name=$2`, due, p.Case); err != nil {
						h.t.Fatal(err)
					}
					h.record(map[string]any{"operation": "schedule_due", "name": p.Case, "due": due})
					cron[p.Case], cronDue[p.Case] = p, due
				case "delayed":
					ids = append(ids, h.trigger("m7_short", phase, p, new(h.now().Add(700*time.Millisecond))))
				default:
					ids = append(ids, h.trigger("m7_"+p.Kind, phase, p, nil))
				}
			}
			for _, name := range slices.Sorted(maps.Keys(cron)) {
				var id int64
				h.wait("cron beat", func(ctx context.Context) (bool, error) {
					var count int
					err := h.pool.QueryRow(ctx, `SELECT coalesce(min(id),0), count(*) FROM `+qualified(h.schema, "job_run")+` WHERE schedule_name=$1`, name).Scan(&id, &count)
					if count > 1 {
						return false, fmt.Errorf("schedule %s created %d runs for one beat", name, count)
					}
					return count == 1, err
				})
				w := scenarioExpected(id, phase, cron[name])
				w.ExpectedAt = new(cronDue[name])
				h.wants[id] = w
				h.record(w)
				ids = append(ids, id)
				if err := h.e.Schedules().Delete(h.ctx, name); err != nil {
					h.t.Fatal(err)
				}
			}
			h.drain(ids)
		}
		h.t.Logf("%s: 100 runs drained", phase)
	}
}

func (h *scenarioHarness) cancelRun(id int64) {
	h.t.Helper()
	err := h.e.Runs().Cancel(h.ctx, id)
	h.record(map[string]any{"operation": "cancel", "id": id, "error": fmt.Sprint(err)})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *scenarioHarness) dedup(rng *rand.Rand) {
	h.t.Helper()
	p := scenarioPayload(rng, scenarioParams{Case: "dedup_pending", Kind: "short", WorkMs: 5}, 1024)
	id := h.trigger("m7_short", "dedup", p, new(h.now().Add(time.Hour)), DedupKey("smoke"))
	w := h.wants[id]
	w.State, w.Invocations = StateCancelled, nil
	h.wants[id] = w
	h.record(w)
	duplicate, err := h.e.Jobs().Trigger(h.ctx, "m7_short", scenarioJSON(p), DedupKey("smoke"))
	h.record(map[string]any{"operation": "duplicate", "id": duplicate, "error": fmt.Sprint(err), "winner": id})
	if !errors.Is(err, ErrDuplicate) || duplicate != id {
		h.t.Fatalf("duplicate: id=%d err=%v, want pending winner %d with ErrDuplicate", duplicate, err, id)
	}
	h.cancelRun(id)
	duplicate, err = h.e.Jobs().Trigger(h.ctx, "m7_short", scenarioJSON(p), DedupKey("smoke"))
	h.record(map[string]any{"operation": "duplicate_after_terminal", "id": duplicate, "error": fmt.Sprint(err), "winner": id})
	if !errors.Is(err, ErrDuplicate) || duplicate != id {
		h.t.Fatalf("duplicate after terminal: id=%d err=%v, want cancelled holder %d with ErrDuplicate", duplicate, err, id)
	}
	p = scenarioPayload(rng, scenarioParams{Case: "dedup_new_key", Kind: "short", WorkMs: 5}, 1024)
	h.drain([]int64{h.trigger("m7_short", "dedup", p, nil, DedupKey("smoke:new"))})
}

func (h *scenarioHarness) mixed(rng *rand.Rand) []int64 {
	h.t.Helper()
	var runningCancels []int64
	for i := range 1000 {
		p := scenarioParams{Case: fmt.Sprintf("mixed_%04d", i), Kind: "short", WorkMs: 5 + i%16}
		switch {
		case i >= 950:
			p.Kind = "cancel"
		case i >= 900:
			p.Kind = "timeout"
		case i >= 850:
			p.Kind = "permanent"
		case i >= 750:
			p.Kind = "retry"
		case i >= 600:
			p.Kind, p.WorkMs = "long", 1000+(i%3)*1000
		}
		size := 1024
		if i%100 == 99 {
			size = 250 << 10
		} else if i%100 >= 90 {
			size = 32 << 10
		}
		p = scenarioPayload(rng, p, size)
		var at *time.Time
		pendingCancel := i >= 950 && i < 975
		if pendingCancel {
			at = new(h.now().Add(time.Hour))
		} else if p.Kind == "short" && i%10 == 0 {
			at = new(h.now().Add(time.Duration(700+(i%24)*100) * time.Millisecond))
		}
		id := h.trigger("m7_"+p.Kind, "mixed", p, at)
		if pendingCancel {
			w := h.wants[id]
			w.Invocations = nil
			h.wants[id] = w
			h.record(w)
			h.cancelRun(id)
		} else if p.Kind == "cancel" {
			runningCancels = append(runningCancels, id)
		}
	}
	return runningCancels
}

func (h *scenarioHarness) cancelEntered(ids []int64) {
	h.t.Helper()
	h.wait("cancel entered executors", func(ctx context.Context) (bool, error) {
		rows, err := h.pool.Query(ctx, `SELECT DISTINCT run_id FROM `+qualified(h.schema, "scenario_invocation")+` WHERE run_id=ANY($1::bigint[])`, ids)
		if err != nil {
			return false, err
		}
		entered, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return false, err
		}
		for _, id := range entered {
			run, err := h.e.Runs().Get(ctx, id)
			if err != nil || run.State != StateRunning {
				return false, fmt.Errorf("cancel target %d did not remain running: %v", id, err)
			}
			h.cancelRun(id)
		}
		ids = slices.DeleteFunc(ids, func(id int64) bool { return slices.Contains(entered, id) })
		return len(ids) == 0, nil
	})
}

func (h *scenarioHarness) workflows(rng *rand.Rand) int64 {
	h.t.Helper()
	var resume int64
	for i := range 10 {
		resume = h.workflow(rng, i)
	}
	return resume
}

func (h *scenarioHarness) workflow(rng *rand.Rand, i int) int64 {
	h.t.Helper()
	name := fmt.Sprintf("m7_wf_%d", i)
	count, phase := 4, "workflow"
	if i == 10 {
		count, phase = 3, "snooze_workflow"
	}
	params := make([]scenarioParams, count)
	nodes := make([]Node, count)
	for j := range count {
		job := fmt.Sprintf("%s_%c", name, 'a'+j)
		if i == 10 {
			job = "m7_snooze_" + []string{"submit", "poll", "verify"}[j]
		}
		p := scenarioParams{Case: job, Kind: "short", WorkMs: 10}
		if i == 10 && j == 1 {
			p.Kind, p.Snoozes, p.SnoozeMs = "snooze", 4, 700
		}
		if (i == 7 || i == 8) && j == 0 {
			p.Kind = "permanent"
		}
		if i == 9 && j == 1 {
			p.Kind, p.Gate = "resume", name
			if _, err := h.pool.Exec(h.ctx, `INSERT INTO `+qualified(h.schema, "scenario_control")+` (key, enabled) VALUES ($1,false)`, name); err != nil {
				h.t.Fatal(err)
			}
		}
		params[j] = scenarioPayload(rng, p, 1024)
		h.declare(job, params[j])
		nodes[j] = Node{Job: job}
		if j > 0 {
			nodes[j].Deps = []string{nodes[j-1].Job}
		}
		if i < 4 && j == 2 {
			nodes[j].Deps = []string{nodes[0].Job}
		}
		if i < 4 && j == 3 {
			nodes[j].Deps = []string{nodes[1].Job, nodes[2].Job}
		}
	}
	if err := h.e.Workflows().Declare(h.ctx, WorkflowSpec{Name: name, Nodes: nodes}); err != nil {
		h.t.Fatal(err)
	}
	started := time.Now()
	id, err := h.e.Workflows().Trigger(h.ctx, name, scenarioJSON(struct{ Value string }{name}))
	elapsed := time.Since(started)
	h.record(map[string]any{"operation": "workflow_trigger", "name": name, "id": id, "error": fmt.Sprint(err), "trigger_ns": elapsed.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	h.latencies[phase+"/trigger"] = append(h.latencies[phase+"/trigger"], elapsed)
	actual, err := h.e.Workflows().GetRun(h.ctx, id)
	if err != nil || len(actual.Nodes) != count {
		h.t.Fatalf("workflow materialization: %v", err)
	}
	byName := map[string]int64{}
	for _, n := range actual.Nodes {
		byName[n.JobName] = n.Id
	}
	for j, n := range nodes {
		w := scenarioExpected(byName[n.Job], phase, params[j])
		deps := map[string]string{}
		for _, name := range n.Deps {
			w.Deps = append(w.Deps, byName[name])
			deps[name] = h.wants[byName[name]].Digest
		}
		w.Digest, w.Key = scenarioDigest(params[j], name, deps), fmt.Sprintf("wf:%d/%s", id, n.Job)
		if (i == 7 || i == 8) && j > 0 {
			w.State, w.Invocations, w.Errors = StateCancelled, nil, []string{"upstream_failed"}
		}
		if i == 9 && j == 1 {
			w.State, w.Attempt, w.Invocations, w.Errors = StateSucceeded, 0, []int{1, 1}, []string{"business"}
		}
		if i == 9 && j > 1 {
			w.Errors = []string{"upstream_failed"}
		}
		h.wants[w.Id] = w
		h.record(w)
	}
	h.workflowWants[id] = WorkflowSucceeded
	if i == 7 || i == 8 {
		h.workflowWants[id] = WorkflowFailed
	}
	return id
}

func (h *scenarioHarness) resumeWorkflow(id int64) {
	h.t.Helper()
	var before *WorkflowRun
	h.wait("initial workflow failure", func(ctx context.Context) (bool, error) {
		var err error
		before, err = h.e.Workflows().GetRun(ctx, id)
		if err != nil {
			return false, err
		}
		return before.State == WorkflowFailed, nil
	})
	for _, node := range before.Nodes {
		want := StateCancelled
		if strings.HasSuffix(node.JobName, "_a") {
			want = StateSucceeded
			if node.StartedAt == nil {
				h.t.Fatal("successful predecessor lacks started_at")
			}
			h.keptStarts[node.Id] = *node.StartedAt
		} else if strings.HasSuffix(node.JobName, "_b") {
			want = StateFailed
		}
		if node.State != want {
			h.t.Fatalf("before Resume: node %s is %s, want %s", node.JobName, node.State, want)
		}
	}
	if _, err := h.pool.Exec(h.ctx, `UPDATE `+qualified(h.schema, "scenario_control")+` SET enabled=true WHERE key=$1`, before.WorkflowName); err != nil {
		h.t.Fatal(err)
	}
	err := h.e.Workflows().Resume(h.ctx, id)
	h.record(map[string]any{"operation": "resume", "id": id, "error": fmt.Sprint(err)})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *scenarioHarness) snoozes(rng *rand.Rand) {
	h.t.Helper()
	h.declare("m7_snooze", scenarioParams{Kind: "snooze"})
	h.declare("m7_snooze_barrier", scenarioParams{Kind: "barrier"})
	const capacity = 3 * 16
	var ids, probes []int64
	deadline := h.now().Add(2 * time.Second)
	for i := range capacity {
		p := scenarioPayload(rng, scenarioParams{Case: fmt.Sprintf("snooze_%02d", i), Kind: "snooze", WorkMs: 5, Snoozes: 2, SnoozeMs: 3000}, 1024)
		ids = append(ids, h.trigger("m7_snooze", "snooze_plain", p, nil))
	}
	var firstDue *time.Time
	h.wait("first Snooze pending snapshots", func(ctx context.Context) (bool, error) {
		var count int
		var now time.Time
		err := h.pool.QueryRow(ctx, `SELECT count(*), min(next_at), clock_timestamp() FROM `+qualified(h.schema, "scenario_invocation")+`
			WHERE run_id=ANY($1::bigint[]) AND next_at IS NOT NULL`, ids).Scan(&count, &firstDue, &now)
		if err == nil && (!now.Before(deadline) || (firstDue != nil && !now.Before(*firstDue))) {
			return false, errors.New("Snooze tasks did not all reach pending before the slot-check deadline")
		}
		return count == capacity, err
	})
	if _, err := h.pool.Exec(h.ctx, `INSERT INTO `+qualified(h.schema, "scenario_control")+` (key, enabled) VALUES ('snooze_slots', false)`); err != nil {
		h.t.Fatal(err)
	}
	for i := range capacity {
		p := scenarioPayload(rng, scenarioParams{Case: fmt.Sprintf("snooze_slot_%02d", i), Kind: "barrier", WorkMs: 5, Gate: "snooze_slots"}, 1024)
		probes = append(probes, h.trigger("m7_snooze_barrier", "snooze_slots", p, nil))
	}
	h.wait("all slots reused before Snooze is due", func(ctx context.Context) (bool, error) {
		var count int
		var now time.Time
		err := h.pool.QueryRow(ctx, `SELECT count(*), clock_timestamp() FROM `+qualified(h.schema, "scenario_invocation")+`
			WHERE run_id=ANY($1::bigint[]) AND returned_at IS NULL`, probes).Scan(&count, &now)
		if err != nil {
			return false, err
		}
		if !now.Before(*firstDue) {
			return false, fmt.Errorf("only %d/%d slots were reusable before Snooze was due", count, capacity)
		}
		if count == capacity {
			h.record(map[string]any{"operation": "snooze_slots_reused", "slots": count, "at": now, "before": firstDue})
		}
		return count == capacity, nil
	})
	if _, err := h.pool.Exec(h.ctx, `UPDATE `+qualified(h.schema, "scenario_control")+` SET enabled=true WHERE key='snooze_slots'`); err != nil {
		h.t.Fatal(err)
	}
	h.drain(append(ids, probes...))
	h.workflow(rng, 10)
	h.drain(slices.Collect(maps.Keys(h.wants)))
	h.t.Log("snooze: 48 repeated jobs, 48 simultaneous slot probes, submit/poll/verify DAG drained")
}

func TestScenarioAcceptance(t *testing.T) {
	if os.Getenv("SKEIN_SCENARIO") == "" {
		t.Skip("set SKEIN_SCENARIO=1 to run M7 scenarios")
	}
	profile := cmp.Or(os.Getenv("SKEIN_SCENARIO_PROFILE"), "smoke")
	if profile != "smoke" {
		t.Fatalf("scenario profile %q is unknown or not implemented; only smoke is available", profile)
	}
	if f := flag.Lookup("test.artifacts"); f == nil || f.Value.String() != "true" {
		t.Fatal("M7 requires -artifacts -outputdir <directory> so evidence survives test cleanup")
	}
	t.Setenv("SKEIN_TEST_REQUIRE_DB", "1")
	h := newScenarioHarness(t)
	rng := rand.New(rand.NewPCG(1, 1))
	for _, kind := range []string{"short", "long", "retry", "permanent", "timeout", "cancel"} {
		h.declare("m7_"+kind, scenarioParams{Kind: kind})
	}
	h.precision(rng)
	held := scenarioPayload(rng, scenarioParams{Case: "held_tx", Kind: "short", WorkMs: 5}, 1024)
	h.drain([]int64{h.triggerTx("transaction", held, 350*time.Millisecond, false)})
	rolledBack := scenarioPayload(rng, scenarioParams{Case: "rolled_back", Kind: "short", WorkMs: 5}, 1024)
	h.triggerTx("transaction", rolledBack, 0, true)
	h.dedup(rng)
	mixedStart := time.Now()
	cancelIds := h.mixed(rng)
	resume := h.workflows(rng)
	h.cancelEntered(cancelIds)
	h.resumeWorkflow(resume)
	h.drain(slices.Collect(maps.Keys(h.wants)))
	h.mixedDuration = time.Since(mixedStart)
	h.snoozes(rng)
	h.stopWorkers()
	h.verify()
}
