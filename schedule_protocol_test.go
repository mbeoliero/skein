package skein

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
)

func protocolEngine(t *testing.T) (*Engine, *pgxpool.Pool, string) {
	t.Helper()
	pool, schema := freshSchema(t)
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	e := startEngine(t, pool, cfg, nil)
	for _, name := range []string{"a", "b"} {
		declare(t, e, JobSpec{Name: name, ExecutorType: "x"})
		declareWorkflow(t, e, WorkflowSpec{Name: "w" + name, Nodes: []Node{{Job: name}}})
	}
	return e, pool, schema
}

func protocolSpec(workflow bool) ScheduleSpec {
	spec := ScheduleSpec{Name: "s", Job: "a", Cron: yearly, Overlap: OverlapAllow}
	if workflow {
		spec.Job, spec.Workflow = "", "wa"
	}
	return spec
}

func protocolBeat(t *testing.T, e *Engine, pool *pgxpool.Pool, schema, name string, beat int) store.Fired {
	t.Helper()
	now, err := e.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	dueAt(t, pool, schema, name, now.Add(-time.Duration(100-beat)*time.Hour))
	scan, err := e.st.ScanDue(t.Context(), nextRun)
	if err != nil || len(scan.Fired) != 1 {
		t.Fatalf("scan beat: %+v, %v", scan, err)
	}
	return scan.Fired[0]
}

func cancelProtocolBeat(t *testing.T, e *Engine, beat store.Fired) {
	t.Helper()
	var err error
	if beat.IsWorkflow {
		err = e.Workflows().CancelRun(t.Context(), beat.RunId)
	} else {
		err = e.Runs().Cancel(t.Context(), beat.RunId)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func resumeProtocolBeat(ctx context.Context, e *Engine, beat store.Fired) error {
	if beat.IsWorkflow {
		return e.Workflows().Resume(ctx, beat.RunId)
	}
	return e.Runs().Resume(ctx, beat.RunId)
}

func protocolSnapshot(t *testing.T, e *Engine, beat store.Fired) []byte {
	t.Helper()
	var data []byte
	var err error
	if beat.IsWorkflow {
		run, nodes, rerr := e.st.GetWorkflowRun(t.Context(), beat.RunId)
		if rerr != nil {
			t.Fatal(rerr)
		}
		data, err = json.Marshal(struct {
			Run   store.WorkflowRun
			Nodes []store.JobRun
		}{Run: run, Nodes: nodes})
	} else {
		run, rerr := e.st.GetJobRun(t.Context(), beat.RunId)
		if rerr != nil {
			t.Fatal(rerr)
		}
		data, err = json.Marshal(run)
	}
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// A fixture owns the real protocol lock while the operation under test reaches it.
func holdScheduleNames(t *testing.T, pool *pgxpool.Pool, schema string, names ...string) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rollbackTx(t, tx) })
	execSql(t, tx, "SET LOCAL search_path = "+pgx.Identifier{schema}.Sanitize()+", pg_temp")
	for _, name := range names {
		if err := store.New().LockScheduleName(t.Context(), tx, name); err != nil {
			t.Fatal(err)
		}
	}
	return tx
}

func TestScheduleSkipCoversHistoricalAllowBeats(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		workflow bool
	}{
		{name: "job"},
		{name: "workflow", workflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, pool, schema := protocolEngine(t)
			spec := protocolSpec(tc.workflow)
			if err := e.Schedules().Put(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
			first := protocolBeat(t, e, pool, schema, "s", 1)
			spec.Overlap = OverlapSkip
			if err := e.Schedules().Put(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
			if beat := protocolBeat(t, e, pool, schema, "s", 2); !beat.Skipped {
				t.Fatalf("skip joined an existing allow beat: %+v", beat)
			}
			cancelProtocolBeat(t, e, first)
			second := protocolBeat(t, e, pool, schema, "s", 3)
			if second.Skipped || second.RunId == 0 {
				t.Fatalf("terminal allow beat blocked next: %+v", second)
			}
			before := protocolSnapshot(t, e, first)
			if err := resumeProtocolBeat(t.Context(), e, first); !errors.Is(err, ErrDuplicate) {
				t.Fatalf("historical allow Resume: %v", err)
			}
			if after := protocolSnapshot(t, e, first); !bytes.Equal(before, after) {
				t.Fatalf("rejected Resume changed snapshot: %s -> %s", before, after)
			}
			if err := resumeProtocolBeat(t.Context(), e, second); !errors.Is(err, ErrNotResumable) {
				t.Fatalf("active beat classification: %v", err)
			}
			cancelProtocolBeat(t, e, second)
			if err := resumeProtocolBeat(t.Context(), e, first); err != nil {
				t.Fatalf("Resume after release: %v", err)
			}
			if beat := protocolBeat(t, e, pool, schema, "s", 4); !beat.Skipped {
				t.Fatalf("scan ignored resumed allow beat: %+v", beat)
			}
		})
	}
}

func TestScheduleNameSurvivesTargetChangeAndRecreation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		workflow  bool
		recreated bool
	}{
		{name: "job_to_workflow"},
		{name: "workflow_to_job", workflow: true},
		{name: "recreated_job_to_workflow", recreated: true},
		{name: "recreated_workflow_to_job", workflow: true, recreated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, pool, schema := protocolEngine(t)
			spec := protocolSpec(tc.workflow)
			if err := e.Schedules().Put(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
			old := protocolBeat(t, e, pool, schema, "s", 1)
			cancelProtocolBeat(t, e, old)
			if tc.recreated {
				if err := e.Schedules().Delete(t.Context(), "s"); err != nil {
					t.Fatal(err)
				}
			}
			// A deleted schedule must not prevent recovery of its history.
			if err := resumeProtocolBeat(t.Context(), e, old); err != nil {
				t.Fatal(err)
			}
			spec.Job, spec.Workflow, spec.Overlap = "b", "", OverlapSkip
			if !tc.workflow {
				spec.Job, spec.Workflow = "", "wb"
			}
			if err := e.Schedules().Put(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
			if beat := protocolBeat(t, e, pool, schema, "s", 2); !beat.Skipped {
				t.Fatalf("new target ignored old target in flight: %+v", beat)
			}
			cancelProtocolBeat(t, e, old)
			current := protocolBeat(t, e, pool, schema, "s", 3)
			if current.Skipped || current.IsWorkflow == old.IsWorkflow {
				t.Fatalf("new target was not fired: %+v", current)
			}
			if err := resumeProtocolBeat(t.Context(), e, old); !errors.Is(err, ErrDuplicate) {
				t.Fatalf("old target Resume ignored new target: %v", err)
			}
		})
	}
}

func TestScheduleScanPagesPastBusyNames(t *testing.T) {
	t.Parallel()
	e, pool, schema := protocolEngine(t)
	const busy, available = 50, 5
	names := make([]string, 0, busy)
	for i := range busy + available {
		name := fmt.Sprintf("s%03d", i)
		if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: name, Job: "a", Cron: yearly}); err != nil {
			t.Fatal(err)
		}
		if i < busy {
			names = append(names, name)
		}
	}
	execSql(t, pool, "UPDATE "+qualified(schema, "schedule")+" SET next_run_at = now() - interval '1 hour'")
	locked := holdScheduleNames(t, pool, schema, names...)
	scan, err := e.st.ScanDue(t.Context(), nextRun)
	if err != nil || scan.Full || len(scan.Fired) != available {
		t.Fatalf("scan past busy first page: %+v, %v", scan, err)
	}
	if err := locked.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	scan, err = e.st.ScanDue(t.Context(), nextRun)
	if err != nil || !scan.Full || len(scan.Fired) != busy {
		t.Fatalf("scan released first page: %+v, %v", scan, err)
	}
	scan, err = e.st.ScanDue(t.Context(), nextRun)
	if err != nil || scan.Full || len(scan.Fired) != 0 {
		t.Fatalf("drained scan: %+v, %v", scan, err)
	}
}

type scheduleCandidatesKey struct{}

type scheduleCandidatesTracer struct {
	once  sync.Once
	after func(context.Context)
}

func (tr *scheduleCandidatesTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, scheduleCandidatesKey{}, strings.HasPrefix(data.SQL, "-- name: DueScheduleCandidates"))
}

func (tr *scheduleCandidatesTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	candidates, _ := ctx.Value(scheduleCandidatesKey{}).(bool)
	if candidates && data.Err == nil {
		tr.once.Do(func() { tr.after(ctx) })
	}
}

func TestScheduleScanDoesNotRepeatMovedCandidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		middle  int
		skipped bool
		full    bool
	}{
		{name: "created", middle: 48},
		{name: "skipped", middle: 48, skipped: true},
		{name: "full", middle: 49, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, pool, schema := protocolEngine(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			now, err := e.st.Now(ctx)
			if err != nil {
				t.Fatal(err)
			}
			overlap := "allow"
			if tc.skipped {
				overlap = "skip"
				if _, err := e.st.TriggerJob(ctx, nil, store.TriggerJobParams{
					JobName: "a", Params: []byte(`{}`), ScheduleName: new("moved"), ScheduledAt: new(now.Add(-6 * time.Hour)),
				}); err != nil {
					t.Fatal(err)
				}
			}
			put := func(name string, due time.Time, policy string) {
				t.Helper()
				if err := e.st.PutSchedule(ctx, store.PutScheduleParams{
					Name: name, JobName: new("a"), Cron: yearly, Timezone: "UTC",
					Overlap: policy, Enabled: true, NextRunAt: due,
				}); err != nil {
					t.Fatal(err)
				}
			}
			put("moved", now.Add(-5*time.Hour), overlap)
			busy := make([]string, 49)
			for i := range busy {
				busy[i] = fmt.Sprintf("busy%02d", i)
				put(busy[i], now.Add(-4*time.Hour), "allow")
			}
			want := map[string]bool{"moved": true}
			for i := range tc.middle {
				name := fmt.Sprintf("middle%02d", i)
				put(name, now.Add(-3*time.Hour), "allow")
				want[name] = true
			}
			holdScheduleNames(t, pool, schema, busy...)

			movedDue := now.Truncate(time.Hour).Add(-time.Hour)
			var putErr error
			updated := false
			tracer := &scheduleCandidatesTracer{after: func(ctx context.Context) {
				// A Put that waited after computing next can commit a later, still-due
				// value after enumeration. Middle candidates must not be jumped over.
				updated = true
				putErr = e.st.PutSchedule(ctx, store.PutScheduleParams{
					Name: "moved", JobName: new("a"), Cron: "0 * * * *", Timezone: "UTC",
					Overlap: overlap, Enabled: true, NextRunAt: movedDue,
				})
			}}
			cfg, err := pgxpool.ParseConfig(testDsn())
			if err != nil {
				t.Fatal(err)
			}
			cfg.ConnConfig.Tracer = tracer
			traced, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(traced.Close)
			scan, err := store.Open(traced, schema).ScanDue(ctx, nextRun)
			if err != nil || !updated || putErr != nil {
				t.Fatalf("scan=%v concurrent Put: updated=%t err=%v", err, updated, putErr)
			}
			if scan.Full != tc.full || len(scan.Fired) != len(want) {
				t.Errorf("Full=%t fired=%d, want Full=%t distinct schedules=%d", scan.Full, len(scan.Fired), tc.full, len(want))
			}
			for _, fired := range scan.Fired {
				if !want[fired.Schedule] {
					t.Errorf("repeated or unexpected schedule: %+v", fired)
				}
				delete(want, fired.Schedule)
				skipped := fired.Schedule == "moved" && tc.skipped
				if fired.Skipped != skipped || fired.Disabled != nil || (fired.RunId == 0) != skipped {
					t.Errorf("incorrect created/skipped result: %+v", fired)
				}
				if fired.Schedule == "moved" && !fired.ScheduledAt.Equal(movedDue) {
					t.Errorf("scheduled_at=%s, want updated due %s", fired.ScheduledAt, movedDue)
				}
			}
			if len(want) != 0 {
				t.Errorf("missed schedules between old and updated due: %v", want)
			}
		})
	}
}

func TestScheduleScanRollsBackWholeBatch(t *testing.T) {
	t.Parallel()
	e, pool, schema := protocolEngine(t)
	for _, name := range []string{"first", "second"} {
		if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: name, Job: "a", Cron: yearly}); err != nil {
			t.Fatal(err)
		}
	}
	execSql(t, pool, "UPDATE "+qualified(schema, "schedule")+" SET next_run_at = now() - interval '1 hour'")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	_, err := e.st.ScanDue(ctx, func(cron, timezone string, after time.Time) (time.Time, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return nextRun(cron, timezone, after)
	})
	if err == nil || calls != 2 {
		t.Fatalf("interrupted second schedule: calls=%d err=%v", calls, err)
	}
	page, _, err := e.Runs().List(t.Context(), RunFilter{}, 0)
	if err != nil || len(page) != 0 {
		t.Fatalf("partial batch committed: %+v, %v", page, err)
	}
	scan, err := e.st.ScanDue(t.Context(), nextRun)
	if err != nil || len(scan.Fired) != 2 {
		t.Fatalf("batch did not retry both original due times: %+v, %v", scan, err)
	}
}

func TestScheduleMutationsWaitForName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"put", "put_absent", "delete", "resume_job", "resume_workflow", "resume_deleted"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, pool, schema := protocolEngine(t)
			spec := protocolSpec(name == "resume_workflow")
			if err := e.Schedules().Put(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
			beat := protocolBeat(t, e, pool, schema, "s", 1)
			cancelProtocolBeat(t, e, beat)
			if name == "put_absent" || name == "resume_deleted" {
				if err := e.Schedules().Delete(t.Context(), "s"); err != nil {
					t.Fatal(err)
				}
			}
			named := namedPool(t, schema)
			cfg := submitOnly(schema)
			cfg.DisableScheduler = true
			other := startEngine(t, named, cfg, nil)
			gate := holdScheduleNames(t, pool, schema, "s")
			done := make(chan error, 1)
			go func() {
				switch name {
				case "put", "put_absent":
					spec.Overlap = OverlapSkip
					done <- other.Schedules().Put(t.Context(), spec)
				case "delete":
					done <- other.Schedules().Delete(t.Context(), "s")
				default:
					done <- resumeProtocolBeat(t.Context(), other, beat)
				}
			}()
			waitBlocked(t, pool, schema)
			execSql(t, pool, "UPDATE "+qualified(schema, "schedule")+" SET next_run_at = now() - interval '1 hour'")
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			scan, err := e.st.ScanDue(ctx, nextRun)
			if err != nil || scan.Full || len(scan.Fired) != 0 {
				t.Fatalf("scan must skip the held name: %+v, %v", scan, err)
			}
			if err := gate.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestScheduledResumeRechecksMissingRunAfterNameLock(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		workflow bool
	}{
		{name: "job"},
		{name: "workflow", workflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, pool, schema := protocolEngine(t)
			if err := e.Schedules().Put(t.Context(), protocolSpec(tc.workflow)); err != nil {
				t.Fatal(err)
			}
			beat := protocolBeat(t, e, pool, schema, "s", 1)
			cancelProtocolBeat(t, e, beat)
			cfg := submitOnly(schema)
			cfg.DisableScheduler = true
			other := startEngine(t, namedPool(t, schema), cfg, nil)
			gate := holdScheduleNames(t, pool, schema, "s")
			done := make(chan error, 1)
			go func() { done <- resumeProtocolBeat(t.Context(), other, beat) }()
			waitBlocked(t, pool, schema)
			table := "job_run"
			if tc.workflow {
				table = "workflow_run"
			}
			// Retention does not take the name lock and may delete this terminal row.
			if _, err := pool.Exec(t.Context(), "DELETE FROM "+qualified(schema, table)+" WHERE id=$1", beat.RunId); err != nil {
				t.Fatal(err)
			}
			if err := gate.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted run after waiting: %v", err)
			}
		})
	}
}

func TestScheduleNameLocksAreSchemaScoped(t *testing.T) {
	t.Parallel()
	_, pool, schema := protocolEngine(t)
	holdScheduleNames(t, pool, schema, "s")
	other, _, _ := protocolEngine(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := other.Schedules().Put(ctx, protocolSpec(false)); err != nil {
		t.Fatalf("another schema's name lock blocked Put: %v", err)
	}
}

func TestScheduleResumeSerializesAcrossRunTables(t *testing.T) {
	t.Parallel()
	e, pool, schema := protocolEngine(t)
	if err := e.Schedules().Put(t.Context(), protocolSpec(false)); err != nil {
		t.Fatal(err)
	}
	plain := protocolBeat(t, e, pool, schema, "s", 1)
	cancelProtocolBeat(t, e, plain)
	spec := protocolSpec(true)
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	workflow := protocolBeat(t, e, pool, schema, "s", 2)
	cancelProtocolBeat(t, e, workflow)
	spec.Overlap = OverlapSkip
	if err := e.Schedules().Put(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	before := protocolSnapshot(t, e, plain)
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	first := startEngine(t, namedPool(t, schema+"_wf"), cfg, nil)
	second := startEngine(t, namedPool(t, schema+"_job"), cfg, nil)
	parent, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rollbackTx(t, parent) })
	if _, err := parent.Exec(t.Context(),
		"SELECT id FROM "+qualified(schema, "workflow_run")+" WHERE id=$1 FOR UPDATE", workflow.RunId,
	); err != nil {
		t.Fatal(err)
	}
	wfDone := make(chan error, 1)
	go func() { wfDone <- resumeProtocolBeat(t.Context(), first, workflow) }()
	// The first Resume owns the name lock while it waits for its parent row.
	waitBlocked(t, pool, schema+"_wf")
	jobDone := make(chan error, 1)
	go func() { jobDone <- resumeProtocolBeat(t.Context(), second, plain) }()
	waitBlocked(t, pool, schema+"_job")
	if err := parent.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-wfDone; err != nil {
		t.Fatalf("first Resume: %v", err)
	}
	if err := <-jobDone; !errors.Is(err, ErrDuplicate) {
		t.Fatalf("plain Resume ignored the admitted workflow: %v", err)
	}
	if after := protocolSnapshot(t, e, plain); !bytes.Equal(before, after) {
		t.Fatalf("losing Resume changed the plain run: %s -> %s", before, after)
	}
}
