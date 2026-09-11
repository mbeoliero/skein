package skein

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

type traceRequestValueKey struct{}

func testTraceContext(t *testing.T, seed byte) (context.Context, trace.SpanContext) {
	t.Helper()
	state, err := trace.ParseTraceState("vendor=queued,host=test")
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{seed}, SpanID: trace.SpanID{seed + 1},
		TraceFlags: trace.FlagsSampled, TraceState: state,
	})
	return trace.ContextWithSpanContext(t.Context(), sc), sc
}

func TestTracePropagationRestoresOnlyRemoteParent(t *testing.T) {
	t.Parallel()
	_, valid := testTraceContext(t, 1)
	cases := []struct {
		name string
		sc   trace.SpanContext
	}{
		{name: "sampled", sc: valid},
		{name: "unsampled", sc: valid.WithTraceFlags(0)},
		{name: "absent"},
		{name: "trace_without_span", sc: trace.NewSpanContext(trace.SpanContextConfig{TraceID: valid.TraceID()})},
		{name: "span_without_trace", sc: trace.NewSpanContext(trace.SpanContextConfig{SpanID: valid.SpanID()})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			member, err := baggage.NewMember("request-secret", "private")
			if err != nil {
				t.Fatal(err)
			}
			bag, err := baggage.New(member)
			if err != nil {
				t.Fatal(err)
			}
			ctx := trace.ContextWithSpanContext(t.Context(), tc.sc)
			ctx = baggage.ContextWithBaggage(ctx, bag)
			ctx = context.WithValue(ctx, traceRequestValueKey{}, "private")
			ctx, cancel := context.WithTimeout(ctx, time.Hour)
			cancel()

			raw := traceExtra(ctx)
			var fields map[string]string
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			for key := range fields {
				if key != "traceparent" && key != "tracestate" {
					t.Errorf("unexpected persisted context field %q", key)
				}
			}
			restored := restoreTrace(raw)
			got := trace.SpanContextFromContext(restored)
			if tc.sc.IsValid() {
				if !got.Equal(tc.sc.WithRemote(true)) {
					t.Errorf("restored parent = %v, want %v", got, tc.sc.WithRemote(true))
				}
				if gotId := traceId(restored); gotId != tc.sc.TraceID().String() {
					t.Errorf("trace id = %q", gotId)
				}
			} else if got.IsValid() || traceId(restored) != "" || len(fields) != 0 {
				t.Errorf("invalid parent became trace context: %s", raw)
			}
			if restored.Err() != nil || restored.Value(traceRequestValueKey{}) != nil {
				t.Error("restored context inherited request cancellation or values")
			}
			if _, ok := restored.Deadline(); ok {
				t.Error("restored context inherited request deadline")
			}
			if baggage.FromContext(restored).Len() != 0 {
				t.Error("restored context inherited baggage")
			}
			if trace.SpanFromContext(restored).IsRecording() {
				t.Error("restoring a remote parent created a recording span")
			}
		})
	}
}

func TestTraceInvalidMetadataHasNoParent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  string
	}{
		{name: "absent"},
		{name: "empty", raw: `{}`},
		{name: "broken_json", raw: `{`},
		{name: "wrong_type", raw: `{"traceparent":42}`},
		{name: "bad_parent", raw: `{"traceparent":"invalid","tracestate":"vendor=queued"}`},
		{name: "zero_trace", raw: `{"traceparent":"00-00000000000000000000000000000000-0100000000000000-01"}`},
		{name: "zero_span", raw: `{"traceparent":"00-01000000000000000000000000000000-0000000000000000-01"}`},
		{name: "owner_only", raw: `{"lease_owner":"worker"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := restoreTrace([]byte(tc.raw))
			if trace.SpanContextFromContext(ctx).IsValid() || traceId(ctx) != "" {
				t.Errorf("invalid metadata restored a parent: %s", tc.raw)
			}
		})
	}
}

func TestExecutionIdentityIsStableWithoutExposingToken(t *testing.T) {
	t.Parallel()
	first, second := uuid.New(), uuid.New()
	id := executionId(first)
	if id == "" || id != executionId(first) {
		t.Fatal("one lease must have a stable, nonempty execution id")
	}
	if id == executionId(second) {
		t.Error("different leases have the same execution id")
	}
	if id == first.String() {
		t.Error("execution id exposes the lease credential")
	}
}

type traceObservation struct {
	Valid           bool
	TraceId         string
	SpanId          string
	Tracestate      string
	Flags           uint8
	Remote          bool
	RequestTraceId  string
	ExecutionId     string
	IdempotencyKey  string
	Attempt         int
	HasBaggage      bool
	HasRequestValue bool
	ContextError    string
	Deadline        time.Time
	DepCount        int
}

func observeTrace(ctx context.Context, req *Request) traceObservation {
	sc := trace.SpanContextFromContext(ctx)
	deadline, _ := ctx.Deadline()
	observed := traceObservation{
		Valid: sc.IsValid(), TraceId: sc.TraceID().String(), SpanId: sc.SpanID().String(),
		Tracestate: sc.TraceState().String(), Flags: uint8(sc.TraceFlags()), Remote: sc.IsRemote(),
		RequestTraceId: req.TraceId, ExecutionId: req.ExecutionId, IdempotencyKey: req.IdempotencyKey,
		Attempt: req.Attempt, HasBaggage: baggage.FromContext(ctx).Len() != 0,
		HasRequestValue: ctx.Value(traceRequestValueKey{}) != nil, Deadline: deadline, DepCount: len(req.Deps),
	}
	if err := ctx.Err(); err != nil {
		observed.ContextError = err.Error()
	}
	return observed
}

func traceProbe(ctx context.Context, req *Request) (RawJSON, error) {
	return json.Marshal(observeTrace(ctx, req))
}

func checkObservedTrace(t *testing.T, got traceObservation, want trace.SpanContext) {
	t.Helper()
	if !got.Valid || !got.Remote || got.TraceId != want.TraceID().String() || got.SpanId != want.SpanID().String() {
		t.Errorf("remote submission parent was not preserved: %+v", got)
	}
	if got.Flags != uint8(want.TraceFlags()) || got.Tracestate != want.TraceState().String() {
		t.Errorf("sampling or tracestate changed: %+v", got)
	}
	if got.RequestTraceId != want.TraceID().String() || got.ExecutionId == "" {
		t.Errorf("request correlation missing: %+v", got)
	}
	if got.HasBaggage || got.HasRequestValue || got.ContextError != "" {
		t.Errorf("request context leaked into execution: %+v", got)
	}
}

func TestTraceAcrossProcessesAndWorkflowBranches(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	sub := startEngine(t, pool, submitOnly(schema), nil)
	for _, name := range []string{"plain", "empty", "root", "left", "right", "join"} {
		declare(t, sub, JobSpec{Name: name, ExecutorType: "trace", Timeout: time.Minute})
	}
	declareWorkflow(t, sub, WorkflowSpec{Name: "fork", Nodes: []Node{
		{Job: "root"},
		{Job: "left", Deps: []string{"root"}},
		{Job: "right", Deps: []string{"root"}},
		{Job: "join", Deps: []string{"left", "right"}},
	}})
	ctx, parent := testTraceContext(t, 10)
	ctx = context.WithValue(ctx, traceRequestValueKey{}, "request-only")
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	requestDeadline, _ := ctx.Deadline()
	id, err := sub.Jobs().Trigger(ctx, "plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	wfId, err := sub.Workflows().Trigger(ctx, "fork", nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyId := trigger(t, sub, "empty", "")
	cancel() // The worker does not exist until the submitting request has ended.
	startHelper(t, schema, "SKEIN_HELPER_MODE=trace")
	plain := waitRun(t, sub, id, StateSucceeded)
	wf := waitWorkflow(t, sub, wfId, WorkflowSucceeded)
	runs := append([]JobRun{*plain}, wf.Nodes...)
	identities := map[string]bool{}
	for _, run := range runs {
		var observed traceObservation
		if err := json.Unmarshal(run.Output, &observed); err != nil {
			t.Fatal(err)
		}
		checkObservedTrace(t, observed, parent)
		if observed.Deadline.IsZero() || !observed.Deadline.Before(requestDeadline) {
			t.Errorf("run %d inherited request deadline: %v", run.Id, observed.Deadline)
		}
		if identities[observed.ExecutionId] {
			t.Errorf("run %d reused another claim's execution id", run.Id)
		}
		identities[observed.ExecutionId] = true
		wantKey := "run:" + strconv.FormatInt(run.Id, 10)
		if run.WorkflowRunId != nil {
			wantKey = "wf:" + strconv.FormatInt(wfId, 10) + "/" + run.JobName
		}
		if observed.IdempotencyKey != wantKey {
			t.Errorf("run %d idempotency key = %q, want %q", run.Id, observed.IdempotencyKey, wantKey)
		}
		if run.JobName == "join" && observed.DepCount != 2 {
			t.Errorf("join received %d predecessors", observed.DepCount)
		}
	}
	var empty traceObservation
	if err := json.Unmarshal(waitRun(t, sub, emptyId, StateSucceeded).Output, &empty); err != nil {
		t.Fatal(err)
	}
	if empty.Valid || empty.RequestTraceId != "" || empty.ExecutionId == "" {
		t.Errorf("untraced submission invented trace context: %+v", empty)
	}
}

func TestTraceAndExecutionIdentityAcrossReexecution(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	sub := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, sub, JobSpec{Name: "j", ExecutorType: "trace", Retry: RetryPolicy{MaxAttempts: 3}})
	ctx, parent := testTraceContext(t, 20)
	id, err := sub.Jobs().Trigger(ctx, "j", nil)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan traceObservation, 5)
	releasing := make(chan struct{})
	var calls atomic.Int32
	cfg := fastConfig(schema)
	cfg.DisableScheduler = true
	cfg.HeartbeatInterval, cfg.LeaseTTL = 500*time.Millisecond, 3*time.Second
	cfg.ShutdownGrace, cfg.CancelTimeout = time.Millisecond, time.Second
	worker := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("trace", func(ctx context.Context, req *Request) (RawJSON, error) {
			observed <- observeTrace(ctx, req)
			switch calls.Add(1) {
			case 1:
				return nil, errors.New("retry this attempt")
			case 2:
				return nil, Snooze(time.Millisecond)
			case 3:
				return nil, Permanent(errors.New("await manual resume"))
			case 4:
				close(releasing)
				<-ctx.Done()
				return nil, ctx.Err()
			default:
				return nil, Permanent(errors.New("unexpected execution before shutdown"))
			}
		})
	})
	waitRun(t, sub, id, StateFailed)
	resumeCtx, _ := testTraceContext(t, 30)
	if err := sub.Runs().Resume(resumeCtx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-releasing:
	case <-time.After(10 * time.Second):
		t.Fatal("resumed executor did not enter")
	}
	if err := worker.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if run := waitRun(t, sub, id, StatePending); run.Attempt != 0 {
		t.Errorf("released resumed run consumed attempt budget: %d", run.Attempt)
	}
	startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("trace", func(ctx context.Context, req *Request) (RawJSON, error) {
			observed <- observeTrace(ctx, req)
			return nil, nil
		})
	})
	waitRun(t, sub, id, StateSucceeded)
	identities := map[string]bool{}
	for i, attempt := range []int{1, 2, 2, 1, 1} {
		select {
		case got := <-observed:
			checkObservedTrace(t, got, parent)
			if got.Attempt != attempt {
				t.Errorf("execution %d attempt = %d, want %d", i+1, got.Attempt, attempt)
			}
			if identities[got.ExecutionId] {
				t.Errorf("execution %d reused an execution id", i+1)
			}
			identities[got.ExecutionId] = true
			if got.IdempotencyKey != "run:"+strconv.FormatInt(id, 10) {
				t.Errorf("execution %d changed the idempotency key", i+1)
			}
		default:
			t.Fatalf("execution %d was not observed", i+1)
		}
	}
}

func TestTraceTransactionalSubmissionAndDedup(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		workflow bool
	}{
		{name: "ordinary"},
		{name: "workflow", workflow: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e := startEngine(t, pool, submitOnly(schema), nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "trace"})
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}})
			submit := func(ctx context.Context, tx pgx.Tx, key string) (int64, error) {
				if tc.workflow {
					if tx != nil {
						return e.Workflows().TriggerTx(ctx, tx, "w", nil, DedupKey(key))
					}
					return e.Workflows().Trigger(ctx, "w", nil, DedupKey(key))
				}
				if tx != nil {
					return e.Jobs().TriggerTx(ctx, tx, "j", nil, DedupKey(key))
				}
				return e.Jobs().Trigger(ctx, "j", nil, DedupKey(key))
			}
			extra := func(id int64) (RawJSON, error) {
				if tc.workflow {
					run, err := e.Workflows().GetRun(t.Context(), id)
					if err != nil {
						return nil, err
					}
					return run.Extra, nil
				}
				run, err := e.Runs().Get(t.Context(), id)
				if err != nil {
					return nil, err
				}
				return run.Extra, nil
			}
			ctx, parent := testTraceContext(t, 40)
			tx, err := pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackTx(t, tx)
			id, err := submit(ctx, tx, "committed")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := extra(id); !errors.Is(err, ErrNotFound) {
				t.Fatalf("uncommitted run visible: %v", err)
			}
			if err := tx.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			otherCtx, _ := testTraceContext(t, 50)
			duplicate, err := submit(otherCtx, nil, "committed")
			if duplicate != id || !errors.Is(err, ErrDuplicate) {
				t.Fatalf("duplicate = %d, %v", duplicate, err)
			}
			raw, err := extra(id)
			if err != nil {
				t.Fatal(err)
			}
			if got := trace.SpanContextFromContext(restoreTrace(raw)); !got.Equal(parent.WithRemote(true)) {
				t.Errorf("dedup replaced the original committed parent: %s", raw)
			}
			rolledBack, err := pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackTx(t, rolledBack)
			rolledBackId, err := submit(ctx, rolledBack, "rolled-back")
			if err != nil {
				t.Fatal(err)
			}
			rollbackTx(t, rolledBack)
			if _, err := extra(rolledBackId); !errors.Is(err, ErrNotFound) {
				t.Fatalf("rolled-back run visible: %v", err)
			}
		})
	}
}

func TestTraceCronDoesNotInheritScannerContext(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "trace"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}})
	ctx, _ := testTraceContext(t, 60)
	now, err := e.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range []ScheduleSpec{
		{Name: "job", Job: "j", Cron: yearly},
		{Name: "workflow", Workflow: "w", Cron: yearly},
	} {
		if err := e.Schedules().Put(ctx, spec); err != nil {
			t.Fatal(err)
		}
		dueAt(t, pool, schema, spec.Name, now.Add(-time.Minute))
	}
	scan, err := e.st.ScanDue(ctx, nextRun)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Fired) != 2 {
		t.Fatalf("created %d schedule beats, want 2", len(scan.Fired))
	}
	for _, fired := range scan.Fired {
		var raw RawJSON
		if fired.IsWorkflow {
			run, err := e.Workflows().GetRun(t.Context(), fired.RunId)
			if err != nil {
				t.Fatal(err)
			}
			raw = run.Extra
		} else {
			run, err := e.Runs().Get(t.Context(), fired.RunId)
			if err != nil {
				t.Fatal(err)
			}
			raw = run.Extra
		}
		var fields map[string]RawJSON
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if len(fields) != 0 {
			t.Errorf("schedule %s persisted scanner context: %s", fired.Schedule, raw)
		}
	}
}
