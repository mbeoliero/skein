package skein

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

// These tests drive claims and settlements directly, so every callback completes
// synchronously and no background loop can consume a fixture or mutate this slice.
func controlObserverConfig(t *testing.T, schema string) (Config, *[]Event) {
	t.Helper()
	var events []Event
	seen := map[string]bool{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	cfg.Observer = ObserverFunc(func(ctx context.Context, event Event) {
		if event.EventId == "" || seen[event.EventId] {
			t.Errorf("missing or repeated event identity: %+v", event)
		}
		seen[event.EventId] = true
		if ctx.Err() != nil || event.TraceId != traceId(ctx) {
			t.Errorf("observer context disagrees with committed correlation: %+v, %v", event, ctx.Err())
		}
		if _, ok := ctx.Deadline(); ok {
			t.Error("observer inherited an operation deadline")
		}
		events = append(events, event)
	})
	return cfg, &events
}

func controlEvent(t *testing.T, events []Event, kind EventType, job string) Event {
	t.Helper()
	var found []Event
	for _, event := range events {
		if event.Type == kind && event.JobName == job {
			found = append(found, event)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want one %s for %q, got %+v", kind, job, events)
	}
	return found[0]
}

func TestObserverOrdinaryControlTransitions(t *testing.T) {
	t.Parallel()
	for _, running := range []bool{false, true} {
		name := "pending"
		if running {
			name = "running"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			cfg, events := controlObserverConfig(t, schema)
			e := startEngine(t, pool, cfg, nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			ctx, parent := testTraceContext(t, 71)
			id, err := e.Jobs().Trigger(ctx, "j", nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(*events) != 0 {
				t.Fatal("submission produced an observer event")
			}
			var claimed store.Claimed
			kind, state, execution := EventRunCancelled, string(StateCancelled), ""
			if running {
				claimed = claimAsDeadHolder(t, pool, schema, "x")
				kind, state, execution = EventRunCancelRequested, string(StateRunning), executionId(claimed.LeaseToken)
			}
			controlCtx, _ := testTraceContext(t, 72)
			for range 2 {
				if err := e.Runs().Cancel(controlCtx, id); err != nil {
					t.Fatal(err)
				}
			}
			if len(*events) != 1 {
				t.Fatalf("repeated cancellation notified %d times: %+v", len(*events), *events)
			}
			event := controlEvent(t, *events, kind, "j")
			if event.RunId != id || event.WorkflowRunId != nil || event.ExecutorType != "x" || event.State != state ||
				event.Reason != "cancel_requested" || event.ExecutionId != execution || event.Duration != 0 ||
				event.TraceId != parent.TraceID().String() || event.Error != "" {
				t.Fatalf("cancel event: %+v", event)
			}
			if running {
				if err := e.Runs().Resume(controlCtx, id); !errors.Is(err, ErrNotResumable) || len(*events) != 1 {
					t.Fatalf("resume running = %v, events %+v", err, *events)
				}
				e.settleResult(e.log, claimed, parseRetry(claimed.RetryPolicy), nil, context.Canceled, errCancelRequested, time.Millisecond)
			}
			*events = nil
			if err := e.Runs().Resume(controlCtx, id); err != nil {
				t.Fatal(err)
			}
			if err := e.Runs().Resume(controlCtx, id); !errors.Is(err, ErrNotResumable) {
				t.Fatalf("repeated resume = %v", err)
			}
			if len(*events) != 1 {
				t.Fatalf("resume events: %+v", *events)
			}
			event = controlEvent(t, *events, EventRunResumed, "j")
			if event.RunId != id || event.State != string(StatePending) || event.Reason != "manual_resume" ||
				event.ExecutionId != "" || event.Duration != 0 || event.TraceId != parent.TraceID().String() {
				t.Fatalf("resume event: %+v", event)
			}
		})
	}
}

func TestObserverWorkflowCancelReportsOnlyCommittedFinalState(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg, events := controlObserverConfig(t, schema)
	e := startEngine(t, pool, cfg, nil)
	for _, name := range []string{"a", "b"} {
		declare(t, e, JobSpec{Name: name, ExecutorType: "x"})
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}, {Job: "b", Deps: []string{"a"}}}})
	ctx, parent := testTraceContext(t, 73)
	id, err := e.Workflows().Trigger(ctx, "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(*events) != 0 {
		t.Fatal("workflow creation produced an observer event")
	}
	for range 2 {
		if err := e.Workflows().CancelRun(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	if len(*events) != 3 {
		t.Fatalf("cancel must report two nodes and only the final parent state: %+v", *events)
	}
	for _, name := range []string{"a", "b"} {
		event := controlEvent(t, *events, EventRunCancelled, name)
		if event.RunId == 0 || event.ExecutorType != "x" || event.Reason != "cancel_requested" || event.State != string(StateCancelled) {
			t.Errorf("cancelled node: %+v", event)
		}
	}
	final := controlEvent(t, *events, EventWorkflowCancelled, "")
	if final.RunId != 0 || final.State != string(WorkflowCancelled) || final.Reason != "cancellation_completed" || final.ExecutorType != "" {
		t.Errorf("cancelled parent: %+v", final)
	}
	for _, event := range *events {
		if event.WorkflowRunId == nil || *event.WorkflowRunId != id || event.WorkflowName != "w" ||
			event.TraceId != parent.TraceID().String() || event.ExecutionId != "" || event.Duration != 0 {
			t.Errorf("unstarted workflow change inherited execution identity or lost correlation: %+v", event)
		}
	}
}

func TestObserverWorkflowFailFastAndResume(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg, events := controlObserverConfig(t, schema)
	e := startEngine(t, pool, cfg, nil)
	for _, name := range []string{"success", "fail", "after"} {
		declare(t, e, JobSpec{Name: name, ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 1}})
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{
		{Job: "success"}, {Job: "fail", Deps: []string{"success"}}, {Job: "after", Deps: []string{"fail"}},
	}})
	ctx, parent := testTraceContext(t, 74)
	id, err := e.Workflows().Trigger(ctx, "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	success := claimAsDeadHolder(t, pool, schema, "x")
	e.settleResult(e.log, success, parseRetry(success.RetryPolicy), RawJSON(`{"kept":true}`), nil, nil, time.Millisecond)
	if len(*events) != 1 || (*events)[0].Type != EventRunSucceeded {
		t.Fatalf("dependency activation must not notify: %+v", *events)
	}
	*events = nil
	failed := claimAsDeadHolder(t, pool, schema, "x")
	e.settleResult(e.log, failed, parseRetry(failed.RetryPolicy), nil, errors.New("provider failed"), nil, 2*time.Millisecond)
	if len(*events) != 3 {
		t.Fatalf("fail-fast must report source, cancelled descendant, and final parent: %+v", *events)
	}
	failedEvent := controlEvent(t, *events, EventRunFailed, "fail")
	if failedEvent.RunId != failed.Id || failedEvent.ExecutionId != executionId(failed.LeaseToken) ||
		failedEvent.Duration != 2*time.Millisecond || failedEvent.Reason != "business" || failedEvent.Error != "provider failed" {
		t.Errorf("source failure: %+v", failedEvent)
	}
	descendant := controlEvent(t, *events, EventRunCancelled, "after")
	if descendant.Reason != "upstream_failed" || descendant.Error != "fail failed" || descendant.State != string(StateCancelled) {
		t.Errorf("downstream cancellation: %+v", descendant)
	}
	final := controlEvent(t, *events, EventWorkflowFailed, "")
	if final.RunId != 0 || final.State != string(WorkflowFailed) || final.Reason != "node_failed" {
		t.Errorf("final failure: %+v", final)
	}
	for _, event := range []Event{descendant, final} {
		if event.ExecutionId != "" || event.Duration != 0 || event.TraceId != parent.TraceID().String() ||
			event.WorkflowRunId == nil || *event.WorkflowRunId != id || event.WorkflowName != "w" {
			t.Errorf("propagated event inherited source execution or lost parent correlation: %+v", event)
		}
	}
	before, err := e.Runs().Get(t.Context(), success.Id)
	if err != nil {
		t.Fatal(err)
	}
	*events = nil
	resumeCtx, _ := testTraceContext(t, 75)
	if err := e.Workflows().Resume(resumeCtx, id); err != nil {
		t.Fatal(err)
	}
	if err := e.Workflows().Resume(resumeCtx, id); !errors.Is(err, ErrNotResumable) {
		t.Fatalf("repeated workflow Resume = %v", err)
	}
	if len(*events) != 3 {
		t.Fatalf("resume must report exactly reset nodes and parent: %+v", *events)
	}
	for name, state := range map[string]RunState{"fail": StatePending, "after": StateBlocked} {
		if event := controlEvent(t, *events, EventRunResumed, name); event.State != string(state) || event.RunId == success.Id {
			t.Errorf("resumed node: %+v", event)
		}
	}
	if event := controlEvent(t, *events, EventWorkflowResumed, ""); event.State != string(WorkflowRunning) || event.RunId != 0 {
		t.Errorf("resumed parent: %+v", event)
	}
	for _, event := range *events {
		if event.Reason != "manual_resume" || event.ExecutionId != "" || event.Duration != 0 ||
			event.TraceId != parent.TraceID().String() || event.WorkflowRunId == nil || *event.WorkflowRunId != id {
			t.Errorf("resume correlation: %+v", event)
		}
	}
	after, err := e.Runs().Get(t.Context(), success.Id)
	if err != nil || after.State != StateSucceeded || !after.StartedAt.Equal(*before.StartedAt) || !bytes.Equal(after.Output, before.Output) {
		t.Fatalf("Resume changed successful node: %+v, %v", after, err)
	}
	*events = nil
	for range 2 {
		claimed := claimAsDeadHolder(t, pool, schema, "x")
		e.settleResult(e.log, claimed, parseRetry(claimed.RetryPolicy), nil, nil, nil, time.Millisecond)
	}
	if len(*events) != 3 {
		t.Fatalf("resumed completion must report two nodes and one parent: %+v", *events)
	}
	completed := controlEvent(t, *events, EventWorkflowSucceeded, "")
	if completed.State != string(WorkflowSucceeded) || completed.Reason != "all_succeeded" || completed.ExecutionId != "" ||
		completed.Duration != 0 || completed.TraceId != parent.TraceID().String() {
		t.Errorf("successful parent after Resume: %+v", completed)
	}
}

func TestObserverWorkflowDeferredFinalUsesAggregate(t *testing.T) {
	t.Parallel()
	for _, failFast := range []bool{false, true} {
		name := "cancel"
		if failFast {
			name = "fail_fast"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			cfg, events := controlObserverConfig(t, schema)
			e := startEngine(t, pool, cfg, nil)
			for _, name := range []string{"first", "last", "after"} {
				declare(t, e, JobSpec{Name: name, ExecutorType: name, Retry: RetryPolicy{MaxAttempts: 1}})
			}
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{
				{Job: "first"}, {Job: "last"}, {Job: "after", Deps: []string{"first"}},
			}})
			ctx, parent := testTraceContext(t, 76)
			id, err := e.Workflows().Trigger(ctx, "w", nil)
			if err != nil {
				t.Fatal(err)
			}
			first := claimAsDeadHolder(t, pool, schema, "first")
			last := claimAsDeadHolder(t, pool, schema, "last")
			reason := "cancel_requested"
			if failFast {
				e.settleResult(e.log, first, parseRetry(first.RetryPolicy), nil, errors.New("first failed"), nil, time.Millisecond)
				reason = "upstream_failed"
			} else if err := e.Workflows().CancelRun(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			requested := controlEvent(t, *events, EventWorkflowCancelRequested, "")
			if requested.State != string(WorkflowCancelling) || requested.Reason != reason || requested.ExecutionId != "" || requested.Duration != 0 {
				t.Errorf("parent cancellation request: %+v", requested)
			}
			controlEvent(t, *events, EventRunCancelled, "after")
			want := 2
			if failFast {
				want++
			}
			if len(*events) != want {
				t.Fatalf("parent finalized before last node settled: %+v", *events)
			}
			if err := e.Workflows().CancelRun(t.Context(), id); err != nil || len(*events) != want {
				t.Fatalf("repeated parent cancellation: %v, %+v", err, *events)
			}
			if !failFast {
				e.settleResult(e.log, first, parseRetry(first.RetryPolicy), nil, context.Canceled, errCancelRequested, time.Millisecond)
			}
			*events = nil
			// This models a sibling that completed before learning cancellation.
			e.settleResult(e.log, last, parseRetry(last.RetryPolicy), nil, nil, nil, time.Millisecond)
			if len(*events) != 2 {
				t.Fatalf("last settle must notify source and aggregate final state: %+v", *events)
			}
			controlEvent(t, *events, EventRunSucceeded, "last")
			kind, state, finalReason := EventWorkflowCancelled, string(WorkflowCancelled), "cancellation_completed"
			if failFast {
				kind, state, finalReason = EventWorkflowFailed, string(WorkflowFailed), "node_failed"
			}
			final := controlEvent(t, *events, kind, "")
			if final.State != state || final.Reason != finalReason || final.ExecutionId != "" || final.Duration != 0 ||
				final.TraceId != parent.TraceID().String() || final.WorkflowRunId == nil || *final.WorkflowRunId != id {
				t.Fatalf("last successful node replaced aggregate outcome: %+v", final)
			}
		})
	}
}

func TestObserverResumeConflictDoesNotNotify(t *testing.T) {
	t.Parallel()
	for _, workflow := range []bool{false, true} {
		name := "job"
		if workflow {
			name = "workflow"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			cfg, events := controlObserverConfig(t, schema)
			e := startEngine(t, pool, cfg, nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}})
			scheduled, err := e.st.Now(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			create := func(at time.Time) (int64, error) {
				if workflow {
					return e.st.TriggerWorkflow(t.Context(), nil, store.TriggerWorkflowParams{
						WorkflowName: "w", Input: []byte(`{}`), ScheduleName: new("beat"),
						ScheduledAt: &at, DedupKey: new("sched:beat"),
					})
				}
				return e.st.TriggerJob(t.Context(), nil, store.TriggerJobParams{
					JobName: "j", Params: []byte(`{}`), ScheduleName: new("beat"),
					ScheduledAt: &at, DedupKey: new("sched:beat"),
				})
			}
			cancel := e.Runs().Cancel
			resume := e.Runs().Resume
			if workflow {
				cancel, resume = e.Workflows().CancelRun, e.Workflows().Resume
			}
			old, err := create(scheduled)
			if err != nil {
				t.Fatal(err)
			}
			if err := cancel(t.Context(), old); err != nil {
				t.Fatal(err)
			}
			if _, err := create(scheduled.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			type runSnapshot struct {
				Job      JobRun
				Workflow WorkflowRun
			}
			snapshot := func() runSnapshot {
				t.Helper()
				if workflow {
					run, err := e.Workflows().GetRun(t.Context(), old)
					if err != nil {
						t.Fatal(err)
					}
					return runSnapshot{Workflow: *run}
				}
				run, err := e.Runs().Get(t.Context(), old)
				if err != nil {
					t.Fatal(err)
				}
				return runSnapshot{Job: *run}
			}
			before := snapshot()
			*events = nil
			if err := resume(t.Context(), old); !errors.Is(err, ErrDuplicate) {
				t.Fatalf("Resume conflict = %v", err)
			}
			if len(*events) != 0 {
				t.Fatalf("rolled-back Resume notified: %+v", *events)
			}
			if after := snapshot(); !reflect.DeepEqual(before, after) {
				t.Fatalf("Resume conflict partially changed run: before %+v, after %+v", before, after)
			}
		})
	}
}
