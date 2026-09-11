package skein

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRequestWorkflowIdentityIsolatedFromSettlement(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		err      error
		workflow WorkflowState
		run      RunState
	}{
		{name: "success", workflow: WorkflowSucceeded, run: StateSucceeded},
		{name: "failure", err: Permanent(errors.New("invalid input")), workflow: WorkflowFailed, run: StateFailed},
		{name: "cancel", err: Cancel(errors.New("stop")), workflow: WorkflowCancelled, run: StateCancelled},
		{name: "retry", err: errors.New("try later"), workflow: WorkflowRunning, run: StatePending},
		{name: "snooze", err: Snooze(time.Hour), workflow: WorkflowRunning, run: StatePending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			cfg := submitOnly(schema)
			cfg.DisableScheduler = true
			recorder := &observerRecorder{}
			cfg.Observer = recorder
			var other int64
			e := startEngine(t, pool, cfg, func(e *Engine) {
				e.Register("x", func(_ context.Context, req *Request) (RawJSON, error) {
					*req.WorkflowRunId = other
					req.RunId, req.JobName = other, "executor-local-name"
					return RawJSON(`{"finished":true}`), tc.err
				})
			})
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 3}})
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}})
			first := triggerWorkflow(t, e, "w", "")
			claimed := claimAsDeadHolder(t, pool, schema, "x")
			other = triggerWorkflow(t, e, "w", "")
			before, err := e.Workflows().GetRun(t.Context(), other)
			if err != nil {
				t.Fatal(err)
			}

			e.run(claimed)
			actual, err := e.Workflows().GetRun(t.Context(), first)
			if err != nil {
				t.Fatal(err)
			}
			if actual.State != tc.workflow || actual.Nodes[0].State != tc.run {
				t.Fatalf("original workflow: parent %s, node %s", actual.State, actual.Nodes[0].State)
			}
			after, err := e.Workflows().GetRun(t.Context(), other)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("request mutation changed another workflow: before %+v, after %+v", before, after)
			}
			if *claimed.WorkflowRunId != first {
				t.Fatalf("executor changed private claim parent to %d", *claimed.WorkflowRunId)
			}
			events := recorder.snapshot()
			if len(events) == 0 {
				t.Fatal("settlement emitted no event")
			}
			for _, event := range events {
				if event.WorkflowRunId == nil || *event.WorkflowRunId != first {
					t.Errorf("event inherited mutated request identity: %+v", event)
				}
			}
		})
	}
}
