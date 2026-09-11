package skein

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

func TestWorkflowResumeRejectsEmptySet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		nodes     int
		scheduled bool
	}{
		{name: "single", nodes: 1},
		{name: "multiple", nodes: 3},
		{name: "scheduled_before_overlap", nodes: 1, scheduled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			cfg, events := controlObserverConfig(t, schema)
			e := startEngine(t, pool, cfg, nil)
			nodes := make([]Node, 0, tc.nodes)
			for i := range tc.nodes {
				name := fmt.Sprintf("n%d", i)
				declare(t, e, JobSpec{Name: name, ExecutorType: "x"})
				nodes = append(nodes, Node{Job: name})
			}
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: nodes})
			var id int64
			if tc.scheduled {
				if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Workflow: "w", Cron: yearly}); err != nil {
					t.Fatal(err)
				}
				id = protocolBeat(t, e, pool, schema, "s", 1).RunId
			} else {
				id = triggerWorkflow(t, e, "w", "")
			}
			claimed, err := e.st.ClaimPending(t.Context(), []string{"x"}, tc.nodes, "test", time.Minute)
			if err != nil || len(claimed) != tc.nodes {
				t.Fatalf("claim: %+v, %v", claimed, err)
			}
			if err := e.Workflows().CancelRun(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			// All executors return success before a heartbeat delivers cancellation.
			for _, c := range claimed {
				st := settlementFor(c, store.Succeeded)
				st.Output = []byte(`{"done":true}`)
				if _, err := e.settle(e.log, st); err != nil {
					t.Fatal(err)
				}
			}
			before, err := e.Workflows().GetRun(t.Context(), id)
			if err != nil || before.State != WorkflowCancelled {
				t.Fatalf("cancelled parent: %+v, %v", before, err)
			}
			for _, node := range before.Nodes {
				if node.State != StateSucceeded {
					t.Fatalf("node did not succeed: %+v", node)
				}
			}
			if tc.scheduled {
				protocolBeat(t, e, pool, schema, "s", 2)
			}
			select {
			case <-e.wake:
			default:
			}
			count := len(*events)
			if err := e.Workflows().Resume(t.Context(), id); !errors.Is(err, ErrNotResumable) {
				t.Fatalf("empty Resume: %v, want ErrNotResumable", err)
			}
			after, err := e.Workflows().GetRun(t.Context(), id)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("empty Resume changed snapshot: before=%+v after=%+v err=%v", before, after, err)
			}
			if len(*events) != count {
				t.Fatalf("empty Resume emitted events: %+v", (*events)[count:])
			}
			select {
			case <-e.wake:
				t.Fatal("empty Resume woke the claimer")
			default:
			}
		})
	}
}
