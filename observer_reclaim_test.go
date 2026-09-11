package skein

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/skein/internal/store"
)

func TestObserverReclaimTracksLeaseBeforeCallback(t *testing.T) {
	t.Parallel()
	entered := make(chan Event, 1)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	var executions, settlements atomic.Int32
	e := &Engine{
		cfg: Config{
			CancelTimeout: time.Minute,
			Observer: ObserverFunc(func(_ context.Context, event Event) {
				if event.Type != EventRunReclaimed {
					settlements.Add(1)
					return
				}
				entered <- event
				<-gate
			}),
		},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics:  nopMetrics{},
		loopCtx:  t.Context(),
		inflight: inflightSet{m: map[int64]*inflight{}},
		executors: map[string]Executor{
			"x": func(context.Context, *Request) (RawJSON, error) {
				executions.Add(1)
				return nil, nil
			},
		},
	}
	claimed := store.Claimed{
		Id: 1, JobName: "j", ExecutorType: "x", Params: []byte("{}"),
		RetryPolicy: []byte(`{"max_attempts":2}`), Timeout: 60,
		LeaseToken: uuid.New(), Extra: []byte("{}"), Reclaimed: true,
	}
	done := make(chan struct{})
	go func() {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("reclaimed run panicked: %v", p)
			}
			close(done)
		}()
		e.run(claimed)
	}()
	defer func() {
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("reclaimed run did not return after releasing observer")
		}
	}()
	select {
	case event := <-entered:
		if event.ExecutionId != executionId(claimed.LeaseToken) || event.Duration != 0 {
			t.Fatalf("reclaim event has wrong execution identity or duration: %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reclaim observer was not called")
	}
	ids, tokens, infs := e.inflight.snapshot()
	if len(infs) != 1 || ids[0] != claimed.Id || tokens[0] != claimed.LeaseToken {
		t.Fatalf("observer ran before its lease was tracked: ids=%v, tokens=%v", ids, tokens)
	}
	// Publish real local lease loss while the observer is blocked, then let run
	// continue. Neither Executor nor a settlement may run after that decision.
	e.drop(infs[0])
	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reclaimed run did not return after lease loss")
	}
	if calls := executions.Load(); calls != 0 {
		t.Errorf("executor called %d times after lease loss during observer", calls)
	}
	if count := settlements.Load(); count != 0 {
		t.Errorf("lease loss produced %d settlement events", count)
	}
}

func TestObserverDirectReclaimCommitsBeforeCallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		workflow bool
		cancel   bool
		shutdown bool
		state    RunState
		event    EventType
	}{
		{name: "plain_cancelled", cancel: true, state: StateCancelled, event: EventRunCancelled},
		{name: "workflow_cancelled", workflow: true, cancel: true, state: StateCancelled, event: EventRunCancelled},
		{name: "attempts_exhausted", state: StateFailed, event: EventRunFailed},
		{name: "admission_denied", shutdown: true, state: StatePending, event: EventRunReleased},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			entered := make(chan Event, 1)
			gate := make(chan struct{})
			release := sync.OnceFunc(func() { close(gate) })
			rec := &observerRecorder{}
			cfg := submitOnly(schema)
			cfg.DisableScheduler = true
			cfg.Observer = ObserverFunc(func(ctx context.Context, event Event) {
				rec.Observe(ctx, event)
				if event.Type != EventRunReclaimed {
					return
				}
				select {
				case entered <- event:
				default:
					t.Error("duplicate reclaimed callback")
					return
				}
				select {
				case <-gate:
				case <-time.After(5 * time.Second):
					t.Error("reclaimed callback was not released")
				}
			})
			var calls atomic.Int32
			e := startEngine(
				t,
				pool,
				cfg,
				func(e *Engine) {
					e.Register("x", func(context.Context, *Request) (RawJSON, error) {
						calls.Add(1)
						return nil, nil
					})
				},
			)
			defer func() {
				release()
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
				defer cancel()
				if !waitGroup(ctx, &e.execs, 5*time.Second) {
					t.Error("direct settlement did not return after callback release")
				}
			}()
			maxAttempts := 1
			if tc.shutdown {
				maxAttempts = 3
			}
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: maxAttempts}})
			submitCtx, parent := testTraceContext(t, 80)
			var workflowId int64
			if tc.workflow {
				declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}})
				var err error
				workflowId, err = e.Workflows().Trigger(submitCtx, "w", nil)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := e.Jobs().Trigger(submitCtx, "j", nil); err != nil {
					t.Fatal(err)
				}
			}
			old := claimAsDeadHolder(t, pool, schema, "x")
			if tc.cancel {
				var err error
				if tc.workflow {
					err = e.Workflows().CancelRun(t.Context(), workflowId)
				} else {
					err = e.Runs().Cancel(t.Context(), old.Id)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			rec.reset()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			_, err := pool.Exec(ctx, "UPDATE "+qualified(schema, "job_run")+
				" SET lease_expires_at=now()-interval '1 second' WHERE id=$1", old.Id)
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := e.st.ClaimExpired(
				ctx,
				[]string{"x"},
				1,
				"recovered",
				time.Minute,
			)
			if err != nil || len(claimed) != 1 {
				t.Fatalf("reclaim: %+v, %v", claimed, err)
			}
			if tc.shutdown {
				e.stopLoops()
			}
			e.dispatch(claimed[0])
			select {
			case event := <-entered:
				if event.Type != EventRunReclaimed || event.ExecutionId != executionId(claimed[0].LeaseToken) {
					t.Fatalf("unexpected first callback: %+v", event)
				}
			case <-ctx.Done():
				t.Fatal("reclaimed callback did not enter")
			}
			// Read once while the first callback is blocked: settlement must
			// already be committed, independently of callback duration or lease TTL.
			run, err := e.Runs().Get(ctx, old.Id)
			if err != nil {
				t.Fatal(err)
			}
			if run.State != tc.state || (run.FinishedAt == nil) != tc.shutdown {
				t.Errorf("callback entered before direct settlement committed: %+v", run)
			}
			if run.LeaseExpiresAt != nil || run.LeaseOwner != "" {
				t.Errorf("callback entered before settlement cleared its lease: %+v", run)
			}
			entries := errorsOf(t, run)
			wantEntries := 1
			if tc.shutdown {
				wantEntries = 2
			}
			if run.Attempt != int(claimed[0].Attempt) {
				t.Errorf("direct settlement changed the reclaim budget: attempt=%d", run.Attempt)
			}
			if len(entries) != wantEntries || entries[0].Kind != "interrupted" {
				t.Errorf("direct settlement changed the reclaim history: %+v", entries)
			}
			if tc.shutdown && len(entries) == 2 {
				if entries[1].Kind != "released" || entries[1].Message != "shutdown" {
					t.Errorf("admission rejection did not record shutdown release: %+v", entries)
				}
			}
			if tc.workflow {
				parent, err := e.Workflows().GetRun(ctx, workflowId)
				if err != nil || parent.State != WorkflowCancelled || parent.FinishedAt == nil {
					t.Errorf("callback entered before parent cancellation committed: %+v, %v", parent, err)
				}
			}
			more, err := e.st.ClaimExpired(
				ctx,
				[]string{"x"},
				1,
				"another-holder",
				time.Minute,
			)
			if err != nil || len(more) != 0 {
				t.Fatalf("terminal run was reclaimed during its callback: %+v, %v", more, err)
			}
			if events := rec.snapshot(); len(events) != 1 || events[0].Type != EventRunReclaimed {
				t.Errorf("terminal notification preceded reclaimed callback: %+v", events)
			}
			release()
			if !waitGroup(ctx, &e.execs, 5*time.Second) {
				t.Fatal("direct settlement did not finish")
			}
			want := []EventType{EventRunReclaimed, tc.event}
			if tc.workflow {
				want = append(want, EventWorkflowCancelled)
			}
			events := rec.snapshot()
			if len(events) != len(want) {
				t.Fatalf("direct settlement events: %+v, want %v", events, want)
			}
			if events[0].State != string(StateRunning) || events[1].State != string(tc.state) {
				t.Errorf("direct settlement reported incorrect states: %+v", events)
			}
			if events[0].EventId == events[1].EventId {
				t.Error("reclaim and settlement shared a notification id")
			}
			for i, event := range events {
				if event.TraceId != parent.TraceID().String() {
					t.Errorf("event %d lost submission trace: %+v", i, event)
				}
				if event.Type != want[i] || event.Duration != 0 {
					t.Errorf("event %d: %+v, want %s without execution duration", i, event, want[i])
				}
				if i < 2 && (event.RunId != old.Id || event.ExecutionId != executionId(claimed[0].LeaseToken) || event.ExecutionId == executionId(old.LeaseToken)) {
					t.Errorf("event %d lost reclaimed execution identity: %+v", i, event)
				}
			}
			if calls.Load() != 0 || e.Health().SlotsUsed != 0 || e.Health().Leases != 0 {
				t.Errorf("direct settlement called Executor or retained resources: calls=%d health=%+v", calls.Load(), e.Health())
			}
		})
	}
}

func TestObserverStaleDirectReclaimPublishesOnlyReclaimed(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &observerRecorder{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler, cfg.Observer = true, rec
	var calls atomic.Int32
	e := startEngine(
		t,
		pool,
		cfg,
		func(e *Engine) {
			e.Register("x", func(context.Context, *Request) (RawJSON, error) {
				calls.Add(1)
				return nil, nil
			})
		},
	)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 1}})
	id := trigger(t, e, "j", "")
	claimAsDeadHolder(t, pool, schema, "x")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, "UPDATE "+qualified(schema, "job_run")+
		" SET lease_expires_at=now()-interval '1 second' WHERE id=$1", id)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := e.st.ClaimExpired(
		ctx,
		[]string{"x"},
		1,
		"recovered",
		time.Minute,
	)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("reclaim: %+v, %v", claimed, err)
	}
	stealLease(t, pool, schema, id, "later-owner")
	before, err := e.st.GetJobRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	e.run(claimed[0])
	after, err := e.st.GetJobRun(ctx, id)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Errorf("stale direct settlement changed the later holder: before=%+v after=%+v err=%v", before, after, err)
	}
	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("stale settlement must retain only its committed claim fact: %+v", events)
	}
	if events[0].Type != EventRunReclaimed || events[0].ExecutionId != executionId(claimed[0].LeaseToken) {
		t.Errorf("stale settlement must retain only its committed claim fact: %+v", events)
	}
	if calls.Load() != 0 {
		t.Errorf("exhausted stale claim called Executor %d times", calls.Load())
	}
}
