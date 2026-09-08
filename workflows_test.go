package skein

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

// M3: workflows (design §9). Executors record what they were handed so the tests can
// check inputs, predecessor outputs and idempotency keys, not just final states.

type recorder struct {
	mu   sync.Mutex
	reqs map[string][]*Request
}

func (r *recorder) add(req *Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reqs == nil {
		r.reqs = map[string][]*Request{}
	}
	r.reqs[req.JobName] = append(r.reqs[req.JobName], req)
}

func (r *recorder) calls(job string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reqs[job])
}

func (r *recorder) last(job string) *Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rs := r.reqs[job]; len(rs) > 0 {
		return rs[len(rs)-1]
	}
	return nil
}

func declareWorkflow(t *testing.T, e *Engine, spec WorkflowSpec) {
	t.Helper()
	if err := e.Workflows().Declare(t.Context(), spec); err != nil {
		t.Fatalf("declare workflow %s: %v", spec.Name, err)
	}
}

func triggerWorkflow(t *testing.T, e *Engine, name, input string, opts ...TriggerOption) int64 {
	t.Helper()
	id, err := e.Workflows().Trigger(t.Context(), name, RawJSON(input), opts...)
	if err != nil {
		t.Fatalf("trigger workflow %s: %v", name, err)
	}
	return id
}

func waitWorkflow(t *testing.T, e *Engine, id int64, state WorkflowState) *WorkflowRun {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		run, err := e.Workflows().GetRun(t.Context(), id)
		if err != nil {
			t.Fatalf("get workflow run %d: %v", id, err)
		}
		if run.State == state {
			return run
		}
		if time.Now().After(deadline) {
			var nodes []string
			for _, n := range run.Nodes {
				nodes = append(nodes, fmt.Sprintf("%s=%s", n.JobName, n.State))
			}
			t.Fatalf("workflow %d is %s, want %s; nodes %s", id, run.State, state, strings.Join(nodes, " "))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func nodeOf(t *testing.T, run *WorkflowRun, job string) *JobRun {
	t.Helper()
	for i := range run.Nodes {
		if run.Nodes[i].JobName == job {
			return &run.Nodes[i]
		}
	}
	t.Fatalf("workflow %d has no node %s", run.Id, job)
	return nil
}

func waitNode(t *testing.T, e *Engine, wf int64, job string, state RunState) {
	t.Helper()
	waitFor(t, job+" to be "+string(state), func() bool {
		run, err := e.Workflows().GetRun(t.Context(), wf)
		return err == nil && nodeOf(t, run, job).State == state
	})
}

func TestWorkflowDeclareValidation(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := submitOnly(schema)
	cfg.MaxNodes = 3
	e := startEngine(t, pool, cfg, nil)
	for _, j := range []string{"a", "b", "c", "d"} {
		declare(t, e, JobSpec{Name: j, ExecutorType: "x"})
	}
	cases := []struct {
		name  string
		nodes []Node
		want  string
	}{
		{"empty", nil, "at least one node"},
		{"cycle", []Node{{Job: "a", Deps: []string{"b"}}, {Job: "b", Deps: []string{"a"}}}, "cycle"},
		{"self", []Node{{Job: "a", Deps: []string{"a"}}}, "itself"},
		{"unknown dep", []Node{{Job: "a", Deps: []string{"zzz"}}}, "not a node"},
		{"duplicate", []Node{{Job: "a"}, {Job: "a"}}, "twice"},
		{"too many", []Node{{Job: "a"}, {Job: "b"}, {Job: "c"}, {Job: "d"}}, "MaxNodes"},
	}
	for _, c := range cases {
		err := e.Workflows().Declare(t.Context(), WorkflowSpec{Name: "w", Nodes: c.nodes})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
		}
	}
	if err := e.Workflows().Declare(t.Context(), WorkflowSpec{Name: "w", Nodes: []Node{{Job: "nope"}}}); !errors.Is(err, ErrNotFound) {
		t.Errorf("undeclared job: %v", err)
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}, {Job: "b", Deps: []string{"a"}}, {Job: "c", Deps: []string{"a", "b"}}}})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}}}) // redeclare replaces the nodes
	if err := e.Jobs().Delete(t.Context(), "a"); !errors.Is(err, ErrReferenced) {
		t.Errorf("delete referenced job: %v", err)
	}
	if err := e.Workflows().Delete(t.Context(), "w"); err != nil {
		t.Errorf("delete workflow: %v", err)
	}
	if err := e.Jobs().Delete(t.Context(), "a"); err != nil {
		t.Errorf("delete job after workflow: %v", err)
	}
}

func TestWorkflowLinearRunsInOrder(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	var rec recorder
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("step", func(ctx context.Context, req *Request) (RawJSON, error) {
			rec.add(req)
			return RawJSON(`{"node":"` + req.JobName + `"}`), nil
		})
	})
	for _, j := range []string{"a", "b", "c"} {
		declare(t, e, JobSpec{Name: j, ExecutorType: "step"})
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}, {Job: "b", Deps: []string{"a"}}, {Job: "c", Deps: []string{"b"}}}})
	id := triggerWorkflow(t, e, "w", `{"k":1}`)
	run := waitWorkflow(t, e, id, WorkflowSucceeded)

	a, b, c := nodeOf(t, run, "a"), nodeOf(t, run, "b"), nodeOf(t, run, "c")
	if a.FinishedAt.After(*b.StartedAt) || b.FinishedAt.After(*c.StartedAt) {
		t.Errorf("nodes overlapped: a %v b %v..%v c %v", a.FinishedAt, b.StartedAt, b.FinishedAt, c.StartedAt)
	}
	req := rec.last("b")
	if string(req.Input) != `{"k": 1}` || string(req.Deps["a"]) != `{"node": "a"}` || len(req.Deps) != 1 {
		t.Errorf("b request input %s deps %v", req.Input, req.Deps)
	}
	if req.IdempotencyKey != fmt.Sprintf("wf:%d/b", id) || req.WorkflowRunId == nil || *req.WorkflowRunId != id {
		t.Errorf("b request key %q wf %v", req.IdempotencyKey, req.WorkflowRunId)
	}
	if len(run.Dag["c"]) != 1 || run.Dag["c"][0] != "b" || len(run.Dag["a"]) != 0 || run.FinishedAt == nil {
		t.Errorf("dag %v finished %v", run.Dag, run.FinishedAt)
	}
}

// 100 parallel predecessors finish at the same time; the join runs exactly once. The
// process has 100 slots and every predecessor waits until all of them are inside the
// executor, so their settles really do race for the join (§9 M3).
func TestFanInActivatesOnce(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	const fan = 100
	var rec recorder
	var entered atomic.Int32
	release := make(chan struct{})
	cfg := fastConfig(schema)
	cfg.Concurrency = fan
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("fast", func(ctx context.Context, req *Request) (RawJSON, error) {
			rec.add(req)
			if req.JobName != "join" {
				if entered.Add(1) == fan {
					close(release)
				}
				<-release
			}
			return nil, nil
		})
	})
	nodes := make([]Node, 0, fan+1)
	deps := make([]string, 0, fan)
	for i := range fan {
		name := fmt.Sprintf("p%02d", i)
		declare(t, e, JobSpec{Name: name, ExecutorType: "fast"})
		nodes = append(nodes, Node{Job: name})
		deps = append(deps, name)
	}
	declare(t, e, JobSpec{Name: "join", ExecutorType: "fast"})
	nodes = append(nodes, Node{Job: "join", Deps: deps})
	declareWorkflow(t, e, WorkflowSpec{Name: "fan", Nodes: nodes})

	run := waitWorkflow(t, e, triggerWorkflow(t, e, "fan", ""), WorkflowSucceeded)
	if rec.calls("join") != 1 || nodeOf(t, run, "join").Attempt != 0 {
		t.Fatalf("join ran %d times", rec.calls("join"))
	}
	if len(rec.last("join").Deps) != fan {
		t.Fatalf("join saw %d predecessor outputs", len(rec.last("join").Deps))
	}
}

// One node fails for good: unstarted nodes are cancelled with upstream_failed, the
// running sibling is cancelled through its heartbeat, the run ends failed with
// exactly one failed node.
func TestFailFastCancelsTheRest(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cause := make(chan error, 1)
	slowStarted := make(chan struct{})
	slowRunning := sync.OnceFunc(func() { close(slowStarted) })
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("bad", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-slowStarted // slow is executing when bad fails: it must be cancelled through its heartbeat, not as unstarted
			return nil, Permanent(errors.New("nope"))
		})
		e.Register("slow", func(ctx context.Context, req *Request) (RawJSON, error) {
			slowRunning()
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return nil, ctx.Err()
		})
		e.Register("ok", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "bad", ExecutorType: "bad"})
	declare(t, e, JobSpec{Name: "slow", ExecutorType: "slow"})
	declare(t, e, JobSpec{Name: "after", ExecutorType: "ok"})
	declare(t, e, JobSpec{Name: "never", ExecutorType: "ok"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{
		{Job: "bad"}, {Job: "slow"}, {Job: "after", Deps: []string{"bad"}}, {Job: "never", Deps: []string{"slow"}},
	}})
	run := waitWorkflow(t, e, triggerWorkflow(t, e, "w", ""), WorkflowFailed)

	select {
	case c := <-cause:
		if !errors.Is(c, errCancelRequested) {
			t.Errorf("slow cancelled with %v", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow never reported its cancellation cause")
	}
	failed := 0
	for _, n := range run.Nodes {
		if !n.State.Terminal() {
			t.Errorf("%s is %s", n.JobName, n.State)
		}
		if n.State == StateFailed {
			failed++
		}
	}
	after := nodeOf(t, run, "after")
	if es := errorsOf(t, after); failed != 1 || after.State != StateCancelled || len(es) != 1 || es[0].Kind != "upstream_failed" {
		t.Errorf("failed nodes %d, after %s %s", failed, after.State, after.Errors)
	}
	if nodeOf(t, run, "slow").State != StateCancelled || nodeOf(t, run, "never").State != StateCancelled {
		t.Errorf("slow %s never %s", nodeOf(t, run, "slow").State, nodeOf(t, run, "never").State)
	}
}

// x fails while y, its pending sibling, is being claimed: the fail-fast cancel waits
// for the claim's lock and skips y, so the node states must be read after it. Read
// before, y counts as cancelled and the run ends failed with y executing (§6.5).
func TestCancellationSkipsClaimInProgress(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fail   bool
		commit bool
	}{
		{name: "fail_fast/commit", fail: true, commit: true},
		{name: "fail_fast/rollback", fail: true},
		{name: "cancel/commit", commit: true},
		{name: "cancel/rollback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			calls := 0
			e := startEngine(t, pool, submitOnly(schema), func(e *Engine) {
				e.Register("y", func(context.Context, *Request) (RawJSON, error) {
					calls++
					return nil, nil
				})
			})
			for _, name := range []string{"x", "y", "z"} {
				declare(t, e, JobSpec{Name: name, ExecutorType: name})
			}
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{
				{Job: "x"}, {Job: "y"}, {Job: "z", Deps: []string{"y"}},
			}})
			id := triggerWorkflow(t, e, "w", "")
			x := claimAsDeadHolder(t, pool, schema, "x")

			// Keep the real claim uncommitted: cancellation must skip y, not wait
			// for a tuple that may already be running when its predicate is rechecked.
			tx, err := pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			execSql(t, tx, "SET LOCAL search_path TO "+schema)
			rows, err := store.New().ClaimPending(t.Context(), tx, store.ClaimPendingParams{
				ExecutorTypes: []string{"y"}, Lim: 1, Owner: "claim", LeaseTtl: time.Minute,
			})
			if err != nil || len(rows) != 1 {
				t.Fatalf("claim y: %v %v", rows, err)
			}
			y := store.Claimed(rows[0])
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if tc.fail {
				s := settlementFor(x, store.Failed)
				s.Err = encodeErr(errEntry{Attempt: 1, Kind: "business", Message: "x"})
				_, err = e.st.Settle(ctx, s)
			} else {
				err = e.Workflows().CancelRun(ctx, id)
			}
			if err != nil {
				t.Fatalf("cancellation must finish while the claim holds y: %v", err)
			}
			run, err := e.Workflows().GetRun(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if run.State != WorkflowCancelling || nodeOf(t, run, "y").State != StatePending || nodeOf(t, run, "z").State != StateCancelled {
				t.Fatalf("workflow %s, y %s, z %s", run.State, nodeOf(t, run, "y").State, nodeOf(t, run, "z").State)
			}
			if tc.commit {
				if err := tx.Commit(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := tx.Rollback(t.Context()); err != nil {
					t.Fatal(err)
				}
				y = claimAsDeadHolder(t, pool, schema, "y")
			}
			e.run(y)
			if !tc.fail {
				if _, err := e.st.Settle(t.Context(), settlementFor(x, store.Cancelled)); err != nil {
					t.Fatal(err)
				}
			}
			want := WorkflowCancelled
			if tc.fail {
				want = WorkflowFailed
			}
			run, err = e.Workflows().GetRun(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if calls != 0 || run.State != want || nodeOf(t, run, "y").State != StateCancelled {
				t.Fatalf("executor calls %d, workflow %s, y %s", calls, run.State, nodeOf(t, run, "y").State)
			}
		})
	}
}

func TestCancelWorkflow(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("slow", func(ctx context.Context, req *Request) (RawJSON, error) { <-ctx.Done(); return nil, ctx.Err() })
	})
	declare(t, e, JobSpec{Name: "slow", ExecutorType: "slow"})
	declare(t, e, JobSpec{Name: "after", ExecutorType: "slow"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "slow"}, {Job: "after", Deps: []string{"slow"}}}})
	id := triggerWorkflow(t, e, "w", "")
	waitNode(t, e, id, "slow", StateRunning)

	if err := e.Workflows().CancelRun(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	run := waitWorkflow(t, e, id, WorkflowCancelled)
	slow, after := nodeOf(t, run, "slow"), nodeOf(t, run, "after")
	if slow.State != StateCancelled || after.State != StateCancelled || string(after.Errors) != "[]" || run.FinishedAt == nil {
		t.Fatalf("slow %s after %s %s", slow.State, after.State, after.Errors)
	}
	if err := e.Workflows().CancelRun(t.Context(), id); err != nil {
		t.Errorf("second cancel: %v", err)
	}
	if err := e.Workflows().CancelRun(t.Context(), id+1000); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown run: %v", err)
	}
	if err := e.Runs().Cancel(t.Context(), slow.Id); err == nil || !strings.Contains(err.Error(), "workflow") {
		t.Errorf("cancelling a node directly: %v", err)
	}
}

// Resume re-runs only the failed chain; succeeded nodes keep their start time and
// output. b fails only once the test has seen d persisted as succeeded: a node still
// inside its executor when the parent turns cancelling is settled cancelled through
// the heartbeat, which would make d a cancelled sibling and the check meaningless.
func TestResumeRerunsFailedChain(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	var rec recorder
	var fixed sync.Map
	bMayFail := make(chan struct{})
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("ok", func(ctx context.Context, req *Request) (RawJSON, error) {
			rec.add(req)
			return RawJSON(`"` + req.JobName + `"`), nil
		})
		e.Register("flaky", func(ctx context.Context, req *Request) (RawJSON, error) {
			rec.add(req)
			if _, ok := fixed.Load("b"); !ok {
				<-bMayFail
				return nil, Permanent(errors.New("not yet"))
			}
			return RawJSON(`"b"`), nil
		})
	})
	declare(t, e, JobSpec{Name: "a", ExecutorType: "ok"})
	declare(t, e, JobSpec{Name: "b", ExecutorType: "flaky"})
	declare(t, e, JobSpec{Name: "c", ExecutorType: "ok"})
	declare(t, e, JobSpec{Name: "d", ExecutorType: "ok"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{
		{Job: "a"}, {Job: "b", Deps: []string{"a"}}, {Job: "c", Deps: []string{"b"}}, {Job: "d", Deps: []string{"a"}},
	}})
	id := triggerWorkflow(t, e, "w", "")
	waitNode(t, e, id, "d", StateSucceeded)
	close(bMayFail)
	failedRun := waitWorkflow(t, e, id, WorkflowFailed)
	aStarted := *nodeOf(t, failedRun, "a").StartedAt
	if nodeOf(t, failedRun, "c").State != StateCancelled || nodeOf(t, failedRun, "d").State != StateSucceeded {
		t.Fatalf("c is %s, d is %s", nodeOf(t, failedRun, "c").State, nodeOf(t, failedRun, "d").State)
	}

	fixed.Store("b", true)
	if err := e.Workflows().Resume(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	run := waitWorkflow(t, e, id, WorkflowSucceeded)
	a, b, c := nodeOf(t, run, "a"), nodeOf(t, run, "b"), nodeOf(t, run, "c")
	if rec.calls("a") != 1 || rec.calls("d") != 1 || !a.StartedAt.Equal(aStarted) || string(a.Output) != `"a"` {
		t.Errorf("succeeded nodes were touched: a calls %d d calls %d started %v/%v", rec.calls("a"), rec.calls("d"), a.StartedAt, aStarted)
	}
	if rec.calls("b") != 2 || b.Attempt != 0 || len(errorsOf(t, b)) != 1 || c.State != StateSucceeded || string(rec.last("c").Deps["b"]) != `"b"` {
		t.Errorf("b calls %d attempt %d errors %s; c %s deps %v", rec.calls("b"), b.Attempt, b.Errors, c.State, rec.last("c").Deps)
	}
	if err := e.Workflows().Resume(t.Context(), id); !errors.Is(err, ErrNotResumable) {
		t.Errorf("resume succeeded run: %v", err)
	}
	if err := e.Workflows().Resume(t.Context(), id+1000); !errors.Is(err, ErrNotFound) {
		t.Errorf("resume unknown: %v", err)
	}
}

// Concurrent workflow Triggers with the same key: one run, every other caller gets
// its id with ErrDuplicate; a loser's INSERT waits for the winner to commit and
// FindInflightWorkflowRun reads a fresh snapshot.
func TestConcurrentWorkflowDedupCreatesOne(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	release := make(chan struct{})
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("block", func(ctx context.Context, req *Request) (RawJSON, error) {
			<-release
			return nil, nil
		})
	})
	declare(t, e, JobSpec{Name: "x", ExecutorType: "block"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "x"}}})
	const n = 16
	ids := make([]int64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { ids[i], errs[i] = e.Workflows().Trigger(t.Context(), "w", nil, DedupKey("k")) })
	}
	wg.Wait()
	winner := expectOneWinner(t, ids, errs)
	close(release)
	waitWorkflow(t, e, winner, WorkflowSucceeded)
}

// Resuming re-occupies the dedup key: an in-flight run with the same key wins.
func TestResumeRejectsDuplicateKey(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	gate := make(chan struct{})
	var first sync.Once
	var firstId int64
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) {
			isFirst := false
			first.Do(func() { firstId = *req.WorkflowRunId; isFirst = true })
			if isFirst || *req.WorkflowRunId == firstId {
				return nil, Permanent(errors.New("first run fails"))
			}
			<-gate
			return nil, nil
		})
	})
	declare(t, e, JobSpec{Name: "x", ExecutorType: "x"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "x"}}})
	id1 := triggerWorkflow(t, e, "w", "", DedupKey("k"))
	waitWorkflow(t, e, id1, WorkflowFailed)

	id2 := triggerWorkflow(t, e, "w", "", DedupKey("k")) // terminal runs free the key
	waitNode(t, e, id2, "x", StateRunning)
	if again, err := e.Workflows().Trigger(t.Context(), "w", nil, DedupKey("k")); !errors.Is(err, ErrDuplicate) || again != id2 {
		t.Errorf("third trigger: %d %v", again, err)
	}
	if err := e.Workflows().Resume(t.Context(), id1); !errors.Is(err, ErrDuplicate) {
		t.Errorf("resume while the key is held: %v", err)
	}
	close(gate)
	waitWorkflow(t, e, id2, WorkflowSucceeded)
	if err := e.Workflows().Resume(t.Context(), id1); err != nil {
		t.Errorf("resume after the key is free: %v", err)
	}
}

// A node whose holder died while the workflow was cancelled: the reclaimer sees the
// parent cancelling, settles the node cancelled and finalises the run (§6.3 step 2).
func TestReclaimedNodeSeesCancellingParent(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	sub := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, sub, JobSpec{Name: "x", ExecutorType: "x"})
	declareWorkflow(t, sub, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "x"}}})
	id := triggerWorkflow(t, sub, "w", "")
	claimAsDeadHolder(t, pool, schema, "x")
	if err := sub.Workflows().CancelRun(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if run, _ := sub.Workflows().GetRun(t.Context(), id); run.State != WorkflowCancelling {
		t.Fatalf("workflow with a running node is %s after cancel", run.State)
	}
	calls := 0
	b := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) { calls++; return nil, nil })
	})
	run := waitWorkflow(t, b, id, WorkflowCancelled)
	if calls != 0 || nodeOf(t, run, "x").State != StateCancelled || nodeOf(t, run, "x").Attempt != 1 {
		t.Fatalf("calls %d node %+v", calls, nodeOf(t, run, "x"))
	}
}

func TestWorkflowTriggerTxVisibleAfterCommit(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("ok", func(ctx context.Context, req *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "a", ExecutorType: "ok"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}}})
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	id, err := e.Workflows().TriggerTx(t.Context(), tx, "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Workflows().GetRun(t.Context(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("visible before commit: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitWorkflow(t, e, id, WorkflowSucceeded)
}
