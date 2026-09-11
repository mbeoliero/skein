package skein

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Inject one bounded round-trip delay for every statement, independent of SQL text.
type workflowLatencyTracer struct {
	queries atomic.Int64
}

func (tr *workflowLatencyTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	tr.queries.Add(1)
	select {
	case <-ctx.Done():
	case <-time.After(30 * time.Millisecond):
	}
	return ctx
}

func (*workflowLatencyTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestLargeWorkflowCreationWithRoundTripLatency(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	api := startEngine(t, pool, submitOnly(schema), nil)
	nodes := make([]Node, 500)
	for i := range nodes {
		name := fmt.Sprintf("node_%03d", i)
		declare(t, api, JobSpec{
			Name: name, ExecutorType: "x", Params: RawJSON(fmt.Sprintf(`{"index":%d}`, i)),
			Timeout: time.Duration(i%5+1) * time.Second, Retry: RetryPolicy{MaxAttempts: i%3 + 1},
		})
		nodes[i] = Node{Job: name}
		if i > 0 {
			nodes[i].Deps = []string{nodes[i-1].Job}
		}
	}
	declareWorkflow(t, api, WorkflowSpec{Name: "large", Nodes: nodes})
	tr := &workflowLatencyTracer{}
	cfg := pool.Config()
	cfg.ConnConfig.Tracer = tr
	slowPool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer slowPool.Close()
	slow, err := New(slowPool, submitOnly(schema))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), claimTimeout)
	defer cancel()
	started := time.Now()
	id, err := slow.Workflows().Trigger(ctx, "large", nil)
	t.Logf("500-node workflow: %d statements, %s elapsed with 30ms added per statement", tr.queries.Load(), time.Since(started))
	if err != nil {
		t.Fatalf("create workflow within the scheduler transaction budget: %v", err)
	}
	run, err := api.Workflows().GetRun(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Nodes) != len(nodes) || len(run.Dag) != len(nodes) {
		t.Fatalf("incomplete workflow: %d nodes, %d DAG entries", len(run.Nodes), len(run.Dag))
	}
	for i, node := range run.Nodes {
		want := StateBlocked
		if i == 0 {
			want = StatePending
		}
		if node.JobName != nodes[i].Job || node.State != want || node.ExecutorType != "x" {
			t.Fatalf("node %d: name %s, state %s, executor %s", i, node.JobName, node.State, node.ExecutorType)
		}
		var params struct {
			Index int `json:"index"`
		}
		if err := json.Unmarshal(node.Params, &params); err != nil {
			t.Fatal(err)
		}
		if params.Index != i || node.Timeout != time.Duration(i%5+1)*time.Second || node.RetryPolicy.MaxAttempts != i%3+1 {
			t.Fatalf("node %d snapshot changed: params %s, timeout %s, retry %+v", i, node.Params, node.Timeout, node.RetryPolicy)
		}
	}
}

func TestWorkflowNodeInsertFailureRollsBackParent(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	for _, name := range []string{"a", "b"} {
		declare(t, e, JobSpec{Name: name, ExecutorType: "x"})
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "a"}, {Job: "b", Deps: []string{"a"}}}})
	fn := qualified(schema, "reject_node")
	execSql(t, pool, "CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.job_name = 'b' THEN RAISE EXCEPTION 'reject second node'; END IF; RETURN NEW; END $$`)
	execSql(t, pool, "CREATE TRIGGER reject_node BEFORE INSERT ON "+qualified(schema, "job_run")+
		" FOR EACH ROW EXECUTE FUNCTION "+fn+"()")
	if _, err := e.Workflows().Trigger(t.Context(), "w", nil, DedupKey("same")); err == nil {
		t.Fatal("workflow succeeded despite rejected node")
	}
	page, _, err := e.Workflows().ListRuns(t.Context(), WorkflowRunFilter{}, 0)
	if err != nil || len(page) != 0 {
		t.Fatalf("parent survived rollback: %d rows, %v", len(page), err)
	}
	nodes, _, err := e.Runs().List(t.Context(), RunFilter{}, 0)
	if err != nil || len(nodes) != 0 {
		t.Fatalf("nodes survived rollback: %d rows, %v", len(nodes), err)
	}
	execSql(t, pool, "DROP TRIGGER reject_node ON "+qualified(schema, "job_run"))
	id, err := e.Workflows().Trigger(t.Context(), "w", nil, DedupKey("same"))
	if err != nil || id == 0 {
		t.Fatalf("retry after rollback: %d, %v", id, err)
	}
	duplicate, err := e.Workflows().Trigger(t.Context(), "w", nil, DedupKey("same"))
	if !errors.Is(err, ErrDuplicate) || duplicate != id {
		t.Fatalf("dedup after successful batch: %d, %v", duplicate, err)
	}
}
