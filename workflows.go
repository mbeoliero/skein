package skein

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mbeoliero/skein/internal/store"
)

// Node is one workflow step: a declared job and the jobs it waits for.
type Node struct {
	Job  string
	Deps []string
}

// WorkflowSpec defines a nonempty DAG of declared jobs, with each job used once.
type WorkflowSpec struct {
	Name  string
	Nodes []Node
}

// WorkflowState is the persisted aggregate state of a workflow run.
type WorkflowState string

const (
	WorkflowRunning    WorkflowState = "running"
	WorkflowCancelling WorkflowState = "cancelling"
	WorkflowSucceeded  WorkflowState = "succeeded"
	WorkflowFailed     WorkflowState = "failed"
	WorkflowCancelled  WorkflowState = "cancelled"
)

func (s WorkflowState) Terminal() bool {
	return s == WorkflowSucceeded || s == WorkflowFailed || s == WorkflowCancelled
}

// WorkflowRun is one execution of a workflow; its nodes are JobRuns.
type WorkflowRun struct {
	Id           int64
	WorkflowName string
	ScheduleName string
	ScheduledAt  *time.Time
	DedupKey     string
	Input        RawJSON
	Extra        RawJSON             // library-managed metadata; tracing belongs to this parent, not each node
	Dag          map[string][]string // job name → direct predecessors, snapshot at trigger
	State        WorkflowState
	CreatedAt    time.Time
	FinishedAt   *time.Time
	Nodes        []JobRun // ordered by job name
}

// Workflows declares DAGs and submits, queries or controls their runs.
type Workflows struct{ e *Engine }

func (e *Engine) Workflows() *Workflows { return &Workflows{e: e} }

// Declare validates the graph (§2.1) and replaces the definition.
func (w *Workflows) Declare(ctx context.Context, spec WorkflowSpec) error {
	if err := validName("workflow", spec.Name); err != nil {
		return err
	}
	nodes, err := validateWorkflow(spec, w.e.cfg.MaxNodes)
	if err != nil {
		return fmt.Errorf("skein: workflow %q: %w", spec.Name, err)
	}
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.JobName
	}
	missing, err := w.e.st.MissingJobs(ctx, names)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: workflow %q references undeclared jobs %q", ErrNotFound, spec.Name, missing)
	}
	return mapErr(w.e.st.DeclareWorkflow(ctx, spec.Name, nodes), spec.Name)
}

// Kahn's algorithm detects dependency cycles.
func validateWorkflow(spec WorkflowSpec, maxNodes int) ([]store.NodeDef, error) {
	if len(spec.Nodes) == 0 {
		return nil, errors.New("needs at least one node")
	}
	if len(spec.Nodes) > maxNodes {
		return nil, fmt.Errorf("%d nodes exceeds MaxNodes %d", len(spec.Nodes), maxNodes)
	}
	indegree := make(map[string]int, len(spec.Nodes))
	for _, n := range spec.Nodes {
		if n.Job == "" {
			return nil, errors.New("node without a job name")
		}
		if _, dup := indegree[n.Job]; dup {
			return nil, fmt.Errorf("job %q appears twice", n.Job)
		}
		indegree[n.Job] = 0
	}
	successors := map[string][]string{}
	defs := make([]store.NodeDef, 0, len(spec.Nodes))
	for _, n := range spec.Nodes {
		deps := slices.Compact(slices.Sorted(slices.Values(n.Deps)))
		for _, d := range deps {
			if d == n.Job {
				return nil, fmt.Errorf("job %q depends on itself", n.Job)
			}
			if _, ok := indegree[d]; !ok {
				return nil, fmt.Errorf("job %q depends on %q, which is not a node", n.Job, d)
			}
			successors[d] = append(successors[d], n.Job)
		}
		indegree[n.Job] = len(deps)
		defs = append(defs, store.NodeDef{JobName: n.Job, Deps: deps})
	}
	var queue []string
	for name, deg := range indegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	seen := 0
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		seen++
		for _, s := range successors[name] {
			if indegree[s]--; indegree[s] == 0 {
				queue = append(queue, s)
			}
		}
	}
	if seen != len(spec.Nodes) {
		return nil, errors.New("dependency cycle")
	}
	return defs, nil
}

// Delete removes the definition and its nodes; a workflow used by a schedule is ErrReferenced.
func (w *Workflows) Delete(ctx context.Context, name string) error {
	return mapErr(w.e.st.DeleteWorkflow(ctx, name), name)
}

// Trigger creates a workflow run with input readable by every node. DedupKey applies;
// At is rejected (nodes without predecessors start at once).
func (w *Workflows) Trigger(ctx context.Context, name string, input RawJSON, opts ...TriggerOption) (int64, error) {
	id, err := w.trigger(ctx, nil, name, input, opts)
	if err == nil {
		w.e.wakeClaimer()
	}
	return id, err
}

// TriggerTx applies Trigger inside the caller's transaction. It accepts DedupKey
// and rejects At. Commit visibility, duplicate ids and search_path restoration
// follow Jobs.TriggerTx; the caller owns commit and rollback.
func (w *Workflows) TriggerTx(ctx context.Context, tx pgx.Tx, name string, input RawJSON, opts ...TriggerOption) (int64, error) {
	if tx == nil {
		return 0, errors.New("skein: TriggerTx needs a transaction")
	}
	return w.trigger(ctx, tx, name, input, opts)
}

func (w *Workflows) trigger(ctx context.Context, tx pgx.Tx, name string, input RawJSON, opts []TriggerOption) (int64, error) {
	var o triggerOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.at != nil {
		return 0, errors.New("skein: At is not supported for workflows")
	}
	in, err := w.e.objectPayload(input)
	if err != nil {
		return 0, fmt.Errorf("skein: trigger workflow %q input: %w", name, err)
	}
	id, err := w.e.st.TriggerWorkflow(ctx, tx, store.TriggerWorkflowParams{
		WorkflowName: name, DedupKey: o.dedup, Input: in, Extra: traceExtra(ctx),
	})
	return id, mapErr(err, name)
}

// GetRun returns the workflow run with all its nodes.
func (w *Workflows) GetRun(ctx context.Context, id int64) (*WorkflowRun, error) {
	row, nodes, err := w.e.st.GetWorkflowRun(ctx, id)
	if err != nil {
		return nil, mapErr(err, strconv.FormatInt(id, 10))
	}
	run, err := workflowRunFromStore(row)
	if err != nil {
		return nil, err
	}
	run.Nodes = make([]JobRun, 0, len(nodes))
	for _, n := range nodes {
		run.Nodes = append(run.Nodes, *jobRunFromStore(n))
	}
	return run, nil
}

func workflowRunFromStore(row store.WorkflowRun) (*WorkflowRun, error) {
	run := &WorkflowRun{
		Id: row.Id, WorkflowName: row.WorkflowName, ScheduleName: deref(row.ScheduleName), ScheduledAt: row.ScheduledAt,
		DedupKey: deref(row.DedupKey), Input: RawJSON(row.Input), Extra: RawJSON(row.Extra), State: WorkflowState(row.State),
		CreatedAt: row.CreatedAt, FinishedAt: row.FinishedAt,
	}
	if err := json.Unmarshal(row.Dag, &run.Dag); err != nil {
		return nil, fmt.Errorf("skein: workflow run %d dag: %w", row.Id, err)
	}
	return run, nil
}

// WorkflowRunFilter restricts a workflow page; empty fields do not filter.
type WorkflowRunFilter struct {
	WorkflowName string
	State        WorkflowState
	Limit        int // default 50, at most 500
}

// ListRuns pages newest first without nodes; cursor is the last id of the previous
// page (0 for the first), next is 0 when there is no further page.
func (w *Workflows) ListRuns(ctx context.Context, f WorkflowRunFilter, cursor int64) (page []WorkflowRun, next int64, err error) {
	limit := pageLimit(f.Limit)
	rows, err := w.e.st.ListWorkflowRuns(ctx, store.ListWorkflowRunsParams{
		WorkflowName: optional(f.WorkflowName), State: optional(string(f.State)), Cursor: cursor, Lim: int32(limit + 1),
	})
	if err != nil {
		return nil, 0, err
	}
	for _, r := range rows[:min(len(rows), limit)] {
		run, err := workflowRunFromStore(r)
		if err != nil {
			return nil, 0, err
		}
		page = append(page, *run)
	}
	if len(rows) > limit {
		next = page[len(page)-1].Id
	}
	return page, next, nil
}

// CancelRun stops a workflow: unstarted nodes end now, running nodes are cancelled
// through their heartbeat, and the last one to settle finalises the run (§2.5).
func (w *Workflows) CancelRun(ctx context.Context, id int64) error {
	changes, err := w.e.st.CancelWorkflow(ctx, id)
	if err == nil {
		w.e.observe(changes)
	}
	return mapErr(err, strconv.FormatInt(id, 10))
}

// Resume re-runs the failed / cancelled nodes and their unsucceeded descendants of a
// failed or cancelled workflow run; succeeded nodes keep their output (§2.5).
// ErrNotResumable also covers a cancelled workflow whose nodes all succeeded: its
// terminal state is preserved. Scheduled runs obey the current overlap policy;
// ErrDuplicate rolls back every node reset and leaves the parent unchanged.
func (w *Workflows) Resume(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("skein: workflow run id must be positive")
	}
	changes, err := w.e.st.Resume(ctx, id)
	if err == nil {
		w.e.wakeClaimer()
		w.e.observe(changes)
	}
	return mapErr(err, strconv.FormatInt(id, 10))
}
