package store

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// NodeDef is one declared workflow node: a job and its direct predecessors.
type NodeDef struct {
	JobName string
	Deps    []string
}

// Dag is workflow_run.dag: job name → direct predecessors.
type Dag map[string][]string

func parseDag(raw []byte) (Dag, error) {
	var d Dag
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("store: dag: %w", err)
	}
	return d, nil
}

// ───────────── definitions (§6.1) ─────────────

// MissingJobs reports which of names have no job definition.
func (s *Store) MissingJobs(ctx context.Context, names []string) (missing []string, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		found, err := s.q.ExistingJobs(ctx, tx, names)
		if err != nil {
			return err
		}
		have := map[string]bool{}
		for _, n := range found {
			have[n] = true
		}
		for _, n := range names {
			if !have[n] {
				missing = append(missing, n)
			}
		}
		return nil
	})
	return missing, err
}

// DeclareWorkflow upserts the workflow row and replaces its nodes.
func (s *Store) DeclareWorkflow(ctx context.Context, name string, nodes []NodeDef) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.q.DeclareWorkflow(ctx, tx, name); err != nil {
			return err
		}
		if err := s.q.DeleteWorkflowNodes(ctx, tx, name); err != nil {
			return err
		}
		for _, n := range nodes {
			deps := n.Deps
			if deps == nil {
				deps = []string{}
			}
			err := s.q.InsertWorkflowNode(ctx, tx, InsertWorkflowNodeParams{WorkflowName: name, JobName: n.JobName, Deps: deps})
			if isPgCode(err, "23503") {
				return fmt.Errorf("%w: job %q", ErrNotFound, n.JobName)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) DeleteWorkflow(ctx context.Context, name string) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		n, err := s.q.DeleteWorkflow(ctx, tx, name)
		if isReferenced(err) {
			return ErrReferenced
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ───────────── submit (§6.1) ─────────────

// TriggerWorkflow snapshots the definition into a workflow_run plus one job_run per
// node. With tx == nil it uses its own transaction. ErrDuplicate carries the id of the
// in-flight run holding the dedup key (0 if it finished meanwhile).
func (s *Store) TriggerWorkflow(ctx context.Context, tx pgx.Tx, p TriggerWorkflowParams) (id int64, err error) {
	fn := func(ctx context.Context, tx pgx.Tx) error {
		id, err = s.triggerWorkflow(ctx, tx, p)
		return err
	}
	if tx == nil {
		err = s.tx(ctx, fn)
	} else {
		err = s.inCallerTx(ctx, tx, fn)
	}
	return id, err
}

// triggerWorkflow fills p.Dag itself from the definition.
func (s *Store) triggerWorkflow(ctx context.Context, tx pgx.Tx, p TriggerWorkflowParams) (int64, error) {
	nodes, err := s.q.WorkflowNodes(ctx, tx, p.WorkflowName)
	if err != nil {
		return 0, err
	}
	if len(nodes) == 0 {
		return 0, ErrNotFound
	}
	dag := Dag{}
	for _, n := range nodes {
		deps := n.Deps
		if deps == nil {
			deps = []string{}
		}
		dag[n.JobName] = deps
	}
	if p.Dag, err = json.Marshal(dag); err != nil {
		return 0, err
	}
	var id int64
	inserted := false
	for range dedupRounds { // same loop as triggerJob (§6.1)
		id, err = s.q.TriggerWorkflow(ctx, tx, p)
		if err == nil {
			inserted = true
			break
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
		if p.DedupKey == nil {
			return 0, ErrDuplicate
		}
		id, err = s.q.FindInflightWorkflowRun(ctx, tx, FindInflightWorkflowRunParams{WorkflowName: p.WorkflowName, DedupKey: *p.DedupKey})
		if err == nil {
			return id, ErrDuplicate
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
	}
	if !inserted {
		return 0, ErrDuplicate
	}
	for _, n := range nodes {
		state := "pending"
		if len(n.Deps) > 0 {
			state = "blocked"
		}
		err := s.q.InsertNodeRun(ctx, tx, InsertNodeRunParams{
			JobName: n.JobName, WorkflowRunId: id, ExecutorType: n.ExecutorType,
			Params: n.Params, Timeout: n.Timeout, RetryPolicy: n.RetryPolicy, State: state,
		})
		if err != nil {
			return 0, err
		}
	}
	return id, nil
}

func (s *Store) GetWorkflowRun(ctx context.Context, id int64) (w WorkflowRun, nodes []JobRun, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.q.WorkflowRunWithNodes(ctx, tx, id)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return ErrNotFound
		}
		w = rows[0].WorkflowRun
		nodes = make([]JobRun, 0, len(rows))
		for _, r := range rows {
			nodes = append(nodes, r.JobRun)
		}
		return nil
	})
	return w, nodes, err
}

// ───────────── claim, node side (§6.3 steps 2 and 4) ─────────────

type NodeContext struct {
	Cancelling bool // the parent is no longer running: settle cancelled, do not execute
	Input      []byte
	Outputs    map[string][]byte // direct predecessors' output by job name
}

func (s *Store) NodeContext(ctx context.Context, workflowRunId int64, jobName string) (nc NodeContext, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		h, err := s.q.WorkflowRunHeader(ctx, tx, workflowRunId)
		if err != nil {
			return err
		}
		if h.State != "running" {
			nc.Cancelling = true
			return nil
		}
		dag, err := parseDag(h.Dag)
		if err != nil {
			return err
		}
		nc.Input = h.Input
		nc.Outputs = map[string][]byte{}
		if deps := dag[jobName]; len(deps) > 0 {
			rows, err := s.q.NodeOutputs(ctx, tx, NodeOutputsParams{WorkflowRunId: workflowRunId, JobNames: deps})
			if err != nil {
				return err
			}
			for _, r := range rows {
				nc.Outputs[r.JobName] = r.Output
			}
		}
		return nil
	})
	return nc, err
}

// ───────────── propagate and finalize (§6.5) ─────────────

func terminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled"
}

// propagate runs inside settle, under the parent's row lock, after node reached the
// terminal state newState.
func (s *Store) propagate(ctx context.Context, tx pgx.Tx, wf int64, dag Dag, wfState, node, newState string) error {
	if newState == "failed" && wfState == "running" { // fail-fast, written before the nodes are read
		if _, err := s.q.MarkWorkflowCancelling(ctx, tx, wf); err != nil {
			return err
		}
		wfState = "cancelling"
		entry, _ := json.Marshal([]map[string]any{{"attempt": 0, "at": time.Now().UTC(), "kind": "upstream_failed", "message": node + " failed"}})
		if _, err := s.q.CancelUnstartedNodes(ctx, tx, CancelUnstartedNodesParams{WorkflowRunId: wf, Err: entry}); err != nil {
			return err
		}
	}
	// Read after cancellation: a skipped claim may still show as pending. Neither
	// pending nor running nodes can become terminal without this parent lock (§6.5).
	rows, err := s.q.NodeStates(ctx, tx, wf)
	if err != nil {
		return err
	}
	states := make(map[string]string, len(rows))
	for _, r := range rows {
		states[r.JobName] = r.State
	}
	states[node] = newState

	if newState == "succeeded" && wfState == "running" {
		var ready []string
		for name, st := range states {
			if st != "blocked" {
				continue
			}
			ok := true
			for _, dep := range dag[name] {
				if states[dep] != "succeeded" {
					ok = false
					break
				}
			}
			if ok {
				ready = append(ready, name)
			}
		}
		if len(ready) > 0 {
			if _, err := s.q.ActivateNodes(ctx, tx, ActivateNodesParams{WorkflowRunId: wf, JobNames: ready}); err != nil {
				return err
			}
			for _, name := range ready {
				states[name] = "pending"
			}
		}
	}
	if final, done := finalState(states, wfState); done {
		return s.q.FinalizeWorkflowRun(ctx, tx, FinalizeWorkflowRunParams{Id: wf, State: final})
	}
	return nil
}

// finalState: once every node is terminal, any failed → failed; else cancelling → cancelled; else succeeded.
func finalState(states map[string]string, wfState string) (string, bool) {
	failed := false
	for _, st := range states {
		if !terminal(st) {
			return "", false
		}
		failed = failed || st == "failed"
	}
	switch {
	case failed:
		return "failed", true
	case wfState == "cancelling":
		return "cancelled", true
	}
	return "succeeded", true
}

// ───────────── cancel (§6.6) ─────────────

// CancelWorkflow ends unlocked unstarted nodes; skipped claims and running nodes
// learn cancellation through the parent check or heartbeat. Repeated cancellation is a no-op.
func (s *Store) CancelWorkflow(ctx context.Context, id int64) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		n, err := s.q.MarkWorkflowCancelling(ctx, tx, id)
		if err != nil {
			return err
		}
		if n == 0 {
			if _, err := s.q.GetWorkflowRun(ctx, tx, id); errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if _, err := s.q.CancelUnstartedNodes(ctx, tx, CancelUnstartedNodesParams{WorkflowRunId: id, Err: []byte("[]")}); err != nil {
			return err
		}
		rows, err := s.q.NodeStates(ctx, tx, id)
		if err != nil {
			return err
		}
		states := make(map[string]string, len(rows))
		for _, r := range rows {
			states[r.JobName] = r.State
		}
		if final, done := finalState(states, "cancelling"); done {
			return s.q.FinalizeWorkflowRun(ctx, tx, FinalizeWorkflowRunParams{Id: id, State: final})
		}
		return nil
	})
}

// ───────────── resume (§6.7) ─────────────

var ErrNotResumable = errors.New("store: workflow run is not failed or cancelled")

// Resume re-queues the failed / cancelled nodes of a failed / cancelled workflow_run;
// succeeded nodes keep their output. A terminal run has only terminal nodes (finalize
// requires it), so "unsucceeded descendants" are already in that set.
func (s *Store) Resume(ctx context.Context, id int64) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		w, err := s.q.LockWorkflowRun(ctx, tx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if w.State != "failed" && w.State != "cancelled" {
			return ErrNotResumable
		}
		dag, err := parseDag(w.Dag)
		if err != nil {
			return err
		}
		rows, err := s.q.NodeStates(ctx, tx, id)
		if err != nil {
			return err
		}
		states := make(map[string]string, len(rows))
		for _, r := range rows {
			states[r.JobName] = r.State
		}
		var names, newStates []string
		for name, st := range states {
			if st != "failed" && st != "cancelled" {
				continue
			}
			next := "pending"
			for _, dep := range dag[name] {
				if states[dep] != "succeeded" {
					next = "blocked"
					break
				}
			}
			names, newStates = append(names, name), append(newStates, next)
		}
		if err := s.q.ResumeNodes(ctx, tx, ResumeNodesParams{WorkflowRunId: id, JobNames: names, States: newStates}); err != nil {
			return err
		}
		err = s.q.ReopenWorkflowRun(ctx, tx, id)
		if isPgCode(err, "23505") {
			return ErrDuplicate
		}
		return err
	})
}

func (s *Store) ListWorkflowRuns(ctx context.Context, p ListWorkflowRunsParams) (rows []WorkflowRun, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err = s.q.ListWorkflowRuns(ctx, tx, p)
		return err
	})
	return rows, err
}
