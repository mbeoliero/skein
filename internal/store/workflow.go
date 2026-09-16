package store

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type NodeDef struct {
	JobName string
	Deps    []string
}

// Dag is workflow_run.dag: job name → direct predecessors.
type Dag map[string][]string

type nodeStates map[string]string

func statesOf(rows []NodeStatesRow) nodeStates {
	states := make(nodeStates, len(rows))
	for _, row := range rows {
		states[row.JobName] = row.State
	}
	return states
}

func (states nodeStates) depsSucceeded(deps []string) bool {
	for _, dep := range deps {
		if states[dep] != "succeeded" {
			return false
		}
	}
	return true
}

func resumable(state string) bool {
	return state == "failed" || state == "cancelled"
}

func (d Dag) resumeSet(states nodeStates) (names, next []string) {
	for name, state := range states {
		if !resumable(state) {
			continue
		}
		nextState := "blocked"
		if states.depsSucceeded(d[name]) {
			nextState = "pending"
		}
		names, next = append(names, name), append(next, nextState)
	}
	return names, next
}

func parseDag(raw []byte) (Dag, error) {
	var d Dag
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("store: dag: %w", err)
	}
	return d, nil
}

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

// TriggerWorkflow snapshots the definition into a workflow_run plus one job_run per
// node. With tx == nil it uses its own transaction. ErrDuplicate carries the id of the
// run holding the dedup key, as TriggerJob describes (0 if it vanished meanwhile).
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

// findWorkflowDedupHolder is findDedupHolder for workflow runs: schedule_name tells
// the user-key index from the overlap=skip one (§1.3).
func (s *Store) findWorkflowDedupHolder(ctx context.Context, tx pgx.Tx, p TriggerWorkflowParams) (int64, error) {
	if p.ScheduleName == nil {
		return s.q.FindDedupWorkflowRun(ctx, tx, FindDedupWorkflowRunParams{WorkflowName: p.WorkflowName, DedupKey: *p.DedupKey})
	}
	return s.q.FindInflightWorkflowBeat(ctx, tx, FindInflightWorkflowBeatParams{WorkflowName: p.WorkflowName, DedupKey: *p.DedupKey})
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
	for range dedupRounds { // same loop as triggerJob (§2.1)
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
		id, err = s.findWorkflowDedupHolder(ctx, tx, p)
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
	batch := InsertNodeRunsParams{
		WorkflowRunId: id,
		JobNames:      make([]string, len(nodes)), ExecutorTypes: make([]string, len(nodes)),
		Params: make([][]byte, len(nodes)), Timeouts: make([]int32, len(nodes)),
		RetryPolicies: make([][]byte, len(nodes)), States: make([]string, len(nodes)),
	}
	for i, n := range nodes {
		state := "pending"
		if len(n.Deps) > 0 {
			state = "blocked"
		}
		batch.JobNames[i], batch.ExecutorTypes[i] = n.JobName, n.ExecutorType
		batch.Params[i], batch.Timeouts[i], batch.RetryPolicies[i] = n.Params, n.Timeout, n.RetryPolicy
		batch.States[i] = state
	}
	if err := s.q.InsertNodeRuns(ctx, tx, batch); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) GetWorkflowRun(ctx context.Context, id int64) (w WorkflowRun, nodes []JobRun, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.q.WorkflowRunWithNodes(ctx, tx, id)
		if err != nil {
			return err
		}
		w, nodes, err = workflowRunFromRows(rows)
		return err
	})
	return w, nodes, err
}

func (s *Store) FindWorkflowRun(ctx context.Context, workflowName, dedupKey string) (w WorkflowRun, nodes []JobRun, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.q.WorkflowRunWithNodesByDedup(ctx, tx, WorkflowRunWithNodesByDedupParams{WorkflowName: workflowName, DedupKey: dedupKey})
		if err != nil {
			return err
		}
		w, nodes, err = workflowRunFromRows(rows)
		return err
	})
	return w, nodes, err
}

// Both readers split the same one-statement parent-plus-nodes shape (§3.3).
func workflowRunFromRows[R WorkflowRunWithNodesRow | WorkflowRunWithNodesByDedupRow](rows []R) (WorkflowRun, []JobRun, error) {
	if len(rows) == 0 {
		return WorkflowRun{}, nil, ErrNotFound
	}
	nodes := make([]JobRun, 0, len(rows))
	for _, r := range rows {
		nodes = append(nodes, WorkflowRunWithNodesRow(r).JobRun)
	}
	return WorkflowRunWithNodesRow(rows[0]).WorkflowRun, nodes, nil
}

type NodeContext struct {
	Cancelling   bool // the parent is no longer running: settle cancelled, do not execute
	WorkflowName string
	Input        []byte
	Extra        []byte
	Outputs      map[string][]byte // direct predecessors' output by job name
}

func (s *Store) NodeContext(ctx context.Context, workflowRunId int64, jobName string) (nc NodeContext, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		h, err := s.q.WorkflowRunHeader(ctx, tx, workflowRunId)
		if err != nil {
			return err
		}
		nc.WorkflowName, nc.Extra = h.WorkflowName, h.Extra
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

func terminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled"
}

type upstreamError struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Message string    `json:"message"`
}

// propagate runs inside settle, under the parent's row lock, after node became terminal.
func (s *Store) propagate(
	ctx context.Context,
	tx pgx.Tx,
	parent changedWorkflow,
	dag Dag,
	node Change,
) ([]Change, error) {
	changes := []Change{}
	var cancelling *Change
	failedOrCancelled := node.State == "failed" || node.State == "cancelled"
	if failedOrCancelled && parent.State == "running" { // fail-fast, written before the nodes are read
		row, err := s.q.MarkWorkflowCancelling(ctx, tx, parent.Id)
		if err != nil {
			return nil, err
		}
		parent = changedWorkflow(row)
		reason := "upstream_" + node.State
		cancelling = new(parent.change(ChangeWorkflowCancelRequested, reason))
		message := node.JobName + " " + node.State
		entry, err := json.Marshal([]upstreamError{{
			At: time.Now().UTC(), Kind: reason, Message: message,
		}})
		if err != nil {
			return nil, err
		}
		cancelled, err := s.q.CancelUnstartedNodes(ctx, tx, CancelUnstartedNodesParams{
			WorkflowRunId: parent.Id, Err: entry,
		})
		if err != nil {
			return nil, err
		}
		for _, row := range cancelled {
			change := parent.nodeChange(changedRun(row), ChangeRunCancelled, reason)
			change.Error = message
			changes = append(changes, change)
		}
	}
	// Read after cancellation: a skipped claim may still show as pending. Neither
	// pending nor running nodes can become terminal without this parent lock (§2.4).
	rows, err := s.q.NodeStates(ctx, tx, parent.Id)
	if err != nil {
		return nil, err
	}
	states := statesOf(rows)
	states[node.JobName] = node.State

	if node.State == "succeeded" && parent.State == "running" {
		var ready []string
		for name, st := range states {
			if st != "blocked" {
				continue
			}
			if states.depsSucceeded(dag[name]) {
				ready = append(ready, name)
			}
		}
		if len(ready) > 0 {
			if _, err := s.q.ActivateNodes(ctx, tx, ActivateNodesParams{WorkflowRunId: parent.Id, JobNames: ready}); err != nil {
				return nil, err
			}
			for _, name := range ready {
				states[name] = "pending"
			}
		}
	}
	if final, done := finalState(states, parent.State); done {
		row, err := s.q.FinalizeWorkflowRun(ctx, tx, FinalizeWorkflowRunParams{Id: parent.Id, State: final})
		if err != nil {
			return nil, err
		}
		changes = append(changes, changedWorkflow(row).finished())
	} else if cancelling != nil {
		// The cancelling state is observable only if this transaction leaves it there.
		changes = append(changes, *cancelling)
	}
	return changes, nil
}

// finalState: once every node is terminal, any failed → failed; else cancelling → cancelled; else succeeded.
func finalState(states nodeStates, wfState string) (string, bool) {
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

// CancelWorkflow ends unlocked unstarted nodes; skipped claims and running nodes
// learn cancellation through the parent check or heartbeat. Repeated cancellation is a no-op.
func (s *Store) CancelWorkflow(ctx context.Context, id int64) ([]Change, error) {
	changes := []Change{}
	err := s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		row, err := s.q.MarkWorkflowCancelling(ctx, tx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = s.q.GetWorkflowRun(ctx, tx, id)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if err != nil {
			return err
		}
		parent := changedWorkflow(row)
		cancelled, err := s.q.CancelUnstartedNodes(ctx, tx, CancelUnstartedNodesParams{
			WorkflowRunId: id, Err: []byte("[]"),
		})
		if err != nil {
			return err
		}
		for _, row := range cancelled {
			changes = append(changes, parent.nodeChange(changedRun(row), ChangeRunCancelled, "cancel_requested"))
		}
		rows, err := s.q.NodeStates(ctx, tx, id)
		if err != nil {
			return err
		}
		states := statesOf(rows)
		if final, done := finalState(states, "cancelling"); done {
			row, err := s.q.FinalizeWorkflowRun(ctx, tx, FinalizeWorkflowRunParams{Id: id, State: final})
			if err != nil {
				return err
			}
			changes = append(changes, changedWorkflow(row).finished())
		} else {
			changes = append(changes, parent.change(ChangeWorkflowCancelRequested, "cancel_requested"))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return changes, nil
}

var ErrNotResumable = errors.New("store: run has no resumable work")

// Resume re-queues the failed / cancelled nodes of a failed / cancelled workflow_run;
// succeeded nodes keep their output. A terminal run has only terminal nodes (finalize
// requires it), so "unsucceeded descendants" are already in that set.
func (s *Store) Resume(ctx context.Context, id int64) ([]Change, error) {
	changes := []Change{}
	err := s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		name, err := s.q.WorkflowResumeIdentity(ctx, tx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var skip bool
		if name != nil {
			skip, err = s.lockScheduleForResume(ctx, tx, *name)
			if err != nil {
				return err
			}
		}
		w, err := s.q.LockWorkflowRun(ctx, tx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !resumable(w.State) {
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
		names, newStates := dag.resumeSet(statesOf(rows))
		if len(names) == 0 {
			return ErrNotResumable
		}
		if skip {
			found, err := s.q.InflightScheduleRunExists(ctx, tx, InflightScheduleRunExistsParams{
				Name: *name, ExcludeWorkflowRunID: id,
			})
			if err != nil {
				return err
			}
			if found {
				return ErrDuplicate
			}
		}
		reset, err := s.q.ResumeNodes(ctx, tx, ResumeNodesParams{WorkflowRunId: id, JobNames: names, States: newStates})
		if err != nil {
			return err
		}
		reopened, err := s.q.ReopenWorkflowRun(ctx, tx, id)
		if isPgCode(err, "23505") {
			return ErrDuplicate
		}
		if err != nil {
			return err
		}
		parent := changedWorkflow(reopened)
		for _, row := range reset {
			changes = append(changes, parent.nodeChange(changedRun(row), ChangeRunResumed, "manual_resume"))
		}
		changes = append(changes, parent.change(ChangeWorkflowResumed, "manual_resume"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return changes, nil
}

func (s *Store) ListWorkflowRuns(ctx context.Context, p ListWorkflowRunsParams) (rows []WorkflowRun, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err = s.q.ListWorkflowRuns(ctx, tx, p)
		return err
	})
	return rows, err
}
