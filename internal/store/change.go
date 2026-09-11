package store

import (
	"cmp"
	"time"
	"uuid"
)

// ChangeType is closed over the facts the library can commit; zero is invalid.
type ChangeType uint8

const (
	ChangeRunSucceeded ChangeType = iota + 1
	ChangeRunRetry
	ChangeRunSnoozed
	ChangeRunReleased
	ChangeRunFailed
	ChangeRunCancelled
	ChangeRunCancelRequested
	ChangeRunResumed
	ChangeRunReclaimed
	ChangeWorkflowCancelRequested
	ChangeWorkflowResumed
	ChangeWorkflowSucceeded
	ChangeWorkflowFailed
	ChangeWorkflowCancelled
)

// Change is a transaction's persisted fact, exposed only after its commit succeeds.
// Token stays inside the library; the public observer receives a derived execution id.
type Change struct {
	Type          ChangeType
	RunId         int64
	WorkflowRunId *int64
	JobName       string
	WorkflowName  string
	ExecutorType  string
	State         string
	Reason        string
	Error         string
	Token         *uuid.UUID
	Extra         []byte
	Duration      time.Duration
}

type changedRun CancelRunRow

func (r changedRun) change(eventType ChangeType, reason string) Change {
	return Change{
		Type: eventType, RunId: r.Id, WorkflowRunId: r.WorkflowRunId,
		JobName: r.JobName, ExecutorType: r.ExecutorType, State: r.State,
		Reason: reason, Token: r.LeaseToken, Extra: r.Extra,
	}
}

func (r changedRun) settled(st Settlement) Change {
	eventType, fallback := ChangeRunFailed, r.State
	switch st.Outcome {
	case Succeeded:
		eventType, fallback = ChangeRunSucceeded, "completed"
	case Cancelled:
		eventType, fallback = ChangeRunCancelled, "cancel_requested"
	case Released:
		eventType, fallback = ChangeRunReleased, "shutdown"
	case Retry:
		eventType, fallback = ChangeRunRetry, "retryable_error"
	case Snoozed:
		eventType, fallback = ChangeRunSnoozed, "snoozed"
	case Interrupted:
		fallback = "attempts_exhausted"
	}
	reason := cmp.Or(st.Reason, fallback)
	if r.State == "cancelled" {
		eventType = ChangeRunCancelled
		if st.Outcome != Cancelled {
			reason = "cancel_requested"
		}
	}
	change := r.change(eventType, reason)
	// Every settlement clears the stored token, but the fact belongs to its holder.
	change.Token = new(st.Token)
	change.Error, change.Duration = st.Error, st.Duration
	return change
}

type changedWorkflow MarkWorkflowCancellingRow

func (w changedWorkflow) change(eventType ChangeType, reason string) Change {
	return Change{
		Type: eventType, WorkflowRunId: new(w.Id), WorkflowName: w.WorkflowName,
		State: w.State, Reason: reason, Extra: w.Extra,
	}
}

func (w changedWorkflow) nodeChange(r changedRun, eventType ChangeType, reason string) Change {
	change := r.change(eventType, reason)
	change.WorkflowName, change.Extra = w.WorkflowName, w.Extra
	return change
}

func (w changedWorkflow) finished() Change {
	eventType, reason := ChangeWorkflowCancelled, "cancellation_completed"
	switch w.State {
	case "succeeded":
		eventType, reason = ChangeWorkflowSucceeded, "all_succeeded"
	case "failed":
		eventType, reason = ChangeWorkflowFailed, "node_failed"
	}
	return w.change(eventType, reason)
}
