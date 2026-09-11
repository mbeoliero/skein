package skein

import (
	"context"
	"time"
	"uuid"

	"github.com/mbeoliero/skein/internal/store"
)

// Observer receives committed changes. Calls can overlap and must return promptly;
// enqueue slow work in a host-owned bounded queue. Delivery is best effort.
type Observer interface {
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

func (f ObserverFunc) Observe(ctx context.Context, event Event) { f(ctx, event) }

// EventType identifies a committed run or workflow transition.
type EventType string

const (
	EventRunSucceeded       EventType = "run.succeeded"
	EventRunRetry           EventType = "run.retry"
	EventRunSnoozed         EventType = "run.snoozed"
	EventRunReleased        EventType = "run.released"
	EventRunFailed          EventType = "run.failed"
	EventRunCancelled       EventType = "run.cancelled"
	EventRunCancelRequested EventType = "run.cancel_requested"
	EventRunResumed         EventType = "run.resumed"
	EventRunReclaimed       EventType = "run.reclaimed"

	EventWorkflowCancelRequested EventType = "workflow.cancel_requested"
	EventWorkflowResumed         EventType = "workflow.resumed"
	EventWorkflowSucceeded       EventType = "workflow.succeeded"
	EventWorkflowFailed          EventType = "workflow.failed"
	EventWorkflowCancelled       EventType = "workflow.cancelled"
)

// Event describes a committed change, even if the run has changed again by the
// time Observe is called. EventId identifies this notification, not a durable log.
type Event struct {
	EventId       string
	Type          EventType
	RunId         int64  // zero for a workflow's own event
	WorkflowRunId *int64 // nil for ordinary runs
	JobName       string
	WorkflowName  string
	ExecutorType  string
	State         string // RunState or WorkflowState, as returned by PostgreSQL
	Reason        string
	Error         string
	ExecutionId   string        // empty when the change has no associated lease
	Duration      time.Duration // actual Executor call only
	TraceId       string        // original submission trace
}

func (e *Engine) observe(changes []store.Change) {
	if e.cfg.Observer == nil {
		return
	}
	for _, change := range changes {
		typ, ok := eventTypeFromStore(change.Type)
		if !ok {
			e.log.Error("unknown committed event type", "type", change.Type, "run_id", change.RunId)
			continue
		}
		ctx := restoreTrace(change.Extra)
		event := Event{
			EventId: uuid.New().String(), Type: typ, RunId: change.RunId,
			JobName: change.JobName, WorkflowName: change.WorkflowName, ExecutorType: change.ExecutorType,
			State: change.State, Reason: change.Reason, Error: change.Error,
			Duration: change.Duration, TraceId: traceId(ctx),
		}
		if change.WorkflowRunId != nil {
			// The callback owns its event; mutating it must not affect another change.
			event.WorkflowRunId = new(*change.WorkflowRunId)
		}
		if change.Token != nil {
			event.ExecutionId = executionId(*change.Token)
		}
		e.callObserver(ctx, event)
	}
}

func eventTypeFromStore(typ store.ChangeType) (EventType, bool) {
	switch typ {
	case store.ChangeRunSucceeded:
		return EventRunSucceeded, true
	case store.ChangeRunRetry:
		return EventRunRetry, true
	case store.ChangeRunSnoozed:
		return EventRunSnoozed, true
	case store.ChangeRunReleased:
		return EventRunReleased, true
	case store.ChangeRunFailed:
		return EventRunFailed, true
	case store.ChangeRunCancelled:
		return EventRunCancelled, true
	case store.ChangeRunCancelRequested:
		return EventRunCancelRequested, true
	case store.ChangeRunResumed:
		return EventRunResumed, true
	case store.ChangeRunReclaimed:
		return EventRunReclaimed, true
	case store.ChangeWorkflowCancelRequested:
		return EventWorkflowCancelRequested, true
	case store.ChangeWorkflowResumed:
		return EventWorkflowResumed, true
	case store.ChangeWorkflowSucceeded:
		return EventWorkflowSucceeded, true
	case store.ChangeWorkflowFailed:
		return EventWorkflowFailed, true
	case store.ChangeWorkflowCancelled:
		return EventWorkflowCancelled, true
	default:
		return "", false
	}
}

func (e *Engine) callObserver(ctx context.Context, event Event) {
	defer func() {
		if p := recover(); p != nil {
			e.log.Error("observer panicked", "event_id", event.EventId, "event_type", event.Type,
				"run_id", event.RunId, "value", p)
		}
	}()
	e.cfg.Observer.Observe(ctx, event)
}

func (e *Engine) observeReclaim(c store.Claimed, extra []byte, workflowName string) {
	if !c.Reclaimed || e.cfg.Observer == nil {
		return
	}
	e.observe([]store.Change{{
		Type: store.ChangeRunReclaimed, RunId: c.Id, WorkflowRunId: c.WorkflowRunId,
		JobName: c.JobName, WorkflowName: workflowName, ExecutorType: c.ExecutorType,
		State: string(StateRunning), Reason: "lease_expired", Token: &c.LeaseToken, Extra: extra,
	}})
}
