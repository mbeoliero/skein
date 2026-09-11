package skein

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"slices"
)

type RawJSON = jsontext.Value

// Executor runs one invocation. Delivery is at-least-once: anything with external side
// effects must deduplicate on req.IdempotencyKey.
type Executor func(ctx context.Context, req *Request) (RawJSON, error)

// Request contains this invocation's snapshot and stable business identity.
// Input and Deps are set only for workflow nodes; callers own the request values.
type Request struct {
	RunId          int64
	ExecutionId    string // unique to this claim; changes even when Attempt stays the same
	TraceId        string // original submission trace; empty when no valid trace was submitted
	JobName        string
	Attempt        int                // failures/interruptions +1; releases and snoozes do not increment it
	Params         RawJSON            // job.params merged with the caller's override
	WorkflowRunId  *int64             // independent copy for workflow nodes; mutations do not change settlement
	Input          RawJSON            // workflow_run.input; nil for plain runs
	Deps           map[string]RawJSON // direct predecessors' output by job name
	IdempotencyKey string             // "run:<id>" or "wf:<wf_id>/<job_name>", stable across attempts and resumes
}

// Register binds an executor type to fn before Start; a duplicate type panics.
func (e *Engine) Register(executorType string, fn Executor) {
	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()
	switch {
	case fn == nil:
		panic("skein: Register needs a function")
	case validName("executor type", executorType) != nil:
		panic(validName("executor type", executorType).Error())
	case e.started.Load():
		panic("skein: Register after Start")
	case e.shutting.Load():
		panic("skein: Register after Shutdown")
	}
	if _, dup := e.executors[executorType]; dup {
		panic(fmt.Sprintf("skein: executor %q registered twice", executorType))
	}
	e.executors[executorType] = fn
	// Republish the sorted snapshot: Stats may run before Start, concurrently with
	// Register, and must never walk the map while it is being written (§3.3).
	types := slices.Sorted(maps.Keys(e.executors))
	e.types.Store(&types)
}

// Register is the typed form: Params is decoded into P first; a decode failure is a
// permanent failure.
func Register[P any](e *Engine, executorType string, fn func(ctx context.Context, req *Request, params P) (RawJSON, error)) {
	if fn == nil {
		panic("skein: Register needs a function") // the wrapper below would hide it until a run
	}
	e.Register(executorType, func(ctx context.Context, req *Request) (RawJSON, error) {
		var p P
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return nil, Permanent(fmt.Errorf("decode params: %w", err))
		}
		return fn(ctx, req, p)
	})
}
