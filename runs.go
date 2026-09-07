package skein

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

type RunState string

const (
	StateBlocked   RunState = "blocked"
	StatePending   RunState = "pending"
	StateRunning   RunState = "running"
	StateSucceeded RunState = "succeeded"
	StateFailed    RunState = "failed"
	StateCancelled RunState = "cancelled"
)

func (s RunState) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCancelled
}

// JobRun is one execution intent: definition snapshot, state, lease and result.
type JobRun struct {
	Id            int64
	JobName       string
	ScheduleName  string // set when a schedule created it
	ScheduledAt   *time.Time
	DedupKey      string
	WorkflowRunId *int64 // set for workflow nodes

	ExecutorType string
	Params       RawJSON
	Timeout      time.Duration
	RetryPolicy  RetryPolicy

	State           RunState
	Attempt         int // starts that ended in failure or interruption
	CancelRequested bool

	RunAt          time.Time
	LeaseExpiresAt *time.Time
	LeaseOwner     string

	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time

	Output RawJSON
	Errors RawJSON // array of {attempt, at, kind, message}
}

type Runs struct{ e *Engine }

func (e *Engine) Runs() *Runs { return &Runs{e: e} }

func (r *Runs) Get(ctx context.Context, id int64) (*JobRun, error) {
	row, err := r.e.st.GetJobRun(ctx, id)
	if err != nil {
		return nil, mapErr(err, "")
	}
	return jobRunFromStore(row), nil
}

type RunFilter struct {
	JobName string
	State   RunState
	Limit   int // default 50, at most 500
}

// List pages newest first (id descending, idx_job_run_job); cursor is the last id of
// the previous page (0 for the first), next is 0 when there is no further page.
func (r *Runs) List(ctx context.Context, f RunFilter, cursor int64) (page []JobRun, next int64, err error) {
	limit := pageLimit(f.Limit)
	rows, err := r.e.st.ListJobRuns(ctx, store.ListJobRunsParams{
		JobName: optional(f.JobName), State: optional(string(f.State)), Cursor: cursor, Lim: int32(limit + 1), // one more row tells whether a next page exists
	})
	if err != nil {
		return nil, 0, err
	}
	for _, row := range rows[:min(len(rows), limit)] {
		page = append(page, *jobRunFromStore(row))
	}
	if len(rows) > limit {
		next = page[len(page)-1].Id
	}
	return page, next, nil
}

func pageLimit(n int) int {
	if n <= 0 {
		return 50
	}
	return min(n, 500)
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func jobRunFromStore(r store.JobRun) *JobRun {
	run := &JobRun{
		Id: r.Id, JobName: r.JobName, ScheduledAt: r.ScheduledAt, WorkflowRunId: r.WorkflowRunId,
		ExecutorType: r.ExecutorType, Params: RawJSON(r.Params), Timeout: time.Duration(r.Timeout) * time.Second,
		State: RunState(r.State), Attempt: int(r.Attempt), CancelRequested: r.CancelRequested,
		RunAt: r.RunAt, LeaseExpiresAt: r.LeaseExpiresAt,
		CreatedAt: r.CreatedAt, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		Output: RawJSON(r.Output), Errors: RawJSON(r.Errors),
	}
	run.RetryPolicy = parseRetry(r.RetryPolicy)
	run.ScheduleName = deref(r.ScheduleName)
	run.DedupKey = deref(r.DedupKey)
	run.LeaseOwner = deref(r.LeaseOwner)
	return run
}

// parseRetry never fails: the CHECK on job guarantees max_attempts >= 1, and a
// document this code cannot read still must not retry forever.
func parseRetry(raw []byte) RetryPolicy {
	var p RetryPolicy
	if err := json.Unmarshal(raw, &p); err != nil || p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	return p
}

func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// Cancel ends a pending run now or asks a running one to stop; its holder sees the
// request at the next heartbeat and settles it as cancelled. Terminal runs are a
// no-op. Workflow nodes are cancelled through Workflows().CancelRun.
func (r *Runs) Cancel(ctx context.Context, id int64) error {
	err := r.e.st.CancelRun(ctx, id)
	if errors.Is(err, store.ErrNode) {
		return fmt.Errorf("skein: run %d is a workflow node; cancel its workflow run", id)
	}
	return mapErr(err, strconv.FormatInt(id, 10))
}
