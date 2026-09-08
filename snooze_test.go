package skein

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnoozePreservesRun(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &metricsRec{}
	cfg := fastConfig(schema)
	cfg.Metrics = rec
	entered := make(chan *Request, 1)
	results := make(chan error)
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("poll", func(ctx context.Context, req *Request) (RawJSON, error) {
			entered <- req
			select {
			case err := <-results:
				if err != nil {
					return RawJSON(strings.Repeat("!", e.cfg.MaxPayload+1)), err
				}
				return RawJSON(`{"done":true}`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "poll", Retry: RetryPolicy{MaxAttempts: 1}})
	id := trigger(t, e, "j", `{"task_id":"fixed"}`, DedupKey("one"))
	for i := range 4 {
		select {
		case req := <-entered:
			if req.RunId != id || req.Attempt != 1 || string(req.Params) != `{"task_id": "fixed"}` {
				t.Fatalf("entry %d: %+v", i, req)
			}
			if req.IdempotencyKey != fmt.Sprintf("run:%d", id) {
				t.Fatalf("changed idempotency key: %s", req.IdempotencyKey)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("executor did not enter")
		}
		if i == 3 {
			results <- nil
			break
		}
		results <- fmt.Errorf("still waiting: %w", Snooze(24*time.Hour))
		run := waitRun(t, e, id, StatePending)
		if run.Attempt != 0 || len(errorsOf(t, run)) != 0 || len(run.Output) != 0 {
			t.Fatalf("snooze consumed attempt or published result: %+v", run)
		}
		if run.LeaseOwner != "" || run.LeaseExpiresAt != nil || run.FinishedAt != nil {
			t.Fatalf("snooze retained lease or finish: %+v", run)
		}
		var tokenCleared bool
		if err := pool.QueryRow(
			t.Context(),
			"SELECT lease_token IS NULL FROM "+qualified(schema, "job_run")+" WHERE id = $1",
			id,
		).Scan(&tokenCleared); err != nil {
			t.Fatal(err)
		}
		if !tokenCleared {
			t.Fatal("snooze retained lease token")
		}
		if string(run.Params) != `{"task_id": "fixed"}` {
			t.Fatalf("params changed: %s", run.Params)
		}
		duplicate, err := e.Jobs().Trigger(t.Context(), "j", RawJSON(`{}`), DedupKey("one"))
		if !errors.Is(err, ErrDuplicate) || duplicate != id {
			t.Fatalf("dedup released while snoozed: id=%d err=%v", duplicate, err)
		}
		execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET run_at = now() WHERE id = "+itoa(id))
	}
	run := waitRun(t, e, id, StateSucceeded)
	if run.Attempt != 0 || len(errorsOf(t, run)) != 0 || string(run.Output) != `{"done": true}` {
		t.Fatalf("final result: %+v", run)
	}
	waitFor(t, "snoozed and succeeded observations", func() bool {
		return rec.get("exec_duration", "executor_type", "poll", "outcome", "snoozed") == 3 &&
			rec.get("exec_duration", "executor_type", "poll", "outcome", "succeeded") == 1
	})
}

func TestSnoozeInvalidAndPermanent(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "zero", err: Snooze(0)},
		{name: "negative", err: Snooze(-time.Nanosecond)},
		{name: "minimum", err: Snooze(time.Duration(math.MinInt64))},
		{name: "permanent_wrapper", err: Permanent(Snooze(time.Hour))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			var calls atomic.Int32
			e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
				e.Register("invalid", func(context.Context, *Request) (RawJSON, error) {
					calls.Add(1)
					return nil, tc.err
				})
			})
			declare(t, e, JobSpec{Name: "j", ExecutorType: "invalid", Retry: RetryPolicy{MaxAttempts: 3}})
			run := waitRun(t, e, trigger(t, e, "j", `{}`), StateFailed)
			if calls.Load() != 1 || run.Attempt != 1 || len(errorsOf(t, run)) != 1 {
				t.Fatalf("invalid/permanent snooze retried: calls=%d run=%+v", calls.Load(), run)
			}
		})
	}
}

func TestSnoozeContextWins(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   RunState
		outcome string
	}{
		{name: "cancel", state: StateCancelled, outcome: "cancelled"},
		{name: "timeout", state: StateFailed, outcome: "failed"},
		{name: "shutdown", state: StatePending, outcome: "released"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			cfg := fastConfig(schema)
			cfg.ShutdownGrace = 50 * time.Millisecond
			cfg.HeartbeatInterval, cfg.LeaseTTL = 500*time.Millisecond, 3*time.Second
			cfg.CancelTimeout = time.Second
			rec := &metricsRec{}
			cfg.Metrics = rec
			entered := make(chan struct{}, 1)
			e := startEngine(t, pool, cfg, func(e *Engine) {
				e.Register("poll", func(ctx context.Context, req *Request) (RawJSON, error) {
					entered <- struct{}{}
					<-ctx.Done()
					return RawJSON(`{"ignored":true}`), fmt.Errorf("poll: %w", Snooze(24*time.Hour))
				})
			})
			declare(t, e, JobSpec{Name: "j", ExecutorType: "poll", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 1}})
			id := trigger(t, e, "j", `{}`)
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("executor did not enter")
			}
			switch tc.name {
			case "cancel":
				if err := e.Runs().Cancel(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			case "shutdown":
				if err := e.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			run := waitRun(t, e, id, tc.state)
			if len(run.Output) != 0 {
				t.Fatalf("context outcome published snooze output: %s", run.Output)
			}
			if tc.name == "timeout" {
				es := errorsOf(t, run)
				if run.Attempt != 1 || len(es) != 1 || es[0].Kind != "timeout" {
					t.Fatalf("timeout did not consume a failed attempt: %+v", run)
				}
			}
			if tc.name == "shutdown" {
				now, err := e.st.Now(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if run.Attempt != 0 || run.RunAt.After(now) {
					t.Fatalf("shutdown snoozed instead of releasing immediately: %+v", run)
				}
			}
			waitFor(t, "context outcome observation", func() bool {
				return rec.get("exec_duration", "executor_type", "poll", "outcome", tc.outcome) == 1
			})
			if got := rec.get("exec_duration", "executor_type", "poll", "outcome", "snoozed"); got != 0 {
				t.Fatalf("context outcome counted %d snoozes", got)
			}
		})
	}
}

func TestSnoozeDurationPersistence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay time.Duration
	}{
		{name: "nanosecond", delay: time.Nanosecond},
		{name: "microsecond", delay: time.Microsecond},
		{name: "fractional_microsecond", delay: time.Microsecond + time.Nanosecond},
		{name: "day", delay: 24 * time.Hour},
		{name: "day_fraction", delay: 24*time.Hour + time.Nanosecond},
		{name: "maximum", delay: time.Duration(math.MaxInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			// Observe the settlement's transaction clock, not a before/after estimate.
			audit := qualified(schema, "snooze_delay")
			fn := qualified(schema, "record_snooze_delay")
			execSql(t, pool, "CREATE TABLE "+audit+" (micros bigint NOT NULL); "+
				"CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN INSERT INTO `+audit+` VALUES ((extract(epoch FROM NEW.run_at - now()) * 1000000)::bigint);
				RETURN NEW; END $$;
				CREATE TRIGGER record_snooze_delay AFTER UPDATE ON `+qualified(schema, "job_run")+`
				FOR EACH ROW WHEN (OLD.state = 'running' AND NEW.state = 'pending') EXECUTE FUNCTION `+fn+"()")

			var calls atomic.Int32
			e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
				e.Register("poll", func(context.Context, *Request) (RawJSON, error) {
					if calls.Add(1) == 1 {
						return RawJSON(`"\u0000"`), Snooze(tc.delay)
					}
					return nil, nil
				})
			})
			declare(t, e, JobSpec{Name: "j", ExecutorType: "poll", Retry: RetryPolicy{MaxAttempts: 1}})
			id := trigger(t, e, "j", `{}`)
			var got int64
			waitFor(t, "persisted snooze delay", func() bool {
				return pool.QueryRow(t.Context(), "SELECT micros FROM "+audit).Scan(&got) == nil
			})
			want := int64(tc.delay / time.Microsecond)
			if tc.delay%time.Microsecond != 0 {
				want++
			}
			if got != want {
				t.Fatalf("delay %s persisted %d microseconds, want %d", tc.delay, got, want)
			}
			if tc.delay >= time.Hour {
				run := waitRun(t, e, id, StatePending)
				if run.Attempt != 0 || len(errorsOf(t, run)) != 0 {
					t.Fatalf("long snooze failed: %+v", run)
				}
				execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET run_at = now() WHERE id = "+itoa(id))
			}
			waitRun(t, e, id, StateSucceeded)
			if calls.Load() != 2 {
				t.Fatalf("calls=%d, want 2", calls.Load())
			}
		})
	}
}

func TestSnoozeReleasesSlotAndWakesOnTime(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := slowPoll(schema)
	cfg.Concurrency = 1
	clock, entries := entryClock(pool)
	first := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("poll", func(ctx context.Context, req *Request) (RawJSON, error) {
			if calls.Add(1) == 1 {
				first <- struct{}{}
				select {
				case <-release:
					return nil, Snooze(700 * time.Millisecond)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return clock(ctx, req)
		})
		e.Register("other", clock)
	})
	declare(t, e, JobSpec{Name: "poll", ExecutorType: "poll", Retry: RetryPolicy{MaxAttempts: 1}})
	declare(t, e, JobSpec{Name: "other", ExecutorType: "other"})
	id := trigger(t, e, "poll", `{}`)
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("first poll did not enter")
	}
	other := trigger(t, e, "other", `{}`)
	close(release)
	select {
	case at := <-entries:
		if !at.Before(runAt(t, pool, schema, id)) {
			t.Fatal("other job did not use the slot before the snooze was due")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snooze did not release the only slot")
	}
	waitRun(t, e, other, StateSucceeded)
	expectEntry(t, "snoozed return", entries, pool, schema, id)
	waitRun(t, e, id, StateSucceeded)
}

func TestSnoozeWorkflowFixedDeadline(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "resume_succeeds"
		if expired {
			name = "resume_does_not_extend_deadline"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			// Advance the business clock without changing the saved deadline or sleeping.
			clock := qualified(schema, "business_clock")
			execSql(t, pool, "CREATE TABLE "+clock+" (at timestamptz NOT NULL); "+
				"INSERT INTO "+clock+" VALUES (clock_timestamp())")

			clockQuery := "SELECT at FROM " + clock
			type task struct {
				TaskId   string    `json:"task_id"`
				Deadline time.Time `json:"deadline"`
			}
			entered := make(chan *Request, 1)
			results := make(chan error)
			var submissions, observations atomic.Int32
			e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
				e.Register("submit", func(ctx context.Context, req *Request) (RawJSON, error) {
					submissions.Add(1)
					var now time.Time
					if err := pool.QueryRow(ctx, clockQuery).Scan(&now); err != nil {
						return nil, err
					}
					return json.Marshal(task{TaskId: req.IdempotencyKey, Deadline: now.Add(24 * time.Hour)})
				})
				e.Register("poll", func(ctx context.Context, req *Request) (RawJSON, error) {
					var submitted task
					if err := json.Unmarshal(req.Deps["submit"], &submitted); err != nil {
						return nil, Permanent(err)
					}
					entered <- req
					select {
					case result := <-results:
						var now time.Time
						if err := pool.QueryRow(ctx, clockQuery).Scan(&now); err != nil {
							return nil, err
						}
						if !now.Before(submitted.Deadline) {
							return nil, Permanent(errors.New("fixed deadline expired"))
						}
						if result != nil {
							return nil, result
						}
						return RawJSON(`{"ready":true}`), nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				})
				e.Register("observe", func(context.Context, *Request) (RawJSON, error) {
					observations.Add(1)
					return nil, nil
				})
			})
			for _, job := range []string{"submit", "poll", "observe"} {
				declare(t, e, JobSpec{Name: job, ExecutorType: job, Retry: RetryPolicy{MaxAttempts: 2}})
			}
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{
				{Job: "submit"}, {Job: "poll", Deps: []string{"submit"}}, {Job: "observe", Deps: []string{"poll"}},
			}})
			id := triggerWorkflow(t, e, "w", `{}`)
			var original *JobRun
			var pollId int64
			for i := range 7 {
				var req *Request
				select {
				case req = <-entered:
				case <-time.After(3 * time.Second):
					t.Fatalf("poll entry %d missing", i)
				}
				wf, err := e.Workflows().GetRun(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				submit := nodeOf(t, wf, "submit")
				if i == 0 {
					original = submit
					pollId = req.RunId
					var submitted task
					if err := json.Unmarshal(submit.Output, &submitted); err != nil {
						t.Fatal(err)
					}
					if submitted.TaskId == "" || submitted.Deadline.IsZero() || submit.StartedAt == nil {
						t.Fatal("submit did not persist task identity and start time")
					}
				}
				if submit.Id != original.Id || string(submit.Output) != string(original.Output) ||
					submit.StartedAt == nil || !submit.StartedAt.Equal(*original.StartedAt) {
					t.Fatalf("submit snapshot changed at poll %d: %+v", i, submit)
				}
				if req.RunId != pollId || string(req.Deps["submit"]) != string(original.Output) {
					t.Fatalf("poll lost submit output at entry %d", i)
				}
				wantAttempt := 1
				if i == 3 || i == 4 {
					wantAttempt = 2
				}
				if req.Attempt != wantAttempt {
					t.Fatalf("entry %d attempt=%d, want %d", i, req.Attempt, wantAttempt)
				}
				if wf.State != WorkflowRunning || nodeOf(t, wf, "observe").State != StateBlocked || observations.Load() != 0 {
					t.Fatalf("workflow advanced before poll success: %+v", wf)
				}
				switch i {
				case 0, 1, 3, 5:
					before := nodeOf(t, wf, "poll")
					results <- Snooze(24 * time.Hour)
					pending := waitRun(t, e, pollId, StatePending)
					if pending.Attempt != before.Attempt || string(pending.Errors) != string(before.Errors) {
						t.Fatalf("snooze changed failure history: before=%+v after=%+v", before, pending)
					}
					if len(pending.Output) != 0 || pending.FinishedAt != nil {
						t.Fatalf("snooze published intermediate completion: %+v", pending)
					}
					execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET run_at = now() WHERE id = "+itoa(pollId))
				case 2:
					results <- errors.New("transient poll failure")
				case 4:
					results <- Permanent(errors.New("operator repair required"))
					waitWorkflow(t, e, id, WorkflowFailed)
					if err := e.Workflows().Resume(t.Context(), id); err != nil {
						t.Fatal(err)
					}
				case 6:
					if expired {
						execSql(t, pool, "UPDATE "+clock+" SET at = at + interval '24 hours'")
					}
					results <- nil
				}
			}
			state := WorkflowSucceeded
			if expired {
				state = WorkflowFailed
			}
			wf := waitWorkflow(t, e, id, state)
			if submissions.Load() != 1 {
				t.Fatalf("submit repeated %d times", submissions.Load())
			}
			if expired {
				if observations.Load() != 0 || !strings.Contains(string(nodeOf(t, wf, "poll").Errors), "fixed deadline expired") {
					t.Fatalf("expired task advanced or lost reason: %+v", wf)
				}
			} else if observations.Load() != 1 {
				t.Fatalf("observer ran %d times", observations.Load())
			}
		})
	}
}
