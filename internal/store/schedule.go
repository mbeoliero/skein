package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ───────────── schedules (§2.1) ─────────────

func (s *Store) PutSchedule(ctx context.Context, p PutScheduleParams) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		err := s.q.PutSchedule(ctx, tx, p)
		if isPgCode(err, "23503") {
			return ErrNotFound // the job or workflow it targets is not declared
		}
		return err
	})
}

func (s *Store) DeleteSchedule(ctx context.Context, name string) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		n, err := s.q.DeleteSchedule(ctx, tx, name)
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

func (s *Store) GetSchedule(ctx context.Context, name string) (row Schedule, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		row, err = s.q.GetSchedule(ctx, tx, name)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	return row, err
}

// Now is the database clock; Schedules.Put computes next_run_at from it.
func (s *Store) Now(ctx context.Context) (now time.Time, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		now, err = s.q.DbNow(ctx, tx)
		return err
	})
	return now, err
}

// NextFunc computes the first fire time of cron in timezone strictly after t.
type NextFunc func(cron, timezone string, after time.Time) (time.Time, error)

// Fired is one run the scan created or skipped, or one schedule it disabled.
type Fired struct {
	Schedule    string
	ScheduledAt time.Time
	Lag         time.Duration // db_now − scheduled_at
	Skipped     bool          // the previous beat is still in flight (overlap = skip)
	Disabled    error         // the rule has no next fire time: enabled = false, no run created
	RunId       int64
	IsWorkflow  bool
}

// dueBatch bounds one tick; a full batch means the caller should scan again at once.
const dueBatch = 50

// Scan is the outcome of one tick: what fired, and when the scheduler should wake
// next (§2.7). Due.Next is nil without an enabled schedule; the caller sleeps
// Next − DbNow less its own elapsed time and never compares the database clock with
// its own.
type Scan struct {
	Fired []Fired
	Due
	Full bool // dueBatch rows were due: more may be waiting
}

// ScanDue is one scheduler tick (§2.1): lock due schedules, advance each to its next
// fire time after the database clock, and create one run per schedule with the missed
// beat's scheduled_at. A dedup conflict is a skip and a rule without a next fire time
// disables that schedule, neither is an error; anything else rolls the whole tick back
// and the next tick retries. The nearest next_run_at is read after the advances in
// the same transaction.
func (s *Store) ScanDue(ctx context.Context, next NextFunc) (sc Scan, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.q.DueSchedules(ctx, tx, dueBatch)
		if err != nil {
			return err
		}
		sc.Full = len(rows) == dueBatch
		fired := make([]Fired, 0, len(rows))
		for _, r := range rows {
			nextAt, err := next(r.Cron, r.Timezone, r.DbNow)
			if err != nil {
				if err := s.q.DisableSchedule(ctx, tx, r.Name); err != nil {
					return err
				}
				fired = append(fired, Fired{Schedule: r.Name, ScheduledAt: r.NextRunAt, Disabled: err})
				continue
			}
			if err := s.q.AdvanceSchedule(ctx, tx, AdvanceScheduleParams{Name: r.Name, NextRunAt: nextAt}); err != nil {
				return err
			}
			var dedup *string
			if r.Overlap == "skip" {
				k := "sched:" + r.Name
				dedup = &k
			}
			f := Fired{Schedule: r.Name, ScheduledAt: r.NextRunAt, Lag: r.DbNow.Sub(r.NextRunAt), IsWorkflow: r.WorkflowName != nil}
			name, due := r.Name, r.NextRunAt
			if r.JobName != nil {
				f.RunId, err = s.triggerJob(ctx, tx, TriggerJobParams{JobName: *r.JobName, Params: []byte("{}"), DedupKey: dedup, ScheduleName: &name, ScheduledAt: &due})
			} else {
				f.RunId, err = s.triggerWorkflow(ctx, tx, TriggerWorkflowParams{WorkflowName: *r.WorkflowName, Input: []byte("{}"), DedupKey: dedup, ScheduleName: &name, ScheduledAt: &due})
			}
			switch {
			case errors.Is(err, ErrDuplicate):
				f.Skipped = true
			case err != nil:
				return err
			}
			fired = append(fired, f)
		}
		sc.Fired = fired
		n, err := s.q.NextScheduleAt(ctx, tx)
		sc.Sampled = time.Now()
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil
		case err != nil:
			return err
		}
		sc.Next, sc.DbNow = &n.NextDue, n.DbNow
		return nil
	})
	return sc, err
}
