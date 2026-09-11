package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Store) PutSchedule(ctx context.Context, p PutScheduleParams) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.q.LockScheduleName(ctx, tx, p.Name); err != nil {
			return err
		}
		err := s.q.PutSchedule(ctx, tx, p)
		if isPgCode(err, "23503") {
			return ErrNotFound // the job or workflow it targets is not declared
		}
		return err
	})
}

func (s *Store) DeleteSchedule(ctx context.Context, name string) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.q.LockScheduleName(ctx, tx, name); err != nil {
			return err
		}
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

// lockScheduleForResume protects the name even after deletion, then reads the
// current rule before any run lock. Existing run keys remain authoritative too.
func (s *Store) lockScheduleForResume(ctx context.Context, tx pgx.Tx, name string) (bool, error) {
	if err := s.q.LockScheduleName(ctx, tx, name); err != nil {
		return false, err
	}
	overlap, err := s.q.LockScheduleRule(ctx, tx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return overlap == "skip", err
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

// Fired reports a created run, skipped beat, or disabled schedule.
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

// Scan is one tick's result; Due.Next is nil when no schedule is enabled (§2.7).
type Scan struct {
	Fired []Fired
	Due
	Full bool // dueBatch schedules were processed: more may be waiting
}

// lockDueSchedules pages beyond busy candidates. Names are tried before row
// locks, and neither acquisition waits while the batch holds other names.
func (s *Store) lockDueSchedules(ctx context.Context, tx pgx.Tx) ([]LockDueScheduleRow, error) {
	locked := make([]LockDueScheduleRow, 0, dueBatch)
	selected := make(map[string]bool, dueBatch)
	cursor := DueScheduleCandidatesParams{Lim: dueBatch}
	for len(locked) < dueBatch {
		candidates, err := s.q.DueScheduleCandidates(ctx, tx, cursor)
		if err != nil {
			return nil, err
		}
		for _, candidate := range candidates {
			cursor.AfterDue, cursor.AfterName = new(candidate.NextRunAt), candidate.Name
			// A due-time change before locking can expose this name again on a later page.
			if selected[candidate.Name] {
				continue
			}
			ok, err := s.q.TryLockScheduleName(ctx, tx, candidate.Name)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			row, err := s.q.LockDueSchedule(ctx, tx, candidate.Name)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return nil, err
			}
			locked = append(locked, row)
			selected[row.Name] = true
			if len(locked) == dueBatch {
				break
			}
		}
		if len(candidates) < dueBatch {
			break
		}
	}
	return locked, nil
}

// ScanDue advances one batch using database time and preserves each missed beat's
// scheduled_at (§2.1). Conflicts skip; rules without a next fire time are disabled.
// Other errors roll back the batch for retry. The next wakeup is read after all
// advances, in the same transaction.
func (s *Store) ScanDue(ctx context.Context, next NextFunc) (sc Scan, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.lockDueSchedules(ctx, tx)
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
			f := Fired{
				Schedule: r.Name, ScheduledAt: r.NextRunAt, Lag: r.DbNow.Sub(r.NextRunAt),
				IsWorkflow: r.WorkflowName != nil,
			}
			var dedup *string
			if r.Overlap == "skip" {
				found, err := s.q.InflightScheduleRunExists(ctx, tx, InflightScheduleRunExistsParams{Name: r.Name})
				if err != nil {
					return err
				}
				if found {
					f.Skipped = true
					fired = append(fired, f)
					continue
				}
				dedup = new("sched:" + r.Name)
			}
			if r.JobName != nil {
				f.RunId, err = s.triggerJob(ctx, tx, TriggerJobParams{
					JobName: *r.JobName, Params: []byte("{}"), DedupKey: dedup,
					ScheduleName: new(r.Name), ScheduledAt: new(r.NextRunAt),
				})
			} else {
				f.RunId, err = s.triggerWorkflow(ctx, tx, TriggerWorkflowParams{
					WorkflowName: *r.WorkflowName, Input: []byte("{}"), DedupKey: dedup,
					ScheduleName: new(r.Name), ScheduledAt: new(r.NextRunAt),
				})
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
