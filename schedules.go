package skein

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/mbeoliero/skein/internal/store"
)

// Overlap says what a schedule does when its previous beat is still in flight.
type Overlap string

const (
	OverlapSkip  Overlap = "skip"  // default: this beat is skipped and counted
	OverlapAllow Overlap = "allow" // this beat runs alongside the previous one
)

// ScheduleSpec answers "when": a cron rule for exactly one job or workflow.
type ScheduleSpec struct {
	Name     string
	Job      string // exactly one of Job and Workflow
	Workflow string
	Cron     string  // standard 5 fields, or @hourly / @daily / @weekly / @monthly; no TZ= prefix, no @every
	Timezone string  // IANA name; default UTC. "Local" is rejected: it would mean whichever host scans
	Overlap  Overlap // default OverlapSkip
	Disabled bool    // design §8 Enabled = false; zero value is enabled
}

type Schedules struct{ e *Engine }

func (e *Engine) Schedules() *Schedules { return &Schedules{e: e} }

// Put upserts the rule. next_run_at is computed from the database clock and only
// replaced when the cron or timezone changed or the schedule was re-enabled (§6.2).
func (s *Schedules) Put(ctx context.Context, spec ScheduleSpec) error {
	if err := validName("schedule", spec.Name); err != nil {
		return err
	}
	if (spec.Job == "") == (spec.Workflow == "") {
		return fmt.Errorf("skein: schedule %q needs exactly one of Job and Workflow", spec.Name)
	}
	tz := cmp.Or(spec.Timezone, "UTC")
	overlap := cmp.Or(spec.Overlap, OverlapSkip)
	if overlap != OverlapSkip && overlap != OverlapAllow {
		return fmt.Errorf("skein: schedule %q overlap %q", spec.Name, overlap)
	}
	sched, loc, err := parseSchedule(spec.Cron, tz)
	if err != nil {
		return fmt.Errorf("skein: schedule %q: %w", spec.Name, err)
	}
	now, err := s.e.st.Now(ctx)
	if err != nil {
		return err
	}
	next, err := nextAfter(sched, loc, spec.Cron, now)
	if err != nil {
		return fmt.Errorf("skein: schedule %q: %w", spec.Name, err)
	}
	p := store.PutScheduleParams{Name: spec.Name, Cron: spec.Cron, Timezone: tz, Overlap: string(overlap), Enabled: !spec.Disabled, NextRunAt: next}
	if spec.Job != "" {
		p.JobName = &spec.Job
	} else {
		p.WorkflowName = &spec.Workflow
	}
	if err := mapErr(s.e.st.PutSchedule(ctx, p), cmp.Or(spec.Job, spec.Workflow)); err != nil {
		return err
	}
	s.e.wakeScheduler() // other instances hear it through the schedule trigger (§6.11)
	return nil
}

func (s *Schedules) Delete(ctx context.Context, name string) error {
	if err := mapErr(s.e.st.DeleteSchedule(ctx, name), name); err != nil {
		return err
	}
	s.e.wakeScheduler()
	return nil
}

// parseSchedule is the one place a rule is parsed: Put validates with it, nextRun
// evaluates with it. Timezone is the only source of the location: robfig's TZ= /
// CRON_TZ= prefix would override it (and panics without a following space), and
// @every is relative to the scan time rather than a grid, so neither is accepted.
func parseSchedule(spec, timezone string) (cron.Schedule, *time.Location, error) {
	switch {
	case strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ="):
		return nil, nil, errors.New("cron: TZ= / CRON_TZ= prefix is not allowed; set Timezone")
	case strings.HasPrefix(spec, "@every"):
		return nil, nil, errors.New("cron: @every is not supported; use 5 fields or @hourly / @daily / @weekly / @monthly")
	case timezone == "Local":
		return nil, nil, errors.New("timezone: Local depends on the host; use an IANA name")
	}
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("cron: %w", err)
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, nil, fmt.Errorf("timezone: %w", err)
	}
	return sched, loc, nil
}

// nextRun is the store.NextFunc: parse, then nextAfter.
func nextRun(spec, timezone string, after time.Time) (time.Time, error) {
	sched, loc, err := parseSchedule(spec, timezone)
	if err != nil {
		return time.Time{}, err
	}
	return nextAfter(sched, loc, spec, after)
}

// nextAfter is the first fire time of sched strictly after t. robfig/cron evaluates
// the fields in loc: a local time that DST skips does not fire, and a local time that
// DST repeats fires once per occurrence. It looks five years ahead and answers the
// zero time beyond that ("0 0 31 2 *" never fires); that is an error here, because a
// zero next_run_at would be due on every tick (§6.2). spec is for the message only.
func nextAfter(sched cron.Schedule, loc *time.Location, spec string, after time.Time) (time.Time, error) {
	next := sched.Next(after.In(loc))
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("cron %q has no fire time in the five years after %s", spec, after.In(loc).Format(time.RFC3339))
	}
	return next, nil
}
