package skein

import (
	json "encoding/json/v2"
	"fmt"
	"slices"
	"testing"
	"time"
	"uuid"
)

type scenarioWant struct {
	Id           int64
	Case         string
	Phase        string
	Kind         string
	Digest       string
	ParamsHash   string
	Key          string
	PayloadBytes int
	State        RunState
	Attempt      int
	Invocations  []int
	Errors       []string
	Deps         []int64
	Precision    bool
	ExpectedAt   *time.Time
	CheckRenewal bool
	SnoozeNs     int64
	TriggerNs    int64
	CommitNs     int64
}

type scenarioInvocation struct {
	Id           uuid.UUID
	RunId        int64
	Attempt      int
	Owner        string
	Token        uuid.UUID
	ParamsHash   string
	Digest       string
	Key          string
	RunAt        time.Time
	StartedAt    time.Time
	ScheduledAt  *time.Time
	LeaseStart   time.Time
	LeasePeak    time.Time
	EnteredAt    time.Time
	ReturnedAt   *time.Time
	ObservedAt   *time.Time
	ExecNs       int64
	Error        string
	SnoozeNs     int64
	NextAt       *time.Time
	PendingClean bool
}

type scenarioEffect struct {
	RunId      int64
	Key        string
	Digest     string
	Invocation uuid.UUID
}

func checkScenarioRun(w scenarioWant, r *JobRun, calls []scenarioInvocation, effects []scenarioEffect) error {
	if r == nil {
		return fmt.Errorf("run %d: missing row", w.Id)
	}
	if r.Id != w.Id || r.State != w.State || r.Attempt != w.Attempt {
		return fmt.Errorf("run %d: want state=%s attempt=%d, got id=%d state=%s attempt=%d", w.Id, w.State, w.Attempt, r.Id, r.State, r.Attempt)
	}
	if r.State == StateSucceeded {
		var digest string
		if err := json.Unmarshal(r.Output, &digest); err != nil || digest != w.Digest {
			return fmt.Errorf("run %d: wrong output %s, want %q", w.Id, r.Output, w.Digest)
		}
	}
	var entries []errEntry
	if err := json.Unmarshal(r.Errors, &entries); err != nil {
		return fmt.Errorf("run %d errors: %w", w.Id, err)
	}
	kinds := make([]string, len(entries))
	for i, entry := range entries {
		kinds[i] = entry.Kind
	}
	if !slices.Equal(kinds, w.Errors) {
		return fmt.Errorf("run %d: errors %v, want %v", w.Id, kinds, w.Errors)
	}
	if w.Precision && (w.Phase == "precision_delayed" || w.Phase == "precision_tx" || w.Phase == "precision_cron") && w.ExpectedAt == nil {
		return fmt.Errorf("run %d: missing independently recorded deadline", w.Id)
	}
	if w.Kind == "snooze" && w.SnoozeNs <= 0 {
		return fmt.Errorf("run %d: missing Snooze intent", w.Id)
	}
	if len(calls) != len(w.Invocations) {
		return fmt.Errorf("run %d: %d invocations, want %d", w.Id, len(calls), len(w.Invocations))
	}
	slices.SortFunc(calls, func(a, b scenarioInvocation) int { return a.EnteredAt.Compare(b.EnteredAt) })
	seen, tokens := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
	for i, c := range calls {
		if c.RunId != w.Id || c.Attempt != w.Invocations[i] || c.Owner == "" || seen[c.Id] || tokens[c.Token] || c.Token == (uuid.UUID{}) {
			return fmt.Errorf("run %d: invalid invocation identity %+v", w.Id, c)
		}
		seen[c.Id], tokens[c.Token] = true, true
		if c.ParamsHash != w.ParamsHash || c.Digest != w.Digest || c.Key != w.Key {
			return fmt.Errorf("run %d: invocation input, dependencies or idempotency key differ", w.Id)
		}
		if w.Phase == "precision_cron" {
			if w.ExpectedAt == nil || c.ScheduledAt == nil || !c.ScheduledAt.Equal(*w.ExpectedAt) {
				return fmt.Errorf("run %d: scheduled_at does not match the intended cron beat", w.Id)
			}
		} else if i == 0 && w.ExpectedAt != nil && !c.RunAt.Equal(*w.ExpectedAt) {
			return fmt.Errorf("run %d: run_at does not match submitted At", w.Id)
		}
		lag := c.EnteredAt.Sub(scenarioDue(w, c, i == 0))
		if lag < 0 || (w.Precision && lag > tight) {
			return fmt.Errorf("run %d (%s): entry lag %s, precision=%t", w.Id, w.Phase, lag, w.Precision)
		}
		if c.ReturnedAt == nil || c.ObservedAt == nil || c.ReturnedAt.Before(c.EnteredAt) || c.ObservedAt.Before(*c.ReturnedAt) || c.ExecNs <= 0 {
			return fmt.Errorf("run %d: incomplete or invalid execution/settlement evidence %+v", w.Id, c)
		}
		if w.SnoozeNs > 0 {
			planned := int64(0)
			if i < len(calls)-1 {
				planned = w.SnoozeNs
			}
			if c.SnoozeNs != planned {
				return fmt.Errorf("run %d: Snooze duration %d, want %d", w.Id, c.SnoozeNs, planned)
			}
			if planned > 0 {
				if c.NextAt == nil || !c.PendingClean {
					return fmt.Errorf("run %d: missing or dirty Snooze pending snapshot", w.Id)
				}
				delay := time.Duration(planned)
				if c.NextAt.Before(c.ReturnedAt.Add(delay)) || c.NextAt.After(c.ObservedAt.Add(delay)) {
					return fmt.Errorf("run %d: Snooze deadline is outside its requested window", w.Id)
				}
			}
			if i > 0 && (calls[i-1].NextAt == nil || !c.RunAt.Equal(*calls[i-1].NextAt)) {
				return fmt.Errorf("run %d: next claim differs from the Snooze deadline", w.Id)
			}
		}
		if w.Phase == "snooze_workflow" && i == len(calls)-1 && (r.StartedAt == nil || !r.StartedAt.Equal(c.StartedAt)) {
			return fmt.Errorf("run %d: workflow node started_at changed after its last invocation", w.Id)
		}
		if w.CheckRenewal && !c.LeasePeak.After(c.LeaseStart) {
			return fmt.Errorf("run %d: long invocation has no observed lease renewal", w.Id)
		}
		if i > 0 && c.Attempt == calls[i-1].Attempt+1 {
			prev := calls[i-1]
			base := min(200*time.Millisecond, 100*time.Millisecond*time.Duration(1<<uint(prev.Attempt-1)))
			if c.RunAt.Before(prev.ReturnedAt.Add(base*8/10)) || c.RunAt.After(prev.ObservedAt.Add(base*12/10)) {
				return fmt.Errorf("run %d: retry run_at is outside its backoff window", w.Id)
			}
		}
	}
	wantEffects := 0
	if len(w.Invocations) > 0 {
		wantEffects = 1
	}
	if len(effects) != wantEffects {
		return fmt.Errorf("run %d: %d effects, want %d", w.Id, len(effects), wantEffects)
	}
	for _, effect := range effects {
		if effect.Key != w.Key || effect.Digest != w.Digest || !seen[effect.Invocation] {
			return fmt.Errorf("run %d: wrong business effect %+v", w.Id, effect)
		}
	}
	return nil
}

func scenarioDue(w scenarioWant, c scenarioInvocation, first bool) time.Time {
	if first && w.ExpectedAt != nil {
		return *w.ExpectedAt
	}
	return c.RunAt
}

func TestScenarioVerifierRejectsBadEvidence(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	want := scenarioWant{Id: 1, State: StateSucceeded, Digest: "correct", ParamsHash: "params", Key: "run:1", Invocations: []int{1}, Precision: true}
	fixture := func() (*JobRun, []scenarioInvocation, []scenarioEffect) {
		id := uuid.New()
		return &JobRun{Id: 1, State: StateSucceeded, Output: RawJSON(`"correct"`), Errors: RawJSON(`[]`)},
			[]scenarioInvocation{{Id: id, RunId: 1, Attempt: 1, Owner: "worker", Token: uuid.New(), ParamsHash: "params", Digest: "correct", Key: "run:1",
				RunAt: at, EnteredAt: at.Add(time.Millisecond), ReturnedAt: new(at.Add(2 * time.Millisecond)), ObservedAt: new(at.Add(3 * time.Millisecond)), ExecNs: int64(time.Millisecond)}},
			[]scenarioEffect{{Key: "run:1", Digest: "correct", Invocation: id}}
	}
	run, calls, effects := fixture()
	if err := checkScenarioRun(want, run, calls, effects); err != nil {
		t.Fatalf("valid evidence: %v", err)
	}
	for _, name := range []string{"wrong_output", "missing_execution", "duplicate_effect", "early_start", "late_start", "wrong_attempt", "missing_settlement", "wrong_input", "unexpected_error", "wrong_effect", "missing_renewal", "wrong_deadline", "missing_cron_clock", "missing_intent"} {
		t.Run(name, func(t *testing.T) {
			run, calls, effects := fixture()
			want := want
			switch name {
			case "wrong_output":
				run.Output = RawJSON(`"wrong"`)
			case "missing_execution":
				calls = nil
			case "duplicate_effect":
				effects = append(effects, effects[0])
			case "early_start":
				calls[0].EnteredAt = at.Add(-time.Millisecond)
			case "late_start":
				calls[0].EnteredAt = at.Add(time.Second)
				calls[0].ReturnedAt = new(at.Add(2 * time.Second))
				calls[0].ObservedAt = new(at.Add(3 * time.Second))
			case "wrong_attempt":
				calls[0].Attempt = 2
			case "missing_settlement":
				calls[0].ObservedAt = nil
			case "wrong_input":
				calls[0].ParamsHash = "wrong"
			case "unexpected_error":
				run.Errors = RawJSON(`[{"kind":"interrupted"}]`)
			case "wrong_effect":
				effects[0].Digest = "wrong"
			case "missing_renewal":
				want.CheckRenewal = true
			case "wrong_deadline":
				want.ExpectedAt = new(at.Add(time.Millisecond))
				calls[0].EnteredAt = at.Add(2 * time.Millisecond)
				calls[0].ReturnedAt = new(at.Add(3 * time.Millisecond))
				calls[0].ObservedAt = new(at.Add(4 * time.Millisecond))
			case "missing_cron_clock":
				want.Phase, want.ExpectedAt = "precision_cron", new(at)
			case "missing_intent":
				want.Phase = "precision_delayed"
			}
			if err := checkScenarioRun(want, run, calls, effects); err == nil {
				t.Fatal("accepted corrupt evidence")
			}
		})
	}
}

func TestScenarioVerifierChecksBackoff(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	want := scenarioWant{Id: 1, State: StateFailed, Attempt: 2, Invocations: []int{1, 2}, Errors: []string{"business", "business"}, Key: "run:1", Digest: "correct", ParamsHash: "params"}
	run := JobRun{Id: 1, State: StateFailed, Attempt: 2, Errors: RawJSON(`[{"kind":"business"},{"kind":"business"}]`)}
	call := func(at time.Time, attempt int) scenarioInvocation {
		return scenarioInvocation{Id: uuid.New(), RunId: 1, Attempt: attempt, Owner: "worker", Token: uuid.New(), ParamsHash: "params", Digest: "correct", Key: "run:1",
			RunAt: at, EnteredAt: at.Add(time.Millisecond), ReturnedAt: new(at.Add(2 * time.Millisecond)), ObservedAt: new(at.Add(3 * time.Millisecond)), ExecNs: int64(time.Millisecond)}
	}
	first := call(at, 1)
	effects := []scenarioEffect{{RunId: 1, Key: "run:1", Digest: "correct", Invocation: first.Id}}
	for _, delay := range []time.Duration{50 * time.Millisecond, 102 * time.Millisecond, 200 * time.Millisecond} {
		err := checkScenarioRun(want, &run, []scenarioInvocation{first, call(at.Add(delay), 2)}, effects)
		if (err == nil) != (delay == 102*time.Millisecond) {
			t.Errorf("retry run_at offset %s: %v", delay, err)
		}
	}
}

func TestScenarioVerifierChecksSnooze(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	want := scenarioWant{Id: 1, Kind: "snooze", State: StateSucceeded, Invocations: []int{1, 1, 1},
		Key: "run:1", Digest: "correct", ParamsHash: "params", SnoozeNs: int64(3 * time.Second), Precision: true}
	fixture := func() (*JobRun, []scenarioInvocation, []scenarioEffect) {
		due := at
		var calls []scenarioInvocation
		for i := range 3 {
			c := scenarioInvocation{Id: uuid.New(), RunId: 1, Attempt: 1, Owner: "worker", Token: uuid.New(),
				ParamsHash: "params", Digest: "correct", Key: "run:1", RunAt: due, StartedAt: due,
				EnteredAt: due.Add(time.Millisecond), ReturnedAt: new(due.Add(2 * time.Millisecond)),
				ObservedAt: new(due.Add(3 * time.Millisecond)), ExecNs: int64(time.Millisecond)}
			if i < 2 {
				c.SnoozeNs, c.PendingClean = want.SnoozeNs, true
				c.NextAt = new(due.Add(3*time.Second + 2500*time.Microsecond))
				due = *c.NextAt
			}
			calls = append(calls, c)
		}
		return &JobRun{Id: 1, State: StateSucceeded, Output: RawJSON(`"correct"`), Errors: RawJSON(`[]`)}, calls,
			[]scenarioEffect{{RunId: 1, Key: "run:1", Digest: "correct", Invocation: calls[0].Id}}
	}
	for _, name := range []string{"valid", "missing_pending", "dirty_pending", "wrong_duration", "early_deadline", "late_deadline", "different_next_claim", "consumed_attempt", "extra_snooze", "missing_intent"} {
		t.Run(name, func(t *testing.T) {
			run, calls, effects := fixture()
			want := want
			switch name {
			case "missing_pending":
				calls[0].NextAt = nil
			case "dirty_pending":
				calls[0].PendingClean = false
			case "wrong_duration":
				calls[0].SnoozeNs++
			case "early_deadline", "late_deadline", "different_next_claim":
				shift := time.Second
				if name == "early_deadline" {
					shift = -shift
				}
				if name != "different_next_claim" {
					calls[0].NextAt = new(calls[0].NextAt.Add(shift))
				}
				// Keep all later clocks internally consistent: a wrong saved run_at must not prove itself correct.
				for i := 1; i < len(calls); i++ {
					c := &calls[i]
					c.RunAt, c.StartedAt, c.EnteredAt = c.RunAt.Add(shift), c.StartedAt.Add(shift), c.EnteredAt.Add(shift)
					c.ReturnedAt, c.ObservedAt = new(c.ReturnedAt.Add(shift)), new(c.ObservedAt.Add(shift))
					if c.NextAt != nil {
						c.NextAt = new(c.NextAt.Add(shift))
					}
				}
			case "consumed_attempt":
				calls[1].Attempt++
			case "extra_snooze":
				calls[2].SnoozeNs = want.SnoozeNs
			case "missing_intent":
				want.SnoozeNs = 0
			}
			err := checkScenarioRun(want, run, calls, effects)
			if (err == nil) != (name == "valid") {
				t.Fatalf("%s: %v", name, err)
			}
		})
	}
}
