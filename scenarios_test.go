package skein

import (
	"bufio"
	"cmp"
	"context"
	"crypto/sha256"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

type scenarioParams struct {
	Case     string `json:"case"`
	Kind     string `json:"kind"`
	WorkMs   int    `json:"work_ms"`
	Blob     string `json:"blob"`
	Gate     string `json:"gate,omitempty"`
	Snoozes  int    `json:"snoozes,omitzero"`
	SnoozeMs int    `json:"snooze_ms,omitzero"`
}

func scenarioJSON[T any](v T) RawJSON {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return RawJSON(b)
}

func scenarioHash(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

func scenarioDigest(p scenarioParams, input string, deps map[string]string) string {
	h := sha256.New()
	h.Write(scenarioJSON(p))
	fmt.Fprintf(h, "\x00%s", input)
	for _, name := range slices.Sorted(maps.Keys(deps)) {
		fmt.Fprintf(h, "\x00%s\x00%s", name, deps[name])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func scenarioConfig(schema string) Config {
	return Config{
		Schema: schema, Concurrency: 16, PollInterval: 5 * time.Second,
		HeartbeatInterval: time.Second, LeaseTTL: 10 * time.Second,
		CancelTimeout: 5 * time.Second, ShutdownGrace: 5 * time.Second,
		BackoffBase: 100 * time.Millisecond, BackoffMax: 200 * time.Millisecond,
	}
}

func scenarioPool(ctx context.Context, name string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(testDsn())
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.ConnConfig.RuntimeParams["application_name"] = name
	return pgxpool.NewWithConfig(ctx, cfg)
}

type scenarioCounters struct {
	nopMetrics
	active, peak, starts, bad, auditOps, auditNs atomic.Int64
}

func (c *scenarioCounters) Count(name string, n int, _ ...string) {
	switch name {
	case "lease_lost_total", "reclaim_total", "listener_reconnect_total", "maintenance_failures_total", "released_alert_total":
		c.bad.Add(int64(n))
	}
}

func (c *scenarioCounters) audit(fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	err := fn(ctx)
	c.auditOps.Add(1)
	c.auditNs.Add(time.Since(started).Nanoseconds())
	if err != nil {
		c.bad.Add(1)
	}
	return err
}

func scenarioSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func scenarioExecute(e *Engine, pool *pgxpool.Pool, c *scenarioCounters, ctx context.Context, req *Request, p scenarioParams) (out RawJSON, execErr error) {
	n := c.active.Add(1)
	defer c.active.Add(-1)
	c.starts.Add(1)
	for {
		peak := c.peak.Load()
		if n <= peak || c.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	var input struct{ Value string }
	if len(req.Input) > 0 {
		if err := json.Unmarshal(req.Input, &input); err != nil {
			return nil, Permanent(err)
		}
	}
	deps := map[string]string{}
	for name, raw := range req.Deps {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, Permanent(err)
		}
		deps[name] = value
	}
	digest, id := scenarioDigest(p, input.Value, deps), uuid.New()
	inv, runs := qualified(e.cfg.Schema, "scenario_invocation"), qualified(e.cfg.Schema, "job_run")
	if err := c.audit(func(ctx context.Context) error {
		tag, err := pool.Exec(ctx, `INSERT INTO `+inv+`
			(id, run_id, attempt, owner, token, params_hash, digest, key, run_at, started_at, scheduled_at, lease_start, lease_peak, entered_at)
			SELECT $1, id, $2, $3, lease_token, $4, $5, $6, run_at, started_at, scheduled_at, lease_expires_at, lease_expires_at, clock_timestamp()
			FROM `+runs+` WHERE id=$7 AND state='running' AND lease_owner=$3 AND attempt=$2-1`,
			id, req.Attempt, e.owner, scenarioHash(scenarioJSON(p)), digest, req.IdempotencyKey, req.RunId)
		if err == nil && tag.RowsAffected() != 1 {
			return errors.New("scenario entry cannot be attributed to its lease")
		}
		return err
	}); err != nil {
		return nil, Permanent(err)
	}
	started := time.Now()
	defer func() {
		ns := time.Since(started).Nanoseconds()
		message := ""
		if execErr != nil {
			message = execErr.Error()
		}
		var snoozeNs int64
		if snooze, ok := errors.AsType[*snoozeError](execErr); ok {
			snoozeNs = int64(snooze.delay)
		}
		if err := c.audit(func(ctx context.Context) error {
			_, err := pool.Exec(ctx, `UPDATE `+inv+` SET returned_at=clock_timestamp(), exec_ns=$2, error=$3, snooze_ns=$4 WHERE id=$1`, id, ns, message, snoozeNs)
			return err
		}); err != nil {
			out, execErr = nil, Permanent(err)
			e.log.Error("scenario return audit failed", "err", err, "run_id", req.RunId)
		}
	}()
	// The simulated business effect commits before failures too, so retries and Resume exercise idempotency.
	if err := c.audit(func(ctx context.Context) error {
		_, err := pool.Exec(ctx, `INSERT INTO `+qualified(e.cfg.Schema, "scenario_effect")+`
			(key, run_id, digest, invocation) VALUES ($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING`, req.IdempotencyKey, req.RunId, digest, id)
		return err
	}); err != nil {
		return nil, Permanent(err)
	}
	switch p.Kind {
	case "timeout", "cancel":
		<-ctx.Done()
		return nil, ctx.Err()
	case "permanent":
		return nil, Permanent(errors.New("planned permanent failure"))
	case "retry":
		if req.Attempt < 3 {
			return nil, errors.New("planned retry")
		}
	case "snooze":
		var calls int
		if err := c.audit(func(ctx context.Context) error {
			return pool.QueryRow(ctx, `SELECT count(*) FROM `+inv+` WHERE run_id=$1`, req.RunId).Scan(&calls)
		}); err != nil {
			return nil, Permanent(err)
		}
		if calls <= p.Snoozes {
			return RawJSON(`"not finished"`), fmt.Errorf("poll pending: %w", Snooze(time.Duration(p.SnoozeMs)*time.Millisecond))
		}
	case "barrier":
		for {
			var enabled bool
			if err := c.audit(func(ctx context.Context) error {
				return pool.QueryRow(ctx, `SELECT enabled FROM `+qualified(e.cfg.Schema, "scenario_control")+` WHERE key=$1`, p.Gate).Scan(&enabled)
			}); err != nil {
				return nil, Permanent(err)
			}
			if enabled {
				break
			}
			if err := scenarioSleep(ctx, 20*time.Millisecond); err != nil {
				return nil, err
			}
		}
	case "resume":
		var enabled bool
		if err := c.audit(func(ctx context.Context) error {
			return pool.QueryRow(ctx, `SELECT enabled FROM `+qualified(e.cfg.Schema, "scenario_control")+` WHERE key=$1`, p.Gate).Scan(&enabled)
		}); err != nil {
			return nil, Permanent(err)
		}
		if !enabled {
			return nil, Permanent(errors.New("planned failure before Resume"))
		}
	case "short", "long":
	default:
		return nil, Permanent(fmt.Errorf("unknown scenario kind %q", p.Kind))
	}
	if err := scenarioSleep(ctx, time.Duration(p.WorkMs)*time.Millisecond); err != nil {
		return nil, err
	}
	return scenarioJSON(digest), nil
}

func scenarioWorkerMain() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := scenarioPool(ctx, "skein_scenario_worker", 8)
	if err != nil {
		return err
	}
	defer pool.Close()
	audit, err := scenarioPool(ctx, "skein_scenario_audit", 2)
	if err != nil {
		return err
	}
	defer audit.Close()
	file, err := os.Create(filepath.Join(os.Getenv("SKEIN_SCENARIO_ARTIFACT_DIR"), fmt.Sprintf("worker-%d.jsonl", os.Getpid())))
	if err != nil {
		return err
	}
	defer file.Close()
	c := &scenarioCounters{}
	cfg := scenarioConfig(os.Getenv("SKEIN_HELPER_SCHEMA"))
	cfg.Logger, cfg.Metrics = slog.New(slog.NewJSONHandler(file, nil)), c
	e, err := New(pool, cfg)
	if err != nil {
		return err
	}
	Register[scenarioParams](e, "scenario", func(ctx context.Context, req *Request, p scenarioParams) (RawJSON, error) {
		return scenarioExecute(e, audit, c, ctx, req, p)
	})
	workers := qualified(cfg.Schema, "scenario_worker")
	if err := c.audit(func(ctx context.Context) error {
		_, err := audit.Exec(ctx, `INSERT INTO `+workers+` (owner, pid) VALUES ($1,$2)`, e.owner, os.Getpid())
		return err
	}); err != nil {
		return err
	}
	if err := e.Start(context.Background()); err != nil {
		return err
	}
	fmt.Println("ready")
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = e.Shutdown(shutdown)
	cancel()
	stats := scenarioJSON(map[string]int64{
		"starts": c.starts.Load(), "peak": c.peak.Load(), "active": c.active.Load(),
		"unexpected": c.bad.Load(), "audit_ops": c.auditOps.Load(), "audit_ns": c.auditNs.Load(),
	})
	saveErr := c.audit(func(ctx context.Context) error {
		_, err := audit.Exec(ctx, `UPDATE `+workers+` SET stopped_at=clock_timestamp(), stats=$2 WHERE owner=$1`, e.owner, []byte(stats))
		return err
	})
	return errors.Join(err, saveErr)
}

type scenarioHarness struct {
	t                   *testing.T
	ctx                 context.Context
	pool                *pgxpool.Pool
	observer            *pgxpool.Pool
	stopObs             context.CancelFunc
	obs                 sync.WaitGroup
	obsOps              atomic.Int64
	e                   *Engine
	schema              string
	dir                 string
	manifest            *os.File
	wants               map[int64]scenarioWant
	stops               []func()
	latencies           map[string][]time.Duration
	started             time.Time
	loadStart           string
	rolledBack          []int64
	workflowWants       map[int64]WorkflowState
	keptStarts          map[int64]time.Time
	actualStates        map[RunState]int
	workerRows          []scenarioWorkerRow
	callCount           int
	effectCount         int
	missingReturns      int
	missingObservations int
	verified            bool
	mixedDuration       time.Duration
}

func newScenarioHarness(t *testing.T) *scenarioHarness {
	t.Helper()
	deadline := time.Now().Add(14 * time.Minute)
	if end, ok := t.Deadline(); ok && end.Add(-time.Minute).Before(deadline) {
		deadline = end.Add(-time.Minute)
	}
	if !deadline.After(time.Now()) {
		t.Fatal("scenario needs at least one minute reserved for cleanup")
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	t.Cleanup(cancel)
	pool, schema := freshSchema(t)
	h := &scenarioHarness{t: t, ctx: ctx, pool: pool, schema: schema, dir: t.ArtifactDir(), loadStart: scenarioCommand(ctx, "uptime"),
		wants: map[int64]scenarioWant{}, latencies: map[string][]time.Duration{}, started: time.Now(),
		workflowWants: map[int64]WorkflowState{}, keptStarts: map[int64]time.Time{}}
	var err error
	h.manifest, err = os.Create(filepath.Join(h.dir, "manifest.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := sync.OnceFunc(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		h.stopWorkers()
		if h.stopObs != nil {
			h.stopObs()
			h.obs.Wait()
		}
		if err := h.manifest.Close(); err != nil {
			t.Error(err)
		}
		h.finish(cleanupCtx)
		if h.observer != nil {
			h.observer.Close()
		}
		if h.e != nil {
			ctx, cancel := context.WithTimeout(cleanupCtx, 5*time.Second)
			if err := h.e.Shutdown(ctx); err != nil {
				t.Error(err)
			}
			cancel()
		}
	})
	t.Cleanup(cleanup)
	inv, effect := qualified(schema, "scenario_invocation"), qualified(schema, "scenario_effect")
	_, err = pool.Exec(ctx, `CREATE TABLE `+inv+` (
		id uuid PRIMARY KEY, run_id bigint NOT NULL, attempt integer NOT NULL, owner text NOT NULL,
		token uuid NOT NULL, params_hash text NOT NULL, digest text NOT NULL, key text NOT NULL,
		run_at timestamptz NOT NULL, started_at timestamptz NOT NULL, scheduled_at timestamptz, lease_start timestamptz NOT NULL, lease_peak timestamptz NOT NULL, entered_at timestamptz NOT NULL,
		returned_at timestamptz, observed_at timestamptz, exec_ns bigint NOT NULL DEFAULT 0, error text NOT NULL DEFAULT '',
		snooze_ns bigint NOT NULL DEFAULT 0, next_at timestamptz, pending_clean boolean NOT NULL DEFAULT false
	);
	CREATE INDEX ON `+inv+` (run_id);
	CREATE TABLE `+effect+` (key text PRIMARY KEY, run_id bigint NOT NULL, digest text NOT NULL, invocation uuid NOT NULL REFERENCES `+inv+`(id));
	CREATE TABLE `+qualified(schema, "scenario_control")+` (key text PRIMARY KEY, enabled boolean NOT NULL);
	CREATE TABLE `+qualified(schema, "scenario_worker")+` (owner text PRIMARY KEY, pid integer NOT NULL, stopped_at timestamptz, stats jsonb NOT NULL DEFAULT '{}');`)
	if err != nil {
		t.Fatal(err)
	}
	var maxConnections int
	if err := pool.QueryRow(ctx, "SELECT current_setting('max_connections')::int").Scan(&maxConnections); err != nil {
		t.Fatal(err)
	}
	if budget := 3*(8+1+2) + int(pool.Config().MaxConns) + 2; budget > maxConnections-5 {
		t.Fatalf("scenario connection budget %d leaves fewer than five server connections free (%d total)", budget, maxConnections)
	}
	cfg := scenarioConfig(schema)
	cfg.DisableWorker, cfg.DisableScheduler = true, true
	h.e, err = New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	h.observer, err = scenarioPool(ctx, "skein_scenario_observer", 2)
	if err != nil {
		t.Fatal(err)
	}
	obsCtx, stopObs := context.WithCancel(ctx)
	h.stopObs = stopObs
	h.obs.Go(func() {
		for obsCtx.Err() == nil {
			callCtx, cancel := context.WithTimeout(obsCtx, time.Second)
			err := h.observe(callCtx)
			cancel()
			if err != nil && obsCtx.Err() == nil {
				t.Errorf("scenario observer: %v", err)
				return
			}
			if scenarioSleep(obsCtx, 20*time.Millisecond) != nil {
				return
			}
		}
	})
	for range 3 {
		cmd := startHelperContext(t, ctx, schema, "SKEIN_HELPER_MODE=scenario", "SKEIN_SCENARIO_ARTIFACT_DIR="+h.dir)
		stop := sync.OnceFunc(func() { stopScenarioWorker(t, cmd) })
		h.stops = append(h.stops, stop)
		// Run the shared, concurrent cleanup before helper fallbacks, including a later startup failure.
		t.Cleanup(cleanup)
	}
	t.Logf("scenario artifacts: %s", h.dir)
	return h
}

func stopScenarioWorker(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Errorf("stop scenario worker: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("scenario worker exited: %v", err)
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scenario worker could not be reaped")
		}
		t.Error("scenario worker did not stop within 15s")
	}
}

func (h *scenarioHarness) stopWorkers() {
	var wg sync.WaitGroup
	for _, stop := range h.stops {
		wg.Go(stop)
	}
	wg.Wait()
}

func (h *scenarioHarness) record[T any](v T) {
	h.t.Helper()
	if _, err := h.manifest.Write(append(scenarioJSON(v), '\n')); err != nil {
		h.t.Fatal(err)
	}
}

func (h *scenarioHarness) wait(what string, ready func(context.Context) (bool, error)) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Minute)
	defer cancel()
	for {
		if h.t.Failed() {
			h.t.Fatalf("%s: stopped after an earlier scenario failure", what)
		}
		ok, err := ready(ctx)
		if err != nil {
			h.t.Fatalf("%s: %v", what, err)
		}
		if ok {
			return
		}
		if err := scenarioSleep(ctx, 20*time.Millisecond); err != nil {
			h.t.Fatalf("%s: stage or scenario budget exhausted: %v", what, err)
		}
	}
}

func (h *scenarioHarness) observe(ctx context.Context) error {
	h.obsOps.Add(2)
	inv, runs := qualified(h.schema, "scenario_invocation"), qualified(h.schema, "job_run")
	_, err := h.observer.Exec(ctx, `UPDATE `+inv+` a SET lease_peak=r.lease_expires_at FROM `+runs+` r
		WHERE a.run_id=r.id AND a.token=r.lease_token AND r.lease_expires_at>a.lease_peak;
		UPDATE `+inv+` a SET observed_at=clock_timestamp(),
			next_at=CASE WHEN a.snooze_ns>0 THEN r.run_at END,
			pending_clean=a.snooze_ns>0 AND r.state='pending' AND r.attempt=a.attempt-1 AND r.errors='[]'::jsonb
				AND r.output IS NULL AND r.finished_at IS NULL AND r.lease_token IS NULL AND r.lease_owner IS NULL AND r.lease_expires_at IS NULL
				AND (r.workflow_run_id IS NULL OR EXISTS (
					SELECT 1 FROM `+qualified(h.schema, "workflow_run")+` w WHERE w.id=r.workflow_run_id AND w.state='running'
					AND NOT EXISTS (SELECT 1 FROM `+runs+` child WHERE child.workflow_run_id=w.id
						AND (w.dag->child.job_name) ? r.job_name AND child.state<>'blocked')))
		FROM `+runs+` r
		WHERE a.run_id=r.id AND a.returned_at IS NOT NULL AND a.observed_at IS NULL AND r.lease_token IS DISTINCT FROM a.token
			AND (a.snooze_ns=0 OR (r.state='pending' AND r.started_at=a.started_at))`)
	return err
}

func (h *scenarioHarness) drain(ids []int64) {
	h.t.Helper()
	h.wait("drain", func(ctx context.Context) (bool, error) {
		var left int
		err := h.pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+qualified(h.schema, "job_run")+` WHERE id=ANY($1::bigint[]) AND state IN ('blocked','pending','running')) +
			(SELECT count(*) FROM `+qualified(h.schema, "scenario_invocation")+` WHERE run_id=ANY($1::bigint[]) AND observed_at IS NULL)`, ids).Scan(&left)
		return left == 0, err
	})
}

func (h *scenarioHarness) now() time.Time {
	h.t.Helper()
	var now time.Time
	if err := h.pool.QueryRow(h.ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		h.t.Fatal(err)
	}
	return now
}

func (h *scenarioHarness) declare(name string, p scenarioParams) {
	h.t.Helper()
	timeout := time.Minute
	retry := RetryPolicy{MaxAttempts: 3}
	if p.Kind == "timeout" {
		timeout, retry.MaxAttempts = time.Second, 2
	} else if p.Kind == "snooze" {
		retry.MaxAttempts = 1
	}
	if err := h.e.Jobs().Declare(h.ctx, JobSpec{Name: name, ExecutorType: "scenario", Params: scenarioJSON(p), Timeout: timeout, Retry: retry}); err != nil {
		h.t.Fatal(err)
	}
}

func scenarioExpected(id int64, phase string, p scenarioParams) scenarioWant {
	w := scenarioWant{Id: id, Case: p.Case, Phase: phase, Kind: p.Kind, State: StateSucceeded,
		Digest: scenarioDigest(p, "", nil), ParamsHash: scenarioHash(scenarioJSON(p)), Key: fmt.Sprintf("run:%d", id),
		PayloadBytes: len(scenarioJSON(p)), Invocations: []int{1}, Precision: strings.HasPrefix(phase, "precision_"), CheckRenewal: p.Kind == "long" && p.WorkMs > 2000}
	switch p.Kind {
	case "snooze":
		w.Invocations = slices.Repeat([]int{1}, p.Snoozes+1)
		w.SnoozeNs, w.Precision = int64(time.Duration(p.SnoozeMs)*time.Millisecond), true
	case "retry":
		w.Attempt, w.Invocations, w.Errors = 2, []int{1, 2, 3}, []string{"business", "business"}
	case "permanent":
		w.State, w.Attempt, w.Errors = StateFailed, 1, []string{"business"}
	case "timeout":
		w.State, w.Attempt, w.Invocations, w.Errors = StateFailed, 2, []int{1, 2}, []string{"timeout", "timeout"}
	case "cancel":
		w.State = StateCancelled
	}
	return w
}

func (h *scenarioHarness) trigger(name, phase string, p scenarioParams, at *time.Time, opts ...TriggerOption) int64 {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	if at != nil {
		opts = append(opts, At(*at))
	}
	started := time.Now()
	id, err := h.e.Jobs().Trigger(ctx, name, scenarioJSON(p), opts...)
	elapsed := time.Since(started)
	h.record(map[string]any{"operation": "trigger", "case": p.Case, "phase": phase, "id": id, "error": fmt.Sprint(err), "trigger_ns": elapsed.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	if _, exists := h.wants[id]; exists {
		h.t.Fatalf("new submission %s unexpectedly reused id %d", p.Case, id)
	}
	w := scenarioExpected(id, phase, p)
	w.TriggerNs, w.ExpectedAt = elapsed.Nanoseconds(), at
	h.wants[id] = w
	h.record(w)
	key := fmt.Sprintf("%s/%s/%dB/trigger", phase, p.Kind, w.PayloadBytes)
	h.latencies[key] = append(h.latencies[key], elapsed)
	return id
}

func scenarioRows[T any](ctx context.Context, pool *pgxpool.Pool, table string) ([]T, error) {
	rows, err := pool.Query(ctx, "SELECT * FROM "+table)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[T])
}

type scenarioWorkerRow struct {
	Owner     string
	Pid       int
	StoppedAt *time.Time
	Stats     []byte
}

func (h *scenarioHarness) verify() {
	h.t.Helper()
	runs := map[int64]JobRun{}
	for cursor := int64(0); ; {
		page, next, err := h.e.Runs().List(h.ctx, RunFilter{Limit: 500}, cursor)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, run := range page {
			runs[run.Id] = run
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(runs) != len(h.wants) {
		h.t.Errorf("database has %d job_run rows, manifest has %d", len(runs), len(h.wants))
	}
	h.actualStates = map[RunState]int{}
	for _, run := range runs {
		h.actualStates[run.State]++
	}
	phaseCounts, mixedKinds, sizes := map[string]int{}, map[string]int{}, map[int]int{}
	for _, want := range h.wants {
		phaseCounts[want.Phase]++
		if want.Phase == "mixed" {
			mixedKinds[want.Kind]++
			sizes[want.PayloadBytes]++
		}
	}
	for phase, count := range map[string]int{"mixed": 1000, "workflow": 40, "precision_immediate": 100, "precision_tx": 100, "precision_delayed": 100, "precision_cron": 100, "precision_retry": 100, "transaction": 1, "dedup": 2, "snooze_plain": 48, "snooze_slots": 48, "snooze_workflow": 3} {
		if phaseCounts[phase] != count {
			h.t.Errorf("%s has %d runs in the manifest, want %d", phase, phaseCounts[phase], count)
		}
	}
	if !maps.Equal(mixedKinds, map[string]int{"short": 600, "long": 150, "retry": 100, "permanent": 50, "timeout": 50, "cancel": 50}) {
		h.t.Errorf("wrong mixed workload distribution: %v", mixedKinds)
	}
	if !maps.Equal(sizes, map[int]int{1024: 900, 32 << 10: 90, 250 << 10: 10}) {
		h.t.Errorf("wrong encoded payload distribution: %v", sizes)
	}
	if len(h.workflowWants) != 11 || len(h.rolledBack) != 1 {
		h.t.Errorf("manifest has %d workflows and %d rollbacks, want 11 and 1", len(h.workflowWants), len(h.rolledBack))
	}
	calls, err := scenarioRows[scenarioInvocation](h.ctx, h.pool, qualified(h.schema, "scenario_invocation"))
	if err != nil {
		h.t.Fatal(err)
	}
	effects, err := scenarioRows[scenarioEffect](h.ctx, h.pool, qualified(h.schema, "scenario_effect"))
	if err != nil {
		h.t.Fatal(err)
	}
	workers, err := scenarioRows[scenarioWorkerRow](h.ctx, h.pool, qualified(h.schema, "scenario_worker"))
	if err != nil {
		h.t.Fatal(err)
	}
	h.workerRows, h.callCount, h.effectCount = workers, len(calls), len(effects)
	byRun, byOwner := map[int64][]scenarioInvocation{}, map[string]int{}
	for _, c := range calls {
		if c.ReturnedAt == nil {
			h.missingReturns++
		}
		if c.ObservedAt == nil {
			h.missingObservations++
		}
		byRun[c.RunId] = append(byRun[c.RunId], c)
		byOwner[c.Owner]++
		if _, ok := h.wants[c.RunId]; !ok {
			h.t.Errorf("invocation for unrecorded run %d", c.RunId)
		}
	}
	byEffect := map[int64][]scenarioEffect{}
	for _, effect := range effects {
		byEffect[effect.RunId] = append(byEffect[effect.RunId], effect)
		if _, ok := h.wants[effect.RunId]; !ok {
			h.t.Errorf("effect for unrecorded run %d", effect.RunId)
		}
	}
	for id, want := range h.wants {
		slices.SortFunc(byRun[id], func(a, b scenarioInvocation) int { return a.EnteredAt.Compare(b.EnteredAt) })
		run, ok := runs[id]
		if !ok {
			h.t.Errorf("missing run %d", id)
			continue
		}
		if err := checkScenarioRun(want, &run, byRun[id], byEffect[id]); err != nil {
			h.t.Error(err)
		}
		prefix := fmt.Sprintf("%s/%s/%dB", want.Phase, want.Kind, want.PayloadBytes)
		for i, c := range byRun[id] {
			if i > 0 && want.SnoozeNs > 0 {
				h.latencies[prefix+"/snooze_reentry"] = append(h.latencies[prefix+"/snooze_reentry"], c.EnteredAt.Sub(c.RunAt))
			}
			h.latencies[prefix+"/entry"] = append(h.latencies[prefix+"/entry"], c.EnteredAt.Sub(scenarioDue(want, c, i == 0)))
			h.latencies[prefix+"/exec"] = append(h.latencies[prefix+"/exec"], time.Duration(c.ExecNs))
			if c.ReturnedAt != nil && c.ObservedAt != nil {
				h.latencies[prefix+"/settle_confirm"] = append(h.latencies[prefix+"/settle_confirm"], c.ObservedAt.Sub(*c.ReturnedAt))
			}
			for _, dep := range want.Deps {
				parent := runs[dep]
				if parent.State != StateSucceeded || parent.FinishedAt == nil || c.EnteredAt.Before(*parent.FinishedAt) {
					h.t.Errorf("run %d entered before dependency %d succeeded", id, dep)
				}
			}
		}
	}
	if len(workers) != 3 {
		h.t.Errorf("recorded %d workers, want 3", len(workers))
	}
	for _, worker := range workers {
		var stats map[string]int64
		if err := json.Unmarshal(worker.Stats, &stats); err != nil {
			h.t.Fatal(err)
		}
		if worker.StoppedAt == nil || stats["unexpected"] != 0 || stats["active"] != 0 || stats["peak"] > 16 || stats["starts"] != int64(byOwner[worker.Owner]) {
			h.t.Errorf("worker %s: stopped=%v stats=%v audit_calls=%d", worker.Owner, worker.StoppedAt, stats, byOwner[worker.Owner])
		}
		delete(byOwner, worker.Owner)
	}
	if len(byOwner) > 0 {
		h.t.Errorf("unregistered audit owners: %v", byOwner)
	}
	for _, id := range h.rolledBack {
		if _, ok := runs[id]; ok {
			h.t.Errorf("rolled-back run %d exists", id)
		}
	}
	for id, state := range h.workflowWants {
		run, err := h.e.Workflows().GetRun(h.ctx, id)
		if err != nil || run.State != state {
			h.t.Errorf("workflow %d: want %s, got %+v, err=%v", id, state, run, err)
		}
	}
	for id, at := range h.keptStarts {
		run := runs[id]
		if run.StartedAt == nil || !run.StartedAt.Equal(at) {
			h.t.Errorf("Resume changed successful node %d started_at", id)
		}
	}
	h.verified = true
}

func scenarioCommand(parent context.Context, name string, args ...string) string {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "N/A"
	}
	return strings.TrimSpace(string(out))
}

func (h *scenarioHarness) dump(ctx context.Context, name, query string) (err error) {
	file, err := os.Create(filepath.Join(h.dir, name+".jsonl"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	rows, err := h.pool.Query(ctx, "SELECT row_to_json(x) FROM ("+query+") x")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		if _, err := file.Write(append(raw, '\n')); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (h *scenarioHarness) finish(cleanupCtx context.Context) {
	if !h.verified {
		h.t.Error("scenario verification did not complete")
	}
	ctx, cancel := context.WithTimeout(cleanupCtx, 10*time.Second)
	defer cancel()
	for name, table := range map[string]string{"invocations": "scenario_invocation", "effects": "scenario_effect", "workers": "scenario_worker", "workflows": "workflow_run", "controls": "scenario_control"} {
		if err := h.dump(ctx, name, "SELECT * FROM "+qualified(h.schema, table)); err != nil {
			h.t.Errorf("save %s evidence: %v", name, err)
		}
	}
	if err := h.dump(ctx, "runs", `SELECT id, job_name, workflow_run_id, state, attempt, run_at, started_at, finished_at, output, errors FROM `+qualified(h.schema, "job_run")); err != nil {
		h.t.Errorf("save run evidence: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(h.dir, "worker-*.jsonl"))
	if err != nil {
		h.t.Error(err)
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			h.t.Error(err)
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record struct {
				Level string `json:"level"`
				Msg   string `json:"msg"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				h.t.Errorf("invalid worker log: %v", err)
			} else if record.Level == "ERROR" {
				h.t.Errorf("worker error in %s: %s", filepath.Base(path), record.Msg)
			}
		}
		if err := scanner.Err(); err != nil {
			h.t.Error(err)
		}
		if err := file.Close(); err != nil {
			h.t.Error(err)
		}
	}
	var pg string
	if err := h.pool.QueryRow(ctx, "SHOW server_version").Scan(&pg); err != nil {
		h.t.Errorf("read PG version: %v", err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# M7 smoke %s\n\n结果：**%s**；核查完成：%t。种子：1。Go：%s；PostgreSQL：%s；系统：%s/%s。\n\n",
		time.Now().UTC().Format(time.RFC3339), map[bool]string{true: "FAIL", false: "PASS"}[h.t.Failed()], h.verified, runtime.Version(), pg, runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&b, "提交：`%s`；工作区：`%s`；场景源码摘要：`%s`。\n\n", scenarioCommand(cleanupCtx, "git", "rev-parse", "HEAD"), strings.ReplaceAll(scenarioCommand(cleanupCtx, "git", "status", "--porcelain"), "\n", "; "), scenarioSourceHash())
	fmt.Fprintln(&b, "3 个进程 × Concurrency 16；Poll 5s；心跳 1s；租约 10s；取消超时 / 退出宽限 5s；重试退避 100–200ms ±20%。")
	fmt.Fprintf(&b, "\n连接预算：每个 worker 8 + LISTEN 1 + audit 2；提交端 %d；观察端 2。观察轮询 20ms，共执行 %d 条 SQL。\n\n", h.pool.Config().MaxConns, h.obsOps.Load())
	fmt.Fprintf(&b, "清单：%d 个 job_run，%d 个工作流，%d 次确认回滚。整轮耗时：%s；混合批次耗时：%s。\n\n", len(h.wants), len(h.workflowWants), len(h.rolledBack), time.Since(h.started).Round(time.Millisecond), h.mixedDuration.Round(time.Millisecond))
	if h.verified {
		fmt.Fprintf(&b, "逐次执行：%d；去重后副作用：%d；缺失返回：%d；缺失结算观察：%d。终态计数：`%v`。\n\n", h.callCount, h.effectCount, h.missingReturns, h.missingObservations, h.actualStates)
	} else {
		fmt.Fprintln(&b, "核查未完成，不能将缺失统计解释为零；请检查原始事件和测试日志。")
	}
	fmt.Fprintf(&b, "机器负载，开始：`%s`；结束：`%s`。\n\n", h.loadStart, scenarioCommand(cleanupCtx, "uptime"))
	fmt.Fprintln(&b, "混合批次：1,000 次普通提交 + 10 个四节点工作流。编码后参数：90% 1KB / 9% 32KB / 1% 250KB；固定种子的 base64 字符集随机数据，不用重复字符填充。精度探针与混合批次分开运行。")
	fmt.Fprintln(&b, "追加 Snooze 独立窗口：48 个普通任务各等待两次（3s），48 个屏障探针在最早到期前同时占满全部槽位；一个 submit → poll → verify 工作流，poll 等待四次（700ms）。共 100 次 Snooze 再启动，max_attempts=1；核对 pending 快照、独立请求时长、依赖输出和后继顺序。")
	fmt.Fprintln(&b, "\n| 场景 / 类型 / 参数大小 / 指标 | 样本数 | p50 | p95 | p99 | 最大值 |\n|---|---:|---|---|---|---|")
	for _, key := range slices.Sorted(maps.Keys(h.latencies)) {
		ds := h.latencies[key]
		p50, p95, p99 := percentiles(ds)
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s |\n", key, len(ds), p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), slices.Max(ds).Round(time.Microsecond))
	}
	fmt.Fprintln(&b, "\n| 实例 | 调用数 | 并发峰值 | fixture SQL 次数 | SQL 累计耗时 | 非预期计数 |\n|---|---:|---:|---:|---|---:|")
	for _, worker := range h.workerRows {
		var stats map[string]int64
		if err := json.Unmarshal(worker.Stats, &stats); err != nil {
			h.t.Error(err)
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %d |\n", worker.Owner, stats["starts"], stats["peak"], stats["audit_ops"], time.Duration(stats["audit_ns"]).Round(time.Millisecond), stats["unexpected"])
	}
	fmt.Fprintln(&b, "\nentry 使用数据库时钟；cron 以 scheduled_at 包含扫描晚点；混合负载 entry 包含排队。settle_confirm 是观察到结算的上界，不是精确提交延迟。fixture SQL 包含审计、幂等副作用与控制查询，不含 JSON / 摘要的 CPU 成本，不能整体扣除当作纯插桩开销。")
	fmt.Fprintln(&b, "\n范围仅 smoke。full / soak 尚未实现；不宣称持续容量、负载下故障接管、30 分钟稳定性、CPU 使用率或心跳 HOT 比例。生成器按固定批次提交，没有目标速率，generator lag 不适用。精度测量包含插桩成本；-race 结果不得用于性能对照。最终结果以 go test 退出码为准，制品写入失败同样使测试失败。")
	report := b.String()
	if err := os.WriteFile(filepath.Join(h.dir, "report.md"), []byte(report), 0o644); err != nil {
		h.t.Error(err)
	}
	if out := os.Getenv("SKEIN_SCENARIO_OUT"); out != "" {
		file, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			_, err = fmt.Fprintln(file, report)
			err = errors.Join(err, file.Close())
		}
		if err != nil {
			h.t.Errorf("save scenario report: %v", err)
		}
	}
	h.t.Logf("smoke report: %s", filepath.Join(h.dir, "report.md"))
}

func scenarioSourceHash() string {
	b, err := os.ReadFile("scenarios_test.go")
	if err != nil {
		return "N/A"
	}
	return scenarioHash(b)
}

func scenarioPayload(rng *rand.Rand, p scenarioParams, size int) scenarioParams {
	p.Blob = ""
	blob := make([]byte, size-len(scenarioJSON(p)))
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	for i := range blob {
		blob[i] = alphabet[rng.IntN(len(alphabet))]
	}
	p.Blob = string(blob)
	return p
}

func (h *scenarioHarness) triggerTx(phase string, p scenarioParams, hold time.Duration, rollback bool) int64 {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var opts []TriggerOption
	var due time.Time
	if strings.HasPrefix(phase, "precision_") {
		due = h.now().Add(700 * time.Millisecond)
		opts = append(opts, At(due))
	}
	started := time.Now()
	id, err := h.e.Jobs().TriggerTx(ctx, tx, "m7_short", scenarioJSON(p), opts...)
	elapsed := time.Since(started)
	h.record(map[string]any{"operation": "trigger_tx", "phase": phase, "id": id, "case": p.Case, "error": fmt.Sprint(err), "trigger_ns": elapsed.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	if hold > 0 {
		// This is simulated caller work inside the transaction, not a scheduler synchronization sleep.
		if err := scenarioSleep(ctx, hold); err != nil {
			h.t.Fatal(err)
		}
	}
	if _, err := h.e.Runs().Get(ctx, id); !errors.Is(err, ErrNotFound) {
		h.t.Fatalf("uncommitted run %d visible: %v", id, err)
	}
	var entries int
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM `+qualified(h.schema, "scenario_invocation")+` WHERE run_id=$1`, id).Scan(&entries); err != nil || entries != 0 {
		h.t.Fatalf("uncommitted run %d entered executor: entries=%d err=%v", id, entries, err)
	}
	if rollback {
		err := tx.Rollback(ctx)
		h.record(map[string]any{"operation": "rollback", "id": id, "error": fmt.Sprint(err)})
		if err != nil {
			h.t.Fatal(err)
		}
		h.rolledBack = append(h.rolledBack, id)
		return id
	}
	commit := time.Now()
	err = tx.Commit(ctx)
	commitDuration := time.Since(commit)
	h.record(map[string]any{"operation": "commit", "id": id, "error": fmt.Sprint(err), "commit_ns": commitDuration.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	w := scenarioExpected(id, phase, p)
	w.TriggerNs, w.CommitNs = elapsed.Nanoseconds(), commitDuration.Nanoseconds()
	if !due.IsZero() {
		w.ExpectedAt = new(due)
	}
	h.wants[id] = w
	h.record(w)
	prefix := fmt.Sprintf("%s/%s/%dB", phase, p.Kind, w.PayloadBytes)
	h.latencies[prefix+"/trigger"] = append(h.latencies[prefix+"/trigger"], elapsed)
	h.latencies[prefix+"/commit"] = append(h.latencies[prefix+"/commit"], commitDuration)
	if !due.IsZero() && !h.now().Before(due) {
		h.t.Fatal("TriggerTx precision precondition failed: commit was not confirmed before At")
	}
	return id
}

func (h *scenarioHarness) precision(rng *rand.Rand) {
	h.t.Helper()
	for _, mode := range []string{"immediate", "tx", "delayed", "cron", "retry"} {
		phase := "precision_" + mode
		for batch := range 10 {
			var ids []int64
			cron := map[string]scenarioParams{}
			cronDue := map[string]time.Time{}
			for n := range 10 {
				p := scenarioParams{Case: fmt.Sprintf("%s_%03d", phase, batch*10+n), Kind: "short", WorkMs: 5}
				if mode == "retry" {
					p.Kind = "retry"
				}
				p = scenarioPayload(rng, p, 1024)
				switch mode {
				case "tx":
					ids = append(ids, h.triggerTx(phase, p, 0, false))
				case "cron":
					h.declare(p.Case, p)
					if err := h.e.Schedules().Put(h.ctx, ScheduleSpec{Name: p.Case, Job: p.Case, Cron: "@yearly", Timezone: "UTC"}); err != nil {
						h.t.Fatal(err)
					}
					due := h.now().Add(700 * time.Millisecond)
					if _, err := h.pool.Exec(h.ctx, `UPDATE `+qualified(h.schema, "schedule")+` SET next_run_at=$1 WHERE name=$2`, due, p.Case); err != nil {
						h.t.Fatal(err)
					}
					h.record(map[string]any{"operation": "schedule_due", "name": p.Case, "due": due})
					cron[p.Case], cronDue[p.Case] = p, due
				case "delayed":
					ids = append(ids, h.trigger("m7_short", phase, p, new(h.now().Add(700*time.Millisecond))))
				default:
					ids = append(ids, h.trigger("m7_"+p.Kind, phase, p, nil))
				}
			}
			for _, name := range slices.Sorted(maps.Keys(cron)) {
				var id int64
				h.wait("cron beat", func(ctx context.Context) (bool, error) {
					var count int
					err := h.pool.QueryRow(ctx, `SELECT coalesce(min(id),0), count(*) FROM `+qualified(h.schema, "job_run")+` WHERE schedule_name=$1`, name).Scan(&id, &count)
					if count > 1 {
						return false, fmt.Errorf("schedule %s created %d runs for one beat", name, count)
					}
					return count == 1, err
				})
				w := scenarioExpected(id, phase, cron[name])
				w.ExpectedAt = new(cronDue[name])
				h.wants[id] = w
				h.record(w)
				ids = append(ids, id)
				if err := h.e.Schedules().Delete(h.ctx, name); err != nil {
					h.t.Fatal(err)
				}
			}
			h.drain(ids)
		}
		h.t.Logf("%s: 100 runs drained", phase)
	}
}

func (h *scenarioHarness) cancelRun(id int64) {
	h.t.Helper()
	err := h.e.Runs().Cancel(h.ctx, id)
	h.record(map[string]any{"operation": "cancel", "id": id, "error": fmt.Sprint(err)})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *scenarioHarness) dedup(rng *rand.Rand) {
	h.t.Helper()
	p := scenarioPayload(rng, scenarioParams{Case: "dedup_pending", Kind: "short", WorkMs: 5}, 1024)
	id := h.trigger("m7_short", "dedup", p, new(h.now().Add(time.Hour)), DedupKey("smoke"))
	w := h.wants[id]
	w.State, w.Invocations = StateCancelled, nil
	h.wants[id] = w
	h.record(w)
	duplicate, err := h.e.Jobs().Trigger(h.ctx, "m7_short", scenarioJSON(p), DedupKey("smoke"))
	h.record(map[string]any{"operation": "duplicate", "id": duplicate, "error": fmt.Sprint(err), "winner": id})
	if !errors.Is(err, ErrDuplicate) || duplicate != id {
		h.t.Fatalf("duplicate: id=%d err=%v, want pending winner %d with ErrDuplicate", duplicate, err, id)
	}
	h.cancelRun(id)
	duplicate, err = h.e.Jobs().Trigger(h.ctx, "m7_short", scenarioJSON(p), DedupKey("smoke"))
	h.record(map[string]any{"operation": "duplicate_after_terminal", "id": duplicate, "error": fmt.Sprint(err), "winner": id})
	if !errors.Is(err, ErrDuplicate) || duplicate != id {
		h.t.Fatalf("duplicate after terminal: id=%d err=%v, want cancelled holder %d with ErrDuplicate", duplicate, err, id)
	}
	p = scenarioPayload(rng, scenarioParams{Case: "dedup_new_key", Kind: "short", WorkMs: 5}, 1024)
	h.drain([]int64{h.trigger("m7_short", "dedup", p, nil, DedupKey("smoke:new"))})
}

func (h *scenarioHarness) mixed(rng *rand.Rand) []int64 {
	h.t.Helper()
	var runningCancels []int64
	for i := range 1000 {
		p := scenarioParams{Case: fmt.Sprintf("mixed_%04d", i), Kind: "short", WorkMs: 5 + i%16}
		switch {
		case i >= 950:
			p.Kind = "cancel"
		case i >= 900:
			p.Kind = "timeout"
		case i >= 850:
			p.Kind = "permanent"
		case i >= 750:
			p.Kind = "retry"
		case i >= 600:
			p.Kind, p.WorkMs = "long", 1000+(i%3)*1000
		}
		size := 1024
		if i%100 == 99 {
			size = 250 << 10
		} else if i%100 >= 90 {
			size = 32 << 10
		}
		p = scenarioPayload(rng, p, size)
		var at *time.Time
		pendingCancel := i >= 950 && i < 975
		if pendingCancel {
			at = new(h.now().Add(time.Hour))
		} else if p.Kind == "short" && i%10 == 0 {
			at = new(h.now().Add(time.Duration(700+(i%24)*100) * time.Millisecond))
		}
		id := h.trigger("m7_"+p.Kind, "mixed", p, at)
		if pendingCancel {
			w := h.wants[id]
			w.Invocations = nil
			h.wants[id] = w
			h.record(w)
			h.cancelRun(id)
		} else if p.Kind == "cancel" {
			runningCancels = append(runningCancels, id)
		}
	}
	return runningCancels
}

func (h *scenarioHarness) cancelEntered(ids []int64) {
	h.t.Helper()
	h.wait("cancel entered executors", func(ctx context.Context) (bool, error) {
		rows, err := h.pool.Query(ctx, `SELECT DISTINCT run_id FROM `+qualified(h.schema, "scenario_invocation")+` WHERE run_id=ANY($1::bigint[])`, ids)
		if err != nil {
			return false, err
		}
		entered, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return false, err
		}
		for _, id := range entered {
			run, err := h.e.Runs().Get(ctx, id)
			if err != nil || run.State != StateRunning {
				return false, fmt.Errorf("cancel target %d did not remain running: %v", id, err)
			}
			h.cancelRun(id)
		}
		ids = slices.DeleteFunc(ids, func(id int64) bool { return slices.Contains(entered, id) })
		return len(ids) == 0, nil
	})
}

func (h *scenarioHarness) workflows(rng *rand.Rand) int64 {
	h.t.Helper()
	var resume int64
	for i := range 10 {
		resume = h.workflow(rng, i)
	}
	return resume
}

func (h *scenarioHarness) workflow(rng *rand.Rand, i int) int64 {
	h.t.Helper()
	name := fmt.Sprintf("m7_wf_%d", i)
	count, phase := 4, "workflow"
	if i == 10 {
		count, phase = 3, "snooze_workflow"
	}
	params := make([]scenarioParams, count)
	nodes := make([]Node, count)
	for j := range count {
		job := fmt.Sprintf("%s_%c", name, 'a'+j)
		if i == 10 {
			job = "m7_snooze_" + []string{"submit", "poll", "verify"}[j]
		}
		p := scenarioParams{Case: job, Kind: "short", WorkMs: 10}
		if i == 10 && j == 1 {
			p.Kind, p.Snoozes, p.SnoozeMs = "snooze", 4, 700
		}
		if (i == 7 || i == 8) && j == 0 {
			p.Kind = "permanent"
		}
		if i == 9 && j == 1 {
			p.Kind, p.Gate = "resume", name
			if _, err := h.pool.Exec(h.ctx, `INSERT INTO `+qualified(h.schema, "scenario_control")+` (key, enabled) VALUES ($1,false)`, name); err != nil {
				h.t.Fatal(err)
			}
		}
		params[j] = scenarioPayload(rng, p, 1024)
		h.declare(job, params[j])
		nodes[j] = Node{Job: job}
		if j > 0 {
			nodes[j].Deps = []string{nodes[j-1].Job}
		}
		if i < 4 && j == 2 {
			nodes[j].Deps = []string{nodes[0].Job}
		}
		if i < 4 && j == 3 {
			nodes[j].Deps = []string{nodes[1].Job, nodes[2].Job}
		}
	}
	if err := h.e.Workflows().Declare(h.ctx, WorkflowSpec{Name: name, Nodes: nodes}); err != nil {
		h.t.Fatal(err)
	}
	started := time.Now()
	id, err := h.e.Workflows().Trigger(h.ctx, name, scenarioJSON(struct{ Value string }{name}))
	elapsed := time.Since(started)
	h.record(map[string]any{"operation": "workflow_trigger", "name": name, "id": id, "error": fmt.Sprint(err), "trigger_ns": elapsed.Nanoseconds()})
	if err != nil {
		h.t.Fatal(err)
	}
	h.latencies[phase+"/trigger"] = append(h.latencies[phase+"/trigger"], elapsed)
	actual, err := h.e.Workflows().GetRun(h.ctx, id)
	if err != nil || len(actual.Nodes) != count {
		h.t.Fatalf("workflow materialization: %v", err)
	}
	byName := map[string]int64{}
	for _, n := range actual.Nodes {
		byName[n.JobName] = n.Id
	}
	for j, n := range nodes {
		w := scenarioExpected(byName[n.Job], phase, params[j])
		deps := map[string]string{}
		for _, name := range n.Deps {
			w.Deps = append(w.Deps, byName[name])
			deps[name] = h.wants[byName[name]].Digest
		}
		w.Digest, w.Key = scenarioDigest(params[j], name, deps), fmt.Sprintf("wf:%d/%s", id, n.Job)
		if (i == 7 || i == 8) && j > 0 {
			w.State, w.Invocations, w.Errors = StateCancelled, nil, []string{"upstream_failed"}
		}
		if i == 9 && j == 1 {
			w.State, w.Attempt, w.Invocations, w.Errors = StateSucceeded, 0, []int{1, 1}, []string{"business"}
		}
		if i == 9 && j > 1 {
			w.Errors = []string{"upstream_failed"}
		}
		h.wants[w.Id] = w
		h.record(w)
	}
	h.workflowWants[id] = WorkflowSucceeded
	if i == 7 || i == 8 {
		h.workflowWants[id] = WorkflowFailed
	}
	return id
}

func (h *scenarioHarness) resumeWorkflow(id int64) {
	h.t.Helper()
	var before *WorkflowRun
	h.wait("initial workflow failure", func(ctx context.Context) (bool, error) {
		var err error
		before, err = h.e.Workflows().GetRun(ctx, id)
		if err != nil {
			return false, err
		}
		return before.State == WorkflowFailed, nil
	})
	for _, node := range before.Nodes {
		want := StateCancelled
		if strings.HasSuffix(node.JobName, "_a") {
			want = StateSucceeded
			if node.StartedAt == nil {
				h.t.Fatal("successful predecessor lacks started_at")
			}
			h.keptStarts[node.Id] = *node.StartedAt
		} else if strings.HasSuffix(node.JobName, "_b") {
			want = StateFailed
		}
		if node.State != want {
			h.t.Fatalf("before Resume: node %s is %s, want %s", node.JobName, node.State, want)
		}
	}
	if _, err := h.pool.Exec(h.ctx, `UPDATE `+qualified(h.schema, "scenario_control")+` SET enabled=true WHERE key=$1`, before.WorkflowName); err != nil {
		h.t.Fatal(err)
	}
	err := h.e.Workflows().Resume(h.ctx, id)
	h.record(map[string]any{"operation": "resume", "id": id, "error": fmt.Sprint(err)})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *scenarioHarness) snoozes(rng *rand.Rand) {
	h.t.Helper()
	h.declare("m7_snooze", scenarioParams{Kind: "snooze"})
	h.declare("m7_snooze_barrier", scenarioParams{Kind: "barrier"})
	const capacity = 3 * 16
	var ids, probes []int64
	deadline := h.now().Add(2 * time.Second)
	for i := range capacity {
		p := scenarioPayload(rng, scenarioParams{Case: fmt.Sprintf("snooze_%02d", i), Kind: "snooze", WorkMs: 5, Snoozes: 2, SnoozeMs: 3000}, 1024)
		ids = append(ids, h.trigger("m7_snooze", "snooze_plain", p, nil))
	}
	var firstDue *time.Time
	h.wait("first Snooze pending snapshots", func(ctx context.Context) (bool, error) {
		var count int
		var now time.Time
		err := h.pool.QueryRow(ctx, `SELECT count(*), min(next_at), clock_timestamp() FROM `+qualified(h.schema, "scenario_invocation")+`
			WHERE run_id=ANY($1::bigint[]) AND next_at IS NOT NULL`, ids).Scan(&count, &firstDue, &now)
		if err == nil && (!now.Before(deadline) || (firstDue != nil && !now.Before(*firstDue))) {
			return false, errors.New("Snooze tasks did not all reach pending before the slot-check deadline")
		}
		return count == capacity, err
	})
	if _, err := h.pool.Exec(h.ctx, `INSERT INTO `+qualified(h.schema, "scenario_control")+` (key, enabled) VALUES ('snooze_slots', false)`); err != nil {
		h.t.Fatal(err)
	}
	for i := range capacity {
		p := scenarioPayload(rng, scenarioParams{Case: fmt.Sprintf("snooze_slot_%02d", i), Kind: "barrier", WorkMs: 5, Gate: "snooze_slots"}, 1024)
		probes = append(probes, h.trigger("m7_snooze_barrier", "snooze_slots", p, nil))
	}
	h.wait("all slots reused before Snooze is due", func(ctx context.Context) (bool, error) {
		var count int
		var now time.Time
		err := h.pool.QueryRow(ctx, `SELECT count(*), clock_timestamp() FROM `+qualified(h.schema, "scenario_invocation")+`
			WHERE run_id=ANY($1::bigint[]) AND returned_at IS NULL`, probes).Scan(&count, &now)
		if err != nil {
			return false, err
		}
		if !now.Before(*firstDue) {
			return false, fmt.Errorf("only %d/%d slots were reusable before Snooze was due", count, capacity)
		}
		if count == capacity {
			h.record(map[string]any{"operation": "snooze_slots_reused", "slots": count, "at": now, "before": firstDue})
		}
		return count == capacity, nil
	})
	if _, err := h.pool.Exec(h.ctx, `UPDATE `+qualified(h.schema, "scenario_control")+` SET enabled=true WHERE key='snooze_slots'`); err != nil {
		h.t.Fatal(err)
	}
	h.drain(append(ids, probes...))
	h.workflow(rng, 10)
	h.drain(slices.Collect(maps.Keys(h.wants)))
	h.t.Log("snooze: 48 repeated jobs, 48 simultaneous slot probes, submit/poll/verify DAG drained")
}

func TestScenarioAcceptance(t *testing.T) {
	if os.Getenv("SKEIN_SCENARIO") == "" {
		t.Skip("set SKEIN_SCENARIO=1 to run M7 scenarios")
	}
	profile := cmp.Or(os.Getenv("SKEIN_SCENARIO_PROFILE"), "smoke")
	if profile != "smoke" {
		t.Fatalf("scenario profile %q is unknown or not implemented; only smoke is available", profile)
	}
	if f := flag.Lookup("test.artifacts"); f == nil || f.Value.String() != "true" {
		t.Fatal("M7 requires -artifacts -outputdir <directory> so evidence survives test cleanup")
	}
	t.Setenv("SKEIN_TEST_REQUIRE_DB", "1")
	h := newScenarioHarness(t)
	rng := rand.New(rand.NewPCG(1, 1))
	for _, kind := range []string{"short", "long", "retry", "permanent", "timeout", "cancel"} {
		h.declare("m7_"+kind, scenarioParams{Kind: kind})
	}
	h.precision(rng)
	held := scenarioPayload(rng, scenarioParams{Case: "held_tx", Kind: "short", WorkMs: 5}, 1024)
	h.drain([]int64{h.triggerTx("transaction", held, 350*time.Millisecond, false)})
	rolledBack := scenarioPayload(rng, scenarioParams{Case: "rolled_back", Kind: "short", WorkMs: 5}, 1024)
	h.triggerTx("transaction", rolledBack, 0, true)
	h.dedup(rng)
	mixedStart := time.Now()
	cancelIds := h.mixed(rng)
	resume := h.workflows(rng)
	h.cancelEntered(cancelIds)
	h.resumeWorkflow(resume)
	h.drain(slices.Collect(maps.Keys(h.wants)))
	h.mixedDuration = time.Since(mixedStart)
	h.snoozes(rng)
	h.stopWorkers()
	h.verify()
}
