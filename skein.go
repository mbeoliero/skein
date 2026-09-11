// Package skein is an embedded job scheduler on PostgreSQL: one-off and delayed jobs,
// cron schedules and DAG workflows, shared by every process that runs an Engine
// against the same database. docs/design.md is the specification.
package skein

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
)

const defaultSchema = "skein"

// Reject aliases before pgx removes NULs or PostgreSQL truncates identifiers.
func validSchema(schema string) error {
	switch {
	case !utf8.ValidString(schema):
		return errors.New("skein: schema must be valid UTF-8")
	case strings.ContainsRune(schema, 0):
		return errors.New("skein: schema must not contain NUL")
	case len(schema) > 63:
		return errors.New("skein: schema must be at most 63 bytes")
	}
	return nil
}

// Zero values use the defaults below; validation follows design §3.1.
type Config struct {
	Schema           string // valid UTF-8, no NUL, at most 63 bytes; default "skein"
	DisableWorker    bool   // no claiming or execution; the scheduler is independent (§1.1)
	DisableScheduler bool   // no schedule scan or retention on this process (§1.1)

	Concurrency       int           // execution slots in this process, not a cluster limit; default 16
	PollInterval      time.Duration // backstop period of the claim and scan loops, ±20% jitter (§2.7); default 5s
	HeartbeatInterval time.Duration // default 15s
	LeaseTTL          time.Duration // default 60s

	DefaultTimeout time.Duration // Declare without Timeout; default 1h
	DefaultRetry   RetryPolicy   // Declare without Retry; default {MaxAttempts: 3}
	BackoffBase    time.Duration // retry backoff when retry_policy has no base_sec; default 5s
	BackoffMax     time.Duration // default 5m

	ShutdownGrace         time.Duration // wait for executors before cancelling them; default 30s
	CancelTimeout         time.Duration // wait after cancelling before giving up on an executor; default 10s
	ReleaseAlertThreshold int           // released entries on one run that trigger the alert; default 3
	MaxPayload            int           // bytes per input document (params template, Trigger override, workflow input) and per output, each checked on its own; stored params are template merged with override, up to twice this; default 256 KiB
	MaxNodes              int           // nodes per workflow; default 500

	RetentionSucceeded  time.Duration // delete succeeded runs after; default 7d
	RetentionFailed     time.Duration // delete failed / cancelled runs after; default 30d
	MaintenanceInterval time.Duration // retention period; default 1h

	Logger   *slog.Logger // default slog.Default()
	Metrics  Metrics      // default discards
	Observer Observer     // optional concurrent post-commit callbacks; must return promptly
}

func (c Config) withDefaults() Config {
	c.Schema = cmp.Or(c.Schema, defaultSchema)
	c.Concurrency = cmp.Or(c.Concurrency, 16)
	c.PollInterval = cmp.Or(c.PollInterval, 5*time.Second)
	c.HeartbeatInterval = cmp.Or(c.HeartbeatInterval, 15*time.Second)
	c.LeaseTTL = cmp.Or(c.LeaseTTL, 60*time.Second)
	c.DefaultTimeout = cmp.Or(c.DefaultTimeout, time.Hour)
	if c.DefaultRetry == (RetryPolicy{}) { // partial policies are validated as given
		c.DefaultRetry = RetryPolicy{MaxAttempts: 3}
	}
	c.BackoffBase = cmp.Or(c.BackoffBase, 5*time.Second)
	c.BackoffMax = cmp.Or(c.BackoffMax, 5*time.Minute)
	c.ShutdownGrace = cmp.Or(c.ShutdownGrace, 30*time.Second)
	c.CancelTimeout = cmp.Or(c.CancelTimeout, 10*time.Second)
	c.ReleaseAlertThreshold = cmp.Or(c.ReleaseAlertThreshold, 3)
	c.MaxPayload = cmp.Or(c.MaxPayload, 256<<10)
	c.MaxNodes = cmp.Or(c.MaxNodes, 500)
	c.RetentionSucceeded = cmp.Or(c.RetentionSucceeded, 7*24*time.Hour)
	c.RetentionFailed = cmp.Or(c.RetentionFailed, 30*24*time.Hour)
	c.MaintenanceInterval = cmp.Or(c.MaintenanceInterval, time.Hour)
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Metrics == nil {
		c.Metrics = nopMetrics{}
	}
	return c
}

// Validate after defaulting; invalid periods must not reach loop tickers.
func (c Config) validate() error {
	if err := validSchema(c.Schema); err != nil {
		return err
	}
	durations := []struct {
		name string
		v    time.Duration
	}{
		{"PollInterval", c.PollInterval}, {"HeartbeatInterval", c.HeartbeatInterval}, {"LeaseTTL", c.LeaseTTL},
		{"DefaultTimeout", c.DefaultTimeout}, {"BackoffBase", c.BackoffBase}, {"BackoffMax", c.BackoffMax},
		{"ShutdownGrace", c.ShutdownGrace}, {"CancelTimeout", c.CancelTimeout},
		{"RetentionSucceeded", c.RetentionSucceeded}, {"RetentionFailed", c.RetentionFailed}, {"MaintenanceInterval", c.MaintenanceInterval},
	}
	for _, d := range durations {
		if d.v <= 0 {
			return fmt.Errorf("skein: %s must be > 0, got %s", d.name, d.v)
		}
	}
	counts := []struct {
		name string
		v    int
	}{{"Concurrency", c.Concurrency}, {"ReleaseAlertThreshold", c.ReleaseAlertThreshold}, {"MaxPayload", c.MaxPayload}, {"MaxNodes", c.MaxNodes}}
	for _, n := range counts {
		if n.v < 1 {
			return fmt.Errorf("skein: %s must be >= 1, got %d", n.name, n.v)
		}
	}
	if err := c.DefaultRetry.validate(); err != nil {
		return fmt.Errorf("skein: DefaultRetry: %w", err)
	}
	switch {
	case c.LeaseTTL <= c.HeartbeatInterval:
		return errors.New("skein: LeaseTTL must exceed HeartbeatInterval")
	case validTimeout(c.DefaultTimeout) != nil:
		return fmt.Errorf("skein: DefaultTimeout %w", validTimeout(c.DefaultTimeout))
	case c.BackoffMax < c.BackoffBase:
		return errors.New("skein: BackoffMax must be >= BackoffBase")
	case c.RetentionFailed < c.RetentionSucceeded:
		return errors.New("skein: RetentionFailed must be >= RetentionSucceeded")
	}
	return nil
}

// Engine is one process's membership in the shared scheduler.
type Engine struct {
	cfg     Config
	st      *store.Store
	log     *slog.Logger
	metrics Metrics
	owner   string // lease_owner: host:pid:rand, diagnostics only

	executors map[string]Executor
	types     atomic.Pointer[[]string] // sorted executor types, republished by Register; loops and Stats read the snapshot
	started   atomic.Bool
	ready     atomic.Bool // successful loop startup; started also covers the schema check

	claimHealth     loopHealth
	heartbeatHealth loopHealth
	scanHealth      loopHealth

	wake      chan struct{}
	wakeSched chan struct{}
	slots     chan struct{} // Concurrency semaphore
	inflight  inflightSet   // leases this process renews
	active    sync.Map      // lease token → run id, one entry per executor goroutine still running
	lastOK    time.Time     // last successful heartbeat's start, monotonic; heartbeat goroutine only (§2.3)
	claimTurn uint8         // effective claim rounds modulo reclaimEvery; owned by the claim loop

	lifecycle sync.Mutex // serializes lifecycle transitions, never startup database I/O
	loopCtx   context.Context
	stopLoops context.CancelFunc
	loops     sync.WaitGroup
	hbCtx     context.Context // heartbeat outlives the claim loop during Shutdown
	stopHb    context.CancelFunc
	hb        sync.WaitGroup
	execs     sync.WaitGroup

	shutOnce sync.Once
	shutting atomic.Bool   // set by the first Shutdown caller before it takes lifecycle
	shutDone chan struct{} // closed when that run has finished; shutErr is readable after
	shutErr  error
}

// New validates cfg and creates an Engine using the caller-owned pool. Call
// Migrate explicitly before Start; New performs no database I/O or background work.
func New(pool *pgxpool.Pool, cfg Config) (*Engine, error) {
	if pool == nil {
		return nil, errors.New("skein: nil pool")
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	host, _ := os.Hostname()
	owner := fmt.Sprintf("%s:%d:%s", cmp.Or(host, "?"), os.Getpid(), uuid.New().String()[:8])
	e := &Engine{
		cfg:       cfg,
		st:        store.Open(pool, cfg.Schema),
		log:       cfg.Logger.With("instance", owner),
		metrics:   cfg.Metrics,
		owner:     owner,
		executors: map[string]Executor{},
		wake:      make(chan struct{}, 1),
		wakeSched: make(chan struct{}, 1),
		shutDone:  make(chan struct{}),
		slots:     make(chan struct{}, cfg.Concurrency),
	}
	e.inflight.m = map[int64]*inflight{}
	e.loopCtx, e.stopLoops = context.WithCancel(context.Background())
	e.hbCtx, e.stopHb = context.WithCancel(context.Background())
	return e, nil
}

// Start checks the schema version and starts the loops. Cancelling ctx is the same
// as calling Shutdown. On error nothing is left running. An Engine is started once;
// after Shutdown it cannot be started again.
func (e *Engine) Start(ctx context.Context) error {
	e.lifecycle.Lock()
	if e.shutting.Load() {
		e.lifecycle.Unlock()
		return errors.New("skein: Start after Shutdown")
	}
	if e.started.Swap(true) {
		e.lifecycle.Unlock()
		return errors.New("skein: Start called twice")
	}
	e.lifecycle.Unlock()

	// §2.6: shutdown cancels startup I/O without waiting for its lifecycle lock.
	startCtx, cancelStart := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(e.loopCtx, cancelStart)
	v, err := e.st.SchemaVersion(startCtx)
	stopCancel()
	cancelStart()

	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()
	if e.shutting.Load() {
		e.started.Store(false)
		return errors.New("skein: Start after Shutdown")
	}
	if err != nil {
		e.started.Store(false)
		return fmt.Errorf("skein: read schema version: %w", err)
	}
	if v != schemaVersion {
		e.started.Store(false)
		return fmt.Errorf("skein: schema version is %d, this build needs %d: run Migrate", v, schemaVersion)
	}
	if !e.cfg.DisableWorker {
		e.lastOK = time.Now()
		e.loops.Go(func() { e.claimLoop(e.loopCtx) })
		e.hb.Go(func() { e.heartbeatLoop(e.hbCtx) })
	}
	if !e.cfg.DisableScheduler {
		e.loops.Go(func() { e.scheduleLoop(e.loopCtx) })
		e.loops.Go(func() { e.maintenanceLoop(e.loopCtx) })
	}
	if !e.cfg.DisableWorker || !e.cfg.DisableScheduler {
		e.loops.Go(func() { e.listenLoop(e.loopCtx) })
	}
	e.ready.Store(true)
	context.AfterFunc(ctx, func() { _ = e.Shutdown(context.Background()) })
	return nil
}

// registeredTypes is the sorted snapshot Register published last. Never nil: pgx
// sends a nil slice as NULL, so ANY(NULL) cannot classify registration in Stats.
func (e *Engine) registeredTypes() []string {
	if p := e.types.Load(); p != nil {
		return *p
	}
	return []string{}
}

// Shutdown drains this process (§2.6). Later callers wait for the first call's
// result or their own ctx error, without inheriting the first caller's budget.
// ErrNotDrained means executors remain running; their leases expire for takeover.
// The first call is bounded by its ctx plus one in-flight claim or scan transaction
// (claimTimeout). Maintenance and heartbeat I/O are cancelled.
func (e *Engine) Shutdown(ctx context.Context) error {
	first := false
	e.shutOnce.Do(func() {
		first = true
		e.shutting.Store(true)
	})
	if !first {
		select {
		case <-e.shutDone:
			return e.shutErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.lifecycle.Lock() // serialize with loop startup, not the schema query
	defer e.lifecycle.Unlock()
	e.shutErr = e.shutdown(ctx)
	close(e.shutDone)
	return e.shutErr
}

func (e *Engine) shutdown(ctx context.Context) error {
	// Stop admission under its lock; an in-flight claim or scan keeps its bounded ctx.
	e.inflight.mu.Lock()
	e.stopLoops()
	e.inflight.mu.Unlock()
	e.loops.Wait()
	// Heartbeat continues through both executor waits.
	if !waitGroup(ctx, &e.execs, e.cfg.ShutdownGrace) {
		e.inflight.cancelAll(errShuttingDown)
		waitGroup(ctx, &e.execs, e.cfg.CancelTimeout)
	}
	// Stop renewal but retain the identities of executors still running.
	e.inflight.drain()
	left := e.activeIds()
	// Cancel heartbeat I/O only after inflight is empty.
	e.stopHb()
	e.hb.Wait()
	if len(left) > 0 {
		e.log.Warn("shutdown left executors running", "run_ids", left)
		return fmt.Errorf("%w: %v", ErrNotDrained, left)
	}
	return nil
}

func waitGroup(ctx context.Context, wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
	case <-ctx.Done():
	}
	return false
}

func (e *Engine) wakeClaimer() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) wakeScheduler() {
	select {
	case e.wakeSched <- struct{}{}:
	default:
	}
}
