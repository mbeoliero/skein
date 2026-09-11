package skein

import (
	"context"
	"crypto/sha256"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sync/atomic"
	"syscall"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
			FROM `+runs+` WHERE id=$7 AND state='running' AND extra->>'lease_owner'=$3 AND attempt=$2-1`,
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
