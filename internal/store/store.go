// Package store is the persistence layer: the sqlc output in *.sql.go plus this file,
// which holds one method per design §2 transaction. Nothing outside this package writes SQL.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrLeaseLost: a fenced statement matched zero rows (§2.9). Roll back, drop, never retry.
	ErrLeaseLost  = errors.New("store: lease lost")
	ErrNotFound   = errors.New("store: not found")
	ErrDuplicate  = errors.New("store: duplicate")
	ErrReferenced = errors.New("store: referenced")
	// ErrInvalidOutput: jsonb rejected a settlement document, the output or the errors
	// entry (SQLSTATE class 22). The text ends up in job_run.errors, hence no "store:" prefix.
	ErrInvalidOutput = errors.New("rejected by the database")
)

type Store struct {
	pool       *pgxpool.Pool
	q          *Queries
	path       string // quoted schema name: CREATE SCHEMA and qualified names
	searchPath string // path + ", pg_temp": the transaction-local search_path
}

// Open quotes the schema once. pg_temp is listed after it on purpose: a temporary
// schema that is not named in search_path is searched before every schema that is,
// so a temp table named job_run in the host's session (TriggerTx, or left on a pooled
// connection) would silently take the library's reads and writes.
func Open(pool *pgxpool.Pool, schema string) *Store {
	path := pgx.Identifier{schema}.Sanitize()
	return &Store{pool: pool, q: New(), path: path, searchPath: path + ", pg_temp"}
}

// beginner is what a transaction is opened on: the pool, or one connection when a
// session-scoped lock must stay on it (Maintain).
type beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// cleanupTimeout bounds what runs after a failure or a cancellation: a rollback, the
// restore of the caller's search_path, the maintenance unlock. Detached from the
// caller's ctx (which may be the reason for the cleanup) but never unbounded.
const cleanupTimeout = 5 * time.Second

func cleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
}

// tx runs fn in its own transaction on the pool.
func (s *Store) tx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	return s.txOn(ctx, s.pool, fn)
}

// txOn runs fn in one transaction on db with search_path set to the skein schema for
// that transaction only (set_config(..., true)); the host's pool is not touched.
func (s *Store) txOn(ctx context.Context, db beginner, fn func(ctx context.Context, tx pgx.Tx) error) (err error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			rctx, cancel := cleanupCtx(ctx)
			defer cancel()
			_ = tx.Rollback(rctx)
		}
	}()
	if err = s.q.SetSearchPath(ctx, tx, s.searchPath); err != nil {
		return err
	}
	if err = fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// inCallerTx runs fn inside the caller's transaction (TriggerTx) and restores the
// caller's search_path before returning, whatever fn returned.
func (s *Store) inCallerTx(ctx context.Context, tx pgx.Tx, fn func(ctx context.Context, tx pgx.Tx) error) error {
	old, err := s.q.CurrentSearchPath(ctx, tx)
	if err != nil {
		return err
	}
	if err := s.q.SetSearchPath(ctx, tx, s.searchPath); err != nil {
		return err
	}
	err = fn(ctx, tx)
	// The restore runs on its own bounded ctx: with the caller's ctx already cancelled
	// pgx would not even send it, and the caller's transaction would go on inside the
	// library's schema. A failed restore is returned alone: the transaction can no
	// longer be trusted, so a business result such as ErrDuplicate must not be acted on.
	rctx, cancel := cleanupCtx(ctx)
	defer cancel()
	if rerr := s.q.SetSearchPath(rctx, tx, old); rerr != nil {
		return fmt.Errorf("store: restore search_path: %w", rerr)
	}
	return err
}

// ───────────── migrations (§2.6, §1.3) ─────────────

type Migration struct {
	Version int
	Sql     string
}

// Migrate creates the schema if needed and applies every migration newer than the
// recorded version, all in one transaction under an advisory lock.
func (s *Store) Migrate(ctx context.Context, migrations []Migration) (err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			rctx, cancel := cleanupCtx(ctx)
			defer cancel()
			_ = tx.Rollback(rctx)
		}
	}()
	// The lock comes first so that it also serializes CREATE SCHEMA: IF NOT EXISTS is
	// not atomic, and of two first migrations racing on the same name the loser would
	// fail on the pg_namespace unique index (§1.3). The lock needs no search_path.
	if err = s.q.MigrateLock(ctx, tx); err != nil {
		return err
	}
	// The only SQL outside queries/: CREATE SCHEMA takes an identifier, not a parameter.
	if _, err = tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+s.path); err != nil {
		return err
	}
	if err = s.q.SetSearchPath(ctx, tx, s.searchPath); err != nil {
		return err
	}
	current, err := s.schemaVersion(ctx, tx)
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if m.Version <= current {
			continue
		}
		if _, err = tx.Exec(ctx, m.Sql); err != nil {
			return fmt.Errorf("migration %d: %w", m.Version, err)
		}
		if err = s.q.RecordSchemaVersion(ctx, tx, int32(m.Version)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// SchemaVersion is 0 before the first migration.
func (s *Store) SchemaVersion(ctx context.Context) (v int, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		v, err = s.schemaVersion(ctx, tx)
		return err
	})
	return v, err
}

func (s *Store) schemaVersion(ctx context.Context, tx pgx.Tx) (int, error) {
	found, err := s.q.SchemaVersionTableExists(ctx, tx, s.path+".schema_version")
	if err != nil || !found {
		return 0, err
	}
	v, err := s.q.CurrentSchemaVersion(ctx, tx)
	return int(v), err
}

// ───────────── definitions (§2.1) ─────────────

func (s *Store) DeclareJob(ctx context.Context, p DeclareJobParams) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return s.q.DeclareJob(ctx, tx, p)
	})
}

func (s *Store) DeleteJob(ctx context.Context, name string) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		n, err := s.q.DeleteJob(ctx, tx, name)
		if isReferenced(err) {
			return ErrReferenced
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// ───────────── submit (§2.1) ─────────────

// TriggerJob snapshots the job into a pending run. With tx == nil it uses its own
// transaction; otherwise it runs inside the caller's. ErrDuplicate carries the id of
// the run holding the dedup key: for a user key any run still in the retention
// window, for an overlap=skip beat the in-flight one (0 if it vanished meanwhile).
func (s *Store) TriggerJob(ctx context.Context, tx pgx.Tx, p TriggerJobParams) (id int64, err error) {
	fn := func(ctx context.Context, tx pgx.Tx) error {
		id, err = s.triggerJob(ctx, tx, p)
		return err
	}
	if tx == nil {
		err = s.tx(ctx, fn)
	} else {
		err = s.inCallerTx(ctx, tx, fn)
	}
	return id, err
}

// dedupRounds bounds the insert / lookup loop of a dedup conflict (§2.1): the holder
// can vanish between the INSERT and the lookup (a user key's run deleted by retention,
// a skip beat that finished), and then the key is free again, so the INSERT is simply
// repeated. Three rounds cover a key that keeps changing hands; after that
// ErrDuplicate carries id 0 and the caller retries.
const dedupRounds = 3

// findDedupHolder looks up the run a conflicting INSERT collided with. The two
// partial indexes are told apart by schedule_name (§1.3): only the scheduler writes
// it, so a user key never matches a beat and the lookup mirrors the index it hit.
func (s *Store) findDedupHolder(ctx context.Context, tx pgx.Tx, p TriggerJobParams) (int64, error) {
	if p.ScheduleName == nil {
		return s.q.FindDedupRun(ctx, tx, FindDedupRunParams{JobName: p.JobName, DedupKey: *p.DedupKey})
	}
	return s.q.FindInflightBeat(ctx, tx, FindInflightBeatParams{JobName: p.JobName, DedupKey: *p.DedupKey})
}

func (s *Store) triggerJob(ctx context.Context, tx pgx.Tx, p TriggerJobParams) (int64, error) {
	for range dedupRounds {
		id, err := s.q.TriggerJob(ctx, tx, p)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
		found, err := s.q.JobExists(ctx, tx, p.JobName)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, ErrNotFound
		}
		if p.DedupKey == nil {
			return 0, ErrDuplicate // the (schedule_name, scheduled_at) index: that beat exists, nothing to look up
		}
		id, err = s.findDedupHolder(ctx, tx, p)
		if err == nil {
			return id, ErrDuplicate
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
	}
	return 0, ErrDuplicate
}

func (s *Store) GetJobRun(ctx context.Context, id int64) (r JobRun, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		r, err = s.q.GetJobRun(ctx, tx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	return r, err
}

// ───────────── claim (§2.2) ─────────────

// Claimed is what both claim branches hand to the worker.
type Claimed ClaimPendingRow

// ClaimPending is branch one: due pending rows, ordered by run_at.
func (s *Store) ClaimPending(ctx context.Context, types []string, limit int, owner string, ttl time.Duration) (out []Claimed, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.q.ClaimPending(ctx, tx, ClaimPendingParams{ExecutorTypes: types, Lim: int32(limit), Owner: owner, LeaseTtl: ttl})
		for _, r := range rows {
			out = append(out, Claimed(r))
		}
		return err
	})
	return out, err
}

// ClaimExpired is branch two: running rows whose lease expired; attempt +1, an interrupted entry appended.
func (s *Store) ClaimExpired(ctx context.Context, types []string, limit int, owner string, ttl time.Duration) (out []Claimed, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.q.ClaimExpired(ctx, tx, ClaimExpiredParams{ExecutorTypes: types, Lim: int32(limit), Owner: owner, LeaseTtl: ttl})
		for _, r := range rows {
			out = append(out, Claimed(r))
		}
		return err
	})
	return out, err
}

// Due is the nearest future due time with the clocks it was read against (§2.7):
// DbNow is clock_timestamp() from the same statement, Sampled the local clock when
// that statement returned, taken inside the transaction so that the COMMIT round trip
// is part of what the caller subtracts from its wait. Next is nil when nothing is ahead.
type Due struct {
	Next    *time.Time
	DbNow   time.Time
	Sampled time.Time
}

// NextPendingAt is the nearest future run_at among types (§2.2 / §2.7): Next is nil
// when nothing is scheduled ahead.
func (s *Store) NextPendingAt(ctx context.Context, types []string) (d Due, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := s.q.NextPendingAt(ctx, tx, types)
		d.Sampled = time.Now()
		if err != nil {
			return err
		}
		for _, r := range rows {
			if d.Next == nil || r.NextDue.Before(*d.Next) {
				d.Next, d.DbNow = &r.NextDue, r.DbNow
			}
		}
		return nil
	})
	return d, err
}

// ───────────── heartbeat (§2.3) ─────────────

// Heartbeat renews every (id, token) pair in one statement; ids absent from the
// result no longer belong to the caller.
func (s *Store) Heartbeat(ctx context.Context, ids []int64, tokens []uuid.UUID, ttl, cancelTimeout time.Duration) (rows []HeartbeatRow, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err = s.q.Heartbeat(ctx, tx, HeartbeatParams{Ids: ids, Tokens: tokens, LeaseTtl: ttl, CancelTimeout: cancelTimeout})
		return err
	})
	return rows, err
}

// ───────────── settle (§2.4) ─────────────

type Outcome int

// The zero Outcome is invalid on purpose: Settle rejects it instead of guessing.
const (
	Succeeded Outcome = iota + 1
	Cancelled
	Released    // graceful shutdown: pending again, attempt unchanged
	Retry       // retryable failure with attempts left
	Failed      // permanent failure or no attempts left
	Interrupted // reclaimed with attempt >= max_attempts (§2.2)
	Snoozed     // normal waiting: pending later, attempt/errors/output unchanged
)

type Settlement struct {
	Id            int64
	Token         uuid.UUID
	WorkflowRunId *int64 // node instance: the parent is locked first and propagate runs after
	JobName       string
	Outcome       Outcome
	Output        []byte        // Succeeded
	Err           []byte        // one-element JSON array: Released, Retry, Failed; optional for Cancelled
	Backoff       time.Duration // Retry
	Delay         time.Duration // Snoozed
}

type Settled struct {
	State         string
	ReleasedCount int // Released only: entries of kind released on this run, for the alert
}

// Settle is the only path out of running. The fence is WHERE lease_token = $token AND
// state = 'running'; zero rows is ErrLeaseLost and nothing is written. For a node the
// parent row is locked first (lock order §2.9) and propagate runs in the same transaction.
func (s *Store) Settle(ctx context.Context, st Settlement) (res Settled, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var wfState string
		var dag Dag
		if st.WorkflowRunId != nil {
			w, err := s.q.LockWorkflowRun(ctx, tx, *st.WorkflowRunId)
			if err != nil {
				return err
			}
			wfState = w.State
			if dag, err = parseDag(w.Dag); err != nil {
				return err
			}
		}
		wfCancelling := wfState == "cancelling"
		var state string
		var err error
		switch st.Outcome {
		case Succeeded:
			state, err = s.q.SettleSucceeded(ctx, tx, SettleSucceededParams{Id: st.Id, Token: st.Token, Output: st.Output})
		case Cancelled:
			state, err = s.q.SettleCancelled(ctx, tx, SettleCancelledParams{Id: st.Id, Token: st.Token, Err: st.Err})
		case Released:
			var row SettleReleasedRow
			row, err = s.q.SettleReleased(ctx, tx, SettleReleasedParams{Id: st.Id, Token: st.Token, Err: st.Err, WfCancelling: wfCancelling})
			state, res.ReleasedCount = row.State, int(row.ReleasedCount)
		case Retry:
			state, err = s.q.SettleRetry(ctx, tx, SettleRetryParams{Id: st.Id, Token: st.Token, Err: st.Err, Backoff: st.Backoff, WfCancelling: wfCancelling})
		case Snoozed:
			if st.Delay <= 0 {
				return fmt.Errorf("store: snooze delay must be > 0, got %s", st.Delay)
			}
			// pgx truncates to microseconds; SQL rounds up without overflowing Duration.
			state, err = s.q.SettleSnoozed(ctx, tx, SettleSnoozedParams{
				Id: st.Id, Token: st.Token, Delay: st.Delay,
				RoundUp: st.Delay%time.Microsecond != 0, WfCancelling: wfCancelling,
			})
		case Failed:
			state, err = s.q.SettleFailed(ctx, tx, SettleFailedParams{Id: st.Id, Token: st.Token, Err: st.Err, WfCancelling: wfCancelling})
		case Interrupted:
			state, err = s.q.SettleInterrupted(ctx, tx, SettleInterruptedParams{Id: st.Id, Token: st.Token, WfCancelling: wfCancelling})
		default:
			return fmt.Errorf("store: unknown outcome %d", st.Outcome)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if reason, ok := dataException(err); ok {
			// the output or the errors entry: either is deterministic, never a retry
			return fmt.Errorf("%w: %s", ErrInvalidOutput, reason)
		}
		if err != nil {
			return err
		}
		res.State = state
		if st.WorkflowRunId != nil && terminal(state) {
			return s.propagate(ctx, tx, *st.WorkflowRunId, dag, wfState, st.JobName, state)
		}
		return nil
	})
	return res, err
}

// isReferenced: ON DELETE RESTRICT raises 23001 (restrict_violation); 23503 is the plain FK violation.
func isReferenced(err error) bool {
	return isPgCode(err, "23001") || isPgCode(err, "23503")
}

func isPgCode(err error, code string) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == code
}

// dataException: SQLSTATE class 22, what jsonb raises for a document it cannot store
// (22P02 invalid syntax, 22P05 \u0000, 22003 numeric overflow); the reason is the
// server's message plus detail.
func dataException(err error) (reason string, ok bool) {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || !strings.HasPrefix(pgErr.Code, "22") {
		return "", false
	}
	return strings.TrimSpace(pgErr.Message + " " + pgErr.Detail), true
}

// ───────────── cancel (§2.5) ─────────────

// ResumeRun never locks a workflow node; terminal state is rechecked by the UPDATE.
func (s *Store) ResumeRun(ctx context.Context, id int64) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		n, err := s.q.ResumeRun(ctx, tx, id)
		if isPgCode(err, "23505") {
			return ErrDuplicate
		}
		if err != nil || n > 0 {
			return err
		}
		isNode, err := s.q.RunIsNode(ctx, tx, id)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return ErrNotFound
		case err != nil:
			return err
		case isNode:
			return ErrNode
		default:
			return ErrNotResumable
		}
	})
}

// ErrNode: the run is a workflow node; cancel and resume go through its workflow.
var ErrNode = errors.New("store: run is a workflow node")

// CancelRun cancels a plain run: pending ends now, running is flagged for its holder,
// in one statement so a row moving between the two is re-checked on its new version.
// Zero rows means the row is terminal (idempotent), a node, or missing.
func (s *Store) CancelRun(ctx context.Context, id int64) error {
	return s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := s.q.CancelRun(ctx, tx, id)
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		isNode, err := s.q.RunIsNode(ctx, tx, id)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return ErrNotFound
		case err != nil:
			return err
		case isNode:
			return ErrNode
		}
		return nil
	})
}
