package skein

import (
	"cmp"
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mbeoliero/skein/internal/store"
)

// RetryPolicy is the job.retry_policy document. Only MaxAttempts is required; the
// others fall back to Config.BackoffBase / BackoffMax and a 20% jitter.
type RetryPolicy struct {
	MaxAttempts int     `json:"max_attempts"`
	BaseSec     int     `json:"base_sec,omitzero"`
	MaxSec      int     `json:"max_sec,omitzero"`
	Jitter      float64 `json:"jitter,omitzero"`
}

// Keep retry seconds within time.Duration's range.
const maxRetrySeconds = 30 * 24 * 60 * 60

// Attempt is stored as smallint; backoff is computed as time.Duration.
func (p RetryPolicy) validate() error {
	switch {
	case p.MaxAttempts < 1 || p.MaxAttempts > math.MaxInt16:
		return fmt.Errorf("max_attempts %d is not in 1..%d", p.MaxAttempts, math.MaxInt16)
	case p.BaseSec < 0 || p.BaseSec > maxRetrySeconds || p.MaxSec < 0 || p.MaxSec > maxRetrySeconds:
		return fmt.Errorf("base_sec and max_sec must be in 0..%d", maxRetrySeconds)
	case !(p.Jitter >= 0 && p.Jitter < 1): // the negated form also rejects NaN
		return errors.New("jitter must be in [0, 1)")
	}
	return nil
}

// JobSpec declares the execution settings copied into subsequently submitted runs.
type JobSpec struct {
	Name         string
	ExecutorType string
	Params       RawJSON       // template, must be an object; default {}
	Timeout      time.Duration // per attempt; default Config.DefaultTimeout
	Retry        RetryPolicy   // default Config.DefaultRetry
}

// Jobs declares definitions and submits ordinary runs.
type Jobs struct{ e *Engine }

func (e *Engine) Jobs() *Jobs { return &Jobs{e: e} }

// Name bytes also consume space in dedup keys, errors and NOTIFY payloads.
const maxNameLen = 255

func validName(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("skein: %s needs a name", kind)
	case len(name) > maxNameLen:
		return fmt.Errorf("skein: %s name is %d bytes, at most %d", kind, len(name), maxNameLen)
	}
	return nil
}

// Declare upserts the definition by name (§2.1).
func (j *Jobs) Declare(ctx context.Context, spec JobSpec) error {
	if err := cmp.Or(validName("job", spec.Name), validName("executor type", spec.ExecutorType)); err != nil {
		return err
	}
	params, err := j.e.objectPayload(spec.Params)
	if err != nil {
		return fmt.Errorf("skein: job %q params: %w", spec.Name, err)
	}
	timeout := cmp.Or(spec.Timeout, j.e.cfg.DefaultTimeout)
	if err := validTimeout(timeout); err != nil {
		return fmt.Errorf("skein: job %q timeout %w", spec.Name, err)
	}
	retry := spec.Retry
	if retry == (RetryPolicy{}) { // partial policies are validated as given
		retry = j.e.cfg.DefaultRetry
	}
	if err := retry.validate(); err != nil {
		return fmt.Errorf("skein: job %q retry policy: %w", spec.Name, err)
	}
	rp, err := json.Marshal(retry)
	if err != nil {
		return err
	}
	return j.e.st.DeclareJob(ctx, store.DeclareJobParams{
		Name: spec.Name, ExecutorType: spec.ExecutorType, Params: params,
		Timeout: int32(timeout / time.Second), RetryPolicy: rp,
	})
}

// Timeouts are stored as whole seconds in an integer column.
func validTimeout(d time.Duration) error {
	if d < time.Second || d/time.Second > math.MaxInt32 {
		return fmt.Errorf("must be between 1s and %d seconds, got %s", math.MaxInt32, d)
	}
	return nil
}

// Delete removes the definition; runs keep their snapshots. A job used by a workflow
// node or a schedule is ErrReferenced.
func (j *Jobs) Delete(ctx context.Context, name string) error {
	return mapErr(j.e.st.DeleteJob(ctx, name), name)
}

// TriggerOption configures submission through At or DedupKey.
type TriggerOption func(*triggerOptions)

type triggerOptions struct {
	at    *time.Time
	dedup *string
}

// At delays the run: it becomes claimable at t.
func At(t time.Time) TriggerOption { return func(o *triggerOptions) { o.at = &t } }

// DedupKey makes Trigger idempotent within the retention window (§2.1): a later
// Trigger of the same job or workflow with the same key returns the existing run's
// id with ErrDuplicate, whatever its state, and ignores the new params. Retry a
// finished run with Resume, not a new Trigger; the key is not an overlap guard, that
// is a schedule's OverlapSkip. Id 0 with ErrDuplicate means the holder vanished
// mid-call: retry.
func DedupKey(k string) TriggerOption { return func(o *triggerOptions) { o.dedup = &k } }

// Trigger creates one pending run of the job with params merged over the template.
func (j *Jobs) Trigger(ctx context.Context, name string, params RawJSON, opts ...TriggerOption) (int64, error) {
	id, err := j.trigger(ctx, nil, name, params, opts)
	if err == nil {
		j.e.wakeClaimer()
	}
	return id, err
}

// TriggerTx applies Trigger inside tx without committing it. The run and its wakeup
// become visible only when the caller commits. ErrDuplicate returns the retained
// holder's id (possibly zero if it vanished), without aborting tx.
// The caller's search_path is restored before return. A SQL or path-restoration
// failure requires the caller to roll back; a restoration failure takes precedence
// over a dedup result. The caller must not use tx concurrently with this call.
func (j *Jobs) TriggerTx(ctx context.Context, tx pgx.Tx, name string, params RawJSON, opts ...TriggerOption) (int64, error) {
	if tx == nil {
		return 0, errors.New("skein: TriggerTx needs a transaction")
	}
	return j.trigger(ctx, tx, name, params, opts)
}

func (j *Jobs) trigger(ctx context.Context, tx pgx.Tx, name string, params RawJSON, opts []TriggerOption) (int64, error) {
	var o triggerOptions
	for _, opt := range opts {
		opt(&o)
	}
	p, err := j.e.objectPayload(params)
	if err != nil {
		return 0, fmt.Errorf("skein: trigger %q params: %w", name, err)
	}
	id, err := j.e.st.TriggerJob(ctx, tx, store.TriggerJobParams{
		JobName: name, Params: p, DedupKey: o.dedup, RunAt: o.at, Extra: traceExtra(ctx),
	})
	return id, mapErr(err, name)
}

// Missing input becomes {}; it must still pass MaxPayload and object validation.
func (e *Engine) objectPayload(raw RawJSON) ([]byte, error) {
	if len(raw) == 0 {
		raw = RawJSON("{}")
	}
	if len(raw) > e.cfg.MaxPayload {
		return nil, fmt.Errorf("%d bytes exceeds MaxPayload %d", len(raw), e.cfg.MaxPayload)
	}
	if !raw.IsValid() || raw.Kind() != '{' {
		return nil, errors.New("must be a JSON object")
	}
	return raw, nil
}

func mapErr(err error, name string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	case errors.Is(err, store.ErrDuplicate):
		return ErrDuplicate
	case errors.Is(err, store.ErrReferenced):
		return fmt.Errorf("%w: %q", ErrReferenced, name)
	case errors.Is(err, store.ErrNotResumable):
		return fmt.Errorf("%w: %s", ErrNotResumable, name)
	}
	return err
}
