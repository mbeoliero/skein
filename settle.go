package skein

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

// settleTimeout bounds the settle transaction; it never uses the executor's ctx.
const settleTimeout = 15 * time.Second

// errEntry is one element of job_run.errors.
type errEntry struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"` // business | timeout | interrupted | panic | upstream_failed | upstream_cancelled | cancelled | released
	Message string    `json:"message"`
}

// errMessageMax bounds one entry's message: errors accumulates across attempts and resumes.
const errMessageMax = 4096

// Executor messages must remain persistable: JSON/JSONB rejection would leave a
// permanent failure running for reclamation (§2.4). Replace invalid UTF-8/NUL,
// truncate, and record a fixed message if encoding still fails.
func encodeErr(e errEntry) []byte {
	e.Message = cleanMessage(e.Message)
	b, err := json.Marshal([]errEntry{e})
	if err != nil {
		e.Message = "error message could not be encoded"
		b, _ = json.Marshal([]errEntry{e})
	}
	return b
}

func cleanMessage(s string) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	s = strings.ReplaceAll(s, "\x00", "\uFFFD")
	if len(s) > errMessageMax {
		s = strings.ToValidUTF8(s[:errMessageMax], "") // drops a rune cut in half
	}
	return s
}

type executionResult struct {
	output RawJSON
	err    error
	cause  error
}

type resultClass struct {
	outcome   store.Outcome
	reason    string
	output    RawJSON
	delay     time.Duration
	entry     errEntry
	retryable bool
}

// classifyResult applies §3.2 without I/O, clock reads or retry sampling. The
// database's fence and cancellation guard still decide the committed state.
func classifyResult(result executionResult, maxPayload int) resultClass {
	out, err, cause := result.output, result.err, result.cause
	class := resultClass{
		outcome: store.Failed, entry: errEntry{Kind: "business"}, retryable: true,
	}
	snooze, _ := errors.AsType[*snoozeError](err)
	cancelled, _ := errors.AsType[*cancelError](err)
	switch {
	case errors.Is(cause, errCancelRequested):
		class.outcome, class.reason = store.Cancelled, "cancel_requested"
	case err == nil && len(out) > maxPayload:
		class.entry.Message = "output exceeds MaxPayload"
		class.reason, class.retryable = "invalid_output", false
	case err == nil && len(out) > 0 && !out.IsValid():
		class.entry.Message = "output is not valid JSON"
		class.reason, class.retryable = "invalid_output", false
	case err == nil:
		class.outcome, class.reason = store.Succeeded, "completed"
		if len(out) > 0 {
			class.output = out
		}
	case errors.Is(cause, errShuttingDown):
		class.outcome, class.reason = store.Released, "shutdown"
		class.entry.Kind, class.entry.Message = "released", "shutdown"
	case errors.Is(cause, errTimeout):
		class.entry.Kind, class.entry.Message = "timeout", err.Error()
		class.reason = "timeout"
	case cancelled != nil && !isPermanent(err):
		class.outcome, class.reason = store.Cancelled, "executor_cancelled"
		class.entry.Kind, class.entry.Message = "cancelled", err.Error()
	case snooze != nil && !isPermanent(err):
		class.outcome, class.delay = store.Snoozed, snooze.delay
		class.reason = "snoozed"
	default:
		if p, ok := errors.AsType[*panicError](err); ok {
			class.entry.Kind, class.entry.Message = "panic", p.Error()
			class.reason = "panic"
		} else {
			class.entry.Message = err.Error()
			class.reason, class.retryable = "business", !isPermanent(err)
		}
	}
	return class
}

func (e *Engine) settleResult(
	log *slog.Logger,
	c store.Claimed,
	policy RetryPolicy,
	out RawJSON,
	err error,
	cause error,
	took time.Duration,
) {
	class := classifyResult(executionResult{output: out, err: err, cause: cause}, e.cfg.MaxPayload)
	attempt := int(c.Attempt) + 1
	st := settlementFor(c, class.outcome)
	st.Duration, st.Output, st.Delay, st.Reason = took, class.output, class.delay, class.reason
	entry := class.entry
	entry.Attempt, entry.At = attempt, time.Now().UTC()
	if entry.Kind == "panic" {
		p, _ := errors.AsType[*panicError](err)
		log.Error("executor panicked", "value", p.value, "stack", string(p.stack))
	}
	log = log.With("duration", took)
	switch st.Outcome {
	case store.Succeeded, store.Snoozed:
	case store.Cancelled:
		if entry.Kind == "cancelled" {
			st.Err = encodeErr(entry)
		}
	case store.Released:
		st.Err = encodeErr(entry)
	case store.Failed:
		st.Err = encodeErr(entry)
		if class.retryable && attempt < policy.MaxAttempts {
			st.Outcome, st.Backoff = store.Retry, backoff(policy, attempt, e.cfg)
		}
		log = log.With("kind", entry.Kind)
	}
	st.Error = cleanMessage(entry.Message)
	state, serr := e.settle(log, st)
	if errors.Is(serr, store.ErrInvalidOutput) {
		// JSONB may reject output or a cleaned error entry. Record its deterministic
		// error as a permanent failure instead of reclaiming an expired lease.
		entry.Message = serr.Error()
		st.Outcome, st.Output, st.Err = store.Failed, nil, encodeErr(entry)
		st.Reason, st.Error = "invalid_output", cleanMessage(entry.Message)
		state, serr = e.settle(log.With("kind", entry.Kind), st)
	}
	if serr == nil {
		e.metrics.Observe("exec_duration", took.Seconds(), "executor_type", c.ExecutorType, "outcome", outcomeLabel(st.Outcome, state))
	}
}

// The cancel-hit CASE may turn a retry, release or snooze into cancelled.
func outcomeLabel(o store.Outcome, state string) string {
	if state != "pending" {
		return state
	}
	switch o {
	case store.Released:
		return "released"
	case store.Snoozed:
		return "snoozed"
	default:
		return "failed"
	}
}

// backoff for the n-th failed attempt: min(max, base × 2^(n−1)) × U[1−j, 1+j).
func backoff(p RetryPolicy, n int, cfg Config) time.Duration {
	base := cfg.BackoffBase
	if p.BaseSec > 0 {
		base = time.Duration(p.BaseSec) * time.Second
	}
	maxD := cfg.BackoffMax
	if p.MaxSec > 0 {
		maxD = time.Duration(p.MaxSec) * time.Second
	}
	j := 0.2
	if p.Jitter > 0 {
		j = p.Jitter
	}
	d := min(float64(maxD), float64(base)*math.Pow(2, float64(n-1)))
	return boundedDuration(d * (1 - j + 2*j*rand.Float64()))
}

func (e *Engine) settle(log *slog.Logger, st store.Settlement) (string, error) {
	res, err := e.commitSettlement(log, st)
	if err != nil {
		return "", err
	}
	e.observe(res.Changes)
	return res.State, nil
}

// Lost leases are discarded; invalid output is returned for permanent-failure
// settlement. Other errors leave the lease to expire for later reclamation.
func (e *Engine) commitSettlement(log *slog.Logger, st store.Settlement) (store.Settled, error) {
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	res, err := e.st.Settle(ctx, st)
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		e.metrics.Count("lease_lost_total", 1)
		log.Warn("settle rejected: lease lost", "outcome", st.Outcome)
		return store.Settled{}, err
	case errors.Is(err, store.ErrInvalidOutput):
		log.Warn("settle rejected: document is not valid jsonb", "err", err)
		return store.Settled{}, err
	case err != nil:
		log.Error("settle failed; row stays running until its lease expires", "outcome", st.Outcome, "err", err)
		return store.Settled{}, err
	}
	log.Info("settled", "state", res.State, "outcome", st.Outcome)
	if st.Outcome == store.Released && res.ReleasedCount >= e.cfg.ReleaseAlertThreshold {
		e.metrics.Count("released_alert_total", 1)
		log.Warn("released alert: run keeps being interrupted by shutdowns", "released", res.ReleasedCount)
	}
	return res, nil
}
