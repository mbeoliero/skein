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
	Kind    string    `json:"kind"` // business | timeout | interrupted | panic | upstream_failed | released
	Message string    `json:"message"`
}

// errMessageMax bounds one entry's message: errors accumulates across attempts and resumes.
const errMessageMax = 4096

// encodeErr is the one place an errors entry is encoded. The message is the only
// text the executor controls, and a failed attempt must always be recordable: a
// document jsonb refuses would leave the row running until its lease expires and
// turn a permanent failure into a retry (§6.5). Invalid UTF-8 and NUL are replaced
// (json/v2 rejects the first, jsonb the second), the text is cut to errMessageMax,
// and an encode failure that survives that is recorded instead of ignored.
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

// settleResult turns what the executor returned into a settlement (§6.5).
func (e *Engine) settleResult(log *slog.Logger, c store.Claimed, policy RetryPolicy, out RawJSON, err error, cause error, took time.Duration) {
	attempt := int(c.Attempt) + 1
	st := settlementFor(c, 0)
	entry := errEntry{Attempt: attempt, At: time.Now().UTC(), Kind: "business"}
	retryable := true
	switch {
	case errors.Is(cause, errCancelRequested):
		st.Outcome = store.Cancelled
	case err == nil && len(out) > e.cfg.MaxPayload:
		entry.Message = "output exceeds MaxPayload"
		retryable = false
	case err == nil && len(out) > 0 && !out.IsValid():
		entry.Message = "output is not valid JSON"
		retryable = false
	case err == nil:
		st.Outcome, st.Output = store.Succeeded, out
	case errors.Is(cause, errShuttingDown):
		st.Outcome = store.Released
		entry.Kind, entry.Message = "released", "shutdown"
	case errors.Is(cause, errTimeout):
		entry.Kind, entry.Message = "timeout", err.Error()
	default:
		if p, ok := errors.AsType[*panicError](err); ok {
			entry.Kind, entry.Message = "panic", p.Error()
			log.Error("executor panicked", "value", p.value, "stack", string(p.stack))
		} else {
			entry.Message = err.Error()
			retryable = !isPermanent(err)
		}
	}
	log = log.With("duration", took)
	switch st.Outcome {
	case store.Succeeded, store.Cancelled:
	case store.Released:
		st.Err = encodeErr(entry)
	default: // a failed attempt: retry with backoff while attempts remain, else terminal
		st.Err = encodeErr(entry)
		if retryable && attempt < policy.MaxAttempts {
			st.Outcome, st.Backoff = store.Retry, backoff(policy, attempt, e.cfg)
		} else {
			st.Outcome = store.Failed
		}
		log = log.With("kind", entry.Kind)
	}
	state, serr := e.settle(log, st)
	if errors.Is(serr, store.ErrInvalidOutput) {
		// Go accepted the output but jsonb did not (a number past numeric), or an
		// errors entry was refused despite cleanMessage: deterministic either way, so a
		// permanent failure carrying the database's reason, not a lease left to expire
		// and be reclaimed with an interrupted entry.
		entry.Message = serr.Error()
		st.Outcome, st.Output, st.Err = store.Failed, nil, encodeErr(entry)
		state, serr = e.settle(log.With("kind", entry.Kind), st)
	}
	if serr == nil {
		e.metrics.Observe("exec_duration", took.Seconds(), "executor_type", c.ExecutorType, "outcome", outcomeLabel(st.Outcome, state))
	}
}

// outcomeLabel is the exec_duration outcome label, succeeded / failed / cancelled /
// released, from the state the row actually took: the cancel-hit CASE in settle may
// have turned a retry or a release into cancelled. pending is a retry (a failed
// attempt) or a release.
func outcomeLabel(o store.Outcome, state string) string {
	if state != "pending" {
		return state
	}
	if o == store.Released {
		return "released"
	}
	return "failed"
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
	return time.Duration(d * (1 - j + 2*j*rand.Float64()))
}

// settle is the only call site of Store.Settle; it returns the state the row took,
// or the error. A lost lease is logged and dropped; a rejected output is the caller's
// to settle again as failed; any other error leaves the row running with its lease,
// to be reclaimed later.
func (e *Engine) settle(log *slog.Logger, st store.Settlement) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	res, err := e.st.Settle(ctx, st)
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		e.metrics.Count("lease_lost_total", 1)
		log.Warn("settle rejected: lease lost", "outcome", st.Outcome)
		return "", err
	case errors.Is(err, store.ErrInvalidOutput):
		log.Warn("settle rejected: document is not valid jsonb", "err", err)
		return "", err
	case err != nil:
		log.Error("settle failed; row stays running until its lease expires", "outcome", st.Outcome, "err", err)
		return "", err
	}
	log.Info("settled", "state", res.State, "outcome", st.Outcome)
	if st.Outcome == store.Released && res.ReleasedCount >= e.cfg.ReleaseAlertThreshold {
		e.metrics.Count("released_alert_total", 1)
		log.Warn("released alert: run keeps being interrupted by shutdowns", "released", res.ReleasedCount)
	}
	return res.State, nil
}
