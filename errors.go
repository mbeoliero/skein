package skein

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrNotFound     = errors.New("skein: not found")
	ErrDuplicate    = errors.New("skein: an in-flight run already holds this dedup key")
	ErrReferenced   = errors.New("skein: definition is referenced by a workflow node or schedule")
	ErrNotDrained   = errors.New("skein: shutdown finished with executors still running")
	ErrNotResumable = errors.New("skein: run is not failed or cancelled")
)

// Context causes; settle reads them back with context.Cause to pick the outcome (§6.5).
var (
	errTimeout         = errors.New("skein: executor timeout")
	errCancelRequested = errors.New("skein: cancel requested")
	errShuttingDown    = errors.New("skein: shutting down")
	errLeaseLost       = errors.New("skein: lease lost")
)

// Permanent marks err as not retryable: settle goes straight to failed.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// Cancel ends this run and its workflow, preserving err as the cancellation reason.
func Cancel(err error) error {
	if err == nil {
		return nil
	}
	return &cancelError{err: err}
}

type cancelError struct{ err error }

func (e *cancelError) Error() string { return e.err.Error() }
func (e *cancelError) Unwrap() error { return e.err }

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func isPermanent(err error) bool {
	_, ok := errors.AsType[*permanentError](err)
	return ok
}

// Snooze requeues the same run after delay without consuming an attempt or saving
// output. Delay must be positive; invalid delays are permanent failures.
func Snooze(delay time.Duration) error {
	if delay <= 0 {
		return Permanent(fmt.Errorf("skein: snooze delay must be > 0, got %s", delay))
	}
	return &snoozeError{delay: delay}
}

type snoozeError struct{ delay time.Duration }

func (e *snoozeError) Error() string { return "skein: snooze for " + e.delay.String() }
