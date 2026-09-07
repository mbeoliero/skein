package skein

import "errors"

var (
	ErrNotFound     = errors.New("skein: not found")
	ErrDuplicate    = errors.New("skein: an in-flight run already holds this dedup key")
	ErrReferenced   = errors.New("skein: definition is referenced by a workflow node or schedule")
	ErrNotDrained   = errors.New("skein: shutdown finished with executors still running")
	ErrNotResumable = errors.New("skein: workflow run is not failed or cancelled")
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

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func isPermanent(err error) bool {
	_, ok := errors.AsType[*permanentError](err)
	return ok
}
