package skein

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/skein/internal/store"
)

func realLeaseConfig(schema string) Config {
	cfg := fastConfig(schema)
	cfg.LeaseTTL = time.Minute
	cfg.HeartbeatInterval = time.Second
	cfg.DefaultTimeout = 5 * time.Minute
	return cfg
}

// Keep the real 60s lease in the normal suite: scaled leases and synthetic token
// replacement cannot demonstrate the README's process recovery acceptance bounds.
func TestRealLeaseProcessRecovery(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"kill", "pause"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			sub := startEngine(t, pool, submitOnly(schema), nil)
			declare(t, sub, JobSpec{
				Name: "j", ExecutorType: "crash", Timeout: 5 * time.Minute,
				Retry: RetryPolicy{MaxAttempts: 3},
			})
			id := trigger(t, sub, "j", "")
			marker := filepath.Join(t.TempDir(), "holder")
			child := startHelper(t, schema, "SKEIN_HELPER_MODE=lease", "SKEIN_HELPER_MARKER="+marker)
			waitMarker := func(want string) {
				t.Helper()
				waitFor(t, "helper "+want, func(ctx context.Context) bool {
					b, err := os.ReadFile(marker)
					if err != nil && !errors.Is(err, os.ErrNotExist) {
						t.Fatal(err)
					}
					return string(b) == want
				})
			}
			waitMarker("executing")

			stopped := time.Now()
			if mode == "kill" {
				if err := child.Process.Kill(); err != nil {
					t.Fatalf("kill holder: %v", err)
				}
				if err := child.Wait(); err == nil {
					t.Fatal("killed holder exited successfully")
				}
			} else {
				if err := child.Process.Signal(syscall.SIGSTOP); err != nil {
					t.Fatalf("stop holder: %v", err)
				}
				// Observe the kernel stop, not just successful signal delivery. This
				// consumes only the stop event; startHelper still reaps the child.
				waitFor(t, "holder stopped", func(ctx context.Context) bool {
					var status syscall.WaitStatus
					pid, err := syscall.Wait4(
						child.Process.Pid,
						&status,
						syscall.WUNTRACED|syscall.WNOHANG,
						nil,
					)
					if err != nil {
						t.Fatalf("wait for stop: %v", err)
					}
					if pid == 0 {
						return false
					}
					if status.Exited() || status.Signaled() {
						t.Fatalf("holder exited instead of stopping: %v", status)
					}
					// No WCONTINUED was requested, so a non-exit event is a stop.
					// Darwin's WaitStatus.Stopped misclassifies SIGSTOP events.
					return true
				})
				stopped = time.Now()
			}
			old := waitRun(t, sub, id, StateRunning)
			oldToken := leaseToken(t, pool, schema, id)
			if old.LeaseExpiresAt == nil || old.StartedAt == nil {
				t.Fatal("holder has no lease timestamps")
			}
			if lifetime := old.LeaseExpiresAt.Sub(*old.StartedAt); lifetime < time.Minute {
				t.Fatalf("holder lease is only %s, want at least 60s", lifetime)
			}

			entered := make(chan time.Time, 1)
			releaseCtx, release := context.WithCancel(t.Context())
			defer release()
			b := startEngine(t, pool, realLeaseConfig(schema), func(e *Engine) {
				e.Register("crash", func(ctx context.Context, req *Request) (RawJSON, error) {
					select {
					case entered <- time.Now():
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					select {
					case <-releaseCtx.Done():
						return RawJSON(`"recovered"`), nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				})
			})
			takeoverCtx, cancel := context.WithDeadline(t.Context(), stopped.Add(75*time.Second))
			defer cancel()
			select {
			case at := <-entered:
				took := at.Sub(stopped)
				if took > 75*time.Second {
					t.Fatalf("takeover took %s, want <=75s", took)
				}
				t.Logf("60s lease: %s takeover in %s", mode, took)
			case <-takeoverCtx.Done():
				t.Fatalf("no takeover within 75s: %v", takeoverCtx.Err())
			}
			recovered := waitRun(t, b, id, StateRunning)
			newToken := leaseToken(t, pool, schema, id)
			if newToken == oldToken || recovered.Attempt != 1 {
				t.Fatalf("takeover did not replace lease and increment attempt: %+v", recovered)
			}
			if recovered.StartedAt == nil || recovered.StartedAt.Before(*old.LeaseExpiresAt) {
				t.Fatalf("takeover started before the old lease expired: %+v", recovered)
			}

			if mode == "pause" {
				// The duration itself is the acceptance condition, not a sleep used
				// to guess when recovery finished. Both owners handshake separately.
				timer := time.NewTimer(time.Until(stopped.Add(61 * time.Second)))
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-t.Context().Done():
					t.Fatal(t.Context().Err())
				}
				t.Logf("holder remained stopped for %s", time.Since(stopped))
				if err := child.Process.Signal(syscall.SIGCONT); err != nil {
					t.Fatalf("resume holder: %v", err)
				}
				waitMarker("drained")

				// Exercise both fences while the replacement is still running; a
				// terminal row alone would not demonstrate rejection of an old token.
				st := store.Open(pool, schema)
				rows, err := st.Heartbeat(
					t.Context(),
					[]int64{id},
					[]uuid.UUID{oldToken},
					time.Minute,
					time.Second,
				)
				if err != nil || len(rows) != 0 {
					t.Fatalf("old heartbeat renewed: %+v, %v", rows, err)
				}
				_, err = st.Settle(t.Context(), store.Settlement{
					Id: id, Token: oldToken, Outcome: store.Succeeded, Output: []byte(`"stale"`),
				})
				if !errors.Is(err, store.ErrLeaseLost) {
					t.Fatalf("old settle: %v, want ErrLeaseLost", err)
				}
				current := waitRun(t, b, id, StateRunning)
				if leaseToken(t, pool, schema, id) != newToken || string(current.Output) != "" {
					t.Fatalf("resumed holder changed the replacement's row: %+v", current)
				}
			}

			release()
			finished := waitRun(t, b, id, StateSucceeded)
			es := errorsOf(t, finished)
			if finished.Attempt != 1 || string(finished.Output) != `"recovered"` || len(es) != 1 {
				t.Fatalf("recovered run: %+v", finished)
			}
			if es[0].Kind != "interrupted" || es[0].Attempt != 1 {
				t.Fatalf("recovery history: %+v", es)
			}
		})
	}
}
