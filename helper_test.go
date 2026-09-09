package skein

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The test binary doubles as the second OS process the design's multi-instance
// checks need: with SKEIN_HELPER=1 it runs an engine the parent can kill or pause
// while it holds a real lease.
func TestMain(m *testing.M) {
	if os.Getenv("SKEIN_HELPER") == "1" {
		helperMain()
		return
	}
	os.Exit(m.Run())
}

func testDsn() string {
	return cmp.Or(os.Getenv("SKEIN_TEST_DSN"), "postgres:///postgres?host=/tmp")
}

func helperMain() {
	if os.Getenv("SKEIN_HELPER_MODE") == "scenario" {
		if err := scenarioWorkerMain(); err != nil {
			fmt.Fprintln(os.Stderr, "helper scenario:", err)
			os.Exit(2)
		}
		return
	}
	pool, err := pgxpool.New(context.Background(), testDsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper pool:", err)
		os.Exit(2)
	}
	cfg := fastConfig(os.Getenv("SKEIN_HELPER_SCHEMA"))
	if os.Getenv("SKEIN_HELPER_MODE") == "bench" {
		cfg = benchWorkerConfig(os.Getenv("SKEIN_HELPER_SCHEMA"))
	}
	leaseMode := os.Getenv("SKEIN_HELPER_MODE") == "lease"
	if leaseMode {
		cfg = realLeaseConfig(cfg.Schema)
	}
	e, err := New(pool, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper new:", err)
		os.Exit(2)
	}
	executing := make(chan struct{})
	e.Register("crash", func(ctx context.Context, req *Request) (RawJSON, error) {
		if !leaseMode {
			select {}
		}
		close(executing)
		<-ctx.Done()
		return RawJSON(`"stale"`), nil
	})
	if os.Getenv("SKEIN_HELPER_MODE") == "snooze" {
		e.Register("snooze", func(ctx context.Context, req *Request) (RawJSON, error) {
			return nil, Snooze(24 * time.Hour)
		})
		e.Register("submit-crash", func(ctx context.Context, req *Request) (RawJSON, error) {
			if _, err := fakeVideoSubmit(ctx, pool, cfg.Schema, req.IdempotencyKey); err != nil {
				return nil, err
			}
			select {} // parent kills us after the provider side effect, before settle
		})
	}
	registerBenchExecutors(e)
	if err := e.Start(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "helper start:", err)
		os.Exit(2)
	}
	fmt.Println("ready") // startHelper waits for this line: the engine is claiming
	if leaseMode {
		marker := os.Getenv("SKEIN_HELPER_MARKER")
		report := func(state string) {
			if err := os.WriteFile(marker, []byte(state), 0o600); err != nil {
				fmt.Fprintln(os.Stderr, "helper marker:", err)
				os.Exit(2)
			}
		}
		<-executing
		report("executing")
		// A returned executor is not enough: wait until result handling has also
		// completed before the parent checks that the new owner's row survived.
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for len(e.activeIds()) != 0 {
			<-tick.C
		}
		report("drained")
	}
	select {}
}

// startHelper launches the helper engine on schema, returns once it reports ready
// (so a bench with three workers knows all three claim, not just one), and kills it
// at cleanup. A helper that exits before ready fails the test.
func startHelper(t *testing.T, schema string, env ...string) *exec.Cmd {
	t.Helper()
	return startHelperContext(t, t.Context(), schema, env...)
}

func startHelperContext(t *testing.T, ctx context.Context, schema string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "SKEIN_HELPER=1", "SKEIN_HELPER_SCHEMA="+schema, "SKEIN_TEST_DSN="+testDsn())
	cmd.Env = append(cmd.Env, env...)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() {
		_ = out.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	type readyResult struct {
		line string
		err  error
	}
	ready := make(chan readyResult, 1)
	go func() {
		line, err := bufio.NewReader(out).ReadString('\n')
		ready <- readyResult{line: line, err: err}
	}()
	select {
	case result := <-ready:
		if result.err != nil || result.line != "ready\n" {
			t.Fatalf("helper did not become ready: %q %v", result.line, result.err)
		}
	case <-ctx.Done():
		t.Fatalf("helper did not become ready: %v", ctx.Err())
	}
	return cmd
}
