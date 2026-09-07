package skein

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The test binary doubles as the second OS process the design's multi-instance
// checks need: with SKEIN_HELPER=1 it runs an engine whose executor never returns,
// so the parent test can SIGKILL a live lease holder.
func TestMain(m *testing.M) {
	if os.Getenv("SKEIN_HELPER") == "1" {
		helperMain()
		return
	}
	os.Exit(m.Run())
}

func testDSN() string {
	return cmp.Or(os.Getenv("SKEIN_TEST_DSN"), "postgres:///postgres?host=/tmp")
}

func helperMain() {
	pool, err := pgxpool.New(context.Background(), testDSN())
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper pool:", err)
		os.Exit(2)
	}
	cfg := fastConfig(os.Getenv("SKEIN_HELPER_SCHEMA"))
	if os.Getenv("SKEIN_HELPER_MODE") == "bench" {
		cfg = benchWorkerConfig(os.Getenv("SKEIN_HELPER_SCHEMA"))
	}
	e, err := New(pool, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper new:", err)
		os.Exit(2)
	}
	e.Register("crash", func(ctx context.Context, req *Request) (RawJSON, error) { select {} })
	registerBenchExecutors(e)
	if err := e.Start(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "helper start:", err)
		os.Exit(2)
	}
	fmt.Println("ready") // startHelper waits for this line: the engine is claiming
	select {}
}

// startHelper launches the helper engine on schema, returns once it reports ready
// (so a bench with three workers knows all three claim, not just one), and kills it
// at cleanup. A helper that exits before ready fails the test.
func startHelper(t *testing.T, schema string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "SKEIN_HELPER=1", "SKEIN_HELPER_SCHEMA="+schema, "SKEIN_TEST_DSN="+testDSN())
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
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	if line, err := bufio.NewReader(out).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("helper did not become ready: %q %v", line, err)
	}
	return cmd
}
