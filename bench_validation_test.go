package skein

import (
	"strings"
	"testing"
	"time"
)

func TestBenchCompletionRejectsInvalidEvidence(t *testing.T) {
	t.Parallel()
	groups := []benchGroup{{job: "short", count: 2}, {job: "long", count: 1}}
	for _, tc := range []struct {
		name      string
		completed map[string]int
		failed    int
		wantError bool
	}{
		{name: "success", completed: map[string]int{"short": 2, "long": 1}},
		{name: "failed", completed: map[string]int{"short": 2, "long": 1}, failed: 1, wantError: true},
		{name: "missing", completed: map[string]int{"short": 2}, wantError: true},
		{name: "wrong_group", completed: map[string]int{"short": 1, "long": 2}, wantError: true},
		{name: "duplicate", completed: map[string]int{"short": 3, "long": 1}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateBenchCompletion(groups, tc.completed, tc.failed); (err != nil) != tc.wantError {
				t.Fatalf("completion validation: %v, want error %v", err, tc.wantError)
			}
		})
	}
}

func TestBenchConfigRejectsInvalidKnobs(t *testing.T) {
	t.Setenv("SKEIN_BENCH_POLL", "")
	t.Setenv("SKEIN_BENCH_CONCURRENCY", "")
	for _, key := range []string{"SKEIN_BENCH_POLL", "SKEIN_BENCH_CONCURRENCY"} {
		for _, value := range []string{"invalid", "0", "-1", "99999999999999999999999999999"} {
			t.Run(key+"="+value, func(t *testing.T) {
				t.Setenv(key, value)
				if _, err := benchWorkerConfig("test"); err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("invalid knob: %v", err)
				}
			})
		}
	}
	cfg, err := benchWorkerConfig("test")
	if err != nil || cfg.PollInterval != time.Second || cfg.Concurrency != 16 {
		t.Fatalf("default config: %+v, %v", cfg, err)
	}
	t.Setenv("SKEIN_BENCH_POLL", "5s")
	t.Setenv("SKEIN_BENCH_CONCURRENCY", "8")
	cfg, err = benchWorkerConfig("test")
	if err != nil || cfg.PollInterval != 5*time.Second || cfg.Concurrency != 8 {
		t.Fatalf("explicit config: %+v, %v", cfg, err)
	}
}
