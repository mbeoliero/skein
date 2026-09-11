package skein

import (
	"math"
	"testing"
	"time"
)

func TestDurationJitterCannotOverflow(t *testing.T) {
	t.Parallel()
	maximum := time.Duration(math.MaxInt64)
	cfg := Config{BackoffBase: maximum, BackoffMax: maximum}
	for range 1000 {
		if got := jitter(maximum); got <= 0 {
			t.Fatalf("poll jitter overflowed: %s", got)
		}
		if got := backoff(RetryPolicy{Jitter: 0.99}, 2, cfg); got <= 0 {
			t.Fatalf("retry backoff overflowed: %s", got)
		}
		if got := jitter(time.Nanosecond); got <= 0 {
			t.Fatalf("positive polling interval rounded to %s", got)
		}
	}
}

func TestBoundedDurationLimits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		nanos float64
		want  time.Duration
	}{
		{name: "overflow", nanos: float64(math.MaxInt64) * 1.2, want: time.Duration(math.MaxInt64)},
		{name: "rounded_limit", nanos: float64(math.MaxInt64), want: time.Duration(math.MaxInt64)},
		{name: "subnanosecond", nanos: 0.8, want: time.Nanosecond},
		{name: "ordinary", nanos: float64(time.Second), want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := boundedDuration(tc.nanos); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
