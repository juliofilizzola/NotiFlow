package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	p := Policy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 300 * time.Millisecond}
	cases := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{3, 150 * time.Millisecond, 300 * time.Millisecond},
		{80, 150 * time.Millisecond, 300 * time.Millisecond},
	}
	for _, c := range cases {
		for range 50 {
			if d := p.Backoff(c.attempt); d < c.min || d > c.max {
				t.Fatalf("Backoff(%d) = %v, want in [%v, %v]", c.attempt, d, c.min, c.max)
			}
		}
	}
}

func TestDoStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{MaxAttempts: 3, BaseDelay: time.Hour, MaxDelay: time.Hour}
	attempts, err := Do(ctx, p, func(context.Context, int) error {
		cancel()
		return errors.New("boom")
	}, nil)
	if attempts != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("attempts=%d err=%v, want 1 and context.Canceled", attempts, err)
	}
}
