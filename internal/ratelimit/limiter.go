package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/juliofilizzola/notiflow/internal/event"
)

var ErrUnknownChannel = errors.New("ratelimit: no limit configured for channel")

type Limit struct {
	PerSecond float64
	Burst     int
}

type Bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func NewBucket(l Limit) *Bucket {
	return &Bucket{rate: l.PerSecond, burst: float64(l.Burst), tokens: float64(l.Burst), last: time.Now()}
}

func (b *Bucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type Limiter struct {
	buckets map[event.Channel]*Bucket
}

func NewLimiter(limits map[event.Channel]Limit) *Limiter {
	buckets := make(map[event.Channel]*Bucket, len(limits))
	for ch, l := range limits {
		buckets[ch] = NewBucket(l)
	}
	return &Limiter{buckets: buckets}
}

func (l *Limiter) Wait(ctx context.Context, ch event.Channel) error {
	b, ok := l.buckets[ch]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownChannel, ch)
	}
	return b.Wait(ctx)
}
