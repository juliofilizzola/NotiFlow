package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/juliofilizzola/notiflow/internal/event"
)

func TestBurstThenThrottle(t *testing.T) {
	l := NewLimiter(map[event.Channel]Limit{event.ChannelSMS: {PerSecond: 50, Burst: 2}})
	ctx := context.Background()

	start := time.Now()
	for range 2 {
		if err := l.Wait(ctx, event.ChannelSMS); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > 10*time.Millisecond {
		t.Fatal("burst tokens should be immediate")
	}
	for range 3 {
		if err := l.Wait(ctx, event.ChannelSMS); err != nil {
			t.Fatal(err)
		}
	}

	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("elapsed %v, expected throttling", elapsed)
	}
}

func TestUnknownChannel(t *testing.T) {
	l := NewLimiter(nil)
	if err := l.Wait(context.Background(), event.ChannelPush); !errors.Is(err, ErrUnknownChannel) {
		t.Fatalf("err = %v, want ErrUnknownChannel", err)
	}
}
