package sender

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/juliofilizzola/notiflow/internal/contract"
	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/retry"
)

type Router map[event.Channel]contract.Sender

func (r Router) Send(ctx context.Context, e event.Event) error {
	s, ok := r[e.Channel]
	if !ok {
		return retry.Permanent(fmt.Errorf("no sender registered for channel %q", e.Channel))
	}
	return s.Send(ctx, e)
}

type Simulated struct {
	Name                 string
	Latency              time.Duration
	TransientFailureRate float64
	PermanentFailureRate float64
	Log                  *slog.Logger
}

func (s Simulated) Send(ctx context.Context, e event.Event) error {
	timer := time.NewTimer(s.Latency)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
	}

	r := rand.Float64()
	switch {
	case r < s.PermanentFailureRate:
		return retry.Permanent(fmt.Errorf("%s: recipient rejected (400)", s.Name))
	case r < s.PermanentFailureRate+s.TransientFailureRate:
		return fmt.Errorf("%s: provider unavailable (503)", s.Name)
	}
	s.Log.Debug("notification sent", "provider", s.Name, "event_id", e.EventID, "user_id", e.UserID)
	return nil
}
