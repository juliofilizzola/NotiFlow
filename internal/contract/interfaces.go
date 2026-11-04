package contract

import (
	"context"
	"time"

	"github.com/juliofilizzola/notiflow/internal/broker"
	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/metrics"
)

type Publisher interface {
	Publish(ctx context.Context, e event.Event) error
}

type Consumer interface {
	Deliveries() <-chan broker.Delivery
}

type DeadLetterPublisher interface {
	PublishDLQ(ctx context.Context, dl broker.DeadLetter) error
}

type IdempotencyStore interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Complete(ctx context.Context, key string, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}

type RateLimiter interface {
	Wait(ctx context.Context, ch event.Channel) error
}

type Sender interface {
	Send(ctx context.Context, e event.Event) error
}

type Recorder interface {
	EventReceived(channel, result string)
	DeliveryAttempt(channel, result string)
	EventProcessed(channel string, outcome metrics.Outcome, d time.Duration)
	QueueLatency(channel string, d time.Duration)
	RateLimitWait(channel string, d time.Duration)
	InFlightAdd(delta int64)
}
