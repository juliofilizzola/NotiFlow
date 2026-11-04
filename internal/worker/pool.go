package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/juliofilizzola/notiflow/internal/broker"
	"github.com/juliofilizzola/notiflow/internal/contract"
	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/metrics"
	"github.com/juliofilizzola/notiflow/internal/retry"
)

const cleanupTimeout = 5 * time.Second

type Config struct {
	Workers        int
	Retry          retry.Policy
	AttemptTimeout time.Duration
	ProcessingTTL  time.Duration
	CompletedTTL   time.Duration
}

type Deps struct {
	Consumer contract.Consumer
	DLQ      contract.DeadLetterPublisher
	Store    contract.IdempotencyStore
	Limiter  contract.RateLimiter
	Sender   contract.Sender
	Metrics  contract.Recorder
	Log      *slog.Logger
}

type Pool struct {
	cfg  Config
	deps Deps
}

func NewPool(cfg Config, deps Deps) *Pool {
	cfg.Workers = max(cfg.Workers, 1)
	return &Pool{cfg: cfg, deps: deps}
}

func (p *Pool) Run(ctx context.Context) {
	deliveries := p.deps.Consumer.Deliveries()
	var wg sync.WaitGroup
	for id := range p.cfg.Workers {
		wg.Go(func() { p.loop(ctx, id, deliveries) })
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, id int, deliveries <-chan broker.Delivery) {
	log := p.deps.Log.With("worker", id)
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			p.handle(ctx, log, d)
		}
	}
}

func (p *Pool) handle(ctx context.Context, log *slog.Logger, d broker.Delivery) {
	e := d.Event
	ch := string(e.Channel)
	start := time.Now()
	m := p.deps.Metrics
	log = log.With("event_id", e.EventID, "channel", ch)

	m.InFlightAdd(1)
	defer m.InFlightAdd(-1)
	if !e.ReceivedAt.IsZero() {
		m.QueueLatency(ch, start.Sub(e.ReceivedAt))
	}

	acquired, err := p.deps.Store.Acquire(ctx, e.EventID, p.cfg.ProcessingTTL)
	if err != nil {
		log.Error("idempotency store unavailable, requeueing", "err", err)
		p.requeue(log, d, start)
		return
	}
	if !acquired {
		log.Info("duplicate event skipped")
		p.ack(log, d)
		m.EventProcessed(ch, metrics.OutcomeDuplicate, time.Since(start))
		return
	}

	attempts, err := retry.Do(ctx, p.cfg.Retry,
		func(ctx context.Context, _ int) error { return p.deliver(ctx, e) },
		func(attempt int, err error, wait time.Duration) {
			log.Warn("delivery failed, retrying", "attempt", attempt, "backoff", wait, "err", err)
		},
	)

	switch {
	case err == nil:
		cctx, cancel := cleanupContext(ctx)
		defer cancel()
		if err := p.deps.Store.Complete(cctx, e.EventID, p.cfg.CompletedTTL); err != nil {
			log.Error("failed to mark event completed", "err", err)
		}
		p.ack(log, d)
		m.EventProcessed(ch, metrics.OutcomeDelivered, time.Since(start))
		log.Info("event delivered", "attempts", attempts)
	case ctx.Err() != nil:
		p.release(ctx, log, e.EventID)
		p.requeue(log, d, start)
	default:
		p.deadLetter(ctx, log, d, attempts, err, start)
	}
}

func (p *Pool) deliver(ctx context.Context, e event.Event) error {
	ch := string(e.Channel)
	waitStart := time.Now()
	if err := p.deps.Limiter.Wait(ctx, e.Channel); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return retry.Permanent(err)
	}
	p.deps.Metrics.RateLimitWait(ch, time.Since(waitStart))

	attemptCtx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
	defer cancel()
	err := p.deps.Sender.Send(attemptCtx, e)
	p.deps.Metrics.DeliveryAttempt(ch, attemptResult(err))
	return err
}

func (p *Pool) deadLetter(ctx context.Context, log *slog.Logger, d broker.Delivery, attempts int, cause error, start time.Time) {
	cctx, cancel := cleanupContext(ctx)
	defer cancel()

	dl := broker.DeadLetter{
		Event:     d.Event,
		Reason:    cause.Error(),
		Attempts:  attempts,
		Permanent: retry.IsPermanent(cause),
		FailedAt:  time.Now().UTC(),
	}
	if err := p.deps.DLQ.PublishDLQ(cctx, dl); err != nil {
		log.Error("dlq publish failed, requeueing", "err", err)
		p.release(ctx, log, d.Event.EventID)
		p.requeue(log, d, start)
		return
	}

	p.release(ctx, log, d.Event.EventID)
	p.ack(log, d)
	p.deps.Metrics.EventProcessed(string(d.Event.Channel), metrics.OutcomeDeadLettered, time.Since(start))
	log.Error("event dead-lettered", "attempts", attempts, "permanent", dl.Permanent, "err", cause)
}

func (p *Pool) ack(log *slog.Logger, d broker.Delivery) {
	if err := d.Ack(); err != nil {
		log.Error("ack failed", "err", err)
	}
}

func (p *Pool) requeue(log *slog.Logger, d broker.Delivery, start time.Time) {
	if err := d.Nack(true); err != nil {
		log.Error("requeue failed", "err", err)
	}
	p.deps.Metrics.EventProcessed(string(d.Event.Channel), metrics.OutcomeRequeued, time.Since(start))
}

func (p *Pool) release(ctx context.Context, log *slog.Logger, key string) {
	cctx, cancel := cleanupContext(ctx)
	defer cancel()
	if err := p.deps.Store.Release(cctx, key); err != nil {
		log.Error("failed to release idempotency key", "err", err)
	}
}

func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
}

func attemptResult(err error) string {
	switch {
	case err == nil:
		return metrics.ResultSuccess
	case retry.IsPermanent(err):
		return metrics.ResultPermanentError
	default:
		return metrics.ResultTransientError
	}
}
