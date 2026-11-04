package broker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/juliofilizzola/notiflow/internal/event"
)

var (
	ErrQueueFull = errors.New("broker: queue full")
	ErrClosed    = errors.New("broker: closed")
)

type Delivery struct {
	Event event.Event
	ack   func() error
	nack  func(requeue bool) error
}

func NewDelivery(e event.Event, ack func() error, nack func(requeue bool) error) Delivery {
	return Delivery{Event: e, ack: ack, nack: nack}
}

func (d Delivery) Ack() error              { return d.ack() }
func (d Delivery) Nack(requeue bool) error { return d.nack(requeue) }

type DeadLetter struct {
	Event     event.Event `json:"event"`
	Reason    string      `json:"reason"`
	Attempts  int         `json:"attempts"`
	Permanent bool        `json:"permanent"`
	FailedAt  time.Time   `json:"failed_at"`
}

type Memory struct {
	mu     sync.RWMutex
	closed bool
	queue  chan Delivery
	dlq    chan DeadLetter
}

func NewMemory(queueSize, dlqSize int) *Memory {
	return &Memory{
		queue: make(chan Delivery, queueSize),
		dlq:   make(chan DeadLetter, dlqSize),
	}
}

func (m *Memory) Publish(ctx context.Context, e event.Event) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	d := NewDelivery(e,
		func() error { return nil },
		func(requeue bool) error {
			if !requeue {
				return nil
			}
			return m.Publish(context.Background(), e)
		},
	)
	select {
	case m.queue <- d:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrQueueFull
	}
}

func (m *Memory) Deliveries() <-chan Delivery { return m.queue }

func (m *Memory) PublishDLQ(ctx context.Context, dl DeadLetter) error {
	select {
	case m.dlq <- dl:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrQueueFull
	}
}

func (m *Memory) DeadLetters() <-chan DeadLetter { return m.dlq }

func (m *Memory) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.queue)
	}
}
