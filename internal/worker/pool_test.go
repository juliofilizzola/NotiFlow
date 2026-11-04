package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juliofilizzola/notiflow/internal/broker"
	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/idempotency"
	"github.com/juliofilizzola/notiflow/internal/metrics"
	"github.com/juliofilizzola/notiflow/internal/retry"
)

var errProviderDown = errors.New("provider unavailable")

type noLimit struct{}

func (noLimit) Wait(context.Context, event.Channel) error { return nil }

type scriptedSender struct {
	mu       sync.Mutex
	calls    map[string]int
	behavior func(call int) error
}

func (s *scriptedSender) Send(_ context.Context, e event.Event) error {
	s.mu.Lock()
	s.calls[e.EventID]++
	n := s.calls[e.EventID]
	s.mu.Unlock()
	return s.behavior(n)
}

type harness struct {
	broker *broker.Memory
	sender *scriptedSender
	reg    *metrics.Registry
	pool   *Pool
}

func newHarness(behavior func(call int) error) *harness {
	b := broker.NewMemory(100, 100)
	s := &scriptedSender{calls: map[string]int{}, behavior: behavior}
	reg := metrics.NewRegistry()
	pool := NewPool(
		Config{
			Workers:        4,
			Retry:          retry.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
			AttemptTimeout: time.Second,
			ProcessingTTL:  time.Minute,
			CompletedTTL:   time.Hour,
		},
		Deps{Consumer: b, DLQ: b, Store: idempotency.NewMemory(), Limiter: noLimit{}, Sender: s, Metrics: reg,
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))},
	)
	return &harness{broker: b, sender: s, reg: reg, pool: pool}
}

func (h *harness) run(t *testing.T, events ...event.Event) {
	t.Helper()
	for _, e := range events {
		if err := h.broker.Publish(context.Background(), e); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	h.broker.Close()
	h.pool.Run(context.Background())
}

func (h *harness) deadLetters() []broker.DeadLetter {
	var out []broker.DeadLetter
	for {
		select {
		case dl := <-h.broker.DeadLetters():
			out = append(out, dl)
		default:
			return out
		}
	}
}

func evt(id string) event.Event {
	return event.Event{EventID: id, UserID: "u1", Type: "welcome", Channel: event.ChannelEmail}
}

func TestTransientFailuresAreRetriedUntilSuccess(t *testing.T) {
	h := newHarness(func(call int) error {
		if call < 3 {
			return errProviderDown
		}
		return nil
	})
	h.run(t, evt("e1"))

	if got := h.sender.calls["e1"]; got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
	if dls := h.deadLetters(); len(dls) != 0 {
		t.Fatalf("unexpected dead letters: %+v", dls)
	}
	var buf bytes.Buffer
	h.reg.Write(&buf)
	for _, want := range []string{
		`notiflow_events_processed_total{channel="email",outcome="delivered"} 1`,
		`notiflow_delivery_attempts_total{channel="email",result="transient_error"} 2`,
		`notiflow_delivery_attempts_total{channel="email",result="success"} 1`,
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("metrics missing %q\n%s", want, buf.String())
		}
	}
}

func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	h := newHarness(func(int) error { return errProviderDown })
	h.run(t, evt("e1"))

	if got := h.sender.calls["e1"]; got != 3 {
		t.Fatalf("calls = %d, want 3 (max attempts)", got)
	}
	dls := h.deadLetters()
	if len(dls) != 1 || dls[0].Attempts != 3 || dls[0].Permanent {
		t.Fatalf("dead letters = %+v, want one transient with 3 attempts", dls)
	}
}

func TestPermanentFailureSkipsRetries(t *testing.T) {
	h := newHarness(func(int) error { return retry.Permanent(errors.New("invalid recipient")) })
	h.run(t, evt("e1"))

	if got := h.sender.calls["e1"]; got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
	if dls := h.deadLetters(); len(dls) != 1 || !dls[0].Permanent {
		t.Fatalf("dead letters = %+v, want one permanent", dls)
	}
}

func TestDuplicateEventIsProcessedOnce(t *testing.T) {
	h := newHarness(func(int) error { return nil })
	h.run(t, evt("e1"), evt("e1"), evt("e2"))

	if h.sender.calls["e1"] != 1 || h.sender.calls["e2"] != 1 {
		t.Fatalf("calls = %v, want each event sent once", h.sender.calls)
	}
}

func TestDeadLetteredEventCanBeReplayed(t *testing.T) {
	store := idempotency.NewMemory()
	h := newHarness(func(int) error { return errProviderDown })
	h.pool.deps.Store = store
	h.run(t, evt("e1"))

	acquired, _ := store.Acquire(context.Background(), "e1", time.Minute)
	if !acquired {
		t.Fatal("key should be released after dead-lettering so the event can be replayed")
	}
}
