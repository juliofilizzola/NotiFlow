package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juliofilizzola/notiflow/internal/broker"
	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/metrics"
)

type stubPublisher struct {
	published []event.Event
	err       error
}

func (s *stubPublisher) Publish(_ context.Context, e event.Event) error {
	if s.err != nil {
		return s.err
	}
	s.published = append(s.published, e)
	return nil
}

func TestIngest(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		pubErr     error
		wantStatus int
		wantPub    int
	}{
		{"valid", `{"event_id":"e1","user_id":"u1","type":"welcome","channel":"email","data":{"name":"Ana"}}`, nil, http.StatusAccepted, 1},
		{"bad channel", `{"event_id":"e1","user_id":"u1","type":"welcome","channel":"fax"}`, nil, http.StatusBadRequest, 0},
		{"missing ids", `{"type":"welcome","channel":"sms"}`, nil, http.StatusBadRequest, 0},
		{"unknown field", `{"event_id":"e1","user_id":"u1","type":"t","channel":"push","extra":1}`, nil, http.StatusBadRequest, 0},
		{"queue full", `{"event_id":"e1","user_id":"u1","type":"t","channel":"push"}`, broker.ErrQueueFull, http.StatusServiceUnavailable, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pub := &stubPublisher{err: c.pubErr}
			reg := metrics.NewRegistry()
			router := NewRouter(pub, reg, reg, slog.New(slog.NewTextHandler(io.Discard, nil)))

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(c.body)))

			if rec.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, c.wantStatus, rec.Body)
			}
			if len(pub.published) != c.wantPub {
				t.Fatalf("published = %d, want %d", len(pub.published), c.wantPub)
			}
			if c.wantPub == 1 && pub.published[0].ReceivedAt.IsZero() {
				t.Fatal("ReceivedAt should be stamped by the API")
			}
		})
	}
}
