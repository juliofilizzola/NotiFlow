package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/juliofilizzola/notiflow/internal/broker"
	"github.com/juliofilizzola/notiflow/internal/contract"
	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/metrics"
)

const maxBodyBytes = 64 << 10

type handler struct {
	pub     contract.Publisher
	metrics contract.Recorder
	log     *slog.Logger
	now     func() time.Time
}

func NewRouter(pub contract.Publisher, rec contract.Recorder, metricsHandler http.Handler, log *slog.Logger) *http.ServeMux {
	h := &handler{pub: pub, metrics: rec, log: log, now: time.Now}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", h.ingest)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /metrics", metricsHandler)
	return mux
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *handler) ingest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var e event.Event
	if err := dec.Decode(&e); err != nil {
		h.metrics.EventReceived("unknown", metrics.ResultInvalid)
		writeJSON(w, http.StatusBadRequest, errorBody{Code: "invalid_json", Message: "body must be a single JSON object with known fields"})
		return
	}
	if err := e.Validate(); err != nil {
		h.metrics.EventReceived(channelLabel(e.Channel), metrics.ResultInvalid)
		writeJSON(w, http.StatusBadRequest, errorBody{Code: "validation_failed", Message: err.Error()})
		return
	}

	e.ReceivedAt = h.now().UTC()
	if err := h.pub.Publish(r.Context(), e); err != nil {
		h.publishFailed(w, e, err)
		return
	}
	h.metrics.EventReceived(string(e.Channel), metrics.ResultAccepted)
	writeJSON(w, http.StatusAccepted, map[string]string{"event_id": e.EventID, "status": "accepted"})
}

func (h *handler) publishFailed(w http.ResponseWriter, e event.Event, err error) {
	result := metrics.ResultPublishError
	if errors.Is(err, broker.ErrQueueFull) {
		result = metrics.ResultQueueFull
	}
	h.metrics.EventReceived(string(e.Channel), result)
	h.log.Error("publish failed", "event_id", e.EventID, "err", err)
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, errorBody{Code: "unavailable", Message: "event not accepted, retry later"})
}

func channelLabel(c event.Channel) string {
	if c.Valid() {
		return string(c)
	}
	return "unknown"
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
