package metrics

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Outcome string

const (
	OutcomeDelivered    Outcome = "delivered"
	OutcomeDuplicate    Outcome = "duplicate"
	OutcomeDeadLettered Outcome = "dead_lettered"
	OutcomeRequeued     Outcome = "requeued"
)

const (
	ResultAccepted       = "accepted"
	ResultInvalid        = "invalid"
	ResultQueueFull      = "queue_full"
	ResultPublishError   = "publish_error"
	ResultSuccess        = "success"
	ResultTransientError = "transient_error"
	ResultPermanentError = "permanent_error"
)

var (
	latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	waitBuckets    = []float64{.001, .01, .05, .1, .5, 1, 5}
)

type Registry struct {
	received     *counterVec
	attempts     *counterVec
	processed    *counterVec
	duration     *histogramVec
	queueLatency *histogramVec
	rateWait     *histogramVec
	inFlight     atomic.Int64
}

func NewRegistry() *Registry {
	return &Registry{
		received:     newCounterVec("notiflow_events_received_total", "Events received by the API, by intake result.", "channel", "result"),
		attempts:     newCounterVec("notiflow_delivery_attempts_total", "Provider send attempts, by result.", "channel", "result"),
		processed:    newCounterVec("notiflow_events_processed_total", "Events that finished processing, by final outcome.", "channel", "outcome"),
		duration:     newHistogramVec("notiflow_event_processing_duration_seconds", "Time from dequeue to final outcome (includes retries and rate-limit waits).", latencyBuckets, "channel", "outcome"),
		queueLatency: newHistogramVec("notiflow_event_queue_latency_seconds", "Time between API acceptance and dequeue by a worker.", latencyBuckets, "channel"),
		rateWait:     newHistogramVec("notiflow_ratelimit_wait_seconds", "Time spent waiting for a provider rate-limit token.", waitBuckets, "channel"),
	}
}

func (r *Registry) EventReceived(channel, result string)   { r.received.inc(channel, result) }
func (r *Registry) DeliveryAttempt(channel, result string) { r.attempts.inc(channel, result) }
func (r *Registry) EventProcessed(channel string, o Outcome, d time.Duration) {
	r.processed.inc(channel, string(o))
	r.duration.observe(d.Seconds(), channel, string(o))
}
func (r *Registry) QueueLatency(channel string, d time.Duration) {
	r.queueLatency.observe(d.Seconds(), channel)
}
func (r *Registry) RateLimitWait(channel string, d time.Duration) {
	r.rateWait.observe(d.Seconds(), channel)
}
func (r *Registry) InFlightAdd(delta int64) { r.inFlight.Add(delta) }

func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	r.Write(w)
}

func (r *Registry) Write(w io.Writer) {
	r.received.write(w)
	r.attempts.write(w)
	r.processed.write(w)
	r.duration.write(w)
	r.queueLatency.write(w)
	r.rateWait.write(w)
	fmt.Fprintf(w, "# HELP notiflow_worker_inflight Events currently being processed.\n# TYPE notiflow_worker_inflight gauge\nnotiflow_worker_inflight %d\n", r.inFlight.Load())
}

type counterSeries struct {
	values []string
	v      float64
}

type counterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	series     map[string]*counterSeries
}

func newCounterVec(name, help string, labels ...string) *counterVec {
	return &counterVec{name: name, help: help, labels: labels, series: make(map[string]*counterSeries)}
}

func (c *counterVec) inc(values ...string) {
	key := strings.Join(values, "\xff")
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.series[key]
	if !ok {
		s = &counterSeries{values: slices.Clone(values)}
		c.series[key] = s
	}
	s.v++
}

func (c *counterVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range slices.Sorted(maps.Keys(c.series)) {
		s := c.series[key]
		fmt.Fprintf(w, "%s{%s} %g\n", c.name, labelPairs(c.labels, s.values), s.v)
	}
}

type histSeries struct {
	values  []string
	buckets []uint64
	sum     float64
	count   uint64
}

type histogramVec struct {
	name, help string
	labels     []string
	bounds     []float64
	mu         sync.Mutex
	series     map[string]*histSeries
}

func newHistogramVec(name, help string, bounds []float64, labels ...string) *histogramVec {
	return &histogramVec{name: name, help: help, labels: labels, bounds: bounds, series: make(map[string]*histSeries)}
}

func (h *histogramVec) observe(v float64, values ...string) {
	key := strings.Join(values, "\xff")
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[key]
	if !ok {
		s = &histSeries{values: slices.Clone(values), buckets: make([]uint64, len(h.bounds))}
		h.series[key] = s
	}
	for i, b := range h.bounds {
		if v <= b {
			s.buckets[i]++
		}
	}
	s.sum += v
	s.count++
}

func (h *histogramVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, key := range slices.Sorted(maps.Keys(h.series)) {
		s := h.series[key]
		labels := labelPairs(h.labels, s.values)
		for i, b := range h.bounds {
			fmt.Fprintf(w, "%s_bucket{%s,le=%q} %d\n", h.name, labels, strconv.FormatFloat(b, 'g', -1, 64), s.buckets[i])
		}
		fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n", h.name, labels, s.count)
		fmt.Fprintf(w, "%s_sum{%s} %g\n%s_count{%s} %d\n", h.name, labels, s.sum, h.name, labels, s.count)
	}
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func labelPairs(names, values []string) string {
	pairs := make([]string, len(names))
	for i, n := range names {
		pairs[i] = n + `="` + labelEscaper.Replace(values[i]) + `"`
	}
	return strings.Join(pairs, ",")
}
