package app

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/juliofilizzola/notiflow/internal/api"
	"github.com/juliofilizzola/notiflow/internal/broker"
	"github.com/juliofilizzola/notiflow/internal/config"
	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/idempotency"
	"github.com/juliofilizzola/notiflow/internal/metrics"
	"github.com/juliofilizzola/notiflow/internal/ratelimit"
	"github.com/juliofilizzola/notiflow/internal/sender"
	"github.com/juliofilizzola/notiflow/internal/worker"
)

type App struct {
	cfg    config.Config
	log    *slog.Logger
	broker *broker.Memory
	pool   *worker.Pool
	server *http.Server
}

func New(cfg config.Config, log *slog.Logger) *App {
	reg := metrics.NewRegistry()
	b := broker.NewMemory(cfg.QueueSize, cfg.DLQSize)

	pool := worker.NewPool(cfg.Worker, worker.Deps{
		Consumer: b,
		DLQ:      b,
		Store:    idempotency.NewMemory(),
		Limiter:  ratelimit.NewLimiter(cfg.ChannelLimits),
		Sender:   newSenders(log),
		Metrics:  reg,
		Log:      log,
	})

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.NewRouter(b, reg, reg, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	return &App{cfg: cfg, log: log, broker: b, pool: pool, server: server}
}

func newSenders(log *slog.Logger) sender.Router {
	return sender.Router{
		event.ChannelEmail: sender.Simulated{Name: "email-provider", Latency: 40 * time.Millisecond, TransientFailureRate: 0.10, PermanentFailureRate: 0.01, Log: log},
		event.ChannelSMS:   sender.Simulated{Name: "sms-provider", Latency: 80 * time.Millisecond, TransientFailureRate: 0.15, PermanentFailureRate: 0.02, Log: log},
		event.ChannelPush:  sender.Simulated{Name: "push-provider", Latency: 15 * time.Millisecond, TransientFailureRate: 0.05, PermanentFailureRate: 0.01, Log: log},
	}
}
