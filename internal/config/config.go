package config

import (
	"os"
	"strconv"
	"time"

	"github.com/juliofilizzola/notiflow/internal/event"
	"github.com/juliofilizzola/notiflow/internal/ratelimit"
	"github.com/juliofilizzola/notiflow/internal/retry"
	"github.com/juliofilizzola/notiflow/internal/worker"
)

type Config struct {
	Addr            string
	QueueSize       int
	DLQSize         int
	ShutdownTimeout time.Duration
	Worker          worker.Config
	ChannelLimits   map[event.Channel]ratelimit.Limit
}

func Load() Config {
	return Config{
		Addr:            envString("NOTIFLOW_ADDR", ":8080"),
		QueueSize:       10_000,
		DLQSize:         1_000,
		ShutdownTimeout: 30 * time.Second,
		Worker: worker.Config{
			Workers:        envInt("NOTIFLOW_WORKERS", 32),
			Retry:          retry.Policy{MaxAttempts: 3, BaseDelay: 200 * time.Millisecond, MaxDelay: 5 * time.Second},
			AttemptTimeout: 10 * time.Second,
			ProcessingTTL:  2 * time.Minute,
			CompletedTTL:   24 * time.Hour,
		},
		ChannelLimits: map[event.Channel]ratelimit.Limit{
			event.ChannelEmail: {PerSecond: 100, Burst: 100},
			event.ChannelSMS:   {PerSecond: 20, Burst: 10},
			event.ChannelPush:  {PerSecond: 500, Burst: 500},
		},
	}
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}
